package model

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/goslu/sid"
	"github.com/SimonSchneider/pefigo/internal/pdb"
	"github.com/SimonSchneider/pefigo/pkg/ui"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

type Account struct {
	ID                    string
	Name                  string
	BalanceUpperLimit     *float64
	CashFlowFrequency     string
	CashFlowDestinationID string
	TypeID                string
	BudgetCategoryID      *string
	IsISK                 bool
	// EventID is the Event that adds the account, or "" in the Baseline.
	EventID   string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// hasCashFlow reports whether the account's interest is paid out on its cash-flow frequency, rather
// than added to it every day.
func (a Account) hasCashFlow() bool {
	return a.CashFlowFrequency != "" || a.CashFlowDestinationID != ""
}

type AccountDetailed struct {
	Account
	LastSnapshot        *AccountSnapshot
	GrowthModel         *GrowthModel
	StartupShareAccount *StartupShareAccount
}

// NetWorth is the sum of every Account's current balance, split into its Assets and its Liabilities,
// which are a positive amount.
type NetWorth struct {
	Total       float64
	Assets      float64
	Liabilities float64
}

// newNetWorth is the Net Worth of accounts, each at its latest Snapshot or, for a Startup Share
// Account, its derived balance.
func newNetWorth(accounts []AccountDetailed) NetWorth {
	var nw NetWorth
	for _, acc := range accounts {
		if acc.LastSnapshot == nil {
			continue
		}
		bal := acc.LastSnapshot.Balance.Mean()
		nw.Total += bal
		if bal > 0 {
			nw.Assets += bal
		} else {
			nw.Liabilities -= bal
		}
	}
	return nw
}

type AccountInput struct {
	ID                    string
	Name                  string
	BalanceUpperLimit     *float64
	CashFlowFrequency     string
	CashFlowDestinationID string
	TypeID                string
	BudgetCategoryID      *string
	IsISK                 bool
	EventID               string
}

func accountFromDB(a pdb.Account) Account {
	return Account{
		ID:                    a.ID,
		Name:                  a.Name,
		BalanceUpperLimit:     a.BalanceUpperLimit,
		CashFlowFrequency:     ui.OrDefault(a.CashFlowFrequency),
		CashFlowDestinationID: ui.OrDefault(a.CashFlowDestinationID),
		TypeID:                ui.OrDefault(a.TypeID),
		BudgetCategoryID:      a.BudgetCategoryID,
		IsISK:                 a.IsIsk != 0,
		EventID:               ui.OrDefault(a.EventID),
		CreatedAt:             time.UnixMilli(a.CreatedAt),
		UpdatedAt:             time.UnixMilli(a.UpdatedAt),
	}
}

func (s *Service) GetAccount(ctx context.Context, id string) (Account, error) {
	acc, err := s.q.GetAccount(ctx, id)
	if err != nil {
		return Account{}, fmt.Errorf("failed to get account: %w", err)
	}
	return accountFromDB(acc), nil
}

// cashFlowError says why the forecast can't pay out a cash flow on frequency to destinationID, or is
// nil when it can. Without a cash-flow frequency and destination the account has none, but a
// destination needs a frequency to be paid out on. It doesn't wrap ErrInvalidInput, so it suits stored
// accounts too.
func cashFlowError(frequency, destinationID string) error {
	if frequency == "" {
		if destinationID != "" {
			return errors.New("a cash-flow destination needs a cash-flow frequency")
		}
		return nil
	}
	return datePatternError("cash-flow frequency", date.Cron(frequency))
}

// checkCashFlow rejects a cash flow the forecast can't pay out.
func (inp AccountInput) checkCashFlow() error {
	if err := cashFlowError(inp.CashFlowFrequency, inp.CashFlowDestinationID); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidInput, err)
	}
	return nil
}

func (s *Service) UpsertAccount(ctx context.Context, inp AccountInput) (Account, error) {
	if err := errors.Join(checkRequired("name", inp.Name), inp.checkCashFlow()); err != nil {
		return Account{}, err
	}
	if _, err := s.eventDate(ctx, inp.EventID); err != nil {
		return Account{}, err
	}
	var (
		q   = s.q
		acc pdb.Account
		err error
	)
	var isIsk int64
	if inp.IsISK {
		isIsk = 1
	}
	if inp.ID != "" {
		acc, err = q.UpdateAccount(ctx, pdb.UpdateAccountParams{
			ID:                    inp.ID,
			Name:                  inp.Name,
			BalanceUpperLimit:     inp.BalanceUpperLimit,
			CashFlowFrequency:     ui.WithDefaultNull(inp.CashFlowFrequency),
			CashFlowDestinationID: ui.WithDefaultNull(inp.CashFlowDestinationID),
			TypeID:                ui.WithDefaultNull(inp.TypeID),
			BudgetCategoryID:      inp.BudgetCategoryID,
			IsIsk:                 isIsk,
			EventID:               ui.WithDefaultNull(inp.EventID),
			UpdatedAt:             time.Now().UnixMilli(),
		})
	} else {
		acc, err = q.CreateAccount(ctx, pdb.CreateAccountParams{
			ID:                    sid.MustNewString(15),
			Name:                  inp.Name,
			BalanceUpperLimit:     inp.BalanceUpperLimit,
			CashFlowFrequency:     ui.WithDefaultNull(inp.CashFlowFrequency),
			CashFlowDestinationID: ui.WithDefaultNull(inp.CashFlowDestinationID),
			TypeID:                ui.WithDefaultNull(inp.TypeID),
			BudgetCategoryID:      inp.BudgetCategoryID,
			IsIsk:                 isIsk,
			EventID:               ui.WithDefaultNull(inp.EventID),
			CreatedAt:             time.Now().UnixMilli(),
			UpdatedAt:             time.Now().UnixMilli(),
		})
	}
	if err != nil {
		return Account{}, fmt.Errorf("failed to upsert account: %w", err)
	}
	return accountFromDB(acc), nil
}

func (s *Service) UpsertAccountWithStartupShares(ctx context.Context, inp AccountInput, startupShares *StartupShareAccountInput) (Account, error) {
	if startupShares != nil {
		// Reject invalid startup shares before saving the account, so nothing is saved.
		if _, err := startupShares.startupShareAccount(); err != nil {
			return Account{}, err
		}
	}
	acc, err := s.UpsertAccount(ctx, inp)
	if err != nil {
		return Account{}, fmt.Errorf("upserting account: %w", err)
	}
	if startupShares != nil {
		startupShares.AccountID = acc.ID
		if _, err := s.UpsertStartupShareAccount(ctx, *startupShares); err != nil {
			return Account{}, fmt.Errorf("upserting startup share account: %w", err)
		}
	} else {
		_, err := s.GetStartupShareAccount(ctx, acc.ID)
		if err == nil {
			if err := s.DeleteStartupShareAccount(ctx, acc.ID); err != nil {
				return Account{}, fmt.Errorf("deleting startup share account: %w", err)
			}
		}
	}
	return acc, nil
}

func (s *Service) DeleteAccount(ctx context.Context, id string) error {
	_, err := s.q.DeleteAccount(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to delete account: %w", err)
	}
	return nil
}

func accountsListFromDB(dbAccs []pdb.Account) []Account {
	accs := make([]Account, len(dbAccs))
	for i := range dbAccs {
		accs[i] = accountFromDB(dbAccs[i])
	}
	return accs
}

func accountsDetailed(accounts []Account, snapshots map[string]AccountSnapshot, growthModels map[string]GrowthModel, startupShareAccounts map[string]StartupShareAccount) []AccountDetailed {
	accs := make([]AccountDetailed, len(accounts))
	for i, acc := range accounts {
		accs[i].Account = acc
		if snapshot, ok := snapshots[acc.ID]; ok {
			accs[i].LastSnapshot = &snapshot
		}
		if gm, ok := growthModels[acc.ID]; ok {
			accs[i].GrowthModel = &gm
		}
		if ssa, ok := startupShareAccounts[acc.ID]; ok {
			accs[i].StartupShareAccount = &ssa
		}
	}
	return accs
}

// latestSnapshots returns the latest Snapshot of every account with one, keyed by account ID.
func (s *Service) latestSnapshots(ctx context.Context) (map[string]AccountSnapshot, error) {
	rows, err := s.q.ListLatestSnapshotPerAccount(ctx)
	if err != nil {
		return nil, err
	}
	snapshots := make(map[string]AccountSnapshot, len(rows))
	for _, row := range rows {
		snapshots[row.AccountID] = accountSnapshotFromDB(row)
	}
	return snapshots, nil
}

func (s *Service) ListAccounts(ctx context.Context) ([]Account, error) {
	accs, err := s.q.ListAccounts(ctx)
	if err != nil {
		return nil, err
	}
	return accountsListFromDB(accs), nil
}

// ListBudgetAccounts returns the Baseline's accounts with a budget category, each with its latest
// Snapshot and the growth model applied on today.
func (s *Service) ListBudgetAccounts(ctx context.Context, today date.Date) ([]AccountDetailed, error) {
	p, err := s.placeEvents(ctx, nil)
	if err != nil {
		return nil, err
	}
	var budgetAccs []Account
	for _, acc := range p.accounts {
		if acc.BudgetCategoryID != nil {
			budgetAccs = append(budgetAccs, acc)
		}
	}
	if len(budgetAccs) == 0 {
		return nil, nil
	}
	snapshots, err := s.latestSnapshots(ctx)
	if err != nil {
		return nil, err
	}
	return accountsDetailed(budgetAccs, snapshots, growthModelsAppliedOn(p, today), nil), nil
}

// ListAccountsDetailed returns every account with its latest Snapshot and the growth model the
// Baseline applies on today, so an account of an unplanned Event has none. A Startup Share account
// without Snapshots shows its derived balance on today instead, dated on the investment round it is
// valued by.
func (s *Service) ListAccountsDetailed(ctx context.Context, today date.Date) ([]AccountDetailed, error) {
	accs, err := s.ListAccounts(ctx)
	if err != nil {
		return nil, err
	}
	snapshots, err := s.latestSnapshots(ctx)
	if err != nil {
		return nil, err
	}
	p, err := s.placeEvents(ctx, nil)
	if err != nil {
		return nil, err
	}
	startupShares, err := s.listStartupShares(ctx)
	if err != nil {
		return nil, err
	}
	startupShareAccounts := make(map[string]StartupShareAccount, len(startupShares))
	ucfg := uncertain.NewConfig(time.Now().UnixMilli(), 1)
	for accountID, ss := range startupShares {
		startupShareAccounts[accountID] = ss.StartupShareAccount
		if _, hasSnapshot := snapshots[accountID]; hasSnapshot {
			continue
		}
		if round := ss.latestRoundOn(today); round != nil {
			snapshots[accountID] = AccountSnapshot{AccountID: accountID, Date: round.Date, Balance: ss.growthOn(today).Balance(ucfg)}
		}
	}
	return accountsDetailed(accs, snapshots, growthModelsAppliedOn(p, today), startupShareAccounts), nil
}

type AccountsView struct {
	Accounts     []AccountDetailed
	AccountTypes AccountTypesWithFilter
	Categories   map[string]TransferTemplateCategory
	Events       map[string]Event
	NetWorth     NetWorth
}

func NewAccountsView(accounts []AccountDetailed, accountTypes []AccountTypeWithFilter, categories []TransferTemplateCategory, events []Event) *AccountsView {
	return &AccountsView{
		Accounts:     accounts,
		AccountTypes: accountTypes,
		Categories:   KeyBy(categories, func(c TransferTemplateCategory) string { return c.ID }),
		Events:       KeyBy(events, func(e Event) string { return e.ID }),
		NetWorth:     newNetWorth(accounts),
	}
}

// GetEvent is the Event id, or nil for "" or an Event that doesn't exist.
func (v *AccountsView) GetEvent(id string) *Event {
	ev, ok := v.Events[id]
	if !ok {
		return nil
	}
	return &ev
}

func (v *AccountsView) GetBudgetCategory(id *string) *TransferTemplateCategory {
	if id == nil {
		return nil
	}
	cat, ok := v.Categories[*id]
	if !ok {
		return nil
	}
	return &cat
}

func (v *AccountsView) GetAccountType(typeID string) AccountTypeWithFilter {
	return v.AccountTypes.GetAccountType(typeID)
}

type AccountEditView struct {
	Account                    Account
	Accounts                   []Account
	Events                     []Event
	GrowthModels               []GrowthModel
	AccountTypes               AccountTypesWithFilter
	Categories                 []TransferTemplateCategory
	StartupShareAccount        *StartupShareAccount
	InvestmentRounds           []InvestmentRound
	ShareChanges               []ShareChange
	Options                    []StartupShareOption
	DerivedStartupShareSummary *DerivedStartupShareSummary
}

func (v *AccountEditView) GetStartupShareSharesOwned() string {
	if v.DerivedStartupShareSummary == nil {
		return "0"
	}
	return ui.FormatWithThousands(v.DerivedStartupShareSummary.SharesOwned)
}

func (v *AccountEditView) GetStartupShareTotalShares() string {
	if v.DerivedStartupShareSummary == nil {
		return "0"
	}
	return ui.FormatWithThousands(v.DerivedStartupShareSummary.TotalShares)
}

func (v *AccountEditView) GetStartupSharePurchasePrice() string {
	if v.DerivedStartupShareSummary == nil {
		return "0"
	}
	return fmt.Sprintf("%.10f", v.DerivedStartupShareSummary.AvgPurchasePricePerShare)
}

// GetStartupShareTaxRate is the Tax Rate as a percentage, or the default one for an account without
// startup shares.
func (v *AccountEditView) GetStartupShareTaxRate() string {
	if v.StartupShareAccount == nil {
		return ui.FormatPercent(DefaultStartupShareTaxRate)
	}
	return ui.FormatPercent(v.StartupShareAccount.TaxRate)
}

// GetStartupShareDiscountFactor is the Valuation Discount as a percentage, or the default one for an
// account without startup shares.
func (v *AccountEditView) GetStartupShareDiscountFactor() string {
	if v.StartupShareAccount == nil {
		return ui.FormatPercent(DefaultValuationDiscount)
	}
	return ui.FormatPercent(v.StartupShareAccount.ValuationDiscountFactor)
}

func (v *AccountEditView) HasStartupShareAccount() bool {
	return v.StartupShareAccount != nil
}

func (v *AccountEditView) IsEdit() bool {
	return v.Account.ID != ""
}

func (v *AccountEditView) AccountFormMode() string {
	if v.HasStartupShareAccount() {
		return "startup"
	}
	return "standard"
}

// GetEventName is the name of the Event id, or "" for none.
func (v *AccountEditView) GetEventName(id string) string {
	for _, ev := range v.Events {
		if ev.ID == id {
			return ev.Name
		}
	}
	return ""
}

func (v *AccountEditView) GetAccountTypeName(typeID string) string {
	return v.AccountTypes.GetAccountType(typeID).Name
}

func (s *Service) GetAccountsPageData(ctx context.Context, excludedTypeIDs []string) (*AccountsView, error) {
	accs, err := s.ListAccountsDetailed(ctx, date.Today())
	if err != nil {
		return nil, fmt.Errorf("listing accounts: %w", err)
	}
	accountTypes, err := s.accountTypesWithExclusions(ctx, excludedTypeIDs)
	if err != nil {
		return nil, err
	}
	categories, err := s.ListCategories(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing categories: %w", err)
	}
	events, err := s.ListEvents(ctx)
	if err != nil {
		return nil, err
	}
	return NewAccountsView(accs, accountTypes, categories, events), nil
}

func (s *Service) GetAccountNewPageData(ctx context.Context, excludedTypeIDs []string) (*AccountEditView, error) {
	accs, err := s.ListAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing accounts: %w", err)
	}
	accountTypes, err := s.accountTypesWithExclusions(ctx, excludedTypeIDs)
	if err != nil {
		return nil, err
	}
	categories, err := s.ListCategories(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing categories: %w", err)
	}
	events, err := s.ListEvents(ctx)
	if err != nil {
		return nil, err
	}
	return &AccountEditView{
		Accounts:     accs,
		Events:       events,
		AccountTypes: accountTypes,
		Categories:   categories,
	}, nil
}

func (s *Service) GetAccountEditPageData(ctx context.Context, accountID string, excludedTypeIDs []string) (*AccountEditView, error) {
	accs, err := s.ListAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing accounts: %w", err)
	}
	acc, err := s.GetAccount(ctx, accountID)
	if err != nil {
		return nil, fmt.Errorf("getting account: %w", err)
	}
	growthModels, err := s.ListAccountGrowthModels(ctx, acc.ID)
	if err != nil {
		return nil, fmt.Errorf("listing growth models: %w", err)
	}
	accountTypes, err := s.accountTypesWithExclusions(ctx, excludedTypeIDs)
	if err != nil {
		return nil, err
	}

	var startupShareAccount *StartupShareAccount
	var investmentRounds []InvestmentRound
	var shareChanges []ShareChange
	var options []StartupShareOption
	var derivedSummary *DerivedStartupShareSummary
	ssa, err := s.GetStartupShareAccount(ctx, acc.ID)
	if err == nil {
		startupShareAccount = &ssa
		investmentRounds, err = s.ListInvestmentRounds(ctx, acc.ID)
		if err != nil {
			return nil, fmt.Errorf("listing investment rounds: %w", err)
		}
		shareChanges, err = s.ListShareChanges(ctx, acc.ID)
		if err != nil {
			return nil, fmt.Errorf("listing share changes: %w", err)
		}
		options, err = s.ListStartupShareOptions(ctx, acc.ID)
		if err != nil {
			return nil, fmt.Errorf("listing startup share options: %w", err)
		}
		today := date.Today()
		var postShares float64
		round, roundErr := s.GetLatestInvestmentRound(ctx, acc.ID, today)
		if roundErr != nil {
			if !errors.Is(roundErr, sql.ErrNoRows) {
				return nil, fmt.Errorf("getting latest investment round: %w", roundErr)
			}
		} else {
			_, postShares = PostMoneyValuationAndShares(round.Valuation, round.PreMoneyShares, round.Investment)
		}
		sharesOwned, avgPrice := DeriveShareState(shareChanges, today)
		derivedSummary = &DerivedStartupShareSummary{
			SharesOwned:              sharesOwned,
			TotalShares:              postShares,
			AvgPurchasePricePerShare: avgPrice,
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("getting startup share account: %w", err)
	}

	categories, err := s.ListCategories(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing categories: %w", err)
	}
	events, err := s.ListEvents(ctx)
	if err != nil {
		return nil, err
	}
	return &AccountEditView{
		Account:                    acc,
		Accounts:                   accs,
		Events:                     events,
		GrowthModels:               growthModels,
		AccountTypes:               accountTypes,
		Categories:                 categories,
		StartupShareAccount:        startupShareAccount,
		InvestmentRounds:           investmentRounds,
		ShareChanges:               shareChanges,
		Options:                    options,
		DerivedStartupShareSummary: derivedSummary,
	}, nil
}
