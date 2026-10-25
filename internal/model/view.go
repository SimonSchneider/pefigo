package model

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/pkg/currency"
	"github.com/SimonSchneider/pefigo/pkg/ui"
)

type TransferTemplateEditView struct {
	TransferTemplate TransferTemplate
	Accounts         []Account
	Categories       []TransferTemplateCategory
}

func (c TransferTemplateEditView) IsEdit() bool {
	return c.TransferTemplate.ID != ""
}

type TransferTemplatesView2 struct {
	Rows            []TransferScheduleRow
	Accounts        map[string]Account
	Categories      map[string]TransferTemplateCategory
	MonthlyIncome   float64
	MonthlyExpenses float64
}

func newTransferTemplatesView2(day *TransferScheduleDay, accounts []Account, categories []TransferTemplateCategory) *TransferTemplatesView2 {
	v := &TransferTemplatesView2{
		Rows:       day.Rows(),
		Accounts:   KeyBy(accounts, func(a Account) string { return a.ID }),
		Categories: KeyBy(categories, func(c TransferTemplateCategory) string { return c.ID }),
	}
	for _, t := range day.Monthly() {
		switch t.Kind() {
		case TransferIncome:
			v.MonthlyIncome += t.Amount
		case TransferExpense:
			v.MonthlyExpenses -= t.Amount
		}
	}
	return v
}

func (v *TransferTemplatesView2) GetBudgetCategory(id *string) *TransferTemplateCategory {
	if id == nil {
		return nil
	}
	cat, ok := v.Categories[*id]
	if !ok {
		return nil
	}
	return &cat
}

func (v *TransferTemplatesView2) GetAccount(id string) *Account {
	a := v.Accounts[id]
	return &a
}

type AccountsView struct {
	Accounts         []AccountDetailed
	AccountTypes     AccountTypesWithFilter
	Categories       map[string]TransferTemplateCategory
	TotalBalance     float64
	TotalAssets      float64
	TotalLiabilities float64
}

func NewAccountsView(accounts []AccountDetailed, accountTypes []AccountTypeWithFilter, categories []TransferTemplateCategory) *AccountsView {
	v := &AccountsView{
		Accounts:     accounts,
		AccountTypes: accountTypes,
		Categories:   KeyBy(categories, func(c TransferTemplateCategory) string { return c.ID }),
	}
	for _, account := range accounts {
		if account.LastSnapshot != nil {
			v.TotalBalance += account.LastSnapshot.Balance.Mean()
			if account.LastSnapshot.Balance.Mean() > 0 {
				v.TotalAssets += account.LastSnapshot.Balance.Mean()
			} else {
				v.TotalLiabilities += account.LastSnapshot.Balance.Mean()
			}
		}
	}
	return v
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

type AccountEditView2 struct {
	Account                    Account
	Accounts                   []Account
	GrowthModels               []GrowthModel
	AccountTypes               AccountTypesWithFilter
	Categories                 []TransferTemplateCategory
	StartupShareAccount        *StartupShareAccount
	InvestmentRounds           []InvestmentRound
	ShareChanges               []ShareChange
	Options                    []StartupShareOption
	DerivedStartupShareSummary *DerivedStartupShareSummary
}

func (v *AccountEditView2) GetStartupShareSharesOwned() string {
	if v.DerivedStartupShareSummary == nil {
		return "0"
	}
	return ui.FormatWithThousands(v.DerivedStartupShareSummary.SharesOwned)
}

func (v *AccountEditView2) GetStartupShareTotalShares() string {
	if v.DerivedStartupShareSummary == nil {
		return "0"
	}
	return ui.FormatWithThousands(v.DerivedStartupShareSummary.TotalShares)
}

func (v *AccountEditView2) GetStartupSharePurchasePrice() string {
	if v.DerivedStartupShareSummary == nil {
		return "0"
	}
	return fmt.Sprintf("%.10f", v.DerivedStartupShareSummary.AvgPurchasePricePerShare)
}

func (v *AccountEditView2) GetStartupShareTaxRate() string {
	if v.StartupShareAccount == nil {
		return "15"
	}
	return fmt.Sprintf("%.2f", v.StartupShareAccount.TaxRate*100)
}

func (v *AccountEditView2) GetStartupShareDiscountFactor() string {
	if v.StartupShareAccount == nil {
		return "50"
	}
	return fmt.Sprintf("%.2f", v.StartupShareAccount.ValuationDiscountFactor*100)
}

func (v *AccountEditView2) HasStartupShareAccount() bool {
	return v.StartupShareAccount != nil
}

func (v *AccountEditView2) GetStartupShareFieldsStyle() string {
	if v.HasStartupShareAccount() {
		return "display: block;"
	}
	return "display: none;"
}

func (v *AccountEditView2) IsEdit() bool {
	return v.Account.ID != ""
}

func (v *AccountEditView2) AccountFormMode() string {
	if v.HasStartupShareAccount() {
		return "startup"
	}
	return "standard"
}

func (v *AccountEditView2) GetAccountTypeName(typeID string) string {
	return v.AccountTypes.GetAccountType(typeID).Name
}

type TransferTemplateCategoriesView struct {
	Categories []TransferTemplateCategory
}

type CategoriesPageView struct {
	AccountTypes []AccountType
	Categories   []TransferTemplateCategory
}

func (s *Service) GetCategoriesPageData(ctx context.Context) (*CategoriesPageView, error) {
	accountTypes, err := s.ListAccountTypes(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing account types: %w", err)
	}
	categories, err := s.ListCategories(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing categories: %w", err)
	}
	return &CategoriesPageView{
		AccountTypes: accountTypes,
		Categories:   categories,
	}, nil
}

type SettingsPageView struct {
	AccountTypes             []AccountType
	Categories               []TransferTemplateCategory
	SpecialDates             []SpecialDate
	SweYearlyParams          []SweYearlyParams
	CurrentCurrency          string
	Currencies               []currency.Currency
	ForecastConfidence       float64
	ForecastSamples          int64
	ForecastSnapshotInterval string
}

func (s *Service) GetSettingsPageData(ctx context.Context) (*SettingsPageView, error) {
	accountTypes, err := s.ListAccountTypes(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing account types: %w", err)
	}
	categories, err := s.ListCategories(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing categories: %w", err)
	}
	specialDates, err := s.ListSpecialDates(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing special dates: %w", err)
	}
	ibbs, err := s.ListSweYearlyParams(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing swe yearly params: %w", err)
	}
	cur, err := s.GetDefaultCurrency(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting default currency: %w", err)
	}
	forecastConfidence, err := s.GetForecastConfidence(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting forecast confidence: %w", err)
	}
	forecastSamples, err := s.GetForecastSamples(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting forecast samples: %w", err)
	}
	forecastSnapshotInterval, err := s.GetForecastSnapshotInterval(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting forecast snapshot interval: %w", err)
	}
	return &SettingsPageView{
		AccountTypes:             accountTypes,
		Categories:               categories,
		SpecialDates:             specialDates,
		SweYearlyParams:          ibbs,
		CurrentCurrency:          cur,
		Currencies:               currency.SupportedCurrencies(),
		ForecastConfidence:       forecastConfidence,
		ForecastSamples:          forecastSamples,
		ForecastSnapshotInterval: forecastSnapshotInterval,
	}, nil
}

type TransferTemplateCategoryEditView struct {
	Category TransferTemplateCategory
}

func (v TransferTemplateCategoryEditView) IsEdit() bool {
	return v.Category.ID != ""
}

type SnapshotsTableView struct {
	Accounts     []Account
	Rows         []SnapshotsRow
	AccountTypes []AccountTypeWithFilter
}

type SnapshotsRow struct {
	Date               date.Date
	Snapshots          []AccountSnapshotCell
	UnsavedSuggestions bool
}

func (s *Service) accountTypesWithExclusions(ctx context.Context, excludedTypeIDs []string) (AccountTypesWithFilter, error) {
	accountTypes, err := s.ListAccountTypes(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing account types: %w", err)
	}
	excludeSet := make(map[string]bool, len(excludedTypeIDs))
	for _, id := range excludedTypeIDs {
		excludeSet[id] = true
	}
	result := make(AccountTypesWithFilter, 0, len(accountTypes))
	for _, at := range accountTypes {
		result = append(result, AccountTypeWithFilter{
			AccountType: at,
			Exclude:     excludeSet[at.ID],
		})
	}
	return result, nil
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
	return NewAccountsView(accs, accountTypes, categories), nil
}

func (s *Service) GetAccountNewPageData(ctx context.Context, excludedTypeIDs []string) (*AccountEditView2, error) {
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
	return &AccountEditView2{
		Accounts:     accs,
		AccountTypes: accountTypes,
		Categories:   categories,
	}, nil
}

func (s *Service) GetAccountEditPageData(ctx context.Context, accountID string, excludedTypeIDs []string) (*AccountEditView2, error) {
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
	return &AccountEditView2{
		Account:                    acc,
		Accounts:                   accs,
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

func (s *Service) GetTransferTemplatesPageData(ctx context.Context) (*TransferTemplatesView2, error) {
	schedule, err := s.TransferSchedule(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading transfer schedule: %w", err)
	}
	accounts, err := s.ListAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing accounts: %w", err)
	}
	categories, err := s.ListCategories(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing categories: %w", err)
	}
	return newTransferTemplatesView2(schedule.On(date.Today()), accounts, categories), nil
}

func (s *Service) GetTransferTemplateNewPageData(ctx context.Context) (*TransferTemplateEditView, error) {
	accs, err := s.ListAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing accounts: %w", err)
	}
	categories, err := s.ListCategories(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing categories: %w", err)
	}
	return &TransferTemplateEditView{
		Accounts:   accs,
		Categories: categories,
	}, nil
}

func (s *Service) GetTransferTemplateEditPageData(ctx context.Context, id string) (*TransferTemplateEditView, error) {
	t, err := s.GetTransferTemplate(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("getting transfer template: %w", err)
	}
	accs, err := s.ListAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing accounts: %w", err)
	}
	categories, err := s.ListCategories(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing categories: %w", err)
	}
	return &TransferTemplateEditView{
		Accounts:         accs,
		TransferTemplate: t,
		Categories:       categories,
	}, nil
}

func (s *Service) GetSnapshotsTablePageData(ctx context.Context, excludedTypeIDs []string) (*SnapshotsTableView, error) {
	allAccounts, err := s.ListAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing accounts: %w", err)
	}
	accountTypes, err := s.accountTypesWithExclusions(ctx, excludedTypeIDs)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(allAccounts))
	accounts := make([]Account, 0, len(allAccounts))
	for _, acc := range allAccounts {
		if !accountTypes.GetAccountType(acc.TypeID).Exclude {
			ids = append(ids, acc.ID)
			accounts = append(accounts, acc)
		}
	}
	snapshots, err := s.ListAccountsSnapshots(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("listing account snapshots: %w", err)
	}
	type dateIDKey struct {
		Date date.Date
		ID   string
	}
	dates := make([]date.Date, 0)
	snaps := make(map[dateIDKey]AccountSnapshot)
	for _, ss := range snapshots {
		snaps[dateIDKey{Date: ss.Date, ID: ss.AccountID}] = ss
		if len(dates) == 0 || dates[len(dates)-1] != ss.Date {
			dates = append(dates, ss.Date)
		}
	}
	for i, j := 0, len(dates)-1; i < j; i, j = i+1, j-1 {
		dates[i], dates[j] = dates[j], dates[i]
	}
	rows := make([]SnapshotsRow, 0)
	for di, d := range dates {
		rows = append(rows, SnapshotsRow{
			Date:      d,
			Snapshots: make([]AccountSnapshotCell, 0, len(accounts)),
		})
		for _, acc := range accounts {
			prevMean := 0.0
			if di < len(dates)-1 {
				prevDate := dates[di+1]
				if prevSnap, ok := snaps[dateIDKey{Date: prevDate, ID: acc.ID}]; ok {
					prevMean = prevSnap.Balance.Mean()
				}
			}
			if snap, ok := snaps[dateIDKey{Date: d, ID: acc.ID}]; ok {
				change := BalanceUnchanged
				if snap.Balance.Mean() > prevMean {
					change = BalanceIncreased
				} else if snap.Balance.Mean() < prevMean {
					change = BalanceDecreased
				}
				rows[len(rows)-1].Snapshots = append(rows[len(rows)-1].Snapshots, AccountSnapshotCell{
					AccountSnapshot: snap,
					Change:          change,
				})
			} else {
				rows[len(rows)-1].Snapshots = append(rows[len(rows)-1].Snapshots, AccountSnapshotCell{
					AccountSnapshot: AccountSnapshot{
						AccountID: acc.ID,
						Date:      d,
					},
					Change: BalanceUnchanged,
				})
			}
		}
	}
	return &SnapshotsTableView{
		Accounts:     accounts,
		Rows:         rows,
		AccountTypes: accountTypes,
	}, nil
}

func (s *Service) ModifySnapshotDateRow(ctx context.Context, oldDate, newDate date.Date) (*SnapshotsRow, error) {
	snaps, err := s.UpdateSnapshotDate(ctx, oldDate, newDate)
	if err != nil {
		return nil, fmt.Errorf("updating snapshot date: %w", err)
	}
	snapsByAccID := KeyBy(snaps, func(ss AccountSnapshot) string { return ss.AccountID })
	accs, err := s.ListAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing accounts: %w", err)
	}
	row := SnapshotsRow{
		Date:      newDate,
		Snapshots: make([]AccountSnapshotCell, len(accs)),
	}
	for i, acc := range accs {
		snap := AccountSnapshot{
			AccountID: acc.ID,
			Date:      newDate,
		}
		if s, ok := snapsByAccID[acc.ID]; ok {
			snap.Balance = s.Balance
		}
		row.Snapshots[i] = AccountSnapshotCell{
			AccountSnapshot: snap,
		}
	}
	return &row, nil
}

func (s *Service) GetEmptySnapshotRow(ctx context.Context) (*SnapshotsRow, error) {
	accounts, err := s.ListAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing accounts: %w", err)
	}
	today := date.Today()
	row := SnapshotsRow{
		Date:               today,
		Snapshots:          make([]AccountSnapshotCell, len(accounts)),
		UnsavedSuggestions: true,
	}
	for i, acc := range accounts {
		row.Snapshots[i] = AccountSnapshotCell{
			AccountSnapshot: AccountSnapshot{
				AccountID: acc.ID,
				Date:      today,
			},
		}
	}
	return &row, nil
}
