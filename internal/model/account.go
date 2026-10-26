package model

import (
	"context"
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
	CreatedAt             time.Time
	UpdatedAt             time.Time
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

type AccountInput struct {
	ID                    string
	Name                  string
	BalanceUpperLimit     *float64
	CashFlowFrequency     string
	CashFlowDestinationID string
	TypeID                string
	BudgetCategoryID      *string
	IsISK                 bool
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

// checkCashFlow rejects a cash flow the forecast can't pay out. Without a cash-flow frequency and
// destination the account has none, but a destination needs a frequency to be paid out on.
func (inp AccountInput) checkCashFlow() error {
	if inp.CashFlowFrequency == "" {
		if inp.CashFlowDestinationID != "" {
			return fmt.Errorf("%w: a cash-flow destination needs a cash-flow frequency", ErrInvalidInput)
		}
		return nil
	}
	return checkDatePattern("cash-flow frequency", date.Cron(inp.CashFlowFrequency))
}

func (s *Service) UpsertAccount(ctx context.Context, inp AccountInput) (Account, error) {
	if err := errors.Join(checkRequired("name", inp.Name), inp.checkCashFlow()); err != nil {
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

func accountsListFromDBDetailed(dbAccs []pdb.Account, snapshots map[string]AccountSnapshot, growthModels map[string]pdb.GrowthModel, startupShareAccounts map[string]StartupShareAccount) []AccountDetailed {
	accs := make([]AccountDetailed, len(dbAccs))
	for i := range dbAccs {
		accs[i].Account = accountFromDB(dbAccs[i])
		if snapshot, ok := snapshots[accs[i].ID]; ok {
			accs[i].LastSnapshot = &snapshot
		}
		if gm, ok := growthModels[accs[i].ID]; ok {
			gmd, err := growthModelFromDB(gm)
			if err != nil {
				panic(err)
			}
			accs[i].GrowthModel = &gmd
		}
		if ssa, ok := startupShareAccounts[accs[i].ID]; ok {
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

func (s *Service) ListBudgetAccounts(ctx context.Context, today date.Date) ([]AccountDetailed, error) {
	budgetAccs, err := s.q.GetBudgetAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list budget accounts: %w", err)
	}
	if len(budgetAccs) == 0 {
		return nil, nil
	}
	snapshots, err := s.latestSnapshots(ctx)
	if err != nil {
		return nil, err
	}
	growthModelsMap, err := s.growthModelsAppliedOn(ctx, today)
	if err != nil {
		return nil, err
	}
	return accountsListFromDBDetailed(budgetAccs, snapshots, growthModelsMap, nil), nil
}

// ListAccountsDetailed returns every account with its latest Snapshot and the growth model applied on
// today. A Startup Share account without Snapshots shows its derived balance on today instead,
// dated on the investment round it is valued by.
func (s *Service) ListAccountsDetailed(ctx context.Context, today date.Date) ([]AccountDetailed, error) {
	accs, err := s.q.ListAccounts(ctx)
	if err != nil {
		return nil, err
	}
	snapshots, err := s.latestSnapshots(ctx)
	if err != nil {
		return nil, err
	}
	growthModelsMap, err := s.growthModelsAppliedOn(ctx, today)
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
	return accountsListFromDBDetailed(accs, snapshots, growthModelsMap, startupShareAccounts), nil
}
