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
	CreatedAt             time.Time
	UpdatedAt             time.Time
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

func (s *Service) UpsertAccount(ctx context.Context, inp AccountInput) (Account, error) {
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

func accountsListFromDBDetailed(dbAccs []pdb.Account, snapshots map[string]pdb.AccountSnapshot, growthModels map[string]pdb.GrowthModel, startupShareAccounts map[string]pdb.StartupShareAccount) []AccountDetailed {
	accs := make([]AccountDetailed, len(dbAccs))
	for i := range dbAccs {
		accs[i].Account = accountFromDB(dbAccs[i])
		if snapshot, ok := snapshots[accs[i].ID]; ok {
			s := accountSnapshotFromDB(snapshot)
			accs[i].LastSnapshot = &s
		}
		if gm, ok := growthModels[accs[i].ID]; ok {
			gmd, err := growthModelFromDB(gm)
			if err != nil {
				panic(err)
			}
			accs[i].GrowthModel = &gmd
		}
		if ssa, ok := startupShareAccounts[accs[i].ID]; ok {
			ssaCore := startupShareAccountFromDB(ssa)
			accs[i].StartupShareAccount = &ssaCore
		}
	}
	return accs
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
	snapshots, err := s.q.ListLatestSnapshotPerAccount(ctx)
	if err != nil {
		return nil, err
	}
	snapshotsMap := make(map[string]pdb.AccountSnapshot)
	for _, snapshot := range snapshots {
		snapshotsMap[snapshot.AccountID] = snapshot
	}
	growthModels, err := s.q.ListActiveGrowthModels(ctx, ptr(int64(today)))
	if err != nil {
		return nil, err
	}
	growthModelsMap := make(map[string]pdb.GrowthModel)
	for _, growthModel := range growthModels {
		growthModelsMap[growthModel.AccountID] = growthModel
	}
	return accountsListFromDBDetailed(budgetAccs, snapshotsMap, growthModelsMap, nil), nil
}

func (s *Service) ListAccountsDetailed(ctx context.Context, today date.Date) ([]AccountDetailed, error) {
	accs, err := s.q.ListAccounts(ctx)
	if err != nil {
		return nil, err
	}
	snapshots, err := s.q.ListLatestSnapshotPerAccount(ctx)
	if err != nil {
		return nil, err
	}
	snapshotsMap := make(map[string]pdb.AccountSnapshot)
	for _, snapshot := range snapshots {
		snapshotsMap[snapshot.AccountID] = snapshot
	}
	growthModels, err := s.q.ListActiveGrowthModels(ctx, ptr(int64(today)))
	if err != nil {
		return nil, err
	}
	growthModelsMap := make(map[string]pdb.GrowthModel)
	for _, growthModel := range growthModels {
		growthModelsMap[growthModel.AccountID] = growthModel
	}
	startupShareAccountsMap := make(map[string]pdb.StartupShareAccount)
	ucfg := uncertain.NewConfig(time.Now().UnixMilli(), 1)
	for _, acc := range accs {
		ssa, err := s.q.GetStartupShareAccount(ctx, acc.ID)
		if err != nil {
			if err != sql.ErrNoRows {
				return nil, fmt.Errorf("failed to get startup share account: %w", err)
			}
			continue
		}
		startupShareAccountsMap[acc.ID] = ssa
		if _, hasSnapshot := snapshotsMap[acc.ID]; !hasSnapshot {
			round, err := s.GetLatestInvestmentRound(ctx, acc.ID, today)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					continue
				}
				return nil, fmt.Errorf("failed to get latest investment round for account %s: %w", acc.ID, err)
			}
			postMoneyValuation, postMoneyShares := PostMoneyValuationAndShares(round.Valuation, round.PreMoneyShares, round.Investment)
			changes, err := s.ListShareChanges(ctx, acc.ID)
			if err != nil {
				return nil, fmt.Errorf("listing share changes for account %s: %w", acc.ID, err)
			}
			sharesOwned, avgPurchasePrice := DeriveShareState(changes, today)
			balance := CalculateStartupShareBalance(
				ucfg,
				uncertain.NewFixed(postMoneyValuation),
				sharesOwned,
				avgPurchasePrice,
				ssa.TaxRate,
				postMoneyShares,
				ssa.ValuationDiscountFactor,
			)
			encoded, err := balance.Encode()
			if err != nil {
				return nil, fmt.Errorf("encoding startup share balance: %w", err)
			}
			snapshotsMap[acc.ID] = pdb.AccountSnapshot{
				AccountID: acc.ID,
				Date:      int64(round.Date),
				Balance:   encoded,
			}
		}
	}
	return accountsListFromDBDetailed(accs, snapshotsMap, growthModelsMap, startupShareAccountsMap), nil
}
