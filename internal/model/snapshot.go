package model

import (
	"context"
	"errors"
	"fmt"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/pdb"
	"github.com/SimonSchneider/pefigo/pkg/finance"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

type BalanceChange int

const (
	BalanceUnchanged BalanceChange = iota
	BalanceIncreased
	BalanceDecreased
)

type AccountSnapshot struct {
	AccountID string
	Date      date.Date
	Balance   uncertain.Value
}

func (a AccountSnapshot) ToFinance() finance.BalanceSnapshot {
	return finance.BalanceSnapshot{
		Date:    a.Date,
		Balance: a.Balance,
	}
}

type AccountSnapshotInput struct {
	Date         date.Date
	Balance      uncertain.Value
	EmptyBalance bool
}

type AccountSnapshotCell struct {
	AccountSnapshot
	Change BalanceChange
}

func accountSnapshotFromDB(s pdb.AccountSnapshot) AccountSnapshot {
	var balance uncertain.Value
	if err := balance.Decode(s.Balance); err != nil {
		panic(fmt.Errorf("decoding balance: %w", err))
	}
	return AccountSnapshot{
		AccountID: s.AccountID,
		Date:      date.Date(s.Date),
		Balance:   balance,
	}
}

// checkSnapshot rejects a Snapshot without an account or a date.
func checkSnapshot(accountID string, day date.Date) error {
	return errors.Join(checkRequired("account", accountID), checkRequired("date", day))
}

func (s *Service) UpsertAccountSnapshot(ctx context.Context, accountID string, inp AccountSnapshotInput) (AccountSnapshot, error) {
	if err := checkSnapshot(accountID, inp.Date); err != nil {
		return AccountSnapshot{}, err
	}
	balance, err := orZero(inp.Balance).Encode()
	if err != nil {
		return AccountSnapshot{}, fmt.Errorf("encoding balance: %w", err)
	}
	snap, err := s.q.UpsertSnapshot(ctx, pdb.UpsertSnapshotParams{
		AccountID: accountID,
		Date:      int64(inp.Date),
		Balance:   balance,
	})
	if err != nil {
		return AccountSnapshot{}, fmt.Errorf("failed to upsert snapshot: %w", err)
	}
	return accountSnapshotFromDB(snap), nil
}

func (s *Service) DeleteAccountSnapshot(ctx context.Context, id string, d date.Date) error {
	if err := s.q.DeleteSnapshot(ctx, pdb.DeleteSnapshotParams{AccountID: id, Date: int64(d)}); err != nil {
		return fmt.Errorf("failed to delete snapshot: %w", err)
	}
	return nil
}

func (s *Service) ListAccountSnapshots(ctx context.Context, id string) ([]AccountSnapshot, error) {
	return s.ListAccountsSnapshots(ctx, []string{id})
}

func (s *Service) ListAccountsSnapshots(ctx context.Context, ids []string) ([]AccountSnapshot, error) {
	snapshots, err := s.q.GetSnapshotsByAccounts(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("failed to list account snapshots: %w", err)
	}
	snapshotList := make([]AccountSnapshot, len(snapshots))
	for i, snap := range snapshots {
		snapshotList[i] = accountSnapshotFromDB(snap)
	}
	return snapshotList, nil
}

func (s *Service) UpdateSnapshotDate(ctx context.Context, oldDate, newDate date.Date) ([]AccountSnapshot, error) {
	if err := errors.Join(checkRequired("old date", oldDate), checkRequired("new date", newDate)); err != nil {
		return nil, err
	}
	snaps, err := s.q.UpdateSnapshotDate(ctx, pdb.UpdateSnapshotDateParams{
		Date:   int64(newDate),
		Date_2: int64(oldDate),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to update snapshot date: %w", err)
	}
	result := make([]AccountSnapshot, len(snaps))
	for i, snap := range snaps {
		result[i] = accountSnapshotFromDB(snap)
	}
	return result, nil
}

func (s *Service) UpsertOrDeleteSnapshot(ctx context.Context, accountID string, inp AccountSnapshotInput) (AccountSnapshotCell, error) {
	if err := checkSnapshot(accountID, inp.Date); err != nil {
		return AccountSnapshotCell{}, err
	}
	if inp.EmptyBalance {
		if err := s.DeleteAccountSnapshot(ctx, accountID, inp.Date); err != nil {
			return AccountSnapshotCell{}, fmt.Errorf("deleting existing snapshot: %w", err)
		}
		return AccountSnapshotCell{
			AccountSnapshot: AccountSnapshot{
				AccountID: accountID,
				Date:      inp.Date,
			},
			Change: BalanceUnchanged,
		}, nil
	}
	snap, err := s.UpsertAccountSnapshot(ctx, accountID, inp)
	if err != nil {
		return AccountSnapshotCell{}, fmt.Errorf("upserting snapshot: %w", err)
	}
	return AccountSnapshotCell{
		AccountSnapshot: snap,
		Change:          BalanceUnchanged,
	}, nil
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
