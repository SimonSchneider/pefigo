package model

import (
	"context"
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

func (s *Service) UpsertAccountSnapshot(ctx context.Context, accountID string, inp AccountSnapshotInput) (AccountSnapshot, error) {
	balance, err := inp.Balance.Encode()
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

func (s *Service) GetAccountSnapshot(ctx context.Context, id string, d date.Date) (AccountSnapshot, error) {
	snap, err := s.q.GetSnapshot(ctx, pdb.GetSnapshotParams{
		AccountID: id,
		Date:      int64(d),
	})
	if err != nil {
		return AccountSnapshot{}, fmt.Errorf("failed to get account snapshot: %w", err)
	}
	return accountSnapshotFromDB(snap), nil
}
