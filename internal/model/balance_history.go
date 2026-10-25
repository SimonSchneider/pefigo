package model

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

// recordedBalance is the balance of an account recorded on one day: a snapshot, or the value of a
// Startup Share account without snapshots on the day of an investment round or share change.
type recordedBalance struct {
	accountID string
	typeID    string // "" for Uncategorized
	day       date.Date
	balance   float64 // the mean of the uncertain balance
}

// balanceHistory is the Balance History of each Account Type that has recorded balances, keyed by
// its ID, with Uncategorized under "". Each point adds up every account of the Account Type at
// its latest recorded balance.
func (s *Service) balanceHistory(ctx context.Context, today date.Date) (map[string][]BalancePoint, error) {
	recorded, err := s.recordedBalances(ctx, today)
	if err != nil {
		return nil, err
	}
	type accountType struct {
		accounts []string // in the order they were first recorded, so every sum adds up the same way
		latest   map[string]float64
	}
	types := make(map[string]*accountType)
	history := make(map[string][]BalancePoint)
	for _, r := range recorded {
		at, ok := types[r.typeID]
		if !ok {
			at = &accountType{latest: make(map[string]float64)}
			types[r.typeID] = at
		}
		if _, ok := at.latest[r.accountID]; !ok {
			at.accounts = append(at.accounts, r.accountID)
		}
		at.latest[r.accountID] = r.balance
		var sum float64
		for _, id := range at.accounts {
			sum += at.latest[id]
		}
		points := history[r.typeID]
		if n := len(points); n > 0 && points[n-1].Date == r.day {
			points[n-1].Balance = sum
		} else {
			history[r.typeID] = append(points, BalancePoint{Date: r.day, Balance: sum})
		}
	}
	return history, nil
}

// recordedBalances are the balances of every account recorded on or before today, by day. A
// Startup Share account counts at its derived value only when it has no snapshots.
func (s *Service) recordedBalances(ctx context.Context, today date.Date) ([]recordedBalance, error) {
	rows, err := s.q.ListSnapshotHistoryWithType(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing snapshot history: %w", err)
	}
	recorded := make([]recordedBalance, 0, len(rows))
	withSnapshots := make(map[string]struct{})
	for _, r := range rows {
		withSnapshots[r.AccountID] = struct{}{}
		if date.Date(r.Date).After(today) {
			continue
		}
		var v uncertain.Value
		if err := v.Decode(r.Balance); err != nil {
			return nil, fmt.Errorf("decoding balance: %w", err)
		}
		recorded = append(recorded, recordedBalance{accountID: r.AccountID, typeID: r.TypeID, day: date.Date(r.Date), balance: v.Mean()})
	}

	startupAccounts, err := s.listStartupShareHistories(ctx, withSnapshots)
	if err != nil {
		return nil, err
	}
	ucfg := uncertain.NewConfig(0, 1)
	for _, accountID := range slices.Sorted(maps.Keys(startupAccounts)) {
		sa := startupAccounts[accountID]
		for _, day := range sa.recordedDays(today) {
			if balance, ok := sa.balanceOn(ucfg, day); ok {
				recorded = append(recorded, recordedBalance{accountID: accountID, typeID: sa.typeID, day: day, balance: balance})
			}
		}
	}
	slices.SortStableFunc(recorded, func(a, b recordedBalance) int { return cmp.Compare(a.day, b.day) })
	return recorded, nil
}
