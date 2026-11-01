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
// Startup Share account without snapshots on a day any other account's snapshot was recorded.
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
// Startup Share account has no Snapshots, as in a Forecast: the ones recorded on it are ignored, and
// it counts at its derived value on every day a Snapshot was recorded, so that day's total includes it.
func (s *Service) recordedBalances(ctx context.Context, today date.Date) ([]recordedBalance, error) {
	rows, err := s.q.ListSnapshotHistoryWithType(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing snapshot history: %w", err)
	}
	// The Startup Share accounts are valued by the Baseline's investment rounds and share changes.
	p, err := s.placeEvents(ctx, nil)
	if err != nil {
		return nil, err
	}
	recorded := make([]recordedBalance, 0, len(rows))
	var snapshotDays []date.Date
	for _, r := range rows {
		if _, ok := p.startupShareAccounts[r.AccountID]; ok || date.Date(r.Date).After(today) {
			continue
		}
		snapshotDays = append(snapshotDays, date.Date(r.Date))
		var v uncertain.Value
		if err := v.Decode(r.Balance); err != nil {
			return nil, fmt.Errorf("decoding balance: %w", err)
		}
		recorded = append(recorded, recordedBalance{accountID: r.AccountID, typeID: r.TypeID, day: date.Date(r.Date), balance: v.Mean()})
	}

	typeIDs := make(map[string]string, len(p.accounts))
	for _, acc := range p.accounts {
		typeIDs[acc.ID] = acc.TypeID
	}
	slices.Sort(snapshotDays)
	snapshotDays = slices.Compact(snapshotDays)
	ucfg := uncertain.NewConfig(0, 1)
	for _, accountID := range slices.Sorted(maps.Keys(p.startupShares)) {
		ss := p.startupShares[accountID]
		for _, day := range snapshotDays {
			if balance, ok := ss.balanceOn(ucfg, day); ok {
				recorded = append(recorded, recordedBalance{accountID: accountID, typeID: typeIDs[accountID], day: day, balance: balance})
			}
		}
	}
	slices.SortStableFunc(recorded, func(a, b recordedBalance) int { return cmp.Compare(a.day, b.day) })
	return recorded, nil
}

// balanceHistoryDays are the days, in order, that any Account Type has a Balance History point on.
func balanceHistoryDays(history map[string][]BalancePoint) []date.Date {
	var days []date.Date
	for _, points := range history {
		for _, p := range points {
			days = append(days, p.Date)
		}
	}
	slices.Sort(days)
	return slices.Compact(days)
}

// balancesAsOf is the balance of an Account Type's Balance History points as of each of days, in
// order: its latest point on or before the day, or 0 before its first.
func balancesAsOf(points []BalancePoint, days []date.Date) []float64 {
	balances := make([]float64, len(days))
	var balance float64
	next := 0
	for i, day := range days {
		for ; next < len(points) && !points[next].Date.After(day); next++ {
			balance = points[next].Balance
		}
		balances[i] = balance
	}
	return balances
}

// totalBalanceHistory is the sum of every Account Type's Balance History, as of each day any of
// them has a point.
func totalBalanceHistory(history map[string][]BalancePoint) []BalancePoint {
	days := balanceHistoryDays(history)
	totals := make([]float64, len(days))
	for _, typeID := range slices.Sorted(maps.Keys(history)) {
		for i, balance := range balancesAsOf(history[typeID], days) {
			totals[i] += balance
		}
	}
	points := make([]BalancePoint, len(days))
	for i, day := range days {
		points[i] = BalancePoint{Date: day, Balance: totals[i]}
	}
	return points
}
