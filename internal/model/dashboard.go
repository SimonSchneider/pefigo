package model

import (
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/pdb"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

type AccountTypeGroup struct {
	AccountType  AccountType
	Accounts     []AccountDetailed
	TotalBalance float64
}

type AccountTypeChartEntry struct {
	Name     string              `json:"name"`
	Color    string              `json:"color"`
	Accounts []AccountChartEntry `json:"accounts"`
}

type AccountChartEntry struct {
	Name    string  `json:"name"`
	Balance float64 `json:"balance"`
}

type SnapshotHistorySeries struct {
	Name  string    `json:"name"`
	Color string    `json:"color"`
	Data  []float64 `json:"data"`

	typeID string // the Account Type, or "" for accounts without one
}

type SnapshotHistoryChartData struct {
	Dates  []string                `json:"dates"`
	Series []SnapshotHistorySeries `json:"series"`
}

type DashboardView struct {
	TotalBalance         float64
	TotalAssets          float64
	TotalLiabilities     float64
	Budget               *BudgetView
	AccountTypeGroups    []AccountTypeGroup
	AccountChartData     []AccountTypeChartEntry
	SnapshotHistoryChart SnapshotHistoryChartData
	HasSpecialDates      bool
}

func (s *Service) GetDashboardData(ctx context.Context) (*DashboardView, error) {
	today := date.Today()

	budget, err := s.GetBudgetData(ctx)
	if err != nil {
		return nil, fmt.Errorf("computing budget view: %w", err)
	}

	accounts, err := s.ListAccountsDetailed(ctx, today)
	if err != nil {
		return nil, fmt.Errorf("listing accounts: %w", err)
	}

	accountTypes, err := s.ListAccountTypes(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing account types: %w", err)
	}

	var totalBalance, totalAssets, totalLiabilities float64
	for _, acc := range accounts {
		if acc.LastSnapshot != nil {
			bal := acc.LastSnapshot.Balance.Mean()
			totalBalance += bal
			if bal > 0 {
				totalAssets += bal
			} else {
				totalLiabilities += bal
			}
		}
	}

	groups := groupAccountsByType(accounts, accountTypes)
	chartData := buildAccountChartData(groups)

	snapshotHistory, err := s.buildSnapshotHistoryChart(ctx, accountTypes)
	if err != nil {
		return nil, fmt.Errorf("building snapshot history chart: %w", err)
	}

	specialDates, err := s.ListSpecialDates(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing special dates: %w", err)
	}

	return &DashboardView{
		TotalBalance:         totalBalance,
		TotalAssets:          totalAssets,
		TotalLiabilities:     totalLiabilities,
		Budget:               budget,
		AccountTypeGroups:    groups,
		AccountChartData:     chartData,
		SnapshotHistoryChart: snapshotHistory,
		HasSpecialDates:      len(specialDates) > 0,
	}, nil
}

func (s *Service) buildSnapshotHistoryChart(ctx context.Context, accountTypes []AccountType) (SnapshotHistoryChartData, error) {
	q := s.q
	rows, err := q.ListSnapshotHistoryWithType(ctx)
	if err != nil {
		return SnapshotHistoryChartData{}, err
	}
	type TypeKey struct {
		Date   int64
		TypeID string
	}
	typeKey := func(date int64, typeID string) TypeKey { return TypeKey{date, typeID} }
	sumByDateAndType := make(map[TypeKey]float64)
	dateSet := make(map[int64]struct{})
	accountsWithSnapshots := make(map[string]struct{})
	for _, r := range rows {
		var v uncertain.Value
		if err := v.Decode(r.Balance); err != nil {
			return SnapshotHistoryChartData{}, fmt.Errorf("decoding balance: %w", err)
		}
		key := typeKey(r.Date, r.TypeID)
		sumByDateAndType[key] += v.Mean()
		dateSet[r.Date] = struct{}{}
		accountsWithSnapshots[r.AccountID] = struct{}{}
	}
	dates := make([]int64, 0, len(dateSet))
	for d := range dateSet {
		dates = append(dates, d)
	}
	sort.Slice(dates, func(i, j int) bool { return dates[i] < dates[j] })

	const snapshotHistoryMaxBars = 24
	dates = downsampleDates(dates, snapshotHistoryMaxBars)

	startupRows, err := q.ListStartupShareSnapshotHistory(ctx)
	if err != nil {
		return SnapshotHistoryChartData{}, fmt.Errorf("listing startup share snapshot history: %w", err)
	}
	if len(startupRows) > 0 {
		type startupAccount struct {
			TypeID       string
			Rounds       []pdb.ListStartupShareSnapshotHistoryRow
			ShareChanges []ShareChange
		}
		startupAccounts := make(map[string]*startupAccount)
		for _, r := range startupRows {
			if _, has := accountsWithSnapshots[r.AccountID]; has {
				continue
			}
			sa, ok := startupAccounts[r.AccountID]
			if !ok {
				sa = &startupAccount{TypeID: r.TypeID}
				startupAccounts[r.AccountID] = sa
			}
			sa.Rounds = append(sa.Rounds, r)
		}
		for accountID, sa := range startupAccounts {
			changes, err := s.ListShareChanges(ctx, accountID)
			if err != nil {
				return SnapshotHistoryChartData{}, fmt.Errorf("listing share changes for %s: %w", accountID, err)
			}
			sa.ShareChanges = changes
		}
		ucfg := uncertain.NewConfig(0, 1)
		for _, sa := range startupAccounts {
			for _, chartDate := range dates {
				var best *pdb.ListStartupShareSnapshotHistoryRow
				for i := range sa.Rounds {
					if sa.Rounds[i].Date <= chartDate {
						best = &sa.Rounds[i]
					}
				}
				if best == nil {
					continue
				}
				postMoneyValuation, postMoneyShares := PostMoneyValuationAndShares(best.Valuation, best.PreMoneyShares, best.Investment)
				sharesOwned, avgPurchasePrice := DeriveShareState(sa.ShareChanges, date.Date(chartDate))
				balance := CalculateStartupShareBalance(
					ucfg,
					uncertain.NewFixed(postMoneyValuation),
					sharesOwned,
					avgPurchasePrice,
					best.TaxRate,
					postMoneyShares,
					best.ValuationDiscountFactor,
				)
				sumByDateAndType[typeKey(chartDate, sa.TypeID)] += balance.Mean()
			}
		}
	}

	typeByName := make(map[string]AccountType)
	for _, at := range accountTypes {
		typeByName[at.ID] = at
	}
	for _, d := range dates {
		if sumByDateAndType[typeKey(d, "")] != 0 {
			if _, ok := typeByName[""]; !ok {
				typeByName[""] = AccountType{ID: "", Name: "Uncategorized", Color: "#999999"}
			}
			break
		}
	}
	typeIDsOrdered := make([]string, 0, len(accountTypes)+1)
	for _, at := range accountTypes {
		typeIDsOrdered = append(typeIDsOrdered, at.ID)
	}
	if _, ok := typeByName[""]; ok {
		typeIDsOrdered = append(typeIDsOrdered, "")
	}
	dateLabels := make([]string, len(dates))
	for i, d := range dates {
		dateLabels[i] = date.Date(d).String()
	}
	series := make([]SnapshotHistorySeries, 0)
	for _, typeID := range typeIDsOrdered {
		at := typeByName[typeID]
		color := at.Color
		if color == "" {
			color = "#999999"
		}
		data := make([]float64, len(dates))
		hasAny := false
		for i, d := range dates {
			v := sumByDateAndType[typeKey(d, typeID)]
			data[i] = math.Round(v*100) / 100
			if v != 0 {
				hasAny = true
			}
		}
		if hasAny {
			series = append(series, SnapshotHistorySeries{
				Name:   at.Name,
				Color:  color,
				Data:   data,
				typeID: typeID,
			})
		}
	}
	return SnapshotHistoryChartData{
		Dates:  dateLabels,
		Series: series,
	}, nil
}

// downsampleDates groups dates into calendar periods (1, 2, 3, 4 or 6 months,
// then whole years), using the smallest period that fits within maxBars, so
// finished periods are evenly spaced and stay put as new snapshots are added.
func downsampleDates(dates []int64, maxBars int) []int64 {
	if len(dates) <= maxBars || maxBars <= 1 {
		return dates
	}
	for months := 1; ; months = nextPeriodMonths(months) {
		if out := lastDatePerPeriod(dates, months); len(out) <= maxBars {
			return out
		}
	}
}

func nextPeriodMonths(months int) int {
	switch months {
	case 1, 2, 3:
		return months + 1
	case 4:
		return 6
	case 6:
		return 12
	default:
		return months + 12
	}
}

// lastDatePerPeriod keeps the last date of each period, so an unfinished final
// period shows the latest date.
func lastDatePerPeriod(dates []int64, months int) []int64 {
	monthIndex := func(d int64) int {
		dd := date.Date(d)
		return dd.Year()*12 + int(dd.Month()) - 1
	}
	out := make([]int64, 0)
	prevPeriod := -1
	for _, d := range dates {
		period := monthIndex(d) / months
		if period == prevPeriod {
			out[len(out)-1] = d
		} else {
			out = append(out, d)
		}
		prevPeriod = period
	}
	return out
}

func groupAccountsByType(accounts []AccountDetailed, accountTypes []AccountType) []AccountTypeGroup {
	typeMap := make(map[string]*AccountTypeGroup)
	for _, at := range accountTypes {
		typeMap[at.ID] = &AccountTypeGroup{
			AccountType: at,
		}
	}

	for _, acc := range accounts {
		group, exists := typeMap[acc.TypeID]
		if !exists {
			continue
		}
		group.Accounts = append(group.Accounts, acc)
		if acc.LastSnapshot != nil {
			group.TotalBalance += acc.LastSnapshot.Balance.Mean()
		}
	}

	groups := make([]AccountTypeGroup, 0, len(typeMap))
	for _, group := range typeMap {
		if len(group.Accounts) > 0 {
			groups = append(groups, *group)
		}
	}
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].TotalBalance > groups[j].TotalBalance
	})
	return groups
}

func buildAccountChartData(groups []AccountTypeGroup) []AccountTypeChartEntry {
	entries := make([]AccountTypeChartEntry, 0, len(groups))
	for _, group := range groups {
		if group.TotalBalance == 0 {
			continue
		}
		color := group.AccountType.Color
		if color == "" {
			color = "#999999"
		}
		accs := make([]AccountChartEntry, 0, len(group.Accounts))
		for _, acc := range group.Accounts {
			bal := 0.0
			if acc.LastSnapshot != nil {
				bal = math.Round(acc.LastSnapshot.Balance.Mean()*100) / 100
			}
			accs = append(accs, AccountChartEntry{
				Name:    acc.Name,
				Balance: bal,
			})
		}
		entries = append(entries, AccountTypeChartEntry{
			Name:     group.AccountType.Name,
			Color:    color,
			Accounts: accs,
		})
	}
	return entries
}
