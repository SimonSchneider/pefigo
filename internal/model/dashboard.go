package model

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sort"

	"github.com/SimonSchneider/goslu/date"
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

type BalanceHistorySeries struct {
	Name  string    `json:"name"`
	Color string    `json:"color"`
	Data  []float64 `json:"data"`
}

type BalanceHistoryChartData struct {
	Dates  []string               `json:"dates"`
	Series []BalanceHistorySeries `json:"series"`
}

type DashboardView struct {
	NetWorth            NetWorth
	Budget              *BudgetView
	AccountTypeGroups   []AccountTypeGroup
	AccountChartData    []AccountTypeChartEntry
	BalanceHistoryChart BalanceHistoryChartData
	HasSpecialDates     bool
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

	groups := groupAccountsByType(accounts, accountTypes)
	chartData := buildAccountChartData(groups)

	balanceHistory, err := s.buildBalanceHistoryChart(ctx, accountTypes, today)
	if err != nil {
		return nil, fmt.Errorf("building balance history chart: %w", err)
	}

	specialDates, err := s.ListSpecialDates(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing special dates: %w", err)
	}

	return &DashboardView{
		NetWorth:            newNetWorth(accounts),
		Budget:              budget,
		AccountTypeGroups:   groups,
		AccountChartData:    chartData,
		BalanceHistoryChart: balanceHistory,
		HasSpecialDates:     len(specialDates) > 0,
	}, nil
}

// buildBalanceHistoryChart is the Balance History as bars: its days thinned out to bars, with every
// Account Type, Uncategorized last, at its Balance History as of each bar's day. An Account Type
// without a balance on any bar has no series.
func (s *Service) buildBalanceHistoryChart(ctx context.Context, accountTypes []AccountType, today date.Date) (BalanceHistoryChartData, error) {
	history, err := s.balanceHistory(ctx, today)
	if err != nil {
		return BalanceHistoryChartData{}, err
	}
	const balanceHistoryMaxBars = 24
	days := downsampleDates(balanceHistoryDays(history), balanceHistoryMaxBars)
	dateLabels := make([]string, len(days))
	for i, d := range days {
		dateLabels[i] = d.String()
	}
	series := make([]BalanceHistorySeries, 0)
	for _, at := range append(slices.Clone(accountTypes), uncategorized) {
		data := balancesAsOf(history[at.ID], days)
		hasAny := false
		for i, v := range data {
			data[i] = math.Round(v*100) / 100
			hasAny = hasAny || v != 0
		}
		if !hasAny {
			continue
		}
		color := at.Color
		if color == "" {
			color = "#999999"
		}
		series = append(series, BalanceHistorySeries{Name: at.Name, Color: color, Data: data})
	}
	return BalanceHistoryChartData{
		Dates:  dateLabels,
		Series: series,
	}, nil
}

// downsampleDates groups dates into calendar periods (1, 2, 3, 4 or 6 months,
// then whole years), using the smallest period that fits within maxBars, so
// finished periods are evenly spaced and stay put as new days are added.
func downsampleDates(dates []date.Date, maxBars int) []date.Date {
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
func lastDatePerPeriod(dates []date.Date, months int) []date.Date {
	monthIndex := func(d date.Date) int {
		return d.Year()*12 + int(d.Month()) - 1
	}
	out := make([]date.Date, 0)
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
