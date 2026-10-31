package model_test

import (
	"slices"
	"testing"
	"time"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
)

// seedMonthlySnapshots records 1000+i on each of days in month i, for the months from to months
// counted from December 2010, all before today.
func seedMonthlySnapshots(t *testing.T, svc *model.Service, accountID string, from, months int, days ...int) {
	t.Helper()
	for i := from; i < months; i++ {
		for _, day := range days {
			d := date.FromTime(time.Date(2010, time.December+time.Month(i), day, 0, 0, 0, 0, time.UTC))
			if _, err := svc.UpsertAccountSnapshot(t.Context(), accountID, model.AccountSnapshotInput{
				Date:    d,
				Balance: newFixedValue(float64(1000 + i)),
			}); err != nil {
				t.Fatalf("upsert snapshot: %v", err)
			}
		}
	}
}

func newBalanceHistoryAccount(t *testing.T, svc *model.Service) model.Account {
	t.Helper()
	return mustTypedAccount(t, svc, "My Savings", mustAccountType(t, svc, "Savings", "#00ff00").ID)
}

func balanceHistoryChart(t *testing.T, svc *model.Service) model.BalanceHistoryChartData {
	t.Helper()
	dashboard, err := svc.GetDashboardData(t.Context())
	if err != nil {
		t.Fatalf("get dashboard data: %v", err)
	}
	return dashboard.BalanceHistoryChart
}

func balanceHistoryChartDates(t *testing.T, svc *model.Service) []string {
	t.Helper()
	return balanceHistoryChart(t, svc).Dates
}

func expectBalanceHistoryChart(t *testing.T, got model.BalanceHistoryChartData, wantDates []string, wantSeries ...model.BalanceHistorySeries) {
	t.Helper()
	if !slices.Equal(got.Dates, wantDates) || !slices.EqualFunc(got.Series, wantSeries, func(a, b model.BalanceHistorySeries) bool {
		return a.Name == b.Name && a.Color == b.Color && slices.Equal(a.Data, b.Data)
	}) {
		t.Fatalf("expected the bars %v with the series %+v, got %v with %+v", wantDates, wantSeries, got.Dates, got.Series)
	}
}

func TestGetDashboardData_BalanceHistoryCalendarAligned(t *testing.T) {
	tests := []struct {
		name      string
		months    int
		days      []int
		wantBars  int
		wantGap   int
		wantFirst string
		wantLast  string
	}{
		{name: "24 months shows every snapshot", months: 24, days: []int{25}, wantBars: 24, wantGap: 1, wantFirst: "2010-12-25", wantLast: "2012-11-25"},
		{name: "25 months uses two-month periods", months: 25, days: []int{25}, wantBars: 13, wantGap: 2, wantFirst: "2010-12-25", wantLast: "2012-12-25"},
		{name: "48 months includes the unfinished period", months: 48, days: []int{25}, wantBars: 17, wantGap: 3, wantFirst: "2010-12-25", wantLast: "2014-11-25"},
		{name: "71 months uses four-month periods", months: 71, days: []int{25}, wantBars: 19, wantGap: 4, wantFirst: "2010-12-25", wantLast: "2016-10-25"},
		{name: "73 months ends on a finished period", months: 73, days: []int{25}, wantBars: 19, wantGap: 4, wantFirst: "2010-12-25", wantLast: "2016-12-25"},
		{name: "several snapshots per month use the last one", months: 30, days: []int{10, 25}, wantBars: 16, wantGap: 2, wantFirst: "2010-12-25", wantLast: "2013-05-25"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := newTestService(t)
			acc := newBalanceHistoryAccount(t, svc)
			seedMonthlySnapshots(t, svc, acc.ID, 0, tt.months, tt.days...)

			dates := balanceHistoryChartDates(t, svc)
			if len(dates) != tt.wantBars {
				t.Fatalf("expected %d bars, got %d: %v", tt.wantBars, len(dates), dates)
			}
			if dates[0] != tt.wantFirst || dates[len(dates)-1] != tt.wantLast {
				t.Fatalf("expected bars from %s to %s, got %s to %s: %v", tt.wantFirst, tt.wantLast, dates[0], dates[len(dates)-1], dates)
			}
			monthIndex := func(s string) int {
				d := mustParseDate(s)
				return d.Year()*12 + int(d.Month())
			}
			for i := 1; i < len(dates)-1; i++ {
				if g := monthIndex(dates[i]) - monthIndex(dates[i-1]); g != tt.wantGap {
					t.Fatalf("expected every bar %d months apart, but %s -> %s is %d months: %v", tt.wantGap, dates[i-1], dates[i], g, dates)
				}
			}
			if g := monthIndex(dates[len(dates)-1]) - monthIndex(dates[len(dates)-2]); g < 1 || g > tt.wantGap {
				t.Fatalf("expected the last bar at most %d months after the previous one, got %d: %v", tt.wantGap, g, dates)
			}
		})
	}
}

func TestGetDashboardData_BalanceHistoryStableAsSnapshotsAreAdded(t *testing.T) {
	svc := newTestService(t)
	acc := newBalanceHistoryAccount(t, svc)

	seedMonthlySnapshots(t, svc, acc.ID, 0, 71, 25)
	before := balanceHistoryChartDates(t, svc)

	seedMonthlySnapshots(t, svc, acc.ID, 71, 72, 25)
	after := balanceHistoryChartDates(t, svc)

	if len(before) != len(after) || !slices.Equal(before[:len(before)-1], after[:len(after)-1]) {
		t.Fatalf("expected finished periods to stay put when a snapshot is added mid-period\nbefore: %v\nafter:  %v", before, after)
	}
	if after[len(after)-1] != "2016-11-25" {
		t.Fatalf("expected the unfinished period to show the newest snapshot 2016-11-25, got %s", after[len(after)-1])
	}
}

func TestBalanceHistoryChartCountsEveryAccountAtItsLatestBalance(t *testing.T) {
	svc := newTestService(t)
	savings := mustAccountType(t, svc, "Savings", "#00ff00")
	mustSnapshot(t, svc, mustTypedAccount(t, svc, "A", savings.ID).ID, mustParseDate("2025-01-01"), 1000)
	mustSnapshot(t, svc, mustTypedAccount(t, svc, "B", savings.ID).ID, mustParseDate("2025-01-15"), 5000)

	expectBalanceHistoryChart(t, balanceHistoryChart(t, svc), []string{"2025-01-01", "2025-01-15"},
		model.BalanceHistorySeries{Name: "Savings", Color: "#00ff00", Data: []float64{1000, 6000}})
}

func TestBalanceHistoryChartLinesUpAccountTypesRecordedOnDifferentDays(t *testing.T) {
	svc := newTestService(t)
	cash := mustAccountType(t, svc, "Cash", "#ff0000")
	savings := mustAccountType(t, svc, "Savings", "#00ff00")
	mustSnapshot(t, svc, mustTypedAccount(t, svc, "Wallet", cash.ID).ID, mustParseDate("2025-01-01"), 100)
	mustSnapshot(t, svc, mustTypedAccount(t, svc, "Bank", savings.ID).ID, mustParseDate("2025-01-15"), 200)

	expectBalanceHistoryChart(t, balanceHistoryChart(t, svc), []string{"2025-01-01", "2025-01-15"},
		model.BalanceHistorySeries{Name: "Cash", Color: "#ff0000", Data: []float64{100, 100}},
		model.BalanceHistorySeries{Name: "Savings", Color: "#00ff00", Data: []float64{0, 200}})
}

func TestBalanceHistoryChartBarsShowTheBalanceAtTheEndOfTheirPeriod(t *testing.T) {
	svc := newTestService(t)
	pension := mustAccountType(t, svc, "Pension", "#0000ff")
	savings := mustAccountType(t, svc, "Savings", "#00ff00")
	// 25 months of Pension thin out to the 25th of every two months, from 2010-12-25.
	seedMonthlySnapshots(t, svc, mustTypedAccount(t, svc, "Monthly", pension.ID).ID, 0, 25, 25)
	// Savings is recorded once early on, and once on a day that is thinned out.
	mustSnapshot(t, svc, mustTypedAccount(t, svc, "Early", savings.ID).ID, mustParseDate("2010-12-01"), 500)
	mustSnapshot(t, svc, mustTypedAccount(t, svc, "Mid", savings.ID).ID, mustParseDate("2011-03-10"), 50)

	var wantDates []string
	var wantPension, wantSavings []float64
	for i := 0; i < 25; i += 2 {
		day := date.FromTime(time.Date(2010, time.December+time.Month(i), 25, 0, 0, 0, 0, time.UTC))
		wantDates = append(wantDates, day.String())
		wantPension = append(wantPension, float64(1000+i))
		if day.Before(mustParseDate("2011-03-10")) {
			wantSavings = append(wantSavings, 500)
		} else {
			wantSavings = append(wantSavings, 550)
		}
	}
	expectBalanceHistoryChart(t, balanceHistoryChart(t, svc), wantDates,
		model.BalanceHistorySeries{Name: "Pension", Color: "#0000ff", Data: wantPension},
		model.BalanceHistorySeries{Name: "Savings", Color: "#00ff00", Data: wantSavings})
}

func TestBalanceHistoryChartEndsToday(t *testing.T) {
	svc := newTestService(t)
	acc := newBalanceHistoryAccount(t, svc)
	today := date.Today()
	mustSnapshot(t, svc, acc.ID, mustParseDate("2025-01-01"), 1000)
	mustSnapshot(t, svc, acc.ID, today, 2000)
	mustSnapshot(t, svc, acc.ID, today.Add(10*date.Day), 9999)

	expectBalanceHistoryChart(t, balanceHistoryChart(t, svc), []string{"2025-01-01", today.String()},
		model.BalanceHistorySeries{Name: "Savings", Color: "#00ff00", Data: []float64{1000, 2000}})
}
