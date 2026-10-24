package model_test

import (
	"slices"
	"testing"
	"time"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
)

func seedMonthlySnapshots(t *testing.T, svc *model.Service, accountID string, from, months int, days ...int) {
	t.Helper()
	for i := from; i < months; i++ {
		for _, day := range days {
			d := date.FromTime(time.Date(2020, time.December+time.Month(i), day, 0, 0, 0, 0, time.UTC))
			if _, err := svc.UpsertAccountSnapshot(t.Context(), accountID, model.AccountSnapshotInput{
				Date:    d,
				Balance: newFixedValue(float64(1000 + i)),
			}); err != nil {
				t.Fatalf("upsert snapshot: %v", err)
			}
		}
	}
}

func newSnapshotHistoryAccount(t *testing.T, svc *model.Service) model.Account {
	t.Helper()
	at, err := svc.UpsertAccountType(t.Context(), model.AccountTypeInput{Name: "Savings", Color: "#00ff00"})
	if err != nil {
		t.Fatalf("create account type: %v", err)
	}
	acc, err := svc.UpsertAccount(t.Context(), model.AccountInput{Name: "My Savings", TypeID: at.ID})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	return acc
}

func snapshotHistoryDates(t *testing.T, svc *model.Service) []string {
	t.Helper()
	dashboard, err := svc.GetDashboardData(t.Context())
	if err != nil {
		t.Fatalf("get dashboard data: %v", err)
	}
	return dashboard.SnapshotHistoryChart.Dates
}

func TestGetDashboardData_SnapshotHistoryCalendarAligned(t *testing.T) {
	tests := []struct {
		name      string
		months    int
		days      []int
		wantBars  int
		wantGap   int
		wantFirst string
		wantLast  string
	}{
		{name: "24 months shows every snapshot", months: 24, days: []int{25}, wantBars: 24, wantGap: 1, wantFirst: "2020-12-25", wantLast: "2022-11-25"},
		{name: "25 months uses two-month periods", months: 25, days: []int{25}, wantBars: 13, wantGap: 2, wantFirst: "2020-12-25", wantLast: "2022-12-25"},
		{name: "48 months includes the unfinished period", months: 48, days: []int{25}, wantBars: 17, wantGap: 3, wantFirst: "2020-12-25", wantLast: "2024-11-25"},
		{name: "71 months uses four-month periods", months: 71, days: []int{25}, wantBars: 19, wantGap: 4, wantFirst: "2020-12-25", wantLast: "2026-10-25"},
		{name: "73 months ends on a finished period", months: 73, days: []int{25}, wantBars: 19, wantGap: 4, wantFirst: "2020-12-25", wantLast: "2026-12-25"},
		{name: "several snapshots per month use the last one", months: 30, days: []int{10, 25}, wantBars: 16, wantGap: 2, wantFirst: "2020-12-25", wantLast: "2023-05-25"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := newTestService(t)
			acc := newSnapshotHistoryAccount(t, svc)
			seedMonthlySnapshots(t, svc, acc.ID, 0, tt.months, tt.days...)

			dates := snapshotHistoryDates(t, svc)
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

func TestGetDashboardData_SnapshotHistoryStableAsSnapshotsAreAdded(t *testing.T) {
	svc := newTestService(t)
	acc := newSnapshotHistoryAccount(t, svc)

	seedMonthlySnapshots(t, svc, acc.ID, 0, 71, 25)
	before := snapshotHistoryDates(t, svc)

	seedMonthlySnapshots(t, svc, acc.ID, 71, 72, 25)
	after := snapshotHistoryDates(t, svc)

	if len(before) != len(after) || !slices.Equal(before[:len(before)-1], after[:len(after)-1]) {
		t.Fatalf("expected finished periods to stay put when a snapshot is added mid-period\nbefore: %v\nafter:  %v", before, after)
	}
	if after[len(after)-1] != "2026-11-25" {
		t.Fatalf("expected the unfinished period to show the newest snapshot 2026-11-25, got %s", after[len(after)-1])
	}
}
