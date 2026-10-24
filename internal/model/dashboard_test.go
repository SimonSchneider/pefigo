package model_test

import (
	"testing"
	"time"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
)

func TestGetDashboardData_SnapshotHistoryEvenlySpaced(t *testing.T) {
	tests := []struct {
		name   string
		months int
	}{
		{name: "71 months", months: 71},
		{name: "25 months", months: 25},
		{name: "48 months", months: 48},
		{name: "24 months", months: 24},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := newTestService(t)
			ctx := t.Context()

			at, err := svc.UpsertAccountType(ctx, model.AccountTypeInput{Name: "Savings", Color: "#00ff00"})
			if err != nil {
				t.Fatalf("create account type: %v", err)
			}
			acc, err := svc.UpsertAccount(ctx, model.AccountInput{Name: "My Savings", TypeID: at.ID})
			if err != nil {
				t.Fatalf("create account: %v", err)
			}
			var last date.Date
			for i := range tt.months {
				last = date.FromTime(time.Date(2020, time.December+time.Month(i), 25, 0, 0, 0, 0, time.UTC))
				if _, err := svc.UpsertAccountSnapshot(ctx, acc.ID, model.AccountSnapshotInput{
					Date:    last,
					Balance: newFixedValue(float64(1000 + i)),
				}); err != nil {
					t.Fatalf("upsert snapshot: %v", err)
				}
			}

			dashboard, err := svc.GetDashboardData(ctx)
			if err != nil {
				t.Fatalf("get dashboard data: %v", err)
			}
			dates := dashboard.SnapshotHistoryChart.Dates
			if len(dates) < 2 || len(dates) > 24 {
				t.Fatalf("expected between 2 and 24 bars, got %d: %v", len(dates), dates)
			}
			if dates[len(dates)-1] != last.String() {
				t.Fatalf("expected last bar %s, got %s", last, dates[len(dates)-1])
			}
			monthIndex := func(s string) int {
				d := mustParseDate(s)
				return d.Year()*12 + int(d.Month())
			}
			gap := monthIndex(dates[1]) - monthIndex(dates[0])
			for i := 2; i < len(dates); i++ {
				if g := monthIndex(dates[i]) - monthIndex(dates[i-1]); g != gap {
					t.Fatalf("expected every bar %d months apart, but %s -> %s is %d months: %v", gap, dates[i-1], dates[i], g, dates)
				}
			}
		})
	}
}
