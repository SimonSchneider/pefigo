package model_test

import (
	"slices"
	"testing"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
)

// recordedPrediction keeps every event of a prediction.
type recordedPrediction struct {
	setup     model.PredictionSetupEvent
	snapshots []model.PredictionBalanceSnapshot
}

func (r *recordedPrediction) Setup(e model.PredictionSetupEvent) error { r.setup = e; return nil }
func (r *recordedPrediction) Close() error                             { return nil }
func (r *recordedPrediction) Snapshot(s model.PredictionBalanceSnapshot) error {
	r.snapshots = append(r.snapshots, s)
	return nil
}

func runPrediction(t *testing.T, svc *model.Service, groupBy model.GroupBy) *recordedPrediction {
	t.Helper()
	rec := &recordedPrediction{}
	if err := svc.RunPrediction(t.Context(), rec, model.PredictionParams{
		Duration: 10 * date.Day, Samples: 1, Quantile: 0.8, SnapshotInterval: "*-*-*", GroupBy: groupBy,
	}); err != nil {
		t.Fatalf("RunPrediction: %v", err)
	}
	return rec
}

func TestGroupedPredictionsStartAfterTodayWithEveryAccount(t *testing.T) {
	svc := newTestService(t)
	savings := mustAccountType(t, svc, "Savings", "#00ff00")
	stale := mustTypedAccount(t, svc, "Stale", savings.ID)
	recent := mustTypedAccount(t, svc, "Recent", savings.ID)
	today := date.Today()
	mustSnapshot(t, svc, stale.ID, today.Add(-200*date.Day), 1000)
	mustSnapshot(t, svc, recent.ID, today.Add(-3*date.Day), 5000)

	for groupBy, id := range map[model.GroupBy]string{model.GroupByType: savings.ID, model.GroupByTotal: "total"} {
		t.Run(string(groupBy), func(t *testing.T) {
			rec := runPrediction(t, svc, groupBy)

			if len(rec.snapshots) == 0 {
				t.Fatal("expected forecast points")
			}
			for _, s := range rec.snapshots {
				if !s.Day.After(today) {
					t.Fatalf("expected no point on or before today, got %+v", s)
				}
			}
			if first := rec.snapshots[0]; first.ID != id || first.Day != today.Add(date.Day) || first.Balance != 6000 {
				t.Fatalf("expected the first point tomorrow to hold both accounts (6000), got %+v", first)
			}
		})
	}

	t.Run("none", func(t *testing.T) {
		rec := runPrediction(t, svc, model.GroupByNone)

		if !slices.ContainsFunc(rec.snapshots, func(s model.PredictionBalanceSnapshot) bool {
			return s.ID == stale.ID && s.Day.Before(today)
		}) {
			t.Fatalf("expected the stale account's estimated points before today, got %+v", rec.snapshots)
		}
	})
}

func TestPredictionGroupsAccountsWithoutAnAccountTypeAsUncategorized(t *testing.T) {
	svc := newTestService(t)
	mustSnapshot(t, svc, mustAccount(t, svc, "No type").ID, date.Today(), 500)

	rec := runPrediction(t, svc, model.GroupByType)

	if len(rec.setup.Entities) != 1 {
		t.Fatalf("expected one entity, got %+v", rec.setup.Entities)
	}
	if e := rec.setup.Entities[0]; e.ID != "" || e.Name != "Uncategorized" || e.Color != "#999999" {
		t.Fatalf("expected the Uncategorized entity, got %+v", e)
	}
}

func TestPredictionHasTheDashboardForecastDateLines(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	for _, sd := range []model.SpecialDateInput{
		{Name: "Retirement", Date: date.Today().Add(20 * date.Year), Color: "#ff0000"},
		{Name: "Graduation", Date: mustParseDate("2020-06-01")},
	} {
		if _, err := svc.UpsertSpecialDate(ctx, sd); err != nil {
			t.Fatalf("create special date: %v", err)
		}
	}

	rec := runPrediction(t, svc, model.GroupByType)

	want := []model.Markline{
		{Date: mustParseDate("2020-06-01"), Name: "Graduation"},
		{Date: date.Today(), Name: "Today"},
		{Date: date.Today().Add(20 * date.Year), Color: "#ff0000", Name: "Retirement"},
	}
	if !slices.Equal(rec.setup.Marklines, want) {
		t.Fatalf("expected the prediction's date lines %+v, got %+v", want, rec.setup.Marklines)
	}
	if got := dashboardForecastHistory(t, svc, nil).Marklines; !slices.Equal(got, want) {
		t.Fatalf("expected the dashboard's date lines %+v, got %+v", want, got)
	}
}
