package model_test

import (
	"errors"
	"math"
	"slices"
	"testing"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

// recordedPrediction keeps every event of a prediction.
type recordedPrediction struct {
	setup     model.PredictionSetupEvent
	snapshots []model.PredictionBalanceSnapshot
	closed    bool
}

func (r *recordedPrediction) Setup(e model.PredictionSetupEvent) error { r.setup = e; return nil }
func (r *recordedPrediction) Close() error                             { r.closed = true; return nil }
func (r *recordedPrediction) Snapshot(s model.PredictionBalanceSnapshot) error {
	r.snapshots = append(r.snapshots, s)
	return nil
}

// pointsOf returns the forecast points of the series id, in the order they were sent.
func (r *recordedPrediction) pointsOf(id string) []model.PredictionBalanceSnapshot {
	var points []model.PredictionBalanceSnapshot
	for _, s := range r.snapshots {
		if s.ID == id {
			points = append(points, s)
		}
	}
	return points
}

func forecastWith(t *testing.T, svc *model.Service, params model.PredictionParams) *recordedPrediction {
	t.Helper()
	rec := &recordedPrediction{}
	if err := svc.RunPrediction(t.Context(), rec, params); err != nil {
		t.Fatalf("RunPrediction: %v", err)
	}
	return rec
}

func runPrediction(t *testing.T, svc *model.Service, groupBy model.GroupBy) *recordedPrediction {
	t.Helper()
	return forecastWith(t, svc, model.PredictionParams{
		Duration: ptr(10 * date.Day), Samples: ptr[int64](1), Quantile: ptr(0.8), SnapshotInterval: "*-*-*", GroupBy: groupBy,
	})
}

func TestForecastsStartOnTheLatestSnapshotWithEveryAccount(t *testing.T) {
	svc := newTestService(t)
	savings := mustAccountType(t, svc, "Savings", "#00ff00")
	stale := mustTypedAccount(t, svc, "Stale", savings.ID)
	recent := mustTypedAccount(t, svc, "Recent", savings.ID)
	start := mustParseDate("2025-03-10")
	mustSnapshot(t, svc, stale.ID, start.Add(-200*date.Day), 1000)
	mustSnapshot(t, svc, recent.ID, start, 5000)

	for groupBy, id := range map[model.GroupBy]string{model.GroupByType: savings.ID, model.GroupByTotal: "total"} {
		t.Run(string(groupBy), func(t *testing.T) {
			rec := runPrediction(t, svc, groupBy)

			// One point a day from the Forecast Start, each holding both accounts: the stale one as recorded.
			want := make([]model.PredictionBalanceSnapshot, 10)
			for i := range want {
				want[i] = model.PredictionBalanceSnapshot{ID: id, Day: start.Add(date.Duration(i) * date.Day), Balance: 6000, LowerBound: 6000, UpperBound: 6000}
			}
			if !slices.Equal(rec.snapshots, want) {
				t.Fatalf("expected the points %+v, got %+v", want, rec.snapshots)
			}
		})
	}

	t.Run("none", func(t *testing.T) {
		rec := runPrediction(t, svc, model.GroupByNone)

		for _, s := range rec.snapshots {
			if s.Day.Before(start) {
				t.Fatalf("expected no point before the Forecast Start %s, got %+v", start, s)
			}
		}
		for id, balance := range map[string]float64{stale.ID: 1000, recent.ID: 5000} {
			points := rec.pointsOf(id)
			if len(points) != 10 || points[0].Day != start || points[0].Balance != balance {
				t.Fatalf("expected %s to have a point on each of the 10 days from %s, starting at %f, got %+v", id, start, balance, points)
			}
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

func TestForecastHasASeriesForEveryAccountTypeWithAnAccount(t *testing.T) {
	svc := newTestService(t)
	savings := mustAccountType(t, svc, "Savings", "#00ff00")
	empty := mustAccountType(t, svc, "Empty", "#00ffff")
	mustAccountType(t, svc, "Unused", "#0000ff")
	mustSnapshot(t, svc, mustTypedAccount(t, svc, "Savings", savings.ID).ID, mustParseDate("2025-03-10"), 1000)
	mustTypedAccount(t, svc, "Not recorded yet", empty.ID)

	rec := runPrediction(t, svc, model.GroupByType)

	var ids []string
	for _, e := range rec.setup.Entities {
		ids = append(ids, e.ID)
	}
	if want := []string{empty.ID, savings.ID}; !slices.Equal(ids, want) {
		t.Fatalf("expected the entities %v, Empty and Savings but not Unused, got %+v", want, rec.setup.Entities)
	}
	for i, id := range []string{empty.ID, savings.ID} {
		points := rec.pointsOf(id)
		if len(points) != 10 {
			t.Fatalf("expected a point on each of the 10 days for %s, got %+v", id, points)
		}
		if want := []float64{0, 1000}[i]; points[0].Balance != want {
			t.Fatalf("expected %s to start at %f, got %+v", id, want, points[0])
		}
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

func TestChartWithoutAForecastEndsOnItsLastDateLine(t *testing.T) {
	for name, specialDates := range map[string][]model.SpecialDateInput{
		"without Special Dates": nil,
		"with Special Dates": {
			{Name: "Graduation", Date: mustParseDate("2020-06-01")},
			{Name: "Retirement", Date: date.Today().Add(20 * date.Year)},
		},
	} {
		t.Run(name, func(t *testing.T) {
			svc := newTestService(t)
			for _, sd := range specialDates {
				if _, err := svc.UpsertSpecialDate(t.Context(), sd); err != nil {
					t.Fatalf("create special date: %v", err)
				}
			}

			rec := runPrediction(t, svc, model.GroupByType)

			if n := len(rec.setup.Marklines); n == 0 || rec.setup.Max != rec.setup.Marklines[n-1].Date {
				t.Fatalf("expected the chart to end on its last date line, got max %s and the date lines %+v", rec.setup.Max, rec.setup.Marklines)
			}
		})
	}
}

func TestChartWithoutAForecastShowsAStartupShareAccountsDerivedHistory(t *testing.T) {
	svc := newTestService(t)
	equity := mustAccountType(t, svc, "Equity", "#ff00ff")
	startup := mustTypedAccount(t, svc, "Startup", equity.ID)
	mustStartupShareAccount(t, svc, startup.ID,
		[]model.InvestmentRoundInput{
			{Date: mustParseDate("2021-01-01"), Valuation: 1e6, PreMoneyShares: 1000},
			{Date: mustParseDate("2021-03-01"), Valuation: 2e6, PreMoneyShares: 1000},
		},
		[]model.ShareChangeInput{{Date: mustParseDate("2021-02-01"), DeltaShares: 10, TotalPrice: 100}},
	)

	rec := forecastWith(t, svc, model.PredictionParams{Duration: ptr(date.Year), Samples: ptr[int64](1), Quantile: ptr(0.0), SnapshotInterval: "*-*-*", GroupBy: model.GroupByType})

	// Its balance on each day of an investment round or share change with shares to value.
	if len(rec.setup.Entities) != 1 || rec.setup.Entities[0].ID != equity.ID {
		t.Fatalf("expected the Equity entity, got %+v", rec.setup.Entities)
	}
	expectPoints(t, rec.setup.Entities[0].Snapshots, point(equity.ID, "2021-02-01", 10000), point(equity.ID, "2021-03-01", 20000))
	if len(rec.snapshots) != 0 {
		t.Fatalf("expected no forecast points without a Forecast, got %+v", rec.snapshots)
	}
}

func TestChartHistoryKeepsAStartupShareAccountAfterItsSharesAreSold(t *testing.T) {
	svc := newTestService(t)
	equity := mustAccountType(t, svc, "Equity", "#ff00ff")
	startup := mustTypedAccount(t, svc, "Startup", equity.ID)
	mustStartupShareAccount(t, svc, startup.ID,
		[]model.InvestmentRoundInput{
			{Date: mustParseDate("2021-01-01"), Valuation: 1e6, PreMoneyShares: 1000},
			{Date: mustParseDate("2021-05-01"), Valuation: 2e6, PreMoneyShares: 1000},
		},
		[]model.ShareChangeInput{
			{Date: mustParseDate("2021-02-01"), DeltaShares: 10, TotalPrice: 100},
			{Date: mustParseDate("2021-04-01"), DeltaShares: -10},
		},
	)

	rec := forecastWith(t, svc, model.PredictionParams{Duration: ptr(date.Year), Samples: ptr[int64](1), Quantile: ptr(0.0), SnapshotInterval: "*-*-*", GroupBy: model.GroupByType})

	// No point before it first has shares, then every day of a round or share change, sold out included.
	if len(rec.setup.Entities) != 1 {
		t.Fatalf("expected the Equity entity, got %+v", rec.setup.Entities)
	}
	expectPoints(t, rec.setup.Entities[0].Snapshots,
		point(equity.ID, "2021-02-01", 10000),
		point(equity.ID, "2021-04-01", 0),
		point(equity.ID, "2021-05-01", 0),
	)
}

func TestAForecastWithNothingGivenRunsWithTheChartPageDefaults(t *testing.T) {
	svc := newTestService(t)
	savings := mustAccountType(t, svc, "Savings", "#00ff00")
	acc := mustTypedAccount(t, svc, "Savings account", savings.ID)
	start := mustParseDate("2025-03-10")
	if _, err := svc.UpsertAccountSnapshot(t.Context(), acc.ID, model.AccountSnapshotInput{Date: start, Balance: uncertain.NewUniform(0, 1000)}); err != nil {
		t.Fatalf("create snapshot: %v", err)
	}

	rec := forecastWith(t, svc, model.PredictionParams{})

	// A year from the Forecast Start,
	if want := start.Add(date.Year); rec.setup.Max != want {
		t.Errorf("expected the Forecast to run for a year, to %s, got %s", want, rec.setup.Max)
	}
	// grouped by Account Type,
	if len(rec.setup.Entities) != 1 || rec.setup.Entities[0].ID != savings.ID {
		t.Errorf("expected the series of the Account Type, got %+v", rec.setup.Entities)
	}
	// with a point on the 28th of every month,
	var wantDays, days []date.Date
	for day := start; day.Before(start.Add(date.Year)); day = day.Add(date.Day) {
		if date.Cron("*-*-28").Matches(day) {
			wantDays = append(wantDays, day)
		}
	}
	for _, s := range rec.snapshots {
		days = append(days, s.Day)
		if s.ID != savings.ID {
			t.Errorf("expected every point in the series of the Account Type, got %+v", s)
		}
		// bounding 80% of outcomes.
		if math.Abs(s.LowerBound-100) > 50 || math.Abs(s.UpperBound-900) > 50 {
			t.Errorf("expected the bounds of 80%% of outcomes, about 100 and 900, got %+v", s)
		}
	}
	if !slices.Equal(days, wantDays) {
		t.Errorf("expected a point on each of %v, got %v", wantDays, days)
	}
}

func TestAForecastWithAnInvalidIntervalOrGroupingIsInvalidInput(t *testing.T) {
	svc := newTestService(t)
	mustSnapshot(t, svc, mustAccount(t, svc, "Checking").ID, mustParseDate("2025-03-10"), 1000)
	invalid := map[string]model.PredictionParams{"group by account": {GroupBy: "account"}}
	for _, interval := range invalidDatePatterns {
		invalid["interval "+string(interval)] = model.PredictionParams{SnapshotInterval: interval}
	}

	for name, params := range invalid {
		t.Run(name, func(t *testing.T) {
			rec := &recordedPrediction{}
			if err := svc.RunPrediction(t.Context(), rec, params); !errors.Is(err, model.ErrInvalidInput) {
				t.Fatalf("expected ErrInvalidInput, got %v", err)
			}
			if rec.setup.Max != 0 || len(rec.snapshots) != 0 || rec.closed {
				t.Fatalf("expected nothing sent, got %+v", rec)
			}
		})
	}
}
