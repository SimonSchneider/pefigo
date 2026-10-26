package model_test

import (
	"math"
	"slices"
	"testing"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
)

// mustStartupShareAccount makes accountID a Startup Share account without tax or discount, with the
// investment rounds and share changes.
func mustStartupShareAccount(t *testing.T, svc *model.Service, accountID string, rounds []model.InvestmentRoundInput, changes []model.ShareChangeInput) {
	t.Helper()
	ctx := t.Context()
	if _, err := svc.UpsertStartupShareAccount(ctx, model.StartupShareAccountInput{AccountID: accountID, TaxRate: ptr(0.0), ValuationDiscountFactor: ptr(1.0)}); err != nil {
		t.Fatalf("create startup share account: %v", err)
	}
	for _, ir := range rounds {
		ir.AccountID = accountID
		if _, err := svc.UpsertInvestmentRound(ctx, ir); err != nil {
			t.Fatalf("create investment round: %v", err)
		}
	}
	for _, sc := range changes {
		sc.AccountID = accountID
		if _, err := svc.UpsertShareChange(ctx, sc); err != nil {
			t.Fatalf("create share change: %v", err)
		}
	}
}

// point is a forecast point without lower and upper bounds.
func point(id string, day string, balance float64) model.PredictionBalanceSnapshot {
	return model.PredictionBalanceSnapshot{ID: id, Day: mustParseDate(day), Balance: balance}
}

func expectPoints(t *testing.T, got []model.PredictionBalanceSnapshot, want ...model.PredictionBalanceSnapshot) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("expected the points %+v, got %+v", want, got)
	}
}

func TestForecastStartsOnTheLatestSnapshotEvenMonthsBeforeToday(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	savings := mustAccount(t, svc, "Savings")
	start := date.Today().Add(-182 * date.Day)
	mustSnapshot(t, svc, checking.ID, start.Add(-30*date.Day), 1000)
	mustSnapshot(t, svc, savings.ID, start, 5000)
	interval := date.Cron("*-*-01")

	rec := forecastWith(t, svc, model.PredictionParams{Duration: date.Year, Samples: 1, SnapshotInterval: interval, GroupBy: model.GroupByTotal})

	first := start
	for !interval.Matches(first) {
		first = first.Add(date.Day)
	}
	if len(rec.snapshots) == 0 || rec.snapshots[0] != (model.PredictionBalanceSnapshot{ID: "total", Day: first, Balance: 6000}) {
		t.Fatalf("expected the first point on %s, the first Forecast Interval day from the Forecast Start %s, with both accounts, got %+v", first, start, rec.snapshots)
	}
}

func TestForecastStartsOnASnapshotDatedAfterToday(t *testing.T) {
	svc := newTestService(t)
	start := date.Today().Add(40 * date.Day)
	mustSnapshot(t, svc, mustAccount(t, svc, "Checking").ID, date.Today().Add(-10*date.Day), 1000)
	mustSnapshot(t, svc, mustAccount(t, svc, "Savings").ID, start, 5000)

	rec := forecastWith(t, svc, model.PredictionParams{Duration: 3 * date.Day, Samples: 1, SnapshotInterval: "*-*-*", GroupBy: model.GroupByTotal})

	want := make([]model.PredictionBalanceSnapshot, 3)
	for i := range want {
		want[i] = model.PredictionBalanceSnapshot{ID: "total", Day: start.Add(date.Duration(i) * date.Day), Balance: 6000}
	}
	expectPoints(t, rec.snapshots, want...)
}

// The Forecast is computed from its Forecast Inputs alone: its points are on days years before today,
// as they would be on any other day.
func TestForecastPointsDependOnlyOnTheForecastInputs(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	mustSnapshot(t, svc, checking.ID, mustParseDate("2021-03-10"), 1000)
	mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Salary", ToAccountID: checking.ID, AmountType: "fixed", AmountFixed: newFixedValue(100),
		Recurrence: "*-*-15", StartDate: mustParseDate("2021-01-01"), Enabled: true,
	})

	rec := forecastWith(t, svc, model.PredictionParams{Duration: 80 * date.Day, Samples: 1, SnapshotInterval: "*-*-28", GroupBy: model.GroupByNone})

	expectPoints(t, rec.snapshots,
		point(checking.ID, "2021-03-28", 1100),
		point(checking.ID, "2021-04-28", 1200),
		point(checking.ID, "2021-05-28", 1300),
	)
}

func TestNoForecastWithoutASnapshot(t *testing.T) {
	for name, seed := range map[string]func(t *testing.T, svc *model.Service){
		"without accounts": func(t *testing.T, svc *model.Service) {},
		"with a Startup Share account alone": func(t *testing.T, svc *model.Service) {
			equity := mustAccountType(t, svc, "Equity", "#ff00ff")
			startup := mustTypedAccount(t, svc, "Startup", equity.ID)
			mustStartupShareAccount(t, svc, startup.ID,
				[]model.InvestmentRoundInput{{Date: mustParseDate("2025-01-01"), Valuation: 1e6, PreMoneyShares: 1000}},
				[]model.ShareChangeInput{{Date: mustParseDate("2025-01-01"), DeltaShares: 10, TotalPrice: 100}},
			)
			// Its Snapshots are ignored, and an account without a Snapshot has none either.
			mustSnapshot(t, svc, startup.ID, mustParseDate("2025-02-01"), 5000)
			mustTypedAccount(t, svc, "Not recorded yet", equity.ID)
		},
	} {
		t.Run(name, func(t *testing.T) {
			svc, timers := newForecastTestService(t, nil)
			seed(t, svc)
			addFutureSpecialDate(t, svc)
			startForecast(svc)

			sub := subscribe(t, svc)
			timers.fire()

			expectEventTypes(t, received(sub.Events), model.ForecastEventStarted, model.ForecastEventDone)
			if rows := subscribe(t, svc).Rows; len(rows) != 0 {
				t.Fatalf("expected the Dashboard Forecast to be empty, got %v", rows)
			}
			rec := forecastWith(t, svc, model.PredictionParams{Duration: date.Year, Samples: 1, SnapshotInterval: "*-*-*", GroupBy: model.GroupByType})
			if len(rec.setup.Marklines) == 0 || len(rec.snapshots) != 0 || !rec.closed {
				t.Fatalf("expected /chart to send its setup and close without forecast points, got %+v", rec)
			}
		})
	}
}

func TestDashboardForecastRunsFromTheForecastStartThroughTheLastSpecialDate(t *testing.T) {
	svc, timers := newForecastTestService(t, nil)
	ctx := t.Context()
	savings := mustAccountType(t, svc, "Savings", "#00ff00")
	stale := mustAccountType(t, svc, "Stale", "#0000ff")
	mustSnapshot(t, svc, mustTypedAccount(t, svc, "Savings", savings.ID).ID, mustParseDate("2021-01-10"), 1000)
	mustSnapshot(t, svc, mustTypedAccount(t, svc, "Stale", stale.ID).ID, mustParseDate("2020-06-01"), 500)
	if err := svc.SetForecastSnapshotInterval(ctx, "*-*-15"); err != nil {
		t.Fatalf("set forecast interval: %v", err)
	}
	// Both Special Dates are before today, but after the Forecast Start.
	for _, d := range []string{"2021-03-01", "2021-06-15"} {
		if _, err := svc.UpsertSpecialDate(ctx, model.SpecialDateInput{Name: d, Date: mustParseDate(d)}); err != nil {
			t.Fatalf("create special date: %v", err)
		}
	}
	startForecast(svc)
	timers.fire()

	var want []model.ForecastCacheRow
	for _, d := range []string{"2021-01-15", "2021-02-15", "2021-03-15", "2021-04-15", "2021-05-15", "2021-06-15"} {
		want = append(want,
			model.ForecastCacheRow{AccountTypeID: savings.ID, BalancePoint: model.BalancePoint{Date: mustParseDate(d), Balance: 1000}},
			model.ForecastCacheRow{AccountTypeID: stale.ID, BalancePoint: model.BalancePoint{Date: mustParseDate(d), Balance: 500}},
		)
	}
	sub := subscribe(t, svc)
	if sub.Status.State != model.ForecastStateDone {
		t.Fatalf("expected the Forecast Run to be Done, got %+v", sub.Status)
	}
	expectSameRows(t, sub.Rows, want)
}

func TestDashboardForecastIsEmptyWhenTheLastSpecialDateIsOnOrBeforeTheForecastStart(t *testing.T) {
	for name, offset := range map[string]date.Duration{"before": -20 * date.Day, "on": 0} {
		t.Run(name, func(t *testing.T) {
			svc, timers := newForecastTestService(t, nil)
			ctx := t.Context()
			// The Forecast Start is after today, and so is the Special Date.
			start := date.Today().Add(100 * date.Day)
			mustSnapshot(t, svc, mustAccount(t, svc, "Checking").ID, start, 1000)
			if err := svc.SetForecastSnapshotInterval(ctx, "*-*-*"); err != nil {
				t.Fatalf("set forecast interval: %v", err)
			}
			if _, err := svc.UpsertSpecialDate(ctx, model.SpecialDateInput{Name: "Retirement", Date: start.Add(offset)}); err != nil {
				t.Fatalf("create special date: %v", err)
			}
			startForecast(svc)

			sub := subscribe(t, svc)
			timers.fire()

			expectEventTypes(t, received(sub.Events), model.ForecastEventStarted, model.ForecastEventDone)
			if rows := subscribe(t, svc).Rows; len(rows) != 0 {
				t.Fatalf("expected the Dashboard Forecast to be empty, got %v", rows)
			}
		})
	}
}

func TestForecastShowsATransferIntoAnAccountWithoutASnapshot(t *testing.T) {
	svc := newTestService(t)
	checkingType := mustAccountType(t, svc, "Checking", "#ff0000")
	savingsType := mustAccountType(t, svc, "Savings", "#00ff00")
	checking := mustTypedAccount(t, svc, "Checking", checkingType.ID)
	savings := mustTypedAccount(t, svc, "New savings", savingsType.ID)
	mustSnapshot(t, svc, checking.ID, mustParseDate("2021-01-10"), 1000)
	mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Save", FromAccountID: checking.ID, ToAccountID: savings.ID, AmountType: "fixed", AmountFixed: newFixedValue(500),
		Recurrence: "*-01-20", StartDate: mustParseDate("2021-01-01"), Enabled: true,
	})

	rec := forecastWith(t, svc, model.PredictionParams{Duration: 30 * date.Day, Samples: 1, SnapshotInterval: "*-*-28", GroupBy: model.GroupByType})

	expectPoints(t, rec.pointsOf(savingsType.ID), point(savingsType.ID, "2021-01-28", 500))
	expectPoints(t, rec.pointsOf(checkingType.ID), point(checkingType.ID, "2021-01-28", 500))
}

func TestForecastValuesAStartupShareAccountWithoutInvestmentRoundsAtZero(t *testing.T) {
	svc := newTestService(t)
	equity := mustAccountType(t, svc, "Equity", "#ff00ff")
	startup := mustTypedAccount(t, svc, "Startup", equity.ID)
	mustStartupShareAccount(t, svc, startup.ID, nil, []model.ShareChangeInput{{Date: mustParseDate("2021-01-01"), DeltaShares: 10, TotalPrice: 100}})
	mustSnapshot(t, svc, mustAccount(t, svc, "Checking").ID, mustParseDate("2021-01-10"), 1000)

	rec := forecastWith(t, svc, model.PredictionParams{Duration: 60 * date.Day, Samples: 100, Quantile: 0.8, SnapshotInterval: "*-*-28", GroupBy: model.GroupByType})

	expectPoints(t, rec.pointsOf(equity.ID), point(equity.ID, "2021-01-28", 0), point(equity.ID, "2021-02-28", 0))
}

func TestForecastStartsAStartupShareAccountAtItsDerivedValue(t *testing.T) {
	svc := newTestService(t)
	equity := mustAccountType(t, svc, "Equity", "#ff00ff")
	startup := mustTypedAccount(t, svc, "Startup", equity.ID)
	mustStartupShareAccount(t, svc, startup.ID,
		[]model.InvestmentRoundInput{
			{Date: mustParseDate("2021-01-01"), Valuation: 1e6, PreMoneyShares: 1000},
			{Date: mustParseDate("2021-04-01"), Valuation: 2e6, PreMoneyShares: 1000},
		},
		[]model.ShareChangeInput{
			{Date: mustParseDate("2021-02-10"), DeltaShares: 10, TotalPrice: 100},
			{Date: mustParseDate("2021-03-05"), DeltaShares: 10, TotalPrice: 100},
		},
	)
	mustSnapshot(t, svc, mustAccount(t, svc, "Checking").ID, mustParseDate("2021-02-10"), 1000)

	rec := forecastWith(t, svc, model.PredictionParams{Duration: 70 * date.Day, Samples: 1, SnapshotInterval: "*-*-10", GroupBy: model.GroupByType})

	// The share change on the Forecast Start counts once: 10 of 1000 shares at 1M, then 20 shares, then
	// 20 shares at 2M from the later investment round.
	expectPoints(t, rec.pointsOf(equity.ID),
		point(equity.ID, "2021-02-10", 10000),
		point(equity.ID, "2021-03-10", 20000),
		point(equity.ID, "2021-04-10", 40000),
	)
}

func TestForecastIgnoresSnapshotsOnAStartupShareAccount(t *testing.T) {
	svc := newTestService(t)
	equity := mustAccountType(t, svc, "Equity", "#ff00ff")
	startup := mustTypedAccount(t, svc, "Startup", equity.ID)
	mustStartupShareAccount(t, svc, startup.ID,
		[]model.InvestmentRoundInput{{Date: mustParseDate("2021-01-01"), Valuation: 1e6, PreMoneyShares: 1000}},
		[]model.ShareChangeInput{{Date: mustParseDate("2021-01-01"), DeltaShares: 10, TotalPrice: 100}},
	)
	mustSnapshot(t, svc, mustAccount(t, svc, "Checking").ID, mustParseDate("2021-02-10"), 1000)
	params := model.PredictionParams{Duration: 30 * date.Day, Samples: 1, SnapshotInterval: "*-*-28", GroupBy: model.GroupByType}
	before := forecastWith(t, svc, params).snapshots

	// One Snapshot would be its Starting Balance, the other would move the Forecast Start.
	mustSnapshot(t, svc, startup.ID, mustParseDate("2021-02-01"), 777)
	mustSnapshot(t, svc, startup.ID, mustParseDate("2021-03-01"), 999)
	after := forecastWith(t, svc, params).snapshots

	expectPoints(t, before, point(equity.ID, "2021-02-28", 10000), point("", "2021-02-28", 1000))
	expectPoints(t, after, before...)
}

func TestForecastExercisesEveryOptionEndingOnADay(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	equity := mustAccountType(t, svc, "Equity", "#ff00ff")
	startup := mustTypedAccount(t, svc, "Startup", equity.ID)
	checking := mustAccount(t, svc, "Checking")
	mustStartupShareAccount(t, svc, startup.ID,
		[]model.InvestmentRoundInput{{Date: mustParseDate("2021-01-01"), Valuation: 1e6, PreMoneyShares: 1000}},
		[]model.ShareChangeInput{{Date: mustParseDate("2021-01-01"), DeltaShares: 10, TotalPrice: 100}},
	)
	for _, shares := range []float64{10, 20} {
		if _, err := svc.UpsertStartupShareOption(ctx, model.StartupShareOptionInput{
			AccountID: startup.ID, SourceAccountID: checking.ID, Shares: shares, StrikePricePerShare: 10,
			GrantDate: mustParseDate("2020-01-01"), EndDate: mustParseDate("2021-03-01"),
		}); err != nil {
			t.Fatalf("create startup share option: %v", err)
		}
	}
	mustSnapshot(t, svc, checking.ID, mustParseDate("2021-02-10"), 100_000)

	rec := forecastWith(t, svc, model.PredictionParams{Duration: 30 * date.Day, Samples: 1, SnapshotInterval: "*-*-10", GroupBy: model.GroupByNone})

	// Both options are exercised on their End Date: 30 more shares, paid 300 from checking.
	points := rec.pointsOf(startup.ID)
	if want := 1_000_300 * (40.0 / 1_030); len(points) != 2 || math.Abs(points[1].Balance-want) > 1e-6 {
		t.Fatalf("expected the startup to be worth %f on 2021-03-10, got %+v", want, points)
	}
	expectPoints(t, rec.pointsOf(checking.ID), point(checking.ID, "2021-02-10", 100_000), point(checking.ID, "2021-03-10", 99_700))
}
