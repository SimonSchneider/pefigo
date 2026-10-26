package model_test

import (
	"testing"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
)

// payday is a monthly cycle of Transfers across the 25th to the 27th, with Snapshots recorded on
// 2025-03-25 after that day's Transfers: Checking at 0, and Savings already holding the month's saving.
type payday struct {
	checking, savings model.AccountType
}

func mustPayday(t *testing.T, svc *model.Service) payday {
	t.Helper()
	p := payday{checking: mustAccountType(t, svc, "Checking", "#ff0000"), savings: mustAccountType(t, svc, "Savings", "#00ff00")}
	checking := mustTypedAccount(t, svc, "Checking", p.checking.ID)
	savings := mustTypedAccount(t, svc, "Savings", p.savings.ID)
	mustSnapshot(t, svc, checking.ID, mustParseDate("2025-03-25"), 0)
	mustSnapshot(t, svc, savings.ID, mustParseDate("2025-03-25"), 10_000)
	for _, tt := range []model.TransferTemplate{
		{Name: "Salary", ToAccountID: checking.ID, AmountFixed: newFixedValue(30_000), Recurrence: "*-*-25"},
		{Name: "Saving", FromAccountID: checking.ID, ToAccountID: savings.ID, AmountFixed: newFixedValue(5_000), Recurrence: "*-*-26"},
		{Name: "Spending", FromAccountID: checking.ID, AmountFixed: newFixedValue(25_000), Recurrence: "*-*-27"},
	} {
		tt.AmountType, tt.Priority, tt.StartDate, tt.Enabled = "fixed", 1, mustParseDate("2025-01-01"), true
		mustTransferTemplate(t, svc, tt)
	}
	return p
}

func mustForecastStartDays(t *testing.T, svc *model.Service, days string) {
	t.Helper()
	if err := svc.SetForecastStartDays(t.Context(), days); err != nil {
		t.Fatalf("set forecast start days: %v", err)
	}
}

// threeDays is a Forecast of three days from the Forecast Start, reported daily, by Account Type.
var threeDays = model.PredictionParams{Duration: 3 * date.Day, Samples: 1, SnapshotInterval: "*-*-*", GroupBy: model.GroupByType}

func TestForecastStartsOnTheSnapshotsDayByDefault(t *testing.T) {
	svc := newTestService(t)
	p := mustPayday(t, svc)
	if days, err := svc.GetForecastStartDays(t.Context()); err != nil || days != "*-*-*" {
		t.Fatalf("expected the Forecast Start Days to be every day by default, got %q (%v)", days, err)
	}

	rec := forecastWith(t, svc, threeDays)

	// The Snapshot's day's Transfers are charged, so the cycle counts again.
	expectPoints(t, rec.snapshots,
		point(p.checking.ID, "2025-03-25", 30_000), point(p.savings.ID, "2025-03-25", 10_000),
		point(p.checking.ID, "2025-03-26", 25_000), point(p.savings.ID, "2025-03-26", 15_000),
		point(p.checking.ID, "2025-03-27", 0), point(p.savings.ID, "2025-03-27", 15_000),
	)
}

func TestForecastStartsOnTheFirstForecastStartDayOnOrAfterTheLatestSnapshot(t *testing.T) {
	svc := newTestService(t)
	p := mustPayday(t, svc)
	mustForecastStartDays(t, svc, "*-*-28")

	rec := forecastWith(t, svc, threeDays)

	// The 25th to the 27th's Transfers are taken to be in the Snapshots, which start as recorded.
	expectPoints(t, rec.snapshots,
		point(p.checking.ID, "2025-03-28", 0), point(p.savings.ID, "2025-03-28", 10_000),
		point(p.checking.ID, "2025-03-29", 0), point(p.savings.ID, "2025-03-29", 10_000),
		point(p.checking.ID, "2025-03-30", 0), point(p.savings.ID, "2025-03-30", 10_000),
	)
}

func TestDashboardForecastStartsOnTheFirstForecastStartDayOnOrAfterTheLatestSnapshot(t *testing.T) {
	svc, timers := newForecastTestService(t, nil)
	ctx := t.Context()
	p := mustPayday(t, svc)
	mustForecastStartDays(t, svc, "*-*-28")
	if err := svc.SetForecastSnapshotInterval(ctx, "*-*-*"); err != nil {
		t.Fatalf("set forecast interval: %v", err)
	}
	if _, err := svc.UpsertSpecialDate(ctx, model.SpecialDateInput{Name: "Retirement", Date: mustParseDate("2025-04-28")}); err != nil {
		t.Fatalf("create special date: %v", err)
	}
	startForecast(svc)
	timers.fire()

	rows := subscribe(t, svc).Rows
	if first := sortedRows(rows); len(first) == 0 || first[0].Date != mustParseDate("2025-03-28") {
		t.Fatalf("expected the first row on 2025-03-28, got %v", first)
	}
	// The March cycle is in the Snapshots, and April's is charged.
	want := map[string]map[string]float64{
		"2025-03-28": {p.checking.ID: 0, p.savings.ID: 10_000},
		"2025-04-24": {p.checking.ID: 0, p.savings.ID: 10_000},
		"2025-04-28": {p.checking.ID: 0, p.savings.ID: 15_000},
	}
	for _, row := range rows {
		if balances, ok := want[row.Date.String()]; ok && balances[row.AccountTypeID] != row.Balance {
			t.Errorf("expected %f for %s on %s, got %f", balances[row.AccountTypeID], row.AccountTypeID, row.Date, row.Balance)
		}
	}
}

func TestForecastStartsOnASnapshotOnAForecastStartDay(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	mustSnapshot(t, svc, checking.ID, mustParseDate("2025-03-28"), 1000)
	mustForecastStartDays(t, svc, "*-*-28")

	rec := forecastWith(t, svc, model.PredictionParams{Duration: date.Day, Samples: 1, SnapshotInterval: "*-*-*", GroupBy: model.GroupByNone})

	expectPoints(t, rec.snapshots, point(checking.ID, "2025-03-28", 1000))
}

func TestForecastStartsOnTheLatestSnapshotWithoutAForecastStartDayAfterIt(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	mustSnapshot(t, svc, checking.ID, mustParseDate("2025-03-25"), 1000)
	mustForecastStartDays(t, svc, "2020-*-*")

	rec := forecastWith(t, svc, model.PredictionParams{Duration: date.Day, Samples: 1, SnapshotInterval: "*-*-*", GroupBy: model.GroupByNone})

	expectPoints(t, rec.snapshots, point(checking.ID, "2025-03-25", 1000))
}

func TestDashboardForecastIsEmptyWhenTheLastSpecialDateIsBeforeTheFirstForecastStartDay(t *testing.T) {
	svc, timers := newForecastTestService(t, nil)
	ctx := t.Context()
	mustPayday(t, svc)
	mustForecastStartDays(t, svc, "*-*-28")
	if err := svc.SetForecastSnapshotInterval(ctx, "*-*-*"); err != nil {
		t.Fatalf("set forecast interval: %v", err)
	}
	if _, err := svc.UpsertSpecialDate(ctx, model.SpecialDateInput{Name: "Holiday", Date: mustParseDate("2025-03-26")}); err != nil {
		t.Fatalf("create special date: %v", err)
	}
	startForecast(svc)

	sub := subscribe(t, svc)
	timers.fire()

	expectEventTypes(t, received(sub.Events), model.ForecastEventStarted, model.ForecastEventDone)
	if rows := subscribe(t, svc).Rows; len(rows) != 0 {
		t.Fatalf("expected the Dashboard Forecast to be empty, got %v", rows)
	}
}

func TestWritingTheForecastStartDaysMarksTheDashboardForecastStale(t *testing.T) {
	svc, timers := newForecastTestService(t, nil)
	ctx := t.Context()
	p := mustPayday(t, svc)
	if err := svc.SetForecastSnapshotInterval(ctx, "*-*-28"); err != nil {
		t.Fatalf("set forecast interval: %v", err)
	}
	if _, err := svc.UpsertSpecialDate(ctx, model.SpecialDateInput{Name: "Retirement", Date: mustParseDate("2025-04-10")}); err != nil {
		t.Fatalf("create special date: %v", err)
	}
	startForecast(svc)
	timers.fire()
	savingsOn := func(rows []model.ForecastCacheRow) []float64 {
		var balances []float64
		for _, row := range rows {
			if row.AccountTypeID == p.savings.ID {
				balances = append(balances, row.Balance)
			}
		}
		return balances
	}
	if got := savingsOn(subscribe(t, svc).Rows); len(got) != 1 || got[0] != 15_000 {
		t.Fatalf("expected the default Forecast Start to charge the cycle again, got %v", got)
	}

	mustForecastStartDays(t, svc, "*-*-28")
	if n := timers.pending(); n != 1 {
		t.Fatalf("expected writing the Forecast Start Days to schedule one Forecast Run, got %d", n)
	}
	timers.fire()

	if got := savingsOn(subscribe(t, svc).Rows); len(got) != 1 || got[0] != 10_000 {
		t.Fatalf("expected the Forecast to start on the 28th with the Snapshots as recorded, got %v", got)
	}
}

func TestForecastStartDaysMustBeADatePattern(t *testing.T) {
	svc := newTestService(t)
	mustForecastStartDays(t, svc, "*-*-28")

	if err := svc.SetForecastStartDays(t.Context(), "28"); err == nil {
		t.Fatal("expected a Forecast Start Days pattern without a year, month and day to be rejected")
	}
	if days, err := svc.GetForecastStartDays(t.Context()); err != nil || days != "*-*-28" {
		t.Fatalf("expected the Forecast Start Days to stay *-*-28, got %q (%v)", days, err)
	}
}
