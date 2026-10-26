package finance_test

import (
	"math"
	"slices"
	"testing"

	"github.com/SimonSchneider/goslu/date"
	finance2 "github.com/SimonSchneider/pefigo/pkg/finance"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

// reportRecorderFunc is a ReportRecorder.
type reportRecorderFunc func(day date.Date, balances map[string]uncertain.Value) error

func (f reportRecorderFunc) OnReport(day date.Date, balances map[string]uncertain.Value) error {
	return f(day, balances)
}

type report struct {
	day      date.Date
	balances map[string]uncertain.Value
}

func parseDay(s string) date.Date {
	return Must(date.ParseDate(s))
}

func near(got, want float64) bool {
	return math.Abs(got-want) <= 1e-9*math.Max(1, math.Abs(want))
}

// runForecast runs a Forecast from start up to, but not on, end and returns its reports in order.
func runForecast(t *testing.T, start, end string, interval date.Cron, entities []finance2.Entity, transfers ...finance2.TransferTemplate) []report {
	t.Helper()
	var reports []report
	recorder := reportRecorderFunc(func(day date.Date, balances map[string]uncertain.Value) error {
		reports = append(reports, report{day: day, balances: balances})
		return nil
	})
	err := finance2.RunForecast(t.Context(), uncertain.NewConfig(1, 1), parseDay(start), parseDay(end), interval,
		entities, transfers, finance2.CompositeRecorder{ReportRecorder: recorder})
	if err != nil {
		t.Fatalf("run forecast: %v", err)
	}
	return reports
}

func reportDays(reports []report) []string {
	days := make([]string, 0, len(reports))
	for _, r := range reports {
		days = append(days, r.day.String())
	}
	return days
}

func TestRunForecastReportsEveryEntityOnEveryReportFromTheForecastStart(t *testing.T) {
	stale := newAccount("Stale", withBalance(parseDay("2023-06-01"), uncertain.NewFixed(1_000)))
	fresh := newAccount("Fresh", withBalance(parseDay("2026-10-24"), uncertain.NewFixed(100_000)))

	reports := runForecast(t, "2026-10-24", "2030-01-02", "*-01-01", mks(*stale, *fresh))

	if want := []string{"2027-01-01", "2028-01-01", "2029-01-01", "2030-01-01"}; !slices.Equal(reportDays(reports), want) {
		t.Fatalf("expected reports on %v, got %v", want, reportDays(reports))
	}
	for _, r := range reports {
		if len(r.balances) != 2 {
			t.Errorf("report on %s holds %d entities, expected 2", r.day, len(r.balances))
		}
		for _, id := range []string{stale.ID, fresh.ID} {
			if _, ok := r.balances[id]; !ok {
				t.Errorf("report on %s is missing %s", r.day, id)
			}
		}
	}
	if got := reports[0].balances[stale.ID].Mean(); got != 1_000 {
		t.Errorf("expected the stale entity to start at its Snapshot's 1000, got %f", got)
	}
	if got := reports[0].balances[fresh.ID].Mean(); got != 100_000 {
		t.Errorf("expected the fresh entity to start at its Snapshot's 100000, got %f", got)
	}
}

func TestRunForecastChargesTheForecastStartsTransfersBeforeItsFirstReport(t *testing.T) {
	acc := newAccount("Checking", withBalance(parseDay("2026-10-24"), uncertain.NewFixed(1_000)))
	deposit := newTransfer("", acc.ID, 1, "*-*-24", withFixed(uncertain.NewFixed(500)))

	reports := runForecast(t, "2026-10-24", "2026-12-01", "*-*-24", mks(*acc), deposit)

	if want := []string{"2026-10-24", "2026-11-24"}; !slices.Equal(reportDays(reports), want) {
		t.Fatalf("expected reports on %v, got %v", want, reportDays(reports))
	}
	for i, want := range []float64{1_500, 2_000} {
		if got := reports[i].balances[acc.ID].Mean(); got != want {
			t.Errorf("expected %f on %s, got %f", want, reports[i].day, got)
		}
	}
}

func TestRunForecastStartsAnEntityWithoutASnapshotAtZero(t *testing.T) {
	checking := newAccount("Checking", withBalance(parseDay("2026-10-24"), uncertain.NewFixed(1_000)))
	savings := newAccount("Savings")
	deposit := newTransfer("", savings.ID, 1, "*-11-15", withFixed(uncertain.NewFixed(500)))

	reports := runForecast(t, "2026-10-24", "2026-12-02", "*-*-01", mks(*checking, *savings), deposit)

	if want := []string{"2026-11-01", "2026-12-01"}; !slices.Equal(reportDays(reports), want) {
		t.Fatalf("expected reports on %v, got %v", want, reportDays(reports))
	}
	for i, want := range []float64{0, 500} {
		got, ok := reports[i].balances[savings.ID]
		if !ok {
			t.Errorf("report on %s is missing the entity without a Snapshot", reports[i].day)
		} else if got.Mean() != want {
			t.Errorf("expected %f on %s, got %f", want, reports[i].day, got.Mean())
		}
	}
}

func TestRunForecastPaysInterestIntoAnEntityWithoutASnapshot(t *testing.T) {
	payout := newAccount("Payout")
	savings := newAccount("Savings",
		withBalance(parseDay("2026-01-01"), uncertain.NewFixed(10_000)),
		withInterest(uncertain.NewFixed(0.10), "*-12-31", payout.ID),
	)

	// The Forecast starts on the payout day, so only that day's interest is paid out.
	reports := runForecast(t, "2026-12-31", "2027-01-02", "*-01-01", mks(*savings, *payout))

	if want := []string{"2027-01-01"}; !slices.Equal(reportDays(reports), want) {
		t.Fatalf("expected reports on %v, got %v", want, reportDays(reports))
	}
	want := 10_000 * (math.Pow(1.10, 1.0/365) - 1)
	if got := reports[0].balances[payout.ID].Mean(); !near(got, want) {
		t.Errorf("expected the interest %f in the entity without a Snapshot, got %f", want, got)
	}
	if got := reports[0].balances[savings.ID].Mean(); got != 10_000 {
		t.Errorf("expected the savings to keep 10000, got %f", got)
	}
}

func TestRunForecastIgnoresSnapshotsAfterTheForecastStart(t *testing.T) {
	recorded := newAccount("Recorded",
		withBalance(parseDay("2026-01-01"), uncertain.NewFixed(1_000)),
		withBalance(parseDay("2026-12-01"), uncertain.NewFixed(5_000)),
	)
	future := newAccount("Future", withBalance(parseDay("2026-12-01"), uncertain.NewFixed(7_000)))

	reports := runForecast(t, "2026-06-01", "2027-01-02", "*-01-01", mks(*recorded, *future))

	if want := []string{"2027-01-01"}; !slices.Equal(reportDays(reports), want) {
		t.Fatalf("expected reports on %v, got %v", want, reportDays(reports))
	}
	if got := reports[0].balances[recorded.ID].Mean(); got != 1_000 {
		t.Errorf("expected the Snapshot on or before the Forecast Start, 1000, got %f", got)
	}
	if got := reports[0].balances[future.ID].Mean(); got != 0 {
		t.Errorf("expected 0 for an entity with only a later Snapshot, got %f", got)
	}
}

func TestRunForecastReportsNothingWhenTheEndIsNotAfterTheStart(t *testing.T) {
	acc := newAccount("Checking", withBalance(parseDay("2026-10-24"), uncertain.NewFixed(1_000)))

	for _, end := range []string{"2026-10-24", "2026-10-01"} {
		if reports := runForecast(t, "2026-10-24", end, "*-*-*", mks(*acc)); len(reports) != 0 {
			t.Errorf("end %s: expected no reports, got %v", end, reportDays(reports))
		}
	}
}

func TestStartupGrowthWithoutAnInvestmentRoundIsZero(t *testing.T) {
	growth := &finance2.StartupGrowth{
		TotalShares:           uncertain.NewFixed(0),
		OwnedShares:           uncertain.NewFixed(100),
		Valuation:             uncertain.NewFixed(0),
		TaxRate:               uncertain.NewFixed(0.3),
		DiscountFactor:        uncertain.NewFixed(1),
		PurchasePricePerShare: uncertain.NewFixed(0),
	}

	if got := growth.Balance(uncertain.NewConfig(1, 1)); !got.IsFixed() || got.Mean() != 0 {
		t.Errorf("expected a fixed 0, got %s", got)
	}
}

func TestRunForecastValuesAStartupShareAccountFromItsFirstInvestmentRoundWithSharesAndAPricePerShare(t *testing.T) {
	startup := newAccount("Startup", func(e *finance2.Entity) {
		e.GrowthModel = &finance2.StartupGrowth{
			TotalShares:           uncertain.NewFixed(0),
			OwnedShares:           uncertain.NewFixed(100),
			Valuation:             uncertain.NewFixed(0),
			TaxRate:               uncertain.NewFixed(0),
			DiscountFactor:        uncertain.NewFixed(1),
			PurchasePricePerShare: uncertain.NewFixed(0),
			InvestmentRounds: map[date.Date]finance2.StartupGrowthInvestmentRound{
				// Without pre-money shares.
				parseDay("2027-02-01"): {PreMoneyValuation: uncertain.NewFixed(1_000_000), PreMoneyShares: uncertain.NewFixed(0), Investment: uncertain.NewFixed(100_000)},
				// 1,100 shares after the round, at 1,000 each.
				parseDay("2027-03-01"): {PreMoneyValuation: uncertain.NewFixed(1_000_000), PreMoneyShares: uncertain.NewFixed(1_000), Investment: uncertain.NewFixed(100_000)},
				// Without a price per share.
				parseDay("2027-04-01"): {PreMoneyValuation: uncertain.NewFixed(0), PreMoneyShares: uncertain.NewFixed(1_000), Investment: uncertain.NewFixed(0)},
			},
		}
	})

	reports := runForecast(t, "2027-01-01", "2027-05-02", "*-*-01", mks(*startup))

	want := map[string]float64{
		"2027-01-01": 0,
		"2027-02-01": 0,
		"2027-03-01": 100_000,
		"2027-04-01": 100_000,
		"2027-05-01": 100_000,
	}
	if len(reports) != len(want) {
		t.Fatalf("expected %d reports, got %v", len(want), reportDays(reports))
	}
	for _, r := range reports {
		if got := r.balances[startup.ID].Mean(); !near(got, want[r.day.String()]) {
			t.Errorf("expected %f on %s, got %f", want[r.day.String()], r.day, got)
		}
	}
}

func TestRunForecastAppliesEveryShareChangeOnADay(t *testing.T) {
	startup := newAccount("Startup", func(e *finance2.Entity) {
		e.GrowthModel = &finance2.StartupGrowth{
			TotalShares:           uncertain.NewFixed(1_000),
			OwnedShares:           uncertain.NewFixed(0),
			Valuation:             uncertain.NewFixed(1_000_000),
			TaxRate:               uncertain.NewFixed(0.5),
			DiscountFactor:        uncertain.NewFixed(1),
			PurchasePricePerShare: uncertain.NewFixed(0),
			ShareChanges: map[date.Date][]finance2.StartupGrowthShareChange{
				parseDay("2027-02-01"): {
					{DeltaShares: uncertain.NewFixed(10), TotalPrice: uncertain.NewFixed(100)},
					{DeltaShares: uncertain.NewFixed(20), TotalPrice: uncertain.NewFixed(400)},
				},
			},
		}
	})

	reports := runForecast(t, "2027-01-01", "2027-02-02", "*-02-01", mks(*startup))

	// 30 of 1,000 shares at 1M are worth 30,000, and half the gain over the 500 paid for them is tax.
	if want := 30_000 - (30_000-500)*0.5; len(reports) != 1 || !near(reports[0].balances[startup.ID].Mean(), want) {
		t.Fatalf("expected %f on 2027-02-01, got %v", want, reports)
	}
}

func TestRunForecastExercisesEveryOptionEndingOnADay(t *testing.T) {
	checking := newAccount("Checking", withBalance(parseDay("2027-01-01"), uncertain.NewFixed(100_000)))
	startup := newAccount("Startup", func(e *finance2.Entity) {
		e.GrowthModel = &finance2.StartupGrowth{
			TotalShares:           uncertain.NewFixed(1_000),
			OwnedShares:           uncertain.NewFixed(0),
			Valuation:             uncertain.NewFixed(1_000_000),
			TaxRate:               uncertain.NewFixed(0),
			DiscountFactor:        uncertain.NewFixed(1),
			PurchasePricePerShare: uncertain.NewFixed(0),
			Options: map[date.Date][]finance2.StartupGrowthOption{
				parseDay("2027-02-01"): {
					{StrikePricePerShare: uncertain.NewFixed(10), NumShares: uncertain.NewFixed(10), SourceAccountID: checking.ID},
					{StrikePricePerShare: uncertain.NewFixed(10), NumShares: uncertain.NewFixed(20), SourceAccountID: checking.ID},
				},
			},
		}
	})

	reports := runForecast(t, "2027-01-01", "2027-02-02", "*-02-01", mks(*checking, *startup))

	if len(reports) != 1 {
		t.Fatalf("expected one report, got %v", reportDays(reports))
	}
	// Both options are exercised: 30 more shares for 300.
	if want := 30.0 / 1_030 * 1_000_300; !near(reports[0].balances[startup.ID].Mean(), want) {
		t.Errorf("expected the startup to be worth %f, got %f", want, reports[0].balances[startup.ID].Mean())
	}
	if got := reports[0].balances[checking.ID].Mean(); got != 99_700 {
		t.Errorf("expected the strike price 300 to be paid from checking, got a balance of %f", got)
	}
}
