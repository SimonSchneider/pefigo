package model_test

import (
	"testing"

	"github.com/SimonSchneider/pefigo/internal/model"
)

func TestPlannedEventsBeforeTheForecastStartCountWhatTheyChangeFromTheStart(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	checking := mustAccount(t, svc, "Checking")
	mustSnapshot(t, svc, checking.ID, mustParseDate("2027-01-01"), 0)
	salary := mustSalary(t, svc, model.Salary{Name: "Salary A", ToAccountID: checking.ID, Recurrence: "*-*-25"},
		model.SalaryAmount{Amount: newFixedValue(50000), StartDate: mustParseDate("2026-01-01")})
	gym := mustBillIn(t, svc, mustBillAccount(t, svc, "Household", checking.ID), "Gym",
		model.BillAmount{Amount: newFixedValue(300), StartDate: mustParseDate("2026-01-01")})
	startup := mustAccount(t, svc, "Startup Co")
	mustStartupShareAccount(t, svc, startup.ID,
		[]model.InvestmentRoundInput{{Date: mustParseDate("2026-01-01"), Valuation: 1e6, PreMoneyShares: 10000}},
		[]model.ShareChangeInput{{Date: mustParseDate("2026-01-01"), DeltaShares: 1000, TotalPrice: 1000}},
	)
	// Before the Forecast Start, a Series A adds a round, a Raise a Salary Amount and a Move ends the Gym.
	seriesA := mustEvent(t, svc, model.EventInput{Name: "Series A", Date: mustParseDate("2026-10-01")})
	mustInvestmentRound(t, svc, model.InvestmentRoundInput{AccountID: startup.ID, EventID: seriesA.ID, DateOffset: days(0), Valuation: 4e6, PreMoneyShares: 10000, Investment: 1e6})
	raise := mustEvent(t, svc, model.EventInput{Name: "Raise", Date: mustParseDate("2026-11-01")})
	if _, err := svc.UpsertSalaryAmount(ctx, model.SalaryAmount{SalaryID: salary.ID, EventID: raise.ID, StartOffset: days(0), Amount: newFixedValue(60000)}); err != nil {
		t.Fatalf("save salary amount: %v", err)
	}
	move := mustEvent(t, svc, model.EventInput{Name: "Move", Date: mustParseDate("2026-12-01")})
	mustBillEnd(t, svc, model.BillEnd{BillID: gym.ID, EventID: move.ID, DateOffset: days(0)})

	rec := forecastThrough(t, svc, "2027-01-31", nil)

	// What they paid and charged before the Forecast Start is taken to be in the Snapshot. From it,
	// Salary A pays the Raise's 60 000, the Gym charges nothing and the Series A round values Startup Co,
	// as a growth model or Transfer Template an Event starts before the Forecast Start still applies.
	if got := balanceOn(t, rec, checking.ID, "2027-01-28"); got != 60000 {
		t.Errorf("expected Checking to hold January's 60 000 Salary and no Gym, got %v", got)
	}
	if got := balanceOn(t, rec, startup.ID, "2027-01-28"); !approxEqual(got, 400000, 0.01) {
		t.Errorf("expected the Series A round to value Startup Co at 400 000 from the Forecast Start, got %v", got)
	}
}
