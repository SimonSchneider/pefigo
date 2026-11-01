package model_test

import (
	"fmt"
	"maps"
	"slices"
	"testing"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
)

// mustPlansWithoutEvents stores a Checking account, Savings and Startup Co accounts with Snapshots on
// 2027-01-01, a Gross Salary paid into Checking, a Gym Bill charged from it and Startup Co's shares,
// and returns the IDs Events change.
func mustPlansWithoutEvents(t *testing.T, svc *model.Service) (checking, salary, gym, startup string) {
	t.Helper()
	start := mustParseDate("2027-01-01")
	acc := mustAccount(t, svc, "Checking")
	mustSnapshot(t, svc, acc.ID, start, 100000)
	sal := mustSalary(t, svc, grossSalaryFrom(t, svc, "Salary A", acc.ID, ""), model.SalaryAmount{Amount: newFixedValue(50000), StartDate: start})
	bill := mustBillIn(t, svc, mustBillAccount(t, svc, "Household", acc.ID), "Gym", model.BillAmount{Amount: newFixedValue(300), StartDate: start})
	st := mustAccount(t, svc, "Startup Co")
	mustStartupShareAccount(t, svc, st.ID,
		[]model.InvestmentRoundInput{{Date: start, Valuation: 1e6, PreMoneyShares: 10000}},
		[]model.ShareChangeInput{{Date: start, DeltaShares: 1000, TotalPrice: 1000}},
	)
	return acc.ID, sal.ID, bill.ID, st.ID
}

// mustUnplannedEventsOfEveryKind adds an unplanned Event of each kind of change to Salaries, Bills and
// Startup Share accounts to the plans of mustPlansWithoutEvents.
func mustUnplannedEventsOfEveryKind(t *testing.T, svc *model.Service, checking, salary, gym, startup string) {
	t.Helper()
	ctx := t.Context()
	unplanned := func(name, day string) string {
		return mustEvent(t, svc, model.EventInput{Name: name, Date: mustParseDate(day), Planned: ptr(false)}).ID
	}
	child := unplanned("Second child", "2027-03-01")
	mustFullParentalLeave(t, svc, model.FullParentalLeave{SalaryID: salary, EventID: child, StartOffset: days(0), EndOffset: days(180), SjukDaysPerWeek: 7})
	if _, err := svc.UpsertPartialParentalLeave(ctx, model.PartialParentalLeave{SalaryID: salary, EventID: child, StartOffset: days(180), EndOffset: days(360), SjukDaysPerYear: 40}); err != nil {
		t.Fatalf("save partial parental leave: %v", err)
	}
	if _, err := svc.UpsertSalaryAdjustment(ctx, model.SalaryAdjustment{SalaryID: salary, EventID: child, ValidFromOffset: days(0), VABDaysPerYear: 20}); err != nil {
		t.Fatalf("save salary adjustment: %v", err)
	}

	raise := unplanned("Raise", "2027-04-01")
	if _, err := svc.UpsertSalaryAmount(ctx, model.SalaryAmount{SalaryID: salary, EventID: raise, StartOffset: days(0), Amount: newFixedValue(60000)}); err != nil {
		t.Fatalf("save salary amount: %v", err)
	}

	newJob := unplanned("New job", "2027-05-01")
	mustSalaryEnd(t, svc, model.SalaryEnd{SalaryID: salary, EventID: newJob, DateOffset: days(0)})
	mustSalary(t, svc, grossSalaryFrom(t, svc, "Salary B", checking, newJob), model.SalaryAmount{Amount: newFixedValue(80000), EventID: newJob, StartOffset: days(0)})

	move := unplanned("Move", "2027-06-01")
	mustBillEnd(t, svc, model.BillEnd{BillID: gym, EventID: move, DateOffset: days(0)})
	if _, err := svc.UpsertBillAmount(ctx, model.BillAmount{BillID: gym, EventID: move, StartOffset: days(-30), Amount: newFixedValue(500)}); err != nil {
		t.Fatalf("save bill amount: %v", err)
	}
	household, err := svc.GetBill(ctx, gym)
	if err != nil {
		t.Fatalf("get bill: %v", err)
	}
	rent, err := svc.UpsertBill(ctx, model.Bill{Name: "Rent", BillAccountID: household.BillAccountID, Enabled: true, EventID: move})
	if err != nil {
		t.Fatalf("save bill: %v", err)
	}
	if _, err := svc.UpsertBillAmount(ctx, model.BillAmount{BillID: rent.ID, EventID: move, StartOffset: days(0), Amount: newFixedValue(9000)}); err != nil {
		t.Fatalf("save bill amount: %v", err)
	}

	seriesA := unplanned("Series A", "2027-07-01")
	mustInvestmentRound(t, svc, model.InvestmentRoundInput{AccountID: startup, EventID: seriesA, DateOffset: days(0), Valuation: 4e6, PreMoneyShares: 10000, Investment: 1e6})
	if _, err := svc.UpsertShareChange(ctx, model.ShareChangeInput{AccountID: startup, EventID: seriesA, DateOffset: days(0), DeltaShares: 500, TotalPrice: 1000}); err != nil {
		t.Fatalf("save share change: %v", err)
	}
}

// baselineByName is every point of the Baseline's Forecast through 2028-06-30, by account name and
// day, and what its Transfer Schedule charges on every day of it, by Transfer name.
func baselineByName(t *testing.T, svc *model.Service) (map[string]float64, map[string]float64) {
	t.Helper()
	rec := forecastThrough(t, svc, "2028-06-30", nil)
	names := map[string]string{}
	for _, e := range rec.setup.Entities {
		names[e.ID] = e.Name
	}
	points := map[string]float64{}
	for _, s := range rec.snapshots {
		points[fmt.Sprintf("%s %s", names[s.ID], s.Day)] = s.Balance
	}
	charged := map[string]float64{}
	schedule := mustTransferSchedule(t, svc)
	for day := range date.Iter(mustParseDate("2027-01-01"), mustParseDate("2028-06-30"), date.Day) {
		for _, c := range schedule.On(day).Charged() {
			charged[fmt.Sprintf("%s %s", c.Name, day)] += c.Amount
		}
	}
	return points, charged
}

func TestUnplannedEventsOfSalariesBillsAndRoundsLeaveTheBaselineAsWithoutThem(t *testing.T) {
	without := newSalaryService(t)
	mustPlansWithoutEvents(t, without)
	with := newSalaryService(t)
	checking, salary, gym, startup := mustPlansWithoutEvents(t, with)
	mustUnplannedEventsOfEveryKind(t, with, checking, salary, gym, startup)

	wantPoints, wantCharged := baselineByName(t, without)
	gotPoints, gotCharged := baselineByName(t, with)

	if len(wantPoints) == 0 || len(wantCharged) == 0 {
		t.Fatalf("expected a Forecast and charges to compare, got %d points and %d charges", len(wantPoints), len(wantCharged))
	}
	if !maps.Equal(gotPoints, wantPoints) {
		t.Errorf("expected the same Forecast as without the Events, got differences on %v", differingKeys(wantPoints, gotPoints))
	}
	if !maps.Equal(gotCharged, wantCharged) {
		t.Errorf("expected the same charges as without the Events, got differences on %v", differingKeys(wantCharged, gotCharged))
	}
}

func differingKeys(want, got map[string]float64) []string {
	var keys []string
	for k := range maps.Keys(want) {
		if g, ok := got[k]; !ok || g != want[k] {
			keys = append(keys, k)
		}
	}
	for k := range maps.Keys(got) {
		if _, ok := want[k]; !ok {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	if len(keys) > 10 {
		keys = keys[:10]
	}
	return keys
}

func TestPlannedEventsOfSalariesBillsAndRoundsChangeTheBaseline(t *testing.T) {
	without := newSalaryService(t)
	mustPlansWithoutEvents(t, without)
	with := newSalaryService(t)
	checking, salary, gym, startup := mustPlansWithoutEvents(t, with)
	mustUnplannedEventsOfEveryKind(t, with, checking, salary, gym, startup)
	events, err := with.ListEvents(t.Context())
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	for _, ev := range events {
		moveEvent(t, with, model.Event{ID: ev.ID, Name: ev.Name, Date: ev.Date, Planned: true}, ev.Date.String())
	}

	wantPoints, _ := baselineByName(t, without)
	gotPoints, _ := baselineByName(t, with)
	for _, name := range []string{"Checking 2027-12-28", "Startup Co 2027-12-28"} {
		if gotPoints[name] == wantPoints[name] {
			t.Errorf("expected the planned Events to change %s from %v", name, wantPoints[name])
		}
	}
}
