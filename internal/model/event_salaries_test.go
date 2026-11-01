package model_test

import (
	"testing"

	"github.com/SimonSchneider/pefigo/internal/model"
)

// chargedOn is what each Transfer the Baseline charges on day moves, by name.
func chargedOn(t *testing.T, svc *model.Service, day string) map[string]float64 {
	t.Helper()
	charged := map[string]float64{}
	for _, c := range mustTransferSchedule(t, svc).On(mustParseDate(day)).Charged() {
		charged[c.Name] += c.Amount
	}
	return charged
}

// newSalaryService is a Service with Stockholm's Tax Tables from 2026 to 2028 and the income and price
// base amounts from 2026.
func newSalaryService(t *testing.T) *model.Service {
	t.Helper()
	svc := newTestService(t, withStockholmTaxTables("2026", "2027", "2028"))
	mustSweYearlyParams(t, svc, "2026-01-01", 80600, 58800)
	return svc
}

func mustFullParentalLeave(t *testing.T, svc *model.Service, leave model.FullParentalLeave) model.FullParentalLeave {
	t.Helper()
	saved, err := svc.UpsertFullParentalLeave(t.Context(), leave)
	if err != nil {
		t.Fatalf("save full parental leave: %v", err)
	}
	return saved
}

func mustSalaryEnd(t *testing.T, svc *model.Service, end model.SalaryEnd) model.SalaryEnd {
	t.Helper()
	saved, err := svc.UpsertSalaryEnd(t.Context(), end)
	if err != nil {
		t.Fatalf("save salary end: %v", err)
	}
	return saved
}

// grossSalaryFrom is a Gross Salary of 50 000 a month from 2027-01-01 into toAccountID.
func grossSalaryFrom(t *testing.T, svc *model.Service, name, toAccountID string, eventID string) model.Salary {
	t.Helper()
	sal := stockholmGrossSalary(name)
	sal.ToAccountID = toAccountID
	sal.EventID = eventID
	return sal
}

func TestAnEventAddsParentalLeaveToASalaryOutsideIt(t *testing.T) {
	svc := newSalaryService(t)
	checking := mustAccount(t, svc, "Checking")
	salaryA := mustSalary(t, svc, grossSalaryFrom(t, svc, "Salary A", checking.ID, ""), model.SalaryAmount{Amount: newFixedValue(50000), StartDate: mustParseDate("2027-01-01")})
	// The Reference Salary has the same six months of full parental leave, stored outside any Event.
	reference := mustSalary(t, svc, grossSalaryFrom(t, svc, "Reference", checking.ID, ""), model.SalaryAmount{Amount: newFixedValue(50000), StartDate: mustParseDate("2027-01-01")})
	mustFullParentalLeave(t, svc, model.FullParentalLeave{SalaryID: reference.ID, StartDate: mustParseDate("2027-06-01"), EndDate: mustParseDate("2027-12-01"), SjukDaysPerWeek: 7})
	child := mustEvent(t, svc, model.EventInput{Name: "Second child", Date: mustParseDate("2027-06-01")})

	leave := mustFullParentalLeave(t, svc, model.FullParentalLeave{SalaryID: salaryA.ID, EventID: child.ID, StartOffset: days(0), EndOffset: days(183), SjukDaysPerWeek: 7})

	if leave.StartDate != mustParseDate("2027-06-01") || leave.EndDate != mustParseDate("2027-12-01") {
		t.Fatalf("expected the leave from 2027-06-01 to 2027-12-01, got %s to %s", leave.StartDate, leave.EndDate)
	}
	working := chargedOn(t, svc, "2027-05-25")["Reference"]
	for _, day := range []string{"2027-05-25", "2027-06-25", "2027-09-25", "2027-11-25", "2027-12-25"} {
		charged := chargedOn(t, svc, day)
		if charged["Salary A"] != charged["Reference"] {
			t.Errorf("on %s: expected Salary A to pay as the Reference, %v, got %v", day, charged["Reference"], charged["Salary A"])
		}
	}
	segments := mustSalarySegments(t, svc, salaryA.ID)
	if len(segments) < 2 || segments[1].StartDate != mustParseDate("2027-06-01") || !segments[1].Breakdown.IsFullParentalLeave {
		t.Fatalf("expected a Salary Segment of full parental leave from 2027-06-01, got %+v", segments)
	}
	compensation := chargedOn(t, svc, "2027-06-25")["Salary A"]
	if compensation == working {
		t.Fatalf("expected the parental leave compensation, not the Net Salary of %v", working)
	}

	moveEvent(t, svc, child, "2027-08-01")

	for day, want := range map[string]float64{"2027-06-25": working, "2027-07-25": working, "2027-08-25": compensation, "2028-01-25": compensation, "2028-02-25": working} {
		if got := chargedOn(t, svc, day)["Salary A"]; got != want {
			t.Errorf("on %s with the Second child moved to 2027-08-01: expected Salary A to pay %v, got %v", day, want, got)
		}
	}
}

func TestAnEventEndsASalaryAndAddsANewOne(t *testing.T) {
	svc := newSalaryService(t)
	ctx := t.Context()
	checking := mustAccount(t, svc, "Checking")
	current := mustSalary(t, svc, grossSalaryFrom(t, svc, "Salary A", checking.ID, ""), model.SalaryAmount{Amount: newFixedValue(50000), StartDate: mustParseDate("2027-01-01")})
	newJob := mustEvent(t, svc, model.EventInput{Name: "New job", Date: mustParseDate("2027-09-01")})

	end := mustSalaryEnd(t, svc, model.SalaryEnd{SalaryID: current.ID, EventID: newJob.ID, DateOffset: days(0)})
	next := mustSalary(t, svc, grossSalaryFrom(t, svc, "Salary B", checking.ID, newJob.ID), model.SalaryAmount{Amount: newFixedValue(80000), EventID: newJob.ID, StartOffset: days(0)})

	if end.Date != mustParseDate("2027-09-01") {
		t.Fatalf("expected the end on the New job's date, got %s", end.Date)
	}
	for day, want := range map[string][2]bool{
		"2027-08-25": {true, false},
		"2027-09-25": {false, true},
		"2028-03-25": {false, true},
	} {
		charged := chargedOn(t, svc, day)
		_, paysA := charged["Salary A"]
		_, paysB := charged["Salary B"]
		if [2]bool{paysA, paysB} != want {
			t.Errorf("on %s: expected Salary A and Salary B paid %v, got %v (%v)", day, want, [2]bool{paysA, paysB}, charged)
		}
	}

	before := mustSalarySegments(t, svc, current.ID)
	last := before[len(before)-1]
	if last.EndDate == nil || *last.EndDate != mustParseDate("2027-09-01") || last.Breakdown.GrossMonthly != 50000 {
		t.Errorf("expected Salary A's Salary Breakdown of 50 000 to end on 2027-09-01, got %+v", before)
	}
	after := mustSalarySegments(t, svc, next.ID)
	if len(after) == 0 || after[0].StartDate != mustParseDate("2027-09-01") || after[0].Breakdown.GrossMonthly != 80000 {
		t.Errorf("expected Salary B's Salary Breakdown of 80 000 from 2027-09-01, got %+v", after)
	}

	edit, err := svc.GetSalaryEditPageData(ctx, current.ID)
	if err != nil {
		t.Fatalf("salary edit page: %v", err)
	}
	if len(edit.Salary.Ends) != 1 || edit.Salary.Ends[0].EventID != newJob.ID || edit.Salary.Ends[0].Date != newJob.Date {
		t.Errorf("expected Salary A's edit page to list its end in the New job, got %+v", edit.Salary.Ends)
	}
}

func TestTheEarliestPlacedEndOfASalaryApplies(t *testing.T) {
	svc := newSalaryService(t)
	checking := mustAccount(t, svc, "Checking")
	current := mustSalary(t, svc, model.Salary{Name: "Salary A", ToAccountID: checking.ID, Recurrence: "*-*-25"}, model.SalaryAmount{Amount: newFixedValue(30000), StartDate: mustParseDate("2027-01-01")})
	early := mustEvent(t, svc, model.EventInput{Name: "Early job", Date: mustParseDate("2027-03-01"), Planned: ptr(false)})
	late := mustEvent(t, svc, model.EventInput{Name: "Late job", Date: mustParseDate("2027-09-01")})
	mustSalaryEnd(t, svc, model.SalaryEnd{SalaryID: current.ID, EventID: early.ID, DateOffset: days(0)})
	mustSalaryEnd(t, svc, model.SalaryEnd{SalaryID: current.ID, EventID: late.ID, DateOffset: days(0)})

	// The unplanned Early job's end doesn't count, so the Late job's does.
	for day, want := range map[string]float64{"2027-04-25": 30000, "2027-08-25": 30000, "2027-09-25": 0} {
		if got := chargedOn(t, svc, day)["Salary A"]; got != want {
			t.Errorf("on %s: expected Salary A to pay %v, got %v", day, want, got)
		}
	}
	moveEvent(t, svc, model.Event{ID: early.ID, Name: early.Name, Date: early.Date, Planned: true}, early.Date.String())
	if got := chargedOn(t, svc, "2027-04-25")["Salary A"]; got != 0 {
		t.Errorf("expected the planned Early job to end Salary A on 2027-03-01, got %v paid on 2027-04-25", got)
	}
}
