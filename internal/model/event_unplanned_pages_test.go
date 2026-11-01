package model_test

import (
	"testing"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
)

func TestASalaryInAnUnplannedEventShowsWhatItWouldPayWithTheEventAtItsDate(t *testing.T) {
	svc := newSalaryService(t)
	ctx := t.Context()
	checking := mustAccount(t, svc, "Checking")
	newJob := mustEvent(t, svc, model.EventInput{Name: "New job", Date: mustParseDate("2027-09-01"), Planned: ptr(false)})
	salaryB := mustSalary(t, svc, grossSalaryFrom(t, svc, "Salary B", checking.ID, newJob.ID),
		model.SalaryAmount{Amount: newFixedValue(80000), EventID: newJob.ID, StartOffset: days(0)})

	page, err := svc.GetSalaryEditPageData(ctx, salaryB.ID)
	if err != nil {
		t.Fatalf("salary page: %v", err)
	}
	if page.Salary.InBaseline() {
		t.Errorf("expected Salary B, in the unplanned New job, out of the Baseline")
	}
	segments, err := page.Salary.Segments()
	if err != nil {
		t.Fatalf("segments: %v", err)
	}
	if len(segments) == 0 || segments[0].StartDate != newJob.Date || segments[0].Breakdown.GrossMonthly != 80000 {
		t.Errorf("expected Salary B's Salary Breakdown of 80 000 from the New job's date, got %+v", segments)
	}
}

func TestABillInAnUnplannedEventShowsWhatItWouldChargeWithTheEventAtItsDate(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	checking := mustAccount(t, svc, "Checking")
	move := mustEvent(t, svc, model.EventInput{Name: "Move", Date: date.Today().Add(-30 * date.Day), Planned: ptr(false)})
	household := mustBillAccount(t, svc, "Household", checking.ID)
	rent, err := svc.UpsertBill(ctx, model.Bill{Name: "Rent", BillAccountID: household.ID, Enabled: true, EventID: move.ID})
	if err != nil {
		t.Fatalf("save bill: %v", err)
	}
	if _, err := svc.UpsertBillAmount(ctx, model.BillAmount{BillID: rent.ID, EventID: move.ID, StartOffset: days(0), Amount: newFixedValue(9000)}); err != nil {
		t.Fatalf("save bill amount: %v", err)
	}

	page, err := svc.GetBillEditPageData(ctx, rent.ID)
	if err != nil {
		t.Fatalf("bill page: %v", err)
	}
	if page.Bill.InBaseline() {
		t.Errorf("expected the Rent, in the unplanned Move, out of the Baseline")
	}
	if got := page.Bill.CurrentAmount(); got != 9000 {
		t.Errorf("expected the Rent to show the 9 000 it would charge with the Move at its date, got %v", got)
	}
}

func TestTheTransferTemplatesPageListsWhatSalariesAndBillsOfUnplannedEventsWouldMoveApart(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	checking := mustAccount(t, svc, "Checking")
	newJob := mustEvent(t, svc, model.EventInput{Name: "New job", Date: mustParseDate("2027-09-01"), Planned: ptr(false)})
	mustSalary(t, svc, model.Salary{Name: "Salary B", ToAccountID: checking.ID, Recurrence: "*-*-25", EventID: newJob.ID},
		model.SalaryAmount{Amount: newFixedValue(40000), EventID: newJob.ID, StartOffset: days(0)})
	move := mustEvent(t, svc, model.EventInput{Name: "Move", Date: mustParseDate("2027-10-01"), Planned: ptr(false)})
	rent, err := svc.UpsertBill(ctx, model.Bill{Name: "Rent", BillAccountID: mustBillAccount(t, svc, "Household", checking.ID).ID, Enabled: true, EventID: move.ID})
	if err != nil {
		t.Fatalf("save bill: %v", err)
	}
	if _, err := svc.UpsertBillAmount(ctx, model.BillAmount{BillID: rent.ID, EventID: move.ID, StartOffset: days(0), Amount: newFixedValue(9000)}); err != nil {
		t.Fatalf("save bill amount: %v", err)
	}

	page, err := svc.GetTransferTemplatesPageData(ctx)
	if err != nil {
		t.Fatalf("transfer templates page: %v", err)
	}
	want := map[string]model.Event{"Salary B": newJob, "Rent": move}
	for _, tt := range page.LeftOut {
		if ev, ok := want[tt.Name]; ok && tt.EventID == ev.ID && tt.StartDate == ev.Date {
			delete(want, tt.Name)
		}
	}
	if len(want) > 0 {
		t.Errorf("expected the page to list %v apart, from their Events' dates, got %+v", want, page.LeftOut)
	}
	for _, row := range page.Rows {
		if row.Name == "Salary B" || row.Name == "Household" {
			t.Errorf("expected nothing of the unplanned Events in the Baseline's rows, got %+v", row)
		}
	}
}
