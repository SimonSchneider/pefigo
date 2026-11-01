package model_test

import (
	"maps"
	"slices"
	"testing"

	"github.com/SimonSchneider/pefigo/internal/model"
)

// eventPartIDs are the IDs of the parts listed for ev, by kind.
func eventPartIDs(ev model.EventView) map[model.EventEntryKind][]string {
	ids := map[model.EventEntryKind][]string{}
	for _, p := range ev.Parts {
		ids[p.Kind] = append(ids[p.Kind], p.ID)
	}
	return ids
}

func TestTheEventsPageListsTheSalariesBillsAndPartsAnEventAdds(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	parts := eventParts(t, svc)
	child := mustEvent(t, svc, model.EventInput{Name: "Second child", Date: mustParseDate("2027-06-01")})
	want := map[model.EventEntryKind][]string{}
	for _, part := range parts {
		id, err := part.save(ctx, "", child.ID, "", 10)
		if err != nil {
			t.Fatalf("save %s: %v", part.kind, err)
		}
		want[part.kind] = []string{id}
	}
	savings := mustAccount(t, svc, "Savings")
	salary := mustSalary(t, svc, model.Salary{Name: "Salary B", ToAccountID: savings.ID, Recurrence: "*-*-25", EventID: child.ID})
	want[model.EventEntrySalary] = []string{salary.ID}

	page, err := svc.GetEventsPageData(ctx)
	if err != nil {
		t.Fatalf("events page: %v", err)
	}
	if got := eventPartIDs(page.Events[0]); !maps.EqualFunc(got, want, slices.Equal) {
		t.Errorf("expected the Second child to list %v, got %v", want, got)
	}
	for _, p := range page.Events[0].Parts {
		if p.Kind == model.EventEntrySalary {
			continue
		}
		if p.StartDate == nil || *p.StartDate != mustParseDate("2027-06-11") || p.StartOffset() != 10 || p.OwnerName == "" {
			t.Errorf("expected %s listed with its owner from +10 days, on 2027-06-11, got %+v", p.Kind, p)
		}
	}
}

func TestDeletingAnEventDeletesTheSalariesBillsAndPartsItAdds(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	checking := mustAccount(t, svc, "Checking")
	household := mustBillAccount(t, svc, "Household", checking.ID)
	gym := mustBillIn(t, svc, household, "Gym", model.BillAmount{Amount: newFixedValue(300), StartDate: mustParseDate("2027-01-01")})
	newJob := mustEvent(t, svc, model.EventInput{Name: "New job", Date: mustParseDate("2027-09-01")})
	child := mustEvent(t, svc, model.EventInput{Name: "Second child", Date: mustParseDate("2028-01-01")})
	salaryB := mustSalary(t, svc, model.Salary{Name: "Salary B", ToAccountID: checking.ID, Recurrence: "*-*-25", EventID: newJob.ID},
		model.SalaryAmount{Amount: newFixedValue(40000), EventID: newJob.ID, StartOffset: days(0)})
	// The Second child adds a leave to the New job's Salary: it is deleted with the Salary.
	mustFullParentalLeave(t, svc, model.FullParentalLeave{SalaryID: salaryB.ID, EventID: child.ID, StartOffset: days(0), EndOffset: days(100), SjukDaysPerWeek: 7})
	gymEnd := mustBillEnd(t, svc, model.BillEnd{BillID: gym.ID, EventID: newJob.ID, DateOffset: days(0)})

	confirm, err := svc.GetEventDeletePageData(ctx, newJob.ID)
	if err != nil {
		t.Fatalf("delete confirmation: %v", err)
	}
	if got := eventPartIDs(confirm.Event); len(got[model.EventEntrySalary]) != 1 || len(got[model.EventEntrySalaryAmount]) != 1 || len(got[model.EventEntryBillEnd]) != 1 {
		t.Errorf("expected the confirmation to list Salary B, its amount and the Gym's end, got %v", got)
	}
	if len(confirm.AlsoDeleted) != 1 || confirm.AlsoDeleted[0].Kind != model.EventEntryFullParentalLeave || confirm.AlsoDeleted[0].EventName != "Second child" {
		t.Errorf("expected the confirmation to list the Second child's leave, deleted with Salary B, got %+v", confirm.AlsoDeleted)
	}

	if err := svc.DeleteEvent(ctx, newJob.ID); err != nil {
		t.Fatalf("delete event: %v", err)
	}
	if _, err := svc.GetSalary(ctx, salaryB.ID); err == nil {
		t.Error("expected Salary B, added by the New job, to be deleted")
	}
	if _, err := svc.GetBillEnd(ctx, gymEnd.ID); err == nil {
		t.Error("expected the Gym's end, added by the New job, to be deleted")
	}
	if got := chargedOn(t, svc, "2028-01-01")["Gym"]; got != 300 {
		t.Errorf("expected the Gym, outside the Event, to charge on, got %v", got)
	}
}
