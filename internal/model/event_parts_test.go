package model_test

import (
	"context"
	"testing"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
)

// eventPart is a kind of dated part an Event can add to a Salary, Bill or Startup Share account, saved
// and read back through the Service as its form and page do.
type eventPart struct {
	kind model.EventEntryKind
	// save saves the part id, or a new one for "", in eventID with its first date offset days after the
	// date of offsetEventID, and returns its ID.
	save func(ctx context.Context, id, eventID, offsetEventID string, offset int64) (string, error)
	// get is the part's Event and first date.
	get func(ctx context.Context, id string) (string, date.Date, error)
}

// eventParts are every kind of dated part, on a Salary, a Bill and a Startup Share account.
func eventParts(t *testing.T, svc *model.Service) []eventPart {
	t.Helper()
	checking := mustAccount(t, svc, "Checking")
	salary := mustSalary(t, svc, model.Salary{Name: "Salary A", ToAccountID: checking.ID, Recurrence: "*-*-25"})
	bill := mustBillIn(t, svc, mustBillAccount(t, svc, "Household", checking.ID), "Gym")
	startup := mustAccount(t, svc, "Startup Co")
	mustStartupShareAccount(t, svc, startup.ID, nil, nil)
	return []eventPart{
		{model.EventEntrySalaryAmount,
			func(ctx context.Context, id, eventID, offsetEventID string, offset int64) (string, error) {
				a, err := svc.UpsertSalaryAmount(ctx, model.SalaryAmount{ID: id, SalaryID: salary.ID, Amount: newFixedValue(30000), EventID: eventID, OffsetEventID: offsetEventID, StartOffset: days(offset)})
				return a.ID, err
			},
			func(ctx context.Context, id string) (string, date.Date, error) {
				a, err := svc.GetSalaryAmount(ctx, id)
				return a.EventID, a.StartDate, err
			}},
		{model.EventEntrySalaryAdjustment,
			func(ctx context.Context, id, eventID, offsetEventID string, offset int64) (string, error) {
				a, err := svc.UpsertSalaryAdjustment(ctx, model.SalaryAdjustment{ID: id, SalaryID: salary.ID, VacationDaysPerYear: 30, EventID: eventID, OffsetEventID: offsetEventID, ValidFromOffset: days(offset)})
				return a.ID, err
			},
			func(ctx context.Context, id string) (string, date.Date, error) {
				a, err := svc.GetSalaryAdjustment(ctx, id)
				return a.EventID, a.ValidFrom, err
			}},
		{model.EventEntryPartialParentalLeave,
			func(ctx context.Context, id, eventID, offsetEventID string, offset int64) (string, error) {
				l, err := svc.UpsertPartialParentalLeave(ctx, model.PartialParentalLeave{ID: id, SalaryID: salary.ID, SjukDaysPerYear: 40, EventID: eventID, OffsetEventID: offsetEventID, StartOffset: days(offset), EndOffset: days(offset + 90)})
				return l.ID, err
			},
			func(ctx context.Context, id string) (string, date.Date, error) {
				l, err := svc.GetPartialParentalLeave(ctx, id)
				return l.EventID, l.StartDate, err
			}},
		{model.EventEntryFullParentalLeave,
			func(ctx context.Context, id, eventID, offsetEventID string, offset int64) (string, error) {
				l, err := svc.UpsertFullParentalLeave(ctx, model.FullParentalLeave{ID: id, SalaryID: salary.ID, SjukDaysPerWeek: 7, EventID: eventID, OffsetEventID: offsetEventID, StartOffset: days(offset), EndOffset: days(offset + 90)})
				return l.ID, err
			},
			func(ctx context.Context, id string) (string, date.Date, error) {
				l, err := svc.GetFullParentalLeave(ctx, id)
				return l.EventID, l.StartDate, err
			}},
		{model.EventEntrySalaryEnd,
			func(ctx context.Context, id, eventID, offsetEventID string, offset int64) (string, error) {
				e, err := svc.UpsertSalaryEnd(ctx, model.SalaryEnd{ID: id, SalaryID: salary.ID, EventID: eventID, OffsetEventID: offsetEventID, DateOffset: days(offset)})
				return e.ID, err
			},
			func(ctx context.Context, id string) (string, date.Date, error) {
				e, err := svc.GetSalaryEnd(ctx, id)
				return e.EventID, e.Date, err
			}},
		{model.EventEntryBillAmount,
			func(ctx context.Context, id, eventID, offsetEventID string, offset int64) (string, error) {
				a, err := svc.UpsertBillAmount(ctx, model.BillAmount{ID: id, BillID: bill.ID, Amount: newFixedValue(300), EventID: eventID, OffsetEventID: offsetEventID, StartOffset: days(offset), EndOffset: days(offset + 90)})
				return a.ID, err
			},
			func(ctx context.Context, id string) (string, date.Date, error) {
				a, err := svc.GetBillAmount(ctx, id)
				return a.EventID, a.StartDate, err
			}},
		{model.EventEntryBillEnd,
			func(ctx context.Context, id, eventID, offsetEventID string, offset int64) (string, error) {
				e, err := svc.UpsertBillEnd(ctx, model.BillEnd{ID: id, BillID: bill.ID, EventID: eventID, OffsetEventID: offsetEventID, DateOffset: days(offset)})
				return e.ID, err
			},
			func(ctx context.Context, id string) (string, date.Date, error) {
				e, err := svc.GetBillEnd(ctx, id)
				return e.EventID, e.Date, err
			}},
		{model.EventEntryInvestmentRound,
			func(ctx context.Context, id, eventID, offsetEventID string, offset int64) (string, error) {
				r, err := svc.UpsertInvestmentRound(ctx, model.InvestmentRoundInput{ID: id, AccountID: startup.ID, Valuation: 1e6, PreMoneyShares: 1000, EventID: eventID, OffsetEventID: offsetEventID, DateOffset: days(offset)})
				return r.ID, err
			},
			func(ctx context.Context, id string) (string, date.Date, error) {
				r, err := svc.GetInvestmentRound(ctx, id)
				return r.EventID, r.Date, err
			}},
		{model.EventEntryShareChange,
			func(ctx context.Context, id, eventID, offsetEventID string, offset int64) (string, error) {
				c, err := svc.UpsertShareChange(ctx, model.ShareChangeInput{ID: id, AccountID: startup.ID, DeltaShares: 100, TotalPrice: 100, EventID: eventID, OffsetEventID: offsetEventID, DateOffset: days(offset)})
				return c.ID, err
			},
			func(ctx context.Context, id string) (string, date.Date, error) {
				c, err := svc.GetShareChange(ctx, id)
				return c.EventID, c.Date, err
			}},
	}
}

func TestADatedPartLeavingOrJoiningAnEventKeepsTheDaysItAppliesOn(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	parts := eventParts(t, svc)
	child := mustEvent(t, svc, model.EventInput{Name: "Second child", Date: mustParseDate("2027-06-01")})
	newJob := mustEvent(t, svc, model.EventInput{Name: "New job", Date: mustParseDate("2027-09-01")})

	for _, part := range parts {
		t.Run(string(part.kind), func(t *testing.T) {
			expect := func(t *testing.T, id, wantEvent, wantDay string) {
				t.Helper()
				eventID, day, err := part.get(ctx, id)
				if err != nil {
					t.Fatalf("get: %v", err)
				}
				if eventID != wantEvent || day != mustParseDate(wantDay) {
					t.Errorf("expected it on %s in %q, got %s in %q", wantDay, wantEvent, day, eventID)
				}
			}
			id, err := part.save(ctx, "", child.ID, "", 10)
			if err != nil {
				t.Fatalf("save in the Second child: %v", err)
			}
			expect(t, id, child.ID, "2027-06-11")

			// Its form gives its days after the Second child's date while it joins the New job.
			if _, err := part.save(ctx, id, newJob.ID, child.ID, 10); err != nil {
				t.Fatalf("join the New job: %v", err)
			}
			expect(t, id, newJob.ID, "2027-06-11")
			moveEvent(t, svc, newJob, "2027-10-01")
			expect(t, id, newJob.ID, "2027-07-11")

			if part.kind.RemovableFromEvent() {
				mustRemoveFromEvent(t, svc, model.EventEntry{Kind: part.kind, ID: id})
			} else if _, err := part.save(ctx, id, "", newJob.ID, -82); err != nil {
				// An end leaves its Event from its own form, which gives its day after the New job's date.
				t.Fatalf("leave the New job from its form: %v", err)
			}
			expect(t, id, "", "2027-07-11")
			moveEvent(t, svc, newJob, "2027-09-01")
			expect(t, id, "", "2027-07-11")
		})
	}
}

func TestDaysAfterAnEventsDateNeedAnEventForEveryDatedPart(t *testing.T) {
	svc := newTestService(t)
	for _, part := range eventParts(t, svc) {
		if _, err := part.save(t.Context(), "", "", "", 10); err == nil {
			t.Errorf("%s: expected days after an Event's date to need an Event", part.kind)
		}
	}
}

func TestASalaryOrBillLeavingItsEventKeepsItsParts(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	checking := mustAccount(t, svc, "Checking")
	newJob := mustEvent(t, svc, model.EventInput{Name: "New job", Date: mustParseDate("2027-09-01")})
	salary := mustSalary(t, svc, model.Salary{Name: "Salary B", ToAccountID: checking.ID, Recurrence: "*-*-25", EventID: newJob.ID},
		model.SalaryAmount{Amount: newFixedValue(40000), StartDate: mustParseDate("2027-09-01")})
	household := mustBillAccount(t, svc, "Household", checking.ID)
	bill, err := svc.UpsertBill(ctx, model.Bill{Name: "Commute", BillAccountID: household.ID, Enabled: true, EventID: newJob.ID})
	if err != nil {
		t.Fatalf("save bill: %v", err)
	}
	if _, err := svc.UpsertBillAmount(ctx, model.BillAmount{BillID: bill.ID, Amount: newFixedValue(900), StartDate: mustParseDate("2027-09-01")}); err != nil {
		t.Fatalf("save bill amount: %v", err)
	}
	moveEvent(t, svc, model.Event{ID: newJob.ID, Name: newJob.Name, Date: newJob.Date, Planned: false}, newJob.Date.String())
	if charged := chargedOn(t, svc, "2027-10-25"); charged["Salary B"] != 0 {
		t.Fatalf("expected the unplanned New job's Salary left out, got %v", charged)
	}

	mustRemoveFromEvent(t, svc, model.EventEntry{Kind: model.EventEntrySalary, ID: salary.ID})
	mustRemoveFromEvent(t, svc, model.EventEntry{Kind: model.EventEntryBill, ID: bill.ID})

	if got := chargedOn(t, svc, "2027-10-25")["Salary B"]; got != 40000 {
		t.Errorf("expected Salary B in the Baseline once it leaves the New job, got %v", got)
	}
	if got := chargedOn(t, svc, "2027-10-01")["Commute"]; got != 900 {
		t.Errorf("expected Commute in the Baseline once it leaves the New job, got %v", got)
	}
}
