package model_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
)

func mustEvent(t *testing.T, svc *model.Service, inp model.EventInput) model.Event {
	t.Helper()
	ev, err := svc.UpsertEvent(t.Context(), inp)
	if err != nil {
		t.Fatalf("save event %q: %v", inp.Name, err)
	}
	return ev
}

// moveEvent puts ev at day, keeping everything else about it.
func moveEvent(t *testing.T, svc *model.Service, ev model.Event, day string) model.Event {
	t.Helper()
	return mustEvent(t, svc, model.EventInput{ID: ev.ID, Name: ev.Name, Date: mustParseDate(day), Planned: ptr(ev.Planned)})
}

func mustEventAccount(t *testing.T, svc *model.Service, name, eventID string) model.Account {
	t.Helper()
	acc, err := svc.UpsertAccount(t.Context(), model.AccountInput{Name: name, EventID: eventID})
	if err != nil {
		t.Fatalf("create account %q: %v", name, err)
	}
	return acc
}

func mustGrowthModel(t *testing.T, svc *model.Service, inp model.AccountGrowthModelInput) model.GrowthModel {
	t.Helper()
	if inp.Type == "" {
		inp.Type = model.GrowthModelFixed
	}
	gm, err := svc.UpsertAccountGrowthModel(t.Context(), inp)
	if err != nil {
		t.Fatalf("create growth model: %v", err)
	}
	return gm
}

func mustGetTransferTemplate(t *testing.T, svc *model.Service, id string) model.TransferTemplate {
	t.Helper()
	tt, err := svc.GetTransferTemplate(t.Context(), id)
	if err != nil {
		t.Fatalf("get transfer template: %v", err)
	}
	return tt
}

func mustGetGrowthModel(t *testing.T, svc *model.Service, id string) model.GrowthModel {
	t.Helper()
	gm, err := svc.GetGrowthModel(t.Context(), id)
	if err != nil {
		t.Fatalf("get growth model: %v", err)
	}
	return gm
}

func mustRemoveFromEvent(t *testing.T, svc *model.Service, entry model.EventEntry) {
	t.Helper()
	if err := svc.RemoveFromEvent(t.Context(), entry); err != nil {
		t.Fatalf("remove %s from its event: %v", entry, err)
	}
}

func mustEventEnd(t *testing.T, svc *model.Service, inp model.EventEndInput) {
	t.Helper()
	if err := svc.UpsertEventEnd(t.Context(), inp); err != nil {
		t.Fatalf("end %s in event: %v", inp.Entry, err)
	}
}

func dateOf(d *date.Date) string {
	if d == nil {
		return "none"
	}
	return d.String()
}

func TestANewEventIsPlannedUnlessMarkedUnplanned(t *testing.T) {
	svc := newTestService(t)
	apartment := mustEvent(t, svc, model.EventInput{Name: "Apartment", Date: mustParseDate("2028-03-01")})
	sabbatical := mustEvent(t, svc, model.EventInput{Name: "Sabbatical", Date: mustParseDate("2027-06-01"), Planned: ptr(false)})

	if !apartment.Planned {
		t.Errorf("expected a new Event to be planned, got %+v", apartment)
	}
	if sabbatical.Planned {
		t.Errorf("expected an Event marked unplanned to be unplanned, got %+v", sabbatical)
	}
	events, err := svc.ListEvents(t.Context())
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if want := []model.Event{sabbatical, apartment}; !slices.Equal(events, want) {
		t.Fatalf("expected the Events by date %+v, got %+v", want, events)
	}
}

func TestMovingAnEventMovesEveryDateOfItsEntries(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	apartment := mustEvent(t, svc, model.EventInput{Name: "Apartment", Date: mustParseDate("2028-03-01")})
	property := mustEventAccount(t, svc, "Property", apartment.ID)
	purchase := mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Purchase", ToAccountID: property.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(3e6),
		Recurrence: "2028-03-01", Enabled: true, EventID: apartment.ID,
	})
	amortization := mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Amortization", FromAccountID: checking.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(5000),
		Recurrence: "*-*-25", StartDate: mustParseDate("2028-04-01"), EndDate: ptr(mustParseDate("2029-03-01")), Enabled: true, EventID: apartment.ID,
	})
	growth := mustGrowthModel(t, svc, model.AccountGrowthModelInput{
		AccountID: property.ID, AnnualRate: newFixedValue(0.02), EndDate: ptr(mustParseDate("2029-03-01")), EventID: apartment.ID,
	})

	if purchase.StartDate != apartment.Date || growth.StartDate != apartment.Date {
		t.Fatalf("expected the entries without a Start Date to start on the Event's date %s, got %s and %s", apartment.Date, purchase.StartDate, growth.StartDate)
	}

	moveEvent(t, svc, apartment, "2030-03-01")

	purchase = mustGetTransferTemplate(t, svc, purchase.ID)
	if purchase.StartDate != mustParseDate("2030-03-01") || purchase.Recurrence != "2030-03-01" {
		t.Errorf("expected the one-off Purchase to move to 2030-03-01, got %s on %s", purchase.Recurrence, purchase.StartDate)
	}
	amortization = mustGetTransferTemplate(t, svc, amortization.ID)
	if amortization.StartDate != mustParseDate("2030-04-01") || dateOf(amortization.EndDate) != "2031-03-01" || amortization.Recurrence != "*-*-25" {
		t.Errorf("expected Amortization on *-*-25 from 2030-04-01 to 2031-03-01, got %s from %s to %s", amortization.Recurrence, amortization.StartDate, dateOf(amortization.EndDate))
	}
	growth = mustGetGrowthModel(t, svc, growth.ID)
	if growth.StartDate != mustParseDate("2030-03-01") || dateOf(growth.EndDate) != "2031-03-01" {
		t.Errorf("expected the growth model from 2030-03-01 to 2031-03-01, got %s to %s", growth.StartDate, dateOf(growth.EndDate))
	}
}

func TestAnEntryJoiningOrLeavingAnEventKeepsTheDatesItAppliesOn(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	ev := mustEvent(t, svc, model.EventInput{Name: "Raise", Date: mustParseDate("2027-01-01")})

	t.Run("transfer template", func(t *testing.T) {
		tt := mustTransferTemplate(t, svc, model.TransferTemplate{
			Name: "Savings", FromAccountID: checking.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(1000),
			Recurrence: "*-*-25", StartDate: mustParseDate("2027-01-25"), EndDate: ptr(mustParseDate("2027-06-01")), Enabled: true,
		})
		entry := model.EventEntry{Kind: model.EventEntryTransferTemplate, ID: tt.ID}

		// Its form joins the Event with the dates it applies on in the Baseline.
		tt.EventID = ev.ID
		mustTransferTemplate(t, svc, tt)
		if got := mustGetTransferTemplate(t, svc, tt.ID); got.EventID != ev.ID || got.StartDate != mustParseDate("2027-01-25") || dateOf(got.EndDate) != "2027-06-01" {
			t.Fatalf("expected the Transfer Template in the Event, still from 2027-01-25 to 2027-06-01, got %+v", got)
		}
		// Stored as +24 days, so it moves with the Event.
		ev = moveEvent(t, svc, ev, "2027-02-01")
		// Moving the Event 31 days moves both dates 31 days.
		if got := mustGetTransferTemplate(t, svc, tt.ID); got.StartDate != mustParseDate("2027-02-25") || dateOf(got.EndDate) != "2027-07-02" {
			t.Fatalf("expected the Transfer Template from 2027-02-25 to 2027-07-02 after the move, got %s to %s", got.StartDate, dateOf(got.EndDate))
		}

		mustRemoveFromEvent(t, svc, entry)
		ev = moveEvent(t, svc, ev, "2027-01-01")
		if got := mustGetTransferTemplate(t, svc, tt.ID); got.EventID != "" || got.StartDate != mustParseDate("2027-02-25") || dateOf(got.EndDate) != "2027-07-02" {
			t.Fatalf("expected the Transfer Template in the Baseline, from 2027-02-25 to 2027-07-02 whatever the Event's date, got %+v", got)
		}
	})

	t.Run("one-off transfer template", func(t *testing.T) {
		tt := mustTransferTemplate(t, svc, model.TransferTemplate{
			Name: "Bonus", ToAccountID: checking.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(1000),
			Recurrence: "2027-03-10", StartDate: mustParseDate("2027-01-01"), Enabled: true,
		})
		entry := model.EventEntry{Kind: model.EventEntryTransferTemplate, ID: tt.ID}

		tt.EventID = ev.ID
		mustTransferTemplate(t, svc, tt)
		ev = moveEvent(t, svc, ev, "2027-01-11")
		if got := mustGetTransferTemplate(t, svc, tt.ID); got.Recurrence != "2027-03-20" {
			t.Fatalf("expected the one-off to move ten days with the Event, got %s", got.Recurrence)
		}
		mustRemoveFromEvent(t, svc, entry)
		ev = moveEvent(t, svc, ev, "2027-01-01")
		if got := mustGetTransferTemplate(t, svc, tt.ID); got.Recurrence != "2027-03-20" {
			t.Fatalf("expected the one-off to stay on 2027-03-20 once it left the Event, got %s", got.Recurrence)
		}
	})

	t.Run("growth model", func(t *testing.T) {
		gm := mustGrowthModel(t, svc, model.AccountGrowthModelInput{AccountID: checking.ID, AnnualRate: newFixedValue(0.01), StartDate: mustParseDate("2027-01-25")})
		entry := model.EventEntry{Kind: model.EventEntryGrowthModel, ID: gm.ID}

		mustGrowthModel(t, svc, model.AccountGrowthModelInput{
			ID: gm.ID, AccountID: gm.AccountID, Type: gm.Type, AnnualRate: gm.AnnualRate, StartDate: gm.StartDate, EndDate: gm.EndDate, EventID: ev.ID,
		})
		if got := mustGetGrowthModel(t, svc, gm.ID); got.EventID != ev.ID || got.StartDate != mustParseDate("2027-01-25") || got.EndDate != nil {
			t.Fatalf("expected the growth model in the Event, still from 2027-01-25, got %+v", got)
		}
		ev = moveEvent(t, svc, ev, "2027-02-01")
		mustRemoveFromEvent(t, svc, entry)
		if got := mustGetGrowthModel(t, svc, gm.ID); got.EventID != "" || got.StartDate != mustParseDate("2027-02-25") {
			t.Fatalf("expected the growth model in the Baseline from 2027-02-25, got %+v", got)
		}
		ev = moveEvent(t, svc, ev, "2027-01-01")
	})

	t.Run("account", func(t *testing.T) {
		acc := mustAccount(t, svc, "Car")
		entry := model.EventEntry{Kind: model.EventEntryAccount, ID: acc.ID}

		if _, err := svc.UpsertAccount(t.Context(), model.AccountInput{ID: acc.ID, Name: acc.Name, EventID: ev.ID}); err != nil {
			t.Fatalf("put the account in the Event: %v", err)
		}
		if got, err := svc.GetAccount(t.Context(), acc.ID); err != nil || got.EventID != ev.ID {
			t.Fatalf("expected the account in the Event, got %+v (%v)", got, err)
		}
		mustRemoveFromEvent(t, svc, entry)
		if got, err := svc.GetAccount(t.Context(), acc.ID); err != nil || got.EventID != "" {
			t.Fatalf("expected the account in the Baseline, got %+v (%v)", got, err)
		}
	})
}

func TestAnEntryCanOnlyJoinAnEventThatExists(t *testing.T) {
	svc := newTestService(t)
	acc := mustAccount(t, svc, "Checking")
	if _, err := svc.UpsertTransferTemplate(t.Context(), model.TransferTemplate{
		Name: "Savings", FromAccountID: acc.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(1000),
		Recurrence: "*-*-25", StartDate: mustParseDate("2027-01-25"), Enabled: true, EventID: "missing",
	}); !errors.Is(err, model.ErrInvalidInput) {
		t.Fatalf("expected invalid input for a Transfer Template in an Event that doesn't exist, got %v", err)
	}
	if _, err := svc.UpsertAccountGrowthModel(t.Context(), model.AccountGrowthModelInput{
		AccountID: acc.ID, Type: model.GrowthModelFixed, StartDate: mustParseDate("2027-01-25"), EventID: "missing",
	}); !errors.Is(err, model.ErrInvalidInput) {
		t.Fatalf("expected invalid input for a growth model in an Event that doesn't exist, got %v", err)
	}
	if _, err := svc.UpsertAccount(t.Context(), model.AccountInput{Name: "Car", EventID: "missing"}); !errors.Is(err, model.ErrInvalidInput) {
		t.Fatalf("expected invalid input for an account in an Event that doesn't exist, got %v", err)
	}
}

func TestAnEventOnlyEndsEntriesOutsideIt(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	ev := mustEvent(t, svc, model.EventInput{Name: "Apartment", Date: mustParseDate("2028-03-01")})
	inside := mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Amortization", FromAccountID: checking.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(5000),
		Recurrence: "*-*-25", Enabled: true, EventID: ev.ID,
	})
	err := svc.UpsertEventEnd(t.Context(), model.EventEndInput{EventID: ev.ID, Entry: model.EventEntry{Kind: model.EventEntryTransferTemplate, ID: inside.ID}})
	if !errors.Is(err, model.ErrInvalidInput) {
		t.Fatalf("expected invalid input for an Event ending its own entry, got %v", err)
	}
	acc := model.EventEntry{Kind: model.EventEntryAccount, ID: checking.ID}
	if err := svc.UpsertEventEnd(t.Context(), model.EventEndInput{EventID: ev.ID, Entry: acc}); !errors.Is(err, model.ErrInvalidInput) {
		t.Fatalf("expected invalid input for an Event ending an account, got %v", err)
	}
}

func TestTheEventsPageListsEachEventWithItsEntriesAndTheEntriesItEnds(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	savings := mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Savings", FromAccountID: checking.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(10000),
		Recurrence: "*-*-25", StartDate: mustParseDate("2027-01-01"), Enabled: true,
	})
	apartment := mustEvent(t, svc, model.EventInput{Name: "Apartment", Date: mustParseDate("2028-03-01")})
	property := mustEventAccount(t, svc, "Property", apartment.ID)
	growth := mustGrowthModel(t, svc, model.AccountGrowthModelInput{AccountID: property.ID, AnnualRate: newFixedValue(0.02), EventID: apartment.ID})
	amortization := mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Amortization", FromAccountID: checking.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(5000),
		Recurrence: "*-*-25", StartDate: mustParseDate("2028-04-01"), Enabled: true, EventID: apartment.ID,
	})
	mustEventEnd(t, svc, model.EventEndInput{EventID: apartment.ID, Entry: model.EventEntry{Kind: model.EventEntryTransferTemplate, ID: savings.ID}})
	mustEvent(t, svc, model.EventInput{Name: "Sabbatical", Date: mustParseDate("2029-01-01"), Planned: ptr(false)})

	page, err := svc.GetEventsPageData(t.Context())
	if err != nil {
		t.Fatalf("events page: %v", err)
	}
	if len(page.Events) != 2 || page.Events[0].Name != "Apartment" || page.Events[1].Name != "Sabbatical" || page.Events[1].Planned {
		t.Fatalf("expected Apartment and the unplanned Sabbatical, got %+v", page.Events)
	}
	ev := page.Events[0]
	if len(ev.Accounts) != 1 || ev.Accounts[0].ID != property.ID {
		t.Errorf("expected the Property account, got %+v", ev.Accounts)
	}
	if len(ev.GrowthModels) != 1 || ev.GrowthModels[0].ID != growth.ID || ev.GrowthModels[0].StartDate != apartment.Date {
		t.Errorf("expected the growth model from the Event's date, got %+v", ev.GrowthModels)
	}
	if len(ev.TransferTemplates) != 1 || ev.TransferTemplates[0].ID != amortization.ID || ev.TransferTemplates[0].StartOffset() != 31*date.Day {
		t.Errorf("expected Amortization 31 days after the Event's date, got %+v", ev.TransferTemplates)
	}
	if len(ev.Ends) != 1 || ev.Ends[0].Entry.ID != savings.ID || ev.Ends[0].Name != "Savings" || ev.Ends[0].EndDate != apartment.Date {
		t.Errorf("expected the Event to end Savings on its date, got %+v", ev.Ends)
	}
}

func TestDeletingAnEventDeletesTheEntriesItAddsAndLiftsTheEndsItDeclares(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	checking := mustAccount(t, svc, "Checking")
	savings := mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Savings", FromAccountID: checking.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(10000),
		Recurrence: "*-*-25", StartDate: mustParseDate("2027-01-01"), Enabled: true,
	})
	apartment := mustEvent(t, svc, model.EventInput{Name: "Apartment", Date: mustParseDate("2028-03-01")})
	property := mustEventAccount(t, svc, "Property", apartment.ID)
	growth := mustGrowthModel(t, svc, model.AccountGrowthModelInput{AccountID: checking.ID, AnnualRate: newFixedValue(0.02), EventID: apartment.ID})
	amortization := mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Amortization", FromAccountID: checking.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(5000),
		Recurrence: "*-*-25", StartDate: mustParseDate("2028-04-01"), Enabled: true, EventID: apartment.ID,
	})
	// Outside the Event, but paying into its account.
	deposit := mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Deposit", FromAccountID: checking.ID, ToAccountID: property.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(100),
		Recurrence: "*-*-01", StartDate: mustParseDate("2028-03-01"), Enabled: true,
	})
	mustEventEnd(t, svc, model.EventEndInput{EventID: apartment.ID, Entry: model.EventEntry{Kind: model.EventEntryTransferTemplate, ID: savings.ID}})

	confirm, err := svc.GetEventDeletePageData(ctx, apartment.ID)
	if err != nil {
		t.Fatalf("delete confirmation: %v", err)
	}
	if len(confirm.Event.Accounts) != 1 || len(confirm.Event.GrowthModels) != 1 || len(confirm.Event.TransferTemplates) != 1 || len(confirm.Event.Ends) != 1 {
		t.Fatalf("expected the confirmation to list the account, growth model and Transfer Template it adds and the end it declares, got %+v", confirm.Event)
	}
	if len(confirm.AlsoDeleted) != 1 || confirm.AlsoDeleted[0].ID != deposit.ID {
		t.Fatalf("expected the confirmation to list Deposit, deleted with the Event's account, got %+v", confirm.AlsoDeleted)
	}

	if err := svc.DeleteEvent(ctx, apartment.ID); err != nil {
		t.Fatalf("delete event: %v", err)
	}
	if _, err := svc.GetAccount(ctx, property.ID); err == nil {
		t.Errorf("expected the Event's account to be deleted")
	}
	if _, err := svc.GetGrowthModel(ctx, growth.ID); err == nil {
		t.Errorf("expected the Event's growth model to be deleted")
	}
	if _, err := svc.GetTransferTemplate(ctx, amortization.ID); err == nil {
		t.Errorf("expected the Event's Transfer Template to be deleted")
	}
	got := mustGetTransferTemplate(t, svc, savings.ID)
	if got.EndDate != nil {
		t.Errorf("expected Savings to keep running, got an End Date %s", dateOf(got.EndDate))
	}
	if state := transferStateOn(t, svc, savings.ID, "2028-06-25"); state != model.TransferActive {
		t.Errorf("expected the lifted end to leave Savings active after the Event's date, got %s", state)
	}
}

// transferStateOn is whether the Baseline's Transfer Template id applies on day.
func transferStateOn(t *testing.T, svc *model.Service, id string, day string) model.TransferState {
	t.Helper()
	for _, tt := range mustTransferSchedule(t, svc).On(mustParseDate(day)).Transfers() {
		if tt.ID == id {
			return tt.ActiveState()
		}
	}
	return "absent"
}

func TestTheBaselineSaysWhichEventEndsATransferTemplate(t *testing.T) {
	svc := newTestService(t)
	p := mustApartmentPlans(t, svc, "2028-03-01")

	for _, tt := range mustTransferSchedule(t, svc).On(mustParseDate("2028-01-25")).Transfers() {
		if tt.ID == p.savingsTT.ID && (tt.EndedByEventID != p.apartment.ID || dateOf(tt.EndDate) != "2028-03-01") {
			t.Fatalf("expected Savings to end on 2028-03-01, ended by the Apartment, got %+v", tt.TransferTemplate)
		}
	}
	if got := mustGetTransferTemplate(t, svc, p.savingsTT.ID); got.EndedByEventID != "" || got.EndDate != nil {
		t.Fatalf("expected Savings as stored to have no End Date of its own, got %+v", got)
	}
}

func TestAnEventListsItsEntriesByDate(t *testing.T) {
	svc := newTestService(t)
	p := mustApartmentPlans(t, svc, "2028-03-01")

	page, err := svc.GetEventsPageData(t.Context())
	if err != nil {
		t.Fatalf("events page: %v", err)
	}
	var names []string
	for _, tt := range page.Events[0].TransferTemplates {
		names = append(names, tt.Name)
	}
	if want := []string{p.purchase.Name, p.smallerSavingsTT.Name, p.amortization.Name}; !slices.Equal(names, want) {
		t.Fatalf("expected the Transfer Templates by Start Date and name %v, got %v", want, names)
	}
}

func TestSavingAnEntryIntoAnEventLiftsTheEndTheEventGaveIt(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	ev := mustEvent(t, svc, model.EventInput{Name: "Raise", Date: mustParseDate("2027-01-01")})
	tt := mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Savings", FromAccountID: checking.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(1000),
		Recurrence: "*-*-25", StartDate: mustParseDate("2026-01-01"), Enabled: true,
	})
	gm := mustGrowthModel(t, svc, model.AccountGrowthModelInput{AccountID: checking.ID, AnnualRate: newFixedValue(0.01), StartDate: mustParseDate("2026-01-01")})
	mustEventEnd(t, svc, model.EventEndInput{EventID: ev.ID, Entry: model.EventEntry{Kind: model.EventEntryTransferTemplate, ID: tt.ID}})
	mustEventEnd(t, svc, model.EventEndInput{EventID: ev.ID, Entry: model.EventEntry{Kind: model.EventEntryGrowthModel, ID: gm.ID}})

	tt.EventID = ev.ID
	mustTransferTemplate(t, svc, tt)
	if _, err := svc.UpsertAccountGrowthModel(t.Context(), model.AccountGrowthModelInput{
		ID: gm.ID, AccountID: checking.ID, Type: gm.Type, AnnualRate: gm.AnnualRate, StartDate: gm.StartDate, EventID: ev.ID,
	}); err != nil {
		t.Fatalf("save growth model: %v", err)
	}

	view, err := svc.GetEventEditPageData(t.Context(), ev.ID)
	if err != nil {
		t.Fatalf("event page: %v", err)
	}
	if len(view.Event.Ends) != 0 {
		t.Fatalf("expected an Event to end no entry in it, got %+v", view.Event.Ends)
	}
}
