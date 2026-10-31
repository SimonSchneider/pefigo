package controller_test

import (
	"net/url"
	"strings"
	"testing"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

func mustDate(t *testing.T, s string) date.Date {
	t.Helper()
	d, err := date.ParseDate(s)
	if err != nil {
		t.Fatalf("parse %s: %v", s, err)
	}
	return d
}

func TestANewEventFromTheFormIsPlannedUnlessMarkedUnplanned(t *testing.T) {
	svc, srv := newSettingsServer(t)

	postForm(t, srv, "/events/", url.Values{"name": {"Apartment"}, "date": {"2028-03-01"}})
	postForm(t, srv, "/events/", url.Values{"name": {"Sabbatical"}, "date": {"2029-01-01"}, "unplanned": {"on"}})

	events, err := svc.ListEvents(t.Context())
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) != 2 || !events[0].Planned || events[1].Planned {
		t.Fatalf("expected a planned Apartment and an unplanned Sabbatical, got %+v", events)
	}
	page := getPage(t, srv, "/events")
	for _, want := range []string{"Apartment", "2028-03-01", "Sabbatical", "Unplanned"} {
		if !strings.Contains(page, want) {
			t.Errorf("expected the Events page to show %q", want)
		}
	}
}

func TestEntryFormsPutTheirEntryInAnEvent(t *testing.T) {
	svc, srv := newSettingsServer(t)
	ctx := t.Context()
	ev, err := svc.UpsertEvent(ctx, model.EventInput{Name: "Apartment", Date: mustDate(t, "2028-03-01")})
	if err != nil {
		t.Fatalf("create event: %v", err)
	}

	location := postForm(t, srv, "/accounts/", url.Values{"name": {"Property"}, "event_id": {ev.ID}})
	accountID := strings.Split(location, "/")[2]
	if acc, err := svc.GetAccount(ctx, accountID); err != nil || acc.EventID != ev.ID {
		t.Fatalf("expected the account in the Event, got %+v (%v)", acc, err)
	}
	if page := getPage(t, srv, location); !strings.Contains(page, `value="`+ev.ID+`" selected`) {
		t.Errorf("expected the account form to select its Event")
	}

	postForm(t, srv, "/growth-models/", url.Values{"account_id": {accountID}, "type": {"fixed"}, "event_id": {ev.ID}})
	growth, err := svc.ListAccountGrowthModels(ctx, accountID)
	if err != nil || len(growth) != 1 || growth[0].EventID != ev.ID || growth[0].StartDate != ev.Date {
		t.Fatalf("expected a growth model in the Event from its date, got %+v (%v)", growth, err)
	}

	location = postForm(t, srv, "/transfers/", url.Values{
		"name": {"Purchase"}, "to_account_id": {accountID}, "amount_type": {"fixed"}, "amount_fixed": {"3000000"}, "recurrence": {"2028-03-01"}, "event_id": {ev.ID}, "enabled": {"on"},
	})
	ttID := strings.Split(location, "/")[2]
	tt, err := svc.GetTransferTemplate(ctx, ttID)
	if err != nil || tt.EventID != ev.ID || tt.StartDate != ev.Date {
		t.Fatalf("expected the Transfer Template in the Event from its date, got %+v (%v)", tt, err)
	}
	if page := getPage(t, srv, "/transfer-templates"); !strings.Contains(page, "Apartment") {
		t.Errorf("expected the Transfer Templates page to show the Event of Purchase")
	}
	if page := getPage(t, srv, "/accounts"); !strings.Contains(page, "Apartment") {
		t.Errorf("expected the accounts page to show the Event of Property")
	}
}

func TestTheEventPageAddsAndRemovesEntriesAndEnds(t *testing.T) {
	svc, srv := newSettingsServer(t)
	ctx := t.Context()
	checking, err := svc.UpsertAccount(ctx, model.AccountInput{Name: "Checking"})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	savings, err := svc.UpsertTransferTemplate(ctx, model.TransferTemplate{
		Name: "Savings", FromAccountID: checking.ID, AmountType: model.TransferAmountFixed, AmountFixed: uncertain.NewFixed(1000),
		Recurrence: "*-*-25", StartDate: mustDate(t, "2027-01-25"), Enabled: true,
	})
	if err != nil {
		t.Fatalf("create transfer template: %v", err)
	}
	gym, err := svc.UpsertTransferTemplate(ctx, model.TransferTemplate{
		Name: "Gym", FromAccountID: checking.ID, AmountType: model.TransferAmountFixed, AmountFixed: uncertain.NewFixed(400),
		Recurrence: "*-*-05", StartDate: mustDate(t, "2027-01-01"), Enabled: true,
	})
	if err != nil {
		t.Fatalf("create transfer template: %v", err)
	}
	location := postForm(t, srv, "/events/", url.Values{"name": {"Raise"}, "date": {"2027-01-01"}})
	eventID := strings.Split(location, "/")[2]
	if location != "/events/"+eventID+"/edit" {
		t.Fatalf("expected saving an Event to lead to its edit page, got %s", location)
	}
	savingsEntry := model.EventEntry{Kind: model.EventEntryTransferTemplate, ID: savings.ID}.String()
	gymEntry := model.EventEntry{Kind: model.EventEntryTransferTemplate, ID: gym.ID}.String()
	if page := getPage(t, srv, location); !strings.Contains(page, `value="`+savingsEntry+`"`) {
		t.Fatalf("expected the edit page to offer adding Savings")
	}

	if got := postForm(t, srv, "/events/"+eventID+"/entries/", url.Values{"entry": {savingsEntry}}); got != location {
		t.Errorf("expected adding an entry to lead back to the Event, got %s", got)
	}
	if tt, err := svc.GetTransferTemplate(ctx, savings.ID); err != nil || tt.EventID != eventID || tt.StartDate != mustDate(t, "2027-01-25") {
		t.Fatalf("expected Savings in the Event, still from 2027-01-25, got %+v (%v)", tt, err)
	}
	postForm(t, srv, "/events/"+eventID+"/ends/", url.Values{"entry": {gymEntry}, "end_date": {"2027-03-01"}})
	page := getPage(t, srv, location)
	for _, want := range []string{"Savings", "+24 days", "Gym", "2027-03-01"} {
		if !strings.Contains(page, want) {
			t.Errorf("expected the Event page to show %q", want)
		}
	}

	postForm(t, srv, "/events/"+eventID+"/entries/remove", url.Values{"entry": {savingsEntry}})
	if tt, err := svc.GetTransferTemplate(ctx, savings.ID); err != nil || tt.EventID != "" {
		t.Fatalf("expected Savings back in the Baseline, got %+v (%v)", tt, err)
	}
	postForm(t, srv, "/events/"+eventID+"/ends/remove", url.Values{"entry": {gymEntry}})
	view, err := svc.GetEventEditPageData(ctx, eventID)
	if err != nil || len(view.Event.Ends) != 0 {
		t.Fatalf("expected the end of Gym lifted, got %+v (%v)", view, err)
	}
}

func TestDeletingAnEventIsConfirmedWithWhatItDeletes(t *testing.T) {
	svc, srv := newSettingsServer(t)
	ctx := t.Context()
	ev, err := svc.UpsertEvent(ctx, model.EventInput{Name: "Apartment", Date: mustDate(t, "2028-03-01")})
	if err != nil {
		t.Fatalf("create event: %v", err)
	}
	property, err := svc.UpsertAccount(ctx, model.AccountInput{Name: "Property", EventID: ev.ID})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}

	if page := getPage(t, srv, "/events/"+ev.ID+"/edit"); !strings.Contains(page, `href="/events/`+ev.ID+`/delete"`) {
		t.Fatalf("expected the edit page to link to the delete confirmation")
	}
	page := getPage(t, srv, "/events/"+ev.ID+"/delete")
	if !strings.Contains(page, "Property") || !strings.Contains(page, `action="/events/`+ev.ID+`/delete"`) {
		t.Fatalf("expected the confirmation to list Property and post the delete")
	}

	if location := postForm(t, srv, "/events/"+ev.ID+"/delete", nil); location != "/events" {
		t.Errorf("expected deleting an Event to lead to the Events page, got %s", location)
	}
	if _, err := svc.GetAccount(ctx, property.ID); err == nil {
		t.Errorf("expected the Event's account deleted")
	}
}
