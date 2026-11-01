package model_test

import (
	"errors"
	"testing"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
)

func days(n int64) *date.Duration {
	d := date.Duration(n)
	return &d
}

func TestAnEntryInAnEventIsGivenInDaysAfterTheEventsDate(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	apartment := mustEvent(t, svc, model.EventInput{Name: "Apartment", Date: mustParseDate("2027-09-10")})
	property := mustEventAccount(t, svc, "Property", apartment.ID)
	amortization := mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Amortization", FromAccountID: checking.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(5000),
		Recurrence: "*-*-25", StartOffset: days(20), EndOffset: days(100), Enabled: true, EventID: apartment.ID,
	})
	growth := mustGrowthModel(t, svc, model.AccountGrowthModelInput{
		AccountID: property.ID, AnnualRate: newFixedValue(0.02), StartOffset: days(-5), EndOffset: days(30), EventID: apartment.ID,
	})

	if amortization.StartDate != mustParseDate("2027-09-30") || dateOf(amortization.EndDate) != "2027-12-19" {
		t.Errorf("expected Amortization from 2027-09-30 to 2027-12-19, 20 and 100 days after the Event, got %s to %s", amortization.StartDate, dateOf(amortization.EndDate))
	}
	if growth.StartDate != mustParseDate("2027-09-05") || dateOf(growth.EndDate) != "2027-10-10" {
		t.Errorf("expected the growth model from 2027-09-05 to 2027-10-10, 5 days before and 30 after the Event, got %s to %s", growth.StartDate, dateOf(growth.EndDate))
	}

	moveEvent(t, svc, apartment, "2027-10-10")

	amortization = mustGetTransferTemplate(t, svc, amortization.ID)
	if amortization.StartDate != mustParseDate("2027-10-30") {
		t.Errorf("expected Amortization to stay 20 days after the moved Event, from 2027-10-30, got %s", amortization.StartDate)
	}
	growth = mustGetGrowthModel(t, svc, growth.ID)
	if growth.StartDate != mustParseDate("2027-10-05") {
		t.Errorf("expected the growth model to stay 5 days before the moved Event, from 2027-10-05, got %s", growth.StartDate)
	}
}

func TestDaysAfterAnEventsDateNeedAnEvent(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	gym := model.TransferTemplate{
		Name: "Gym", FromAccountID: checking.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(300),
		Recurrence: "*-*-01", StartDate: mustParseDate("2027-01-01"), Enabled: true,
	}
	startIn20 := gym
	startIn20.StartOffset = days(20)
	endIn20 := gym
	endIn20.EndOffset = days(20)
	for _, inp := range []model.TransferTemplate{startIn20, endIn20} {
		if _, err := svc.UpsertTransferTemplate(t.Context(), inp); !errors.Is(err, model.ErrInvalidInput) {
			t.Errorf("expected days after an Event's date to need an Event, got %v", err)
		}
	}
	if _, err := svc.UpsertAccountGrowthModel(t.Context(), model.AccountGrowthModelInput{
		AccountID: checking.ID, Type: model.GrowthModelFixed, StartOffset: days(0),
	}); !errors.Is(err, model.ErrInvalidInput) {
		t.Errorf("expected a growth model's days after an Event's date to need an Event, got %v", err)
	}
}

func TestATransferTemplateWithoutARecurrenceIsAOneOffOnItsStartDate(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	bonus := mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Bonus", ToAccountID: checking.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(20000),
		StartDate: mustParseDate("2027-03-01"), Enabled: true,
	})
	if bonus.Recurring() || bonus.Recurrence != "2027-03-01" {
		t.Errorf("expected Bonus to be a one-off on its Start Date 2027-03-01, got %q", bonus.Recurrence)
	}

	apartment := mustEvent(t, svc, model.EventInput{Name: "Apartment", Date: mustParseDate("2027-09-10")})
	purchase := mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Purchase", FromAccountID: checking.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(600000),
		StartOffset: days(5), Enabled: true, EventID: apartment.ID,
	})
	if purchase.Recurrence != "2027-09-15" {
		t.Errorf("expected Purchase to be a one-off 5 days after the Event, on 2027-09-15, got %q", purchase.Recurrence)
	}
	moveEvent(t, svc, apartment, "2027-10-10")
	if purchase = mustGetTransferTemplate(t, svc, purchase.ID); purchase.Recurrence != "2027-10-15" {
		t.Errorf("expected Purchase to stay 5 days after the moved Event, on 2027-10-15, got %q", purchase.Recurrence)
	}
}

func TestTheTransferTemplateFormGivesAnEntryInAnEventInDaysAfterItsDate(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	apartment := mustEvent(t, svc, model.EventInput{Name: "Apartment", Date: mustParseDate("2027-09-10")})
	amortization := mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Amortization", FromAccountID: checking.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(5000),
		Recurrence: "*-*-25", StartOffset: days(20), EndOffset: days(100), Enabled: true, EventID: apartment.ID,
	})
	purchase := mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Purchase", FromAccountID: checking.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(600000),
		StartOffset: days(-3), Enabled: true, EventID: apartment.ID,
	})
	gym := mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Gym", FromAccountID: checking.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(300),
		Recurrence: "*-*-01", StartDate: mustParseDate("2027-01-01"), EndDate: ptr(mustParseDate("2028-01-01")), Enabled: true,
	})

	form := func(id string) *model.TransferTemplateEditView {
		t.Helper()
		view, err := svc.GetTransferTemplateEditPageData(t.Context(), id)
		if err != nil {
			t.Fatalf("get transfer template edit page: %v", err)
		}
		return view
	}
	if f := form(amortization.ID); f.StartInput() != "+20" || f.EndInput() != "+100" || f.RecurrenceInput() != "*-*-25" {
		t.Errorf("expected Amortization's form to give +20 to +100 on *-*-25, got %q to %q on %q", f.StartInput(), f.EndInput(), f.RecurrenceInput())
	}
	if f := form(purchase.ID); f.StartInput() != "-3" || f.EndInput() != "" || f.RecurrenceInput() != "" {
		t.Errorf("expected Purchase's form to give a one-off on -3, with no recurrence, got %q to %q on %q", f.StartInput(), f.EndInput(), f.RecurrenceInput())
	}
	if f := form(gym.ID); f.StartInput() != "2027-01-01" || f.EndInput() != "2028-01-01" || f.RecurrenceInput() != "*-*-01" {
		t.Errorf("expected Gym's form, outside an Event, to give its dates, got %q to %q on %q", f.StartInput(), f.EndInput(), f.RecurrenceInput())
	}
	inApartment, err := svc.GetTransferTemplateNewPageData(t.Context(), apartment.ID)
	if err != nil {
		t.Fatalf("get new transfer template page: %v", err)
	}
	if inApartment.StartInput() != "" || inApartment.Event() == nil || inApartment.Event().Name != "Apartment" {
		t.Errorf("expected a new Transfer Template's form in the Apartment to have no Start and name its Event, got %q in %+v", inApartment.StartInput(), inApartment.Event())
	}
}

func TestAnEntryChangingItsEventKeepsTheDatesItsFormGave(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	checking := mustAccount(t, svc, "Checking")
	apartment := mustEvent(t, svc, model.EventInput{Name: "Apartment", Date: mustParseDate("2027-09-10")})
	wedding := mustEvent(t, svc, model.EventInput{Name: "Wedding", Date: mustParseDate("2028-01-01")})
	amortization := model.TransferTemplate{
		Name: "Amortization", FromAccountID: checking.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(5000),
		Recurrence: "*-*-25", StartOffset: days(20), Enabled: true, EventID: apartment.ID,
	}
	amortization = mustTransferTemplate(t, svc, amortization)
	save := func(eventID, offsetEventID string, start int64) model.TransferTemplate {
		t.Helper()
		inp := amortization
		inp.EventID, inp.OffsetEventID, inp.StartOffset = eventID, offsetEventID, days(start)
		tt, err := svc.UpsertTransferTemplate(ctx, inp)
		if err != nil {
			t.Fatalf("save Amortization: %v", err)
		}
		return tt
	}

	if tt := save(apartment.ID, apartment.ID, 30); tt.StartDate != mustParseDate("2027-10-10") {
		t.Errorf("expected +30 in the same Event to move the start to 2027-10-10, got %s", tt.StartDate)
	}
	if tt := save(wedding.ID, apartment.ID, 30); tt.EventID != wedding.ID || tt.StartDate != mustParseDate("2027-10-10") {
		t.Errorf("expected joining the Wedding to keep 2027-10-10, got %s in %q", tt.StartDate, tt.EventID)
	}
	moveEvent(t, svc, wedding, "2028-01-11")
	if tt := mustGetTransferTemplate(t, svc, amortization.ID); tt.StartDate != mustParseDate("2027-10-20") {
		t.Errorf("expected Amortization to move with the Wedding to 2027-10-20, got %s", tt.StartDate)
	}
	if tt := save("", wedding.ID, -83); tt.EventID != "" || tt.StartDate != mustParseDate("2027-10-20") {
		t.Errorf("expected leaving the Wedding to keep 2027-10-20, got %s in %q", tt.StartDate, tt.EventID)
	}

	property := mustEventAccount(t, svc, "Property", apartment.ID)
	growth := mustGrowthModel(t, svc, model.AccountGrowthModelInput{AccountID: property.ID, StartOffset: days(-5), EventID: apartment.ID})
	growth = mustGrowthModel(t, svc, model.AccountGrowthModelInput{ID: growth.ID, AccountID: property.ID, StartOffset: days(-5), OffsetEventID: apartment.ID})
	if growth.EventID != "" || growth.StartDate != mustParseDate("2027-09-05") {
		t.Errorf("expected the growth model leaving the Apartment to keep 2027-09-05, got %s in %q", growth.StartDate, growth.EventID)
	}
}
