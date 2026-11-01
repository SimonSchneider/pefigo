package model_test

import (
	"testing"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
)

func mustBillEnd(t *testing.T, svc *model.Service, end model.BillEnd) model.BillEnd {
	t.Helper()
	saved, err := svc.UpsertBillEnd(t.Context(), end)
	if err != nil {
		t.Fatalf("save bill end: %v", err)
	}
	return saved
}

func TestAnEventAddsABillInAnotherCurrencyFromItsDate(t *testing.T) {
	svc := newTestService(t, newRateServer(t).option())
	ctx := t.Context()
	checking := mustAccount(t, svc, "Checking")
	subscriptions := mustBillAccount(t, svc, "Subscriptions", checking.ID)
	abroad := mustEvent(t, svc, model.EventInput{Name: "Moving abroad", Date: mustParseDate("2027-06-01")})

	bill, err := svc.UpsertBill(ctx, model.Bill{Name: "Streaming", BillAccountID: subscriptions.ID, Enabled: true, EventID: abroad.ID})
	if err != nil {
		t.Fatalf("save bill: %v", err)
	}
	amount, err := svc.UpsertBillAmount(ctx, model.BillAmount{
		BillID: bill.ID, Amount: newFixedValue(100), Currency: "EUR", Period: model.BillPeriodMonthly, EventID: abroad.ID, StartOffset: days(0),
	})
	if err != nil {
		t.Fatalf("save bill amount: %v", err)
	}

	if amount.StartDate != abroad.Date {
		t.Fatalf("expected the Bill Amount from the Event's date, got %s", amount.StartDate)
	}
	// 100 EUR at the Exchange Rate of 11.47 SEK.
	for day, want := range map[string]float64{"2027-05-01": 0, "2027-06-01": 1147, "2027-07-01": 1147} {
		if got := chargedOn(t, svc, day)["Streaming"]; !approxEqual(got, want, 0.01) {
			t.Errorf("on %s: expected Streaming to charge %v, got %v", day, want, got)
		}
	}

	moveEvent(t, svc, abroad, "2027-08-01")
	for day, want := range map[string]float64{"2027-07-01": 0, "2027-08-01": 1147} {
		if got := chargedOn(t, svc, day)["Streaming"]; !approxEqual(got, want, 0.01) {
			t.Errorf("on %s with the Event moved to 2027-08-01: expected Streaming to charge %v, got %v", day, want, got)
		}
	}
}

func TestAnEventEndsABillOnItsDate(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	checking := mustAccount(t, svc, "Checking")
	household := mustBillAccount(t, svc, "Household", checking.ID)
	gym := mustBillIn(t, svc, household, "Gym",
		model.BillAmount{Amount: newFixedValue(300), StartDate: mustParseDate("2027-01-01")},
		model.BillAmount{Amount: newFixedValue(400), StartDate: mustParseDate("2027-09-01")},
	)
	move := mustEvent(t, svc, model.EventInput{Name: "Move", Date: mustParseDate("2027-06-01")})

	end := mustBillEnd(t, svc, model.BillEnd{BillID: gym.ID, EventID: move.ID, DateOffset: days(0)})

	if end.Date != move.Date {
		t.Fatalf("expected the end on the Event's date, got %s", end.Date)
	}
	for day, want := range map[string]float64{"2027-05-01": 300, "2027-06-01": 0, "2027-09-01": 0, "2028-01-01": 0} {
		if got := chargedOn(t, svc, day)["Gym"]; got != want {
			t.Errorf("on %s: expected Gym to charge %v, got %v", day, want, got)
		}
	}

	edit, err := svc.GetBillEditPageData(ctx, gym.ID)
	if err != nil {
		t.Fatalf("bill edit page: %v", err)
	}
	if len(edit.Bill.Ends) != 1 || edit.Bill.Ends[0].EventID != move.ID || edit.Bill.Ends[0].Date != move.Date {
		t.Errorf("expected the Gym's edit page to list its end in the Move, got %+v", edit.Bill.Ends)
	}
}

func TestTheBillsPageShowsWhatTheBaselineCharges(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	checking := mustAccount(t, svc, "Checking")
	household := mustBillAccount(t, svc, "Household", checking.ID)
	lastMonth := date.Today().Add(-30 * date.Day)
	ended := mustEvent(t, svc, model.EventInput{Name: "Cancel", Date: lastMonth, Planned: ptr(false)})
	gym := mustBillIn(t, svc, household, "Gym", model.BillAmount{Amount: newFixedValue(300), StartDate: mustParseDate("2025-01-01")})
	mustBillEnd(t, svc, model.BillEnd{BillID: gym.ID, EventID: ended.ID, DateOffset: days(0)})

	currentAmount := func(t *testing.T) float64 {
		t.Helper()
		page, err := svc.GetBillAccountsPageData(ctx)
		if err != nil {
			t.Fatalf("bills page: %v", err)
		}
		return page.BillAccounts[0].Bills[0].CurrentAmount()
	}
	if got := currentAmount(t); got != 300 {
		t.Errorf("expected the unplanned Cancel not to end the Gym in the Baseline, got %v a month", got)
	}
	moveEvent(t, svc, model.Event{ID: ended.ID, Name: ended.Name, Date: ended.Date, Planned: true}, ended.Date.String())
	if got := currentAmount(t); got != 0 {
		t.Errorf("expected the planned Cancel to have ended the Gym, got %v a month", got)
	}
}
