package controller_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
	"github.com/SimonSchneider/pefigo/pkg/currency"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

// rowWith is the table row of page that names name, or "" without one.
func rowWith(page, name string) string {
	for _, row := range strings.Split(page, "<tr")[1:] {
		row, _, _ = strings.Cut(row, "</tr>")
		if strings.Contains(row, ">"+name+"<") {
			return row
		}
	}
	return ""
}

// unplannedEntriesServer is a server with Salary A and Power in the Baseline, Salary B and Rent in an
// unplanned Move, and Streaming, in EUR, in the Move too, whose Exchange Rate can't be fetched.
func unplannedEntriesServer(t *testing.T) *httptest.Server {
	t.Helper()
	rates := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(rates.Close)
	svc, srv := newSettingsServer(t, model.WithCurrencyOptions(currency.WithBaseURL(rates.URL)))
	ctx := t.Context()
	checking, err := svc.UpsertAccount(ctx, model.AccountInput{Name: "Checking"})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	move, err := svc.UpsertEvent(ctx, model.EventInput{Name: "Move", Date: date.Today().Add(-30 * date.Day), Planned: new(bool)})
	if err != nil {
		t.Fatalf("create event: %v", err)
	}
	atTheEvent := date.Duration(0)
	for _, sal := range []model.Salary{
		{Name: "Salary A", ToAccountID: checking.ID, Recurrence: "*-*-25", Enabled: true},
		{Name: "Salary B", ToAccountID: checking.ID, Recurrence: "*-*-25", Enabled: true, EventID: move.ID},
	} {
		saved, err := svc.UpsertSalary(ctx, sal)
		if err != nil {
			t.Fatalf("create salary: %v", err)
		}
		amount := model.SalaryAmount{SalaryID: saved.ID, Amount: uncertain.NewFixed(30000), StartDate: date.Today().Add(-60 * date.Day)}
		if sal.EventID != "" {
			amount = model.SalaryAmount{SalaryID: saved.ID, Amount: uncertain.NewFixed(40000), EventID: move.ID, StartOffset: &atTheEvent}
		}
		if _, err := svc.UpsertSalaryAmount(ctx, amount); err != nil {
			t.Fatalf("create salary amount: %v", err)
		}
	}
	household, err := svc.UpsertBillAccount(ctx, model.BillAccount{Name: "Household", FromAccountID: checking.ID, Recurrence: "*-*-01", Enabled: true})
	if err != nil {
		t.Fatalf("create bill account: %v", err)
	}
	for _, bill := range []struct {
		model.Bill
		model.BillAmount
	}{
		{model.Bill{Name: "Power"}, model.BillAmount{Amount: uncertain.NewFixed(500), StartDate: date.Today().Add(-60 * date.Day)}},
		{model.Bill{Name: "Rent", EventID: move.ID}, model.BillAmount{Amount: uncertain.NewFixed(9000), EventID: move.ID, StartOffset: &atTheEvent}},
		{model.Bill{Name: "Streaming", EventID: move.ID}, model.BillAmount{Amount: uncertain.NewFixed(10), Currency: "EUR", EventID: move.ID, StartOffset: &atTheEvent}},
	} {
		bill.Bill.BillAccountID, bill.Bill.Enabled = household.ID, true
		saved, err := svc.UpsertBill(ctx, bill.Bill)
		if err != nil {
			t.Fatalf("create bill: %v", err)
		}
		bill.BillAmount.BillID = saved.ID
		if _, err := svc.UpsertBillAmount(ctx, bill.BillAmount); err != nil {
			t.Fatalf("create bill amount: %v", err)
		}
	}
	return srv
}

func TestTheListsMarkWhatUnplannedEventsWouldPayAndChargeAsNotInTheBaseline(t *testing.T) {
	srv := unplannedEntriesServer(t)

	for path, names := range map[string][2]string{"/salaries": {"Salary A", "Salary B"}, "/bills": {"Power", "Rent"}} {
		page := getPage(t, srv, path)
		inBaseline, unplanned := rowWith(page, names[0]), rowWith(page, names[1])
		if inBaseline == "" || unplanned == "" {
			t.Fatalf("expected %s to list %s and %s", path, names[0], names[1])
		}
		if strings.Contains(inBaseline, "not in the Baseline") {
			t.Errorf("expected %s not to mark %s, in the Baseline", path, names[0])
		}
		if !strings.Contains(unplanned, "not in the Baseline") {
			t.Errorf("expected %s to mark what %s would pay or charge as not in the Baseline", path, names[1])
		}
	}
}

func TestAnUnplannedBillWhoseExchangeRateCantBeFetchedShowsAsUnavailable(t *testing.T) {
	srv := unplannedEntriesServer(t)

	if row := rowWith(getPage(t, srv, "/bills"), "Streaming"); !strings.Contains(row, "unavailable") {
		t.Errorf("expected the Bills page to say what the Streaming would charge is unavailable, got %s", row)
	}
	if row := rowWith(getPage(t, srv, "/transfer-templates"), "Streaming"); !strings.Contains(row, "unavailable") || !strings.Contains(row, "EUR") {
		t.Errorf("expected the Transfer Templates page to list the Streaming as unavailable, and why, got %s", row)
	}
}
