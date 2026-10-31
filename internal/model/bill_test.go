package model_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SimonSchneider/pefigo/internal/model"
	"github.com/SimonSchneider/pefigo/pkg/currency"
)

// rateServer serves the Exchange Rate of 1 EUR, 11.47 SEK, until it is made to fail.
type rateServer struct {
	*httptest.Server
	failing atomic.Bool
}

func newRateServer(t *testing.T) *rateServer {
	t.Helper()
	srv := &rateServer{}
	srv.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if srv.failing.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		fmt.Fprint(w, `{"amount":1.0,"base":"EUR","date":"2026-03-25","rates":{"SEK":11.47}}`)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func (srv *rateServer) option() model.ServiceOption {
	return model.WithCurrencyOptions(currency.WithBaseURL(srv.URL))
}

// mustEuroBill saves a Bill of 100 EUR a month, from 2025-01-01, in its own Bill account.
func mustEuroBill(t *testing.T, svc *model.Service) model.Bill {
	t.Helper()
	return mustBill(t, svc, mustAccount(t, svc, "Checking").ID, "*-*-01", model.Bill{Name: "Spotify EU"},
		model.BillAmount{Amount: newFixedValue(100), Period: model.BillPeriodMonthly, Currency: "EUR", StartDate: mustParseDate("2025-01-01")})
}

func TestEveryBillConsumerGetsBillAmountsInTheDefaultCurrency(t *testing.T) {
	svc := newTestService(t, newRateServer(t).option())
	ctx := t.Context()
	bill := mustEuroBill(t, svc)

	page, err := svc.GetBillAccountsPageData(ctx)
	if err != nil {
		t.Fatalf("get bills page data: %v", err)
	}
	accountEdit, err := svc.GetBillAccountEditPageData(ctx, bill.BillAccountID)
	if err != nil {
		t.Fatalf("get bill account edit page data: %v", err)
	}
	billEdit, err := svc.GetBillEditPageData(ctx, bill.ID)
	if err != nil {
		t.Fatalf("get bill edit page data: %v", err)
	}
	for name, got := range map[string]model.Bill{
		"Bills page":             page.BillAccounts[0].Bills[0],
		"Bill account edit page": accountEdit.BillAccount.Bills[0],
		"Bill edit page":         billEdit.Bill,
	} {
		if !approxEqual(got.CurrentAmount(), 1147, 0.01) || !approxEqual(got.YearlyAmount(), 13764, 0.01) {
			t.Errorf("%s: got %v a month and %v a year, want 1 147 and 13 764 SEK", name, got.CurrentAmount(), got.YearlyAmount())
		}
	}
	if amt := billEdit.Bill.Amounts[0]; amt.Amount.Mean() != 100 || amt.Currency != "EUR" {
		t.Errorf("expected the Bill Amount to keep its 100 EUR for the edit form, got %v %s", amt.Amount.Mean(), amt.Currency)
	}

	var charged []float64
	for _, tt := range allTransferTemplates(t, svc) {
		if tt.Source.EntityID == bill.ID {
			charged = append(charged, tt.AmountFixed.Mean())
		}
	}
	if len(charged) != 1 || !approxEqual(charged[0], 1147, 0.01) {
		t.Errorf("expected the Transfer Schedule to charge 1 147 SEK, got %v", charged)
	}
}

func TestAYearlyBillAmountInAnotherCurrencyIsConvertedMonthlyAndYearly(t *testing.T) {
	svc := newTestService(t, newRateServer(t).option())
	mustBill(t, svc, mustAccount(t, svc, "Checking").ID, "*-*-01", model.Bill{Name: "Insurance"},
		model.BillAmount{Amount: newFixedValue(1200), Period: model.BillPeriodYearly, Currency: "EUR", StartDate: mustParseDate("2025-01-01")})

	page, err := svc.GetBillAccountsPageData(t.Context())
	if err != nil {
		t.Fatalf("get bills page data: %v", err)
	}
	got := page.BillAccounts[0].Bills[0]
	if !approxEqual(got.CurrentAmount(), 1147, 0.01) || !approxEqual(got.YearlyAmount(), 13764, 0.01) {
		t.Errorf("got %v a month and %v a year, want 1 147 and 13 764 SEK", got.CurrentAmount(), got.YearlyAmount())
	}
}

func TestABillInTheDefaultCurrencyIsNotConverted(t *testing.T) {
	srv := newRateServer(t)
	srv.failing.Store(true)
	svc := newTestService(t, srv.option())
	mustBill(t, svc, mustAccount(t, svc, "Checking").ID, "*-*-01", model.Bill{Name: "Rent"},
		model.BillAmount{Amount: newFixedValue(200), Currency: "SEK", StartDate: mustParseDate("2025-01-01")},
	)
	mustBill(t, svc, mustAccount(t, svc, "Savings").ID, "*-*-01", model.Bill{Name: "Gym"},
		model.BillAmount{Amount: newFixedValue(300), StartDate: mustParseDate("2025-01-01")},
	)

	page, err := svc.GetBillAccountsPageData(t.Context())
	if err != nil {
		t.Fatalf("expected Bills in the Default Currency to need no Exchange Rate, got %v", err)
	}
	for _, ba := range page.BillAccounts {
		for _, b := range ba.Bills {
			if want := map[string]float64{"Rent": 200, "Gym": 300}[b.Name]; b.CurrentAmount() != want {
				t.Errorf("%s: got %v, want %v", b.Name, b.CurrentAmount(), want)
			}
		}
	}
}

func TestABillConvertsAtTheLastRateFetchedWhenTheRateServerFails(t *testing.T) {
	srv := newRateServer(t)
	svc := newTestService(t, srv.option())
	ctx := t.Context()
	mustEuroBill(t, svc)
	if _, err := svc.GetBillAccountsPageData(ctx); err != nil {
		t.Fatalf("get bills page data: %v", err)
	}
	// The rate was fetched a year ago, and the rate server now fails.
	if _, err := svc.DB().ExecContext(ctx, "UPDATE api_cache SET created_at = ?", time.Now().AddDate(-1, 0, 0).Unix()); err != nil {
		t.Fatalf("age the cached rate: %v", err)
	}
	srv.failing.Store(true)

	page, err := svc.GetBillAccountsPageData(ctx)
	if err != nil {
		t.Fatalf("expected the year-old rate, got %v", err)
	}
	if got := page.BillAccounts[0].Bills[0].CurrentAmount(); !approxEqual(got, 1147, 0.01) {
		t.Errorf("got %v, want 1 147 SEK at the year-old rate", got)
	}
}

func TestABillWithoutAnyExchangeRateFetchedFailsNamingTheCurrencyPair(t *testing.T) {
	srv := newRateServer(t)
	srv.failing.Store(true)
	svc := newTestService(t, srv.option())
	ctx := t.Context()
	bill := mustEuroBill(t, svc)

	_, pageErr := svc.GetBillAccountsPageData(ctx)
	_, accountErr := svc.GetBillAccountEditPageData(ctx, bill.BillAccountID)
	_, billErr := svc.GetBillEditPageData(ctx, bill.ID)
	_, scheduleErr := svc.TransferSchedule(ctx)
	for name, err := range map[string]error{
		"Bills page":             pageErr,
		"Bill account edit page": accountErr,
		"Bill edit page":         billErr,
		"Transfer Schedule":      scheduleErr,
	} {
		if err == nil || !strings.Contains(err.Error(), "EUR to SEK") {
			t.Errorf("%s: expected an error naming EUR to SEK, got %v", name, err)
		}
	}
}

func TestAFailedDefaultCurrencyReadFailsTheBills(t *testing.T) {
	svc := newTestService(t, newRateServer(t).option())
	ctx := t.Context()
	mustEuroBill(t, svc)
	if _, err := svc.DB().ExecContext(ctx, "DROP TABLE app_setting"); err != nil {
		t.Fatalf("drop settings: %v", err)
	}

	if _, err := svc.GetBillAccountsPageData(ctx); err == nil {
		t.Error("expected the Bills page to fail without the Default Currency")
	}
	if _, err := svc.TransferSchedule(ctx); err == nil {
		t.Error("expected the Transfer Schedule to fail without the Default Currency")
	}
}
