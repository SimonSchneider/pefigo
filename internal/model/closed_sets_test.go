package model_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/SimonSchneider/pefigo/internal/model"
	"github.com/SimonSchneider/pefigo/pkg/currency"
)

func TestSavingAGrowthModelTypeOutsideFixedAndLognormalIsInvalidInput(t *testing.T) {
	svc := newTestService(t)
	acc := mustAccount(t, svc, "Savings")
	save := func(modelType string) (model.GrowthModel, error) {
		return svc.UpsertAccountGrowthModel(t.Context(), model.AccountGrowthModelInput{
			AccountID: acc.ID, Type: modelType, AnnualRate: newFixedValue(0.05), AnnualVolatility: newFixedValue(0.1), StartDate: mustParseDate("2026-01-01"),
		})
	}

	for _, modelType := range []string{"", "linear", "Fixed"} {
		if _, err := save(modelType); !errors.Is(err, model.ErrInvalidInput) {
			t.Errorf("growth model type %q: expected ErrInvalidInput, got %v", modelType, err)
		}
	}
	for _, modelType := range []string{"fixed", "lognormal"} {
		gm, err := save(modelType)
		if err != nil {
			t.Fatalf("growth model type %q: %v", modelType, err)
		}
		if gm.Type != modelType {
			t.Errorf("expected the growth model type %q, got %q", modelType, gm.Type)
		}
	}
}

func TestSavingATransferTemplateAmountTypeOutsideFixedAndPercentIsInvalidInput(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	save := func(amountType string) (model.TransferTemplate, error) {
		return svc.UpsertTransferTemplate(t.Context(), model.TransferTemplate{
			Name: "Savings", FromAccountID: checking.ID, AmountType: amountType, AmountFixed: newFixedValue(300), AmountPercent: 0.1,
			Recurrence: "*-*-25", StartDate: mustParseDate("2026-01-01"), Enabled: true,
		})
	}

	for _, amountType := range []string{"", "fraction", "Fixed"} {
		if _, err := save(amountType); !errors.Is(err, model.ErrInvalidInput) {
			t.Errorf("amount type %q: expected ErrInvalidInput, got %v", amountType, err)
		}
	}
	for _, amountType := range []string{"fixed", "percent"} {
		tt, err := save(amountType)
		if err != nil {
			t.Fatalf("amount type %q: %v", amountType, err)
		}
		if tt.AmountType != amountType {
			t.Errorf("expected the amount type %q, got %q", amountType, tt.AmountType)
		}
	}
}

// mustBillForAmounts saves a Bill, in its own Bill account, to save Bill Amounts on.
func mustBillForAmounts(t *testing.T, svc *model.Service) model.Bill {
	t.Helper()
	return mustBill(t, svc, mustAccount(t, svc, "Checking").ID, "*-*-01", model.Bill{Name: "Streaming"})
}

func TestSavingABillAmountPeriodOutsideMonthlyAndYearlyIsInvalidInput(t *testing.T) {
	svc := newTestService(t)
	bill := mustBillForAmounts(t, svc)
	save := func(period string) (model.BillAmount, error) {
		return svc.UpsertBillAmount(t.Context(), model.BillAmount{BillID: bill.ID, Amount: newFixedValue(100), Period: period, StartDate: mustParseDate("2026-01-01")})
	}

	for _, period := range []string{"weekly", "Monthly"} {
		if _, err := save(period); !errors.Is(err, model.ErrInvalidInput) {
			t.Errorf("period %q: expected ErrInvalidInput, got %v", period, err)
		}
	}
	for _, period := range []string{"monthly", "yearly"} {
		amt, err := save(period)
		if err != nil {
			t.Fatalf("period %q: %v", period, err)
		}
		if amt.Period != period {
			t.Errorf("expected the period %q, got %q", period, amt.Period)
		}
	}
}

func TestSavingABillAmountWithoutAPeriodSavesItMonthly(t *testing.T) {
	svc := newTestService(t)
	bill := mustBillForAmounts(t, svc)

	saved, err := svc.UpsertBillAmount(t.Context(), model.BillAmount{BillID: bill.ID, Amount: newFixedValue(100), StartDate: mustParseDate("2026-01-01")})
	if err != nil {
		t.Fatalf("save bill amount: %v", err)
	}
	amounts, err := svc.ListBillAmounts(t.Context(), bill.ID)
	if err != nil {
		t.Fatalf("list bill amounts: %v", err)
	}
	if saved.Period != "monthly" || len(amounts) != 1 || amounts[0].Period != "monthly" {
		t.Fatalf("expected the bill amount saved monthly, got %q and %+v", saved.Period, amounts)
	}
}

func TestSavingAnUnsupportedDefaultCurrencyIsInvalidInput(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()

	for _, code := range []string{"", "XYZ", "eur"} {
		if err := svc.SetDefaultCurrency(ctx, code); !errors.Is(err, model.ErrInvalidInput) {
			t.Errorf("default currency %q: expected ErrInvalidInput, got %v", code, err)
		}
	}
	if got, err := svc.GetDefaultCurrency(ctx); err != nil || got != "SEK" {
		t.Fatalf("expected the default currency to stay SEK, got %q (%v)", got, err)
	}
	for _, c := range currency.SupportedCurrencies() {
		if err := svc.SetDefaultCurrency(ctx, c.Code); err != nil {
			t.Fatalf("default currency %s: %v", c.Code, err)
		}
		if got, err := svc.GetDefaultCurrency(ctx); err != nil || got != c.Code {
			t.Errorf("expected the default currency %s, got %q (%v)", c.Code, got, err)
		}
	}
}

func TestSavingABillAmountInAnUnsupportedCurrencyIsInvalidInput(t *testing.T) {
	svc := newTestService(t)
	bill := mustBillForAmounts(t, svc)
	save := func(code string) (model.BillAmount, error) {
		return svc.UpsertBillAmount(t.Context(), model.BillAmount{BillID: bill.ID, Amount: newFixedValue(100), Period: "monthly", Currency: code, StartDate: mustParseDate("2026-01-01")})
	}

	for _, code := range []string{"XYZ", "eur"} {
		if _, err := save(code); !errors.Is(err, model.ErrInvalidInput) {
			t.Errorf("bill amount currency %q: expected ErrInvalidInput, got %v", code, err)
		}
	}
	// Without a currency, the Bill Amount is in the default currency.
	codes := []string{""}
	for _, c := range currency.SupportedCurrencies() {
		codes = append(codes, c.Code)
	}
	for _, code := range codes {
		amt, err := save(code)
		if err != nil {
			t.Fatalf("bill amount currency %q: %v", code, err)
		}
		if amt.Currency != code {
			t.Errorf("expected the bill amount currency %q, got %q", code, amt.Currency)
		}
	}
}

func TestAForecastRunWithAStoredGrowthModelOfAnUnknownTypeFails(t *testing.T) {
	svc, timers := newForecastTestService(t, nil)
	ctx := t.Context()
	acc := mustAccount(t, svc, "Savings")
	mustSnapshot(t, svc, acc.ID, mustParseDate("2026-01-01"), 10000)
	gm, err := svc.UpsertAccountGrowthModel(ctx, model.AccountGrowthModelInput{
		AccountID: acc.ID, Type: "fixed", AnnualRate: newFixedValue(0.05), AnnualVolatility: newFixedValue(0), StartDate: mustParseDate("2026-01-01"),
	})
	if err != nil {
		t.Fatalf("create growth model: %v", err)
	}
	// A type no longer known, as stored by another version of PeFi.
	if _, err := svc.DB().ExecContext(ctx, `UPDATE growth_model SET model_type = 'exponential' WHERE id = ?`, gm.ID); err != nil {
		t.Fatalf("store an unknown growth model type: %v", err)
	}
	addFutureSpecialDate(t, svc)
	startForecast(svc)

	sub := subscribe(t, svc)
	timers.fire()

	got := received(sub.Events)
	expectEventTypes(t, got, model.ForecastEventStarted, model.ForecastEventFailed)
	if msg := got[len(got)-1].Message; !strings.Contains(msg, gm.ID) || !strings.Contains(msg, `"exponential"`) {
		t.Fatalf("expected the Failed message to name growth model %s and its type, got %q", gm.ID, msg)
	}
}
