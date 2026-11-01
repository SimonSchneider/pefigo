package model_test

import (
	"slices"
	"testing"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
)

// The pages below evaluate the Transfer Schedule as of today, so their Bill changes price today.
func mustBillWithPriceChangeToday(t *testing.T, svc *model.Service, fromAccountID string, budgetCategoryID *string) {
	t.Helper()
	mustBill(t, svc, fromAccountID, "*-*-01", model.Bill{Name: "Music", BudgetCategoryID: budgetCategoryID},
		model.BillAmount{Amount: newFixedValue(100), StartDate: mustParseDate("2025-01-01")},
		model.BillAmount{Amount: newFixedValue(200), StartDate: date.Today()},
	)
}

func TestTransferTemplatesPageChargesTodaysBillPriceChangeOnce(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	mustBillWithPriceChangeToday(t, svc, checking.ID, nil)

	view, err := svc.GetTransferTemplatesPageData(t.Context())
	if err != nil {
		t.Fatalf("GetTransferTemplatesPageData: %v", err)
	}

	if view.MonthlyExpenses != -200 {
		t.Errorf("expected monthly expenses -200, got %f", view.MonthlyExpenses)
	}
}

func TestBudgetChargesTodaysBillPriceChangeOnce(t *testing.T) {
	svc := newTestService(t)
	color := "#ff0000"
	media, err := svc.UpsertCategory(t.Context(), model.TransferTemplateCategoryInput{Name: "Media", Color: &color})
	if err != nil {
		t.Fatalf("create category: %v", err)
	}
	checking := mustAccount(t, svc, "Checking")
	mustBillWithPriceChangeToday(t, svc, checking.ID, &media.ID)

	budget, err := svc.GetBudgetData(t.Context())
	if err != nil {
		t.Fatalf("GetBudgetData: %v", err)
	}

	if budget.GrandTotal != 200 {
		t.Errorf("expected a budget of 200, got %f", budget.GrandTotal)
	}
}

func TestTransferChartChargesTodaysBillPriceChangeOnce(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	mustBillWithPriceChangeToday(t, svc, checking.ID, nil)

	data, err := svc.GetTransferChartData(t.Context(), model.GroupByAccount)
	if err != nil {
		t.Fatalf("GetTransferChartData: %v", err)
	}

	want := []model.TransferChartLink{{Source: "Checking", Target: "Expenses", Value: 200, Label: "Music"}}
	if !slices.Equal(data.Links, want) {
		t.Errorf("expected links %v, got %v", want, data.Links)
	}
}

func TestTransfersCalculatorChargesABillPriceChangeOnceOnItsRecurrenceDay(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	mustBill(t, svc, checking.ID, "*-*-01", model.Bill{Name: "Music"},
		model.BillAmount{Amount: newFixedValue(100), StartDate: mustParseDate("2025-01-01")},
		model.BillAmount{Amount: newFixedValue(200), StartDate: mustParseDate("2025-03-01")},
	)

	for _, tc := range []struct {
		day  string
		want []float64
	}{
		{"2025-02-01", []float64{100}},
		{"2025-03-01", []float64{200}},
		{"2025-04-01", []float64{200}},
	} {
		view, err := svc.ComputeTransfersView(t.Context(), mustParseDate(tc.day), nil)
		if err != nil {
			t.Fatalf("ComputeTransfersView: %v", err)
		}
		var got []float64
		for _, tt := range view.TransferTemplates {
			got = append(got, tt.AmountFixed.Mean())
		}
		if !slices.Equal(got, tc.want) {
			t.Errorf("on %s: expected charges %v, got %v", tc.day, tc.want, got)
		}
	}
}

func TestTransfersCalculatorLeavesOutDisabledAndEndedTransferTemplates(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	day := mustParseDate("2025-03-01")
	starts := mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Starts today", FromAccountID: checking.ID, AmountType: "fixed", AmountFixed: newFixedValue(100),
		Recurrence: "*-*-01", StartDate: day, Enabled: true,
	})
	endsToday := mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Ends today", FromAccountID: checking.ID, AmountType: "fixed", AmountFixed: newFixedValue(100),
		Recurrence: "*-*-01", StartDate: mustParseDate("2025-01-01"), Enabled: true,
	})
	mustEndTransferTemplate(t, svc, endsToday.ID, day)
	mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Disabled", FromAccountID: checking.ID, AmountType: "fixed", AmountFixed: newFixedValue(100),
		Recurrence: "*-*-01", StartDate: mustParseDate("2025-01-01"), Enabled: false,
	})

	view, err := svc.ComputeTransfersView(t.Context(), day, nil)
	if err != nil {
		t.Fatalf("ComputeTransfersView: %v", err)
	}

	var got []string
	for _, tt := range view.TransferTemplates {
		got = append(got, tt.ID)
	}
	if want := []string{starts.ID}; !slices.Equal(got, want) {
		t.Errorf("expected only %q, got %v", starts.Name, got)
	}
}

// recordedForecast keeps the forecast balance of each Account per snapshot day.
type recordedForecast map[string]map[date.Date]float64

func (r recordedForecast) Setup(model.PredictionSetupEvent) error { return nil }
func (r recordedForecast) Close() error                           { return nil }
func (r recordedForecast) Snapshot(s model.PredictionBalanceSnapshot) error {
	if r[s.ID] == nil {
		r[s.ID] = make(map[date.Date]float64)
	}
	r[s.ID][s.Day] = s.Balance
	return nil
}

func TestForecastChargesABillPriceChangeOnceOnItsRecurrenceDay(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	checking := mustAccount(t, svc, "Checking")
	start := mustParseDate("2025-01-15")
	if _, err := svc.UpsertAccountSnapshot(ctx, checking.ID, model.AccountSnapshotInput{Date: start, Balance: newFixedValue(0)}); err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	mustBill(t, svc, checking.ID, "*-*-01", model.Bill{Name: "Music"},
		model.BillAmount{Amount: newFixedValue(100), StartDate: mustParseDate("2025-01-01")},
		model.BillAmount{Amount: newFixedValue(200), StartDate: mustParseDate("2025-03-01")},
	)

	forecast := recordedForecast{}
	err := svc.RunPrediction(ctx, forecast, model.PredictionParams{
		Duration: ptr(mustParseDate("2025-04-06").Sub(start)), Samples: ptr[int64](1), Quantile: ptr(0.8), SnapshotInterval: "*-*-02", GroupBy: model.GroupByNone,
	})
	if err != nil {
		t.Fatalf("RunPrediction: %v", err)
	}

	for day, want := range map[string]float64{
		"2025-02-02": -100,
		"2025-03-02": -300,
		"2025-04-02": -500,
	} {
		if got := forecast[checking.ID][mustParseDate(day)]; got != want {
			t.Errorf("on %s: expected a balance of %f, got %f", day, want, got)
		}
	}
}

func TestAccountsDetailedLeaveOutAGrowthModelOnItsEndDate(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	savings := mustAccount(t, svc, "Savings")
	end := mustParseDate("2025-06-01")
	if _, err := svc.UpsertAccountGrowthModel(ctx, model.AccountGrowthModelInput{
		AccountID: savings.ID, Type: "fixed", AnnualRate: newFixedValue(0.05), AnnualVolatility: newFixedValue(0),
		StartDate: mustParseDate("2025-01-01"), EndDate: &end,
	}); err != nil {
		t.Fatalf("create growth model: %v", err)
	}

	for day, applies := range map[string]bool{"2025-01-01": true, "2025-05-31": true, "2025-06-01": false} {
		accounts, err := svc.ListAccountsDetailed(ctx, mustParseDate(day))
		if err != nil {
			t.Fatalf("ListAccountsDetailed: %v", err)
		}
		if got := accounts[0].GrowthModel != nil; got != applies {
			t.Errorf("on %s: expected the growth model to apply: %t, got %t", day, applies, got)
		}
	}
}

func TestForecastAndAccountsApplyTheLatestStartingGrowthModel(t *testing.T) {
	ended := mustParseDate("2025-06-01")
	for name, earlierEnd := range map[string]*date.Date{"after an ended model": &ended, "over an earlier model": nil} {
		t.Run(name, func(t *testing.T) {
			svc := newTestService(t)
			ctx := t.Context()
			savings := mustAccount(t, svc, "Savings")
			today := date.Today()
			if _, err := svc.UpsertAccountSnapshot(ctx, savings.ID, model.AccountSnapshotInput{Date: today, Balance: newFixedValue(100_000)}); err != nil {
				t.Fatalf("create snapshot: %v", err)
			}
			for _, gm := range []model.AccountGrowthModelInput{
				{Type: "fixed", AnnualRate: newFixedValue(0), StartDate: mustParseDate("2025-01-01"), EndDate: earlierEnd},
				{Type: "fixed", AnnualRate: newFixedValue(0.10), StartDate: mustParseDate("2025-06-01")},
			} {
				gm.AccountID, gm.AnnualVolatility = savings.ID, newFixedValue(0)
				if _, err := svc.UpsertAccountGrowthModel(ctx, gm); err != nil {
					t.Fatalf("create growth model: %v", err)
				}
			}

			accounts, err := svc.ListAccountsDetailed(ctx, today)
			if err != nil {
				t.Fatalf("ListAccountsDetailed: %v", err)
			}
			if gm := accounts[0].GrowthModel; gm == nil || gm.AnnualRate.Mean() != 0.10 {
				t.Errorf("expected the account to show the 10%% growth model, got %+v", gm)
			}
			forecast := recordedForecast{}
			if err := svc.RunPrediction(ctx, forecast, model.PredictionParams{
				Duration: ptr(30 * date.Day), Samples: ptr[int64](1), Quantile: ptr(0.8), SnapshotInterval: "*-*-*", GroupBy: model.GroupByNone,
			}); err != nil {
				t.Fatalf("RunPrediction: %v", err)
			}
			if got := forecast[savings.ID][today.Add(29*date.Day)]; got <= 100_000 {
				t.Errorf("expected the forecast to grow at 10%%, got a balance of %f after 4 weeks", got)
			}
		})
	}
}
