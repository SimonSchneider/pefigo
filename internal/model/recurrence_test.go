package model_test

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
)

// invalidDatePatterns are recurrences date.Cron can't match days with: not a year, a month and a day
// separated by dashes, or with a part that is neither *, */n with a positive n, nor an unsigned integer.
var invalidDatePatterns = []date.Cron{"*-*", "abc", "*-*-*-*", "*-*-*/0", "*-13x-01", "*-*-", "*/-*-*", "*-*/x-01", "*-*-+1", " *-*-25"}

var validDatePatterns = []date.Cron{"*-*-25", "*-*/3-01", "2026-01-01", "*-*-*", "*-*-1"}

func TestSavingATransferTemplateWithoutAValidRecurrenceIsInvalidInput(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	save := func(recurrence date.Cron) (model.TransferTemplate, error) {
		return svc.UpsertTransferTemplate(t.Context(), model.TransferTemplate{
			Name: "Rent", FromAccountID: checking.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(1000),
			Recurrence: recurrence, StartDate: mustParseDate("2026-01-01"), Enabled: true,
		})
	}

	// A Transfer Template recurrence is required: an empty one isn't stored.
	for _, recurrence := range append([]date.Cron{""}, invalidDatePatterns...) {
		if _, err := save(recurrence); !errors.Is(err, model.ErrInvalidInput) {
			t.Errorf("recurrence %q: expected ErrInvalidInput, got %v", recurrence, err)
		}
	}
	if templates, err := svc.ListTransferTemplates(t.Context()); err != nil || len(templates) != 0 {
		t.Fatalf("expected no transfer template saved, got %+v (%v)", templates, err)
	}
	for _, recurrence := range validDatePatterns {
		tt, err := save(recurrence)
		if err != nil {
			t.Fatalf("recurrence %q: %v", recurrence, err)
		}
		if tt.Recurrence != recurrence {
			t.Errorf("expected the recurrence %q, got %q", recurrence, tt.Recurrence)
		}
	}
}

func TestSavingASalaryWithAnInvalidRecurrenceIsInvalidInput(t *testing.T) {
	svc := newTestService(t)
	save := func(recurrence date.Cron) (model.Salary, error) {
		return svc.UpsertSalary(t.Context(), model.Salary{Name: "Acme", Recurrence: recurrence, Enabled: true})
	}

	for _, recurrence := range invalidDatePatterns {
		if _, err := save(recurrence); !errors.Is(err, model.ErrInvalidInput) {
			t.Errorf("recurrence %q: expected ErrInvalidInput, got %v", recurrence, err)
		}
	}
	for _, recurrence := range validDatePatterns {
		sal, err := save(recurrence)
		if err != nil {
			t.Fatalf("recurrence %q: %v", recurrence, err)
		}
		if sal.Recurrence != recurrence {
			t.Errorf("expected the recurrence %q, got %q", recurrence, sal.Recurrence)
		}
	}
}

func TestSavingASalaryWithoutARecurrenceSavesThe25th(t *testing.T) {
	svc := newTestService(t)

	saved, err := svc.UpsertSalary(t.Context(), model.Salary{Name: "Acme", Enabled: true})
	if err != nil {
		t.Fatalf("save salary: %v", err)
	}
	got, err := svc.GetSalary(t.Context(), saved.ID)
	if err != nil {
		t.Fatalf("get salary: %v", err)
	}
	if saved.Recurrence != "*-*-25" || got.Recurrence != "*-*-25" {
		t.Fatalf("expected the salary saved with the recurrence *-*-25, got %q and %q", saved.Recurrence, got.Recurrence)
	}
}

func TestSavingABillAccountWithAnInvalidRecurrenceIsInvalidInput(t *testing.T) {
	svc := newTestService(t)
	save := func(recurrence date.Cron) (model.BillAccount, error) {
		return svc.UpsertBillAccount(t.Context(), model.BillAccount{Name: "Bills", Recurrence: recurrence, Enabled: true})
	}

	for _, recurrence := range invalidDatePatterns {
		if _, err := save(recurrence); !errors.Is(err, model.ErrInvalidInput) {
			t.Errorf("recurrence %q: expected ErrInvalidInput, got %v", recurrence, err)
		}
	}
	for _, recurrence := range validDatePatterns {
		ba, err := save(recurrence)
		if err != nil {
			t.Fatalf("recurrence %q: %v", recurrence, err)
		}
		if ba.Recurrence != recurrence {
			t.Errorf("expected the recurrence %q, got %q", recurrence, ba.Recurrence)
		}
	}
}

func TestSavingABillAccountWithoutARecurrenceSavesThe1st(t *testing.T) {
	svc := newTestService(t)

	saved, err := svc.UpsertBillAccount(t.Context(), model.BillAccount{Name: "Bills", Enabled: true})
	if err != nil {
		t.Fatalf("save bill account: %v", err)
	}
	got, err := svc.GetBillAccount(t.Context(), saved.ID)
	if err != nil {
		t.Fatalf("get bill account: %v", err)
	}
	if saved.Recurrence != "*-*-01" || got.Recurrence != "*-*-01" {
		t.Fatalf("expected the bill account saved with the recurrence *-*-01, got %q and %q", saved.Recurrence, got.Recurrence)
	}
}

func TestSavingAnAccountWithACashFlowDestinationButNoFrequencyIsInvalidInput(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	checking := mustAccount(t, svc, "Checking")

	if _, err := svc.UpsertAccount(ctx, model.AccountInput{Name: "Savings", CashFlowDestinationID: checking.ID}); !errors.Is(err, model.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput, got %v", err)
	}
	savings, err := svc.UpsertAccount(ctx, model.AccountInput{Name: "Savings"})
	if err != nil {
		t.Fatalf("save an account without a cash flow: %v", err)
	}
	if savings.CashFlowFrequency != "" || savings.CashFlowDestinationID != "" {
		t.Fatalf("expected an account without a cash flow, got %+v", savings)
	}
	if _, err := svc.UpsertAccount(ctx, model.AccountInput{ID: savings.ID, Name: "Savings", CashFlowDestinationID: checking.ID}); !errors.Is(err, model.ErrInvalidInput) {
		t.Fatalf("updating: expected ErrInvalidInput, got %v", err)
	}
	if got, err := svc.GetAccount(ctx, savings.ID); err != nil || got.CashFlowDestinationID != "" {
		t.Fatalf("expected the account unchanged, got %+v (%v)", got, err)
	}
}

func TestSavingAnAccountWithAnInvalidCashFlowFrequencyIsInvalidInput(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	save := func(frequency date.Cron, destinationID string) (model.Account, error) {
		return svc.UpsertAccount(t.Context(), model.AccountInput{Name: "Savings", CashFlowFrequency: string(frequency), CashFlowDestinationID: destinationID})
	}

	for _, destinationID := range []string{"", checking.ID} {
		for _, frequency := range invalidDatePatterns {
			if _, err := save(frequency, destinationID); !errors.Is(err, model.ErrInvalidInput) {
				t.Errorf("frequency %q, destination %q: expected ErrInvalidInput, got %v", frequency, destinationID, err)
			}
		}
		for _, frequency := range validDatePatterns {
			acc, err := save(frequency, destinationID)
			if err != nil {
				t.Fatalf("frequency %q, destination %q: %v", frequency, destinationID, err)
			}
			if acc.CashFlowFrequency != string(frequency) || acc.CashFlowDestinationID != destinationID {
				t.Errorf("expected the cash flow %q to %q, got %q to %q", frequency, destinationID, acc.CashFlowFrequency, acc.CashFlowDestinationID)
			}
		}
	}
}

func TestSavingAnInvalidForecastIntervalOrForecastStartDaysIsInvalidInput(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()

	for _, pattern := range append([]date.Cron{""}, invalidDatePatterns...) {
		if err := svc.SetForecastSnapshotInterval(ctx, string(pattern)); !errors.Is(err, model.ErrInvalidInput) {
			t.Errorf("Forecast Interval %q: expected ErrInvalidInput, got %v", pattern, err)
		}
		if err := svc.SetForecastStartDays(ctx, string(pattern)); !errors.Is(err, model.ErrInvalidInput) {
			t.Errorf("Forecast Start Days %q: expected ErrInvalidInput, got %v", pattern, err)
		}
	}
	if interval, err := svc.GetForecastSnapshotInterval(ctx); err != nil || interval != "*-01-01" {
		t.Errorf("expected the Forecast Interval to stay *-01-01, got %q (%v)", interval, err)
	}
	if days, err := svc.GetForecastStartDays(ctx); err != nil || days != "*-*-*" {
		t.Errorf("expected the Forecast Start Days to stay *-*-*, got %q (%v)", days, err)
	}
	for _, pattern := range validDatePatterns {
		if err := svc.SetForecastSnapshotInterval(ctx, string(pattern)); err != nil {
			t.Fatalf("Forecast Interval %q: %v", pattern, err)
		}
		if err := svc.SetForecastStartDays(ctx, string(pattern)); err != nil {
			t.Fatalf("Forecast Start Days %q: %v", pattern, err)
		}
	}
}

// storedInvalidRecurrences each store a recurrence that isn't a valid date pattern past the Service, as
// an older PeFi could have, and return the ID of the row holding it, or the name of the setting, and
// the stored value.
var storedInvalidRecurrences = []struct {
	name  string
	store func(t *testing.T, svc *model.Service, checking model.Account) (row, value string)
}{
	{"transfer template", func(t *testing.T, svc *model.Service, checking model.Account) (string, string) {
		tt := mustTransferTemplate(t, svc, model.TransferTemplate{
			Name: "Rent", FromAccountID: checking.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(1000),
			Recurrence: "*-*-01", StartDate: mustParseDate("2025-01-01"), Enabled: true,
		})
		mustExec(t, svc, `UPDATE transfer_template SET recurrence = '*-*' WHERE id = ?`, tt.ID)
		return tt.ID, "*-*"
	}},
	{"empty transfer template recurrence", func(t *testing.T, svc *model.Service, checking model.Account) (string, string) {
		tt := mustTransferTemplate(t, svc, model.TransferTemplate{
			Name: "Rent", FromAccountID: checking.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(1000),
			Recurrence: "*-*-01", StartDate: mustParseDate("2025-01-01"), Enabled: true,
		})
		mustExec(t, svc, `UPDATE transfer_template SET recurrence = '' WHERE id = ?`, tt.ID)
		return tt.ID, ""
	}},
	{"salary", func(t *testing.T, svc *model.Service, checking model.Account) (string, string) {
		sal, err := svc.UpsertSalary(t.Context(), model.Salary{Name: "Acme", ToAccountID: checking.ID, Recurrence: "*-*-25", Enabled: true})
		if err != nil {
			t.Fatalf("create salary: %v", err)
		}
		if _, err := svc.UpsertSalaryAmount(t.Context(), model.SalaryAmount{SalaryID: sal.ID, Amount: newFixedValue(30_000), StartDate: mustParseDate("2025-01-01")}); err != nil {
			t.Fatalf("create salary amount: %v", err)
		}
		mustExec(t, svc, `UPDATE salary SET recurrence = 'abc' WHERE id = ?`, sal.ID)
		return sal.ID, "abc"
	}},
	{"bill account", func(t *testing.T, svc *model.Service, checking model.Account) (string, string) {
		bill := mustBill(t, svc, checking.ID, "*-*-01", model.Bill{Name: "Music"},
			model.BillAmount{Amount: newFixedValue(100), StartDate: mustParseDate("2025-01-01")})
		mustExec(t, svc, `UPDATE bill_account SET recurrence = '*-*-*/0' WHERE id = ?`, bill.BillAccountID)
		return bill.BillAccountID, "*-*-*/0"
	}},
	{"cash-flow frequency", func(t *testing.T, svc *model.Service, checking model.Account) (string, string) {
		savings, err := svc.UpsertAccount(t.Context(), model.AccountInput{Name: "Savings", CashFlowFrequency: "*-12-31", CashFlowDestinationID: checking.ID})
		if err != nil {
			t.Fatalf("create account: %v", err)
		}
		mustExec(t, svc, `UPDATE account SET cash_flow_frequency = NULL WHERE id = ?`, savings.ID)
		return savings.ID, ""
	}},
	{"Forecast Start Days", func(t *testing.T, svc *model.Service, _ model.Account) (string, string) {
		mustForecastStartDays(t, svc, "*-*-28")
		mustExec(t, svc, `UPDATE app_setting SET value = '28' WHERE key = 'forecast_start_days'`)
		return "Forecast Start Days", "28"
	}},
	{"Forecast Interval", func(t *testing.T, svc *model.Service, _ model.Account) (string, string) {
		if err := svc.SetForecastSnapshotInterval(t.Context(), "*-*-28"); err != nil {
			t.Fatalf("set forecast interval: %v", err)
		}
		mustExec(t, svc, `UPDATE app_setting SET value = '*-13x-01' WHERE key = 'forecast_snapshot_interval'`)
		return "Forecast Interval", "*-13x-01"
	}},
}

func mustExec(t *testing.T, svc *model.Service, query string, args ...any) {
	t.Helper()
	if _, err := svc.DB().ExecContext(t.Context(), query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

func TestAForecastRunWithAStoredInvalidRecurrenceFailsNamingItsRow(t *testing.T) {
	for _, tc := range storedInvalidRecurrences {
		t.Run(tc.name, func(t *testing.T) {
			svc, timers := newForecastTestService(t, nil)
			checking := mustAccount(t, svc, "Checking")
			mustSnapshot(t, svc, checking.ID, mustParseDate("2025-01-01"), 10_000)
			row, value := tc.store(t, svc, checking)
			addFutureSpecialDate(t, svc)
			logger := startForecast(svc)

			sub := subscribe(t, svc)
			timers.fire()

			got := received(sub.Events)
			expectEventTypes(t, got, model.ForecastEventStarted, model.ForecastEventFailed)
			if msg := got[len(got)-1].Message; !strings.Contains(msg, row) || !strings.Contains(msg, strconv.Quote(value)) {
				t.Fatalf("expected the Failed message to name %s and its recurrence %q, got %q", row, value, msg)
			}
			if strings.Contains(logger.String(), "panicked") {
				t.Fatalf("expected the Forecast Run to fail without a panic, got %s", logger)
			}
		})
	}
}

func TestTheTransfersOfADayWithAStoredInvalidRecurrenceAreAnErrorNamingItsRow(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	tt := mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Rent", FromAccountID: checking.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(1000),
		Recurrence: "*-*-01", StartDate: mustParseDate("2025-01-01"), Enabled: true,
	})
	mustExec(t, svc, `UPDATE transfer_template SET recurrence = '' WHERE id = ?`, tt.ID)

	_, err := svc.ComputeTransfersView(t.Context(), mustParseDate("2025-02-01"), nil)
	if err == nil || !strings.Contains(err.Error(), tt.ID) {
		t.Fatalf("expected an error naming transfer template %s, got %v", tt.ID, err)
	}
	// It is stored data that is wrong, not what the caller asked for.
	if errors.Is(err, model.ErrInvalidInput) {
		t.Fatalf("expected a stored invalid recurrence not to be invalid input, got %v", err)
	}
}

func TestTheTransferSchedulePagesListAStoredInvalidRecurrence(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	tt := mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Rent", FromAccountID: checking.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(1000),
		Recurrence: "*-*-01", StartDate: mustParseDate("2025-01-01"), Enabled: true,
	})
	mustExec(t, svc, `UPDATE transfer_template SET recurrence = '' WHERE id = ?`, tt.ID)

	// Listing matches no day, so the Transfer Template can still be found and fixed.
	rows := mustTransferSchedule(t, svc).On(mustParseDate("2025-02-01")).Rows()
	if len(rows) != 1 || rows[0].ID != tt.ID || rows[0].Recurrence != "" {
		t.Fatalf("expected the transfer template listed with its empty recurrence, got %+v", rows)
	}
}
