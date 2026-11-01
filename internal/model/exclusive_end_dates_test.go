package model_test

import (
	"errors"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo"
	"github.com/SimonSchneider/pefigo/internal/model"
	"github.com/ncruces/go-sqlite3/vfs/memdb"
)

// migrationsBefore returns the embedded migrations that sort before version.
func migrationsBefore(t *testing.T, version string) fs.FS {
	t.Helper()
	entries, err := fs.ReadDir(pefigo.StaticEmbeddedFS, "static/migrations")
	if err != nil {
		t.Fatalf("read migrations: %v", err)
	}
	before := fstest.MapFS{}
	for _, e := range entries {
		if e.Name() >= version {
			continue
		}
		data, err := fs.ReadFile(pefigo.StaticEmbeddedFS, "static/migrations/"+e.Name())
		if err != nil {
			t.Fatalf("read migration %s: %v", e.Name(), err)
		}
		before["static/migrations/"+e.Name()] = &fstest.MapFile{Data: data}
	}
	return before
}

func TestMigratingToExclusiveEndDatesKeepsChargingOnTheOldEndDate(t *testing.T) {
	ctx := t.Context()
	conn := memdb.TestDB(t)
	old, err := model.Open(ctx, migrationsBefore(t, "0029"), "static/migrations", conn)
	if err != nil {
		t.Fatalf("open database before exclusive End Dates: %v", err)
	}
	// The rows are stored as the schema before exclusive End Dates holds them.
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := old.DB().ExecContext(ctx, query, args...); err != nil {
			t.Fatalf("seed the database before exclusive End Dates: %v", err)
		}
	}
	encoded := func(v float64) string {
		t.Helper()
		s, err := newFixedValue(v).Encode()
		if err != nil {
			t.Fatalf("encode %v: %v", v, err)
		}
		return s
	}
	lastDay := mustParseDate("2025-12-01")
	start := mustParseDate("2025-01-01")
	checking := model.Account{ID: "checking"}
	exec(`INSERT INTO account (id, name, created_at, updated_at) VALUES (?, 'Checking', 0, 0)`, checking.ID)
	tt := model.TransferTemplate{ID: "gym"}
	exec(`INSERT INTO transfer_template (id, name, from_account_id, amount_type, amount_fixed, amount_percent, priority, recurrence, start_date, end_date, enabled, created_at, updated_at)
		VALUES (?, 'Gym', ?, 'fixed', ?, 0, 0, '*-*-01', ?, ?, TRUE, 0, 0)`, tt.ID, checking.ID, encoded(300), int64(start), int64(lastDay))
	exec(`INSERT INTO bill_account (id, name, from_account_id, recurrence, created_at, updated_at) VALUES ('music-account', 'Music', ?, '*-*-01', 0, 0)`, checking.ID)
	exec(`INSERT INTO bill (id, bill_account_id, name, created_at, updated_at) VALUES ('music', 'music-account', 'Music', 0, 0)`)
	exec(`INSERT INTO bill_amount (id, bill_id, amount, start_date, end_date, created_at, updated_at) VALUES ('music-amount', 'music', ?, ?, ?, 0, 0)`,
		encoded(100), int64(start), int64(lastDay))
	gm := model.GrowthModel{ID: "growth"}
	exec(`INSERT INTO growth_model (id, account_id, model_type, annual_growth_rate, annual_volatility, start_date, end_date, created_at, updated_at)
		VALUES (?, ?, 'fixed', ?, ?, ?, ?, 0, 0)`, gm.ID, checking.ID, encoded(0.05), encoded(0), int64(start), int64(lastDay))
	old.DB().Close()

	svc, err := model.Open(ctx, pefigo.StaticEmbeddedFS, "static/migrations", conn)
	if err != nil {
		t.Fatalf("migrate to exclusive End Dates: %v", err)
	}
	t.Cleanup(func() { svc.DB().Close() })

	wantEnd := mustParseDate("2025-12-02")
	if got, err := svc.GetTransferTemplate(ctx, tt.ID); err != nil || got.EndDate == nil || *got.EndDate != wantEnd {
		t.Errorf("expected the Transfer Template to end on %s, got %v (%v)", wantEnd, got.EndDate, err)
	}
	if got, err := svc.GetGrowthModel(ctx, gm.ID); err != nil || got.EndDate == nil || *got.EndDate != wantEnd {
		t.Errorf("expected the growth model to end on %s, got %v (%v)", wantEnd, got.EndDate, err)
	}
	charged := map[string]float64{}
	for _, c := range mustTransferSchedule(t, svc).On(lastDay).Charged() {
		charged[c.Name] = c.Amount
	}
	if charged["Gym"] != 300 || charged["Music"] != 100 {
		t.Errorf("expected Gym and Music to still charge on %s, got %v", lastDay, charged)
	}
	accounts, err := svc.ListAccountsDetailed(ctx, lastDay)
	if err != nil {
		t.Fatalf("list accounts: %v", err)
	}
	if len(accounts) != 1 || accounts[0].GrowthModel == nil {
		t.Errorf("expected the growth model to still apply on %s, got %+v", lastDay, accounts)
	}
}

func TestSavingAnEndDateOnOrBeforeItsStartDateFails(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	checking := mustAccount(t, svc, "Checking")
	bill := mustBill(t, svc, checking.ID, "*-*-01", model.Bill{Name: "Music"})
	salary, err := svc.UpsertSalary(ctx, model.Salary{Name: "Acme", ToAccountID: checking.ID, Recurrence: "*-*-25", Enabled: true})
	if err != nil {
		t.Fatalf("create salary: %v", err)
	}
	start := mustParseDate("2025-03-01")

	for _, end := range []date.Date{start, start.Add(-1 * date.Day)} {
		for entry, save := range map[string]func() error{
			"Transfer Template": func() error {
				return discard(svc.UpsertTransferTemplate(ctx, model.TransferTemplate{
					Name: "Gym", FromAccountID: checking.ID, AmountType: "fixed", AmountFixed: newFixedValue(300),
					Recurrence: "*-*-01", StartDate: start, EndDate: &end, Enabled: true,
				}))
			},
			"Bill Amount": func() error {
				return discard(svc.UpsertBillAmount(ctx, model.BillAmount{BillID: bill.ID, Amount: newFixedValue(100), StartDate: start, EndDate: &end}))
			},
			"growth model": func() error {
				return discard(svc.UpsertAccountGrowthModel(ctx, model.AccountGrowthModelInput{
					AccountID: checking.ID, Type: "fixed", AnnualRate: newFixedValue(0.05), AnnualVolatility: newFixedValue(0),
					StartDate: start, EndDate: &end,
				}))
			},
			"partial parental leave": func() error {
				return discard(svc.UpsertPartialParentalLeave(ctx, model.PartialParentalLeave{SalaryID: salary.ID, StartDate: start, EndDate: end}))
			},
			"full parental leave": func() error {
				return discard(svc.UpsertFullParentalLeave(ctx, model.FullParentalLeave{SalaryID: salary.ID, StartDate: start, EndDate: end}))
			},
		} {
			err := save()
			if !errors.Is(err, model.ErrEndDateNotAfterStartDate) {
				t.Errorf("%s from %s to %s: expected ErrEndDateNotAfterStartDate, got %v", entry, start, end, err)
			}
			if !errors.Is(err, model.ErrInvalidInput) {
				t.Errorf("%s from %s to %s: expected ErrInvalidInput, got %v", entry, start, end, err)
			}
		}
	}
}
