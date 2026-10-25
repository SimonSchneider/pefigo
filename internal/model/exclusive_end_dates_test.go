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
	checking := mustAccount(t, old, "Checking")
	lastDay := mustParseDate("2025-12-01")
	tt := mustTransferTemplate(t, old, model.TransferTemplate{
		Name: "Gym", FromAccountID: checking.ID, AmountType: "fixed", AmountFixed: newFixedValue(300),
		Recurrence: "*-*-01", StartDate: mustParseDate("2025-01-01"), EndDate: &lastDay, Enabled: true,
	})
	mustBill(t, old, checking.ID, "*-*-01", model.Bill{Name: "Music"},
		model.BillAmount{Amount: newFixedValue(100), StartDate: mustParseDate("2025-01-01"), EndDate: &lastDay},
	)
	gm, err := old.UpsertAccountGrowthModel(ctx, model.AccountGrowthModelInput{
		AccountID: checking.ID, Type: "fixed", AnnualRate: newFixedValue(0.05), AnnualVolatility: newFixedValue(0),
		StartDate: mustParseDate("2025-01-01"), EndDate: &lastDay,
	})
	if err != nil {
		t.Fatalf("create growth model: %v", err)
	}
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
			if err := save(); !errors.Is(err, model.ErrEndDateNotAfterStartDate) {
				t.Errorf("%s from %s to %s: expected ErrEndDateNotAfterStartDate, got %v", entry, start, end, err)
			}
		}
	}
}
