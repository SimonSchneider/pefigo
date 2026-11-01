package model_test

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
)

// forecastInputIDs holds the IDs of one stored row of every Forecast Input, for mutations to change.
type forecastInputIDs struct {
	accountType           string
	account               string
	category              string
	snapshotDate          date.Date
	growthModel           string
	specialDate           string
	transferTemplate      string
	salary                string
	salaryAmount          string
	salaryAdjustment      string
	partialParentalLeave  string
	fullParentalLeave     string
	billAccount           string
	bill                  string
	billAmount            string
	investmentRound       string
	shareChange           string
	startupShareOption    string
	sweYearlyParams       string
	event                 string
	eventTransferTemplate string // a Transfer Template in event
}

type forecastInputMutation struct {
	// name is the Service method, optionally followed by "/" and the path through it.
	name   string
	mutate func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error
}

func forecastInputMutations() []forecastInputMutation {
	return []forecastInputMutation{
		// account_type.go
		{"UpsertAccountType", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return discard(svc.UpsertAccountType(ctx, model.AccountTypeInput{Name: "Pension", Color: "#0000ff"}))
		}},
		{"DeleteAccountType", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return svc.DeleteAccountType(ctx, ids.accountType)
		}},
		// account.go
		{"UpsertAccount", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return discard(svc.UpsertAccount(ctx, model.AccountInput{Name: "Savings"}))
		}},
		{"UpsertAccountWithStartupShares", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return discard(svc.UpsertAccountWithStartupShares(ctx, model.AccountInput{Name: "Startup"}, &model.StartupShareAccountInput{TaxRate: ptr(0.3), ValuationDiscountFactor: ptr(1.0)}))
		}},
		{"DeleteAccount", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return svc.DeleteAccount(ctx, ids.account)
		}},
		// bill.go
		{"UpsertBillAccount", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return discard(svc.UpsertBillAccount(ctx, model.BillAccount{Name: "Streaming", Recurrence: "*-*-15", Enabled: true}))
		}},
		{"DeleteBillAccount", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return svc.DeleteBillAccount(ctx, ids.billAccount)
		}},
		{"UpsertBill", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return discard(svc.UpsertBill(ctx, model.Bill{BillAccountID: ids.billAccount, Name: "Spotify", Enabled: true}))
		}},
		{"DeleteBill", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return svc.DeleteBill(ctx, ids.bill)
		}},
		{"UpsertBillAmount", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return discard(svc.UpsertBillAmount(ctx, model.BillAmount{BillID: ids.bill, Amount: newFixedValue(150), Period: "monthly", StartDate: mustParseDate("2026-06-01")}))
		}},
		{"DeleteBillAmount", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return svc.DeleteBillAmount(ctx, ids.billAmount)
		}},
		// category.go
		{"UpsertCategory", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return discard(svc.UpsertCategory(ctx, model.TransferTemplateCategoryInput{Name: "Food"}))
		}},
		{"DeleteCategory", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return svc.DeleteCategory(ctx, ids.category)
		}},
		// event.go
		{"UpsertEvent", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return discard(svc.UpsertEvent(ctx, model.EventInput{Name: "Sabbatical", Date: mustParseDate("2029-01-01")}))
		}},
		{"DeleteEvent", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return svc.DeleteEvent(ctx, ids.event)
		}},
		{"RemoveFromEvent", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return svc.RemoveFromEvent(ctx, model.EventEntry{Kind: model.EventEntryTransferTemplate, ID: ids.eventTransferTemplate})
		}},
		// growth_model.go
		{"UpsertAccountGrowthModel", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return discard(svc.UpsertAccountGrowthModel(ctx, model.AccountGrowthModelInput{
				AccountID: ids.account, Type: "fixed", AnnualRate: newFixedValue(0.07), AnnualVolatility: newFixedValue(0), StartDate: mustParseDate("2027-01-01"),
			}))
		}},
		{"DeleteAccountGrowthModel", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return svc.DeleteAccountGrowthModel(ctx, ids.growthModel)
		}},
		// salary.go
		{"UpsertSalary", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return discard(svc.UpsertSalary(ctx, model.Salary{Name: "Side job", Recurrence: "*-*-25", Enabled: true}))
		}},
		{"DeleteSalary", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return svc.DeleteSalary(ctx, ids.salary)
		}},
		{"UpsertSalaryAmount", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return discard(svc.UpsertSalaryAmount(ctx, model.SalaryAmount{SalaryID: ids.salary, Amount: newFixedValue(45000), StartDate: mustParseDate("2027-01-01")}))
		}},
		{"DeleteSalaryAmount", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return svc.DeleteSalaryAmount(ctx, ids.salaryAmount)
		}},
		{"UpsertSalaryAdjustment", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return discard(svc.UpsertSalaryAdjustment(ctx, model.SalaryAdjustment{SalaryID: ids.salary, ValidFrom: mustParseDate("2027-01-01"), VacationDaysPerYear: 30}))
		}},
		{"DeleteSalaryAdjustment", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return svc.DeleteSalaryAdjustment(ctx, ids.salaryAdjustment)
		}},
		{"UpsertPartialParentalLeave", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return discard(svc.UpsertPartialParentalLeave(ctx, model.PartialParentalLeave{
				SalaryID: ids.salary, StartDate: mustParseDate("2028-01-01"), EndDate: mustParseDate("2029-01-01"), SjukDaysPerYear: 20,
			}))
		}},
		{"DeletePartialParentalLeave", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return svc.DeletePartialParentalLeave(ctx, ids.partialParentalLeave)
		}},
		{"UpsertFullParentalLeave", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return discard(svc.UpsertFullParentalLeave(ctx, model.FullParentalLeave{
				SalaryID: ids.salary, StartDate: mustParseDate("2028-01-01"), EndDate: mustParseDate("2028-06-01"), SjukDaysPerWeek: 5,
			}))
		}},
		{"DeleteFullParentalLeave", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return svc.DeleteFullParentalLeave(ctx, ids.fullParentalLeave)
		}},
		// settings.go
		{"SetDefaultCurrency", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return svc.SetDefaultCurrency(ctx, "EUR")
		}},
		{"SetForecastSettings", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return svc.SetForecastSettings(ctx, model.ForecastSettingsInput{Samples: ptr[int64](500), Interval: "*-*-01", StartDays: "*-*-28"})
		}},
		// snapshot.go
		{"UpsertAccountSnapshot", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return discard(svc.UpsertAccountSnapshot(ctx, ids.account, model.AccountSnapshotInput{Date: mustParseDate("2026-02-01"), Balance: newFixedValue(2000)}))
		}},
		{"DeleteAccountSnapshot", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return svc.DeleteAccountSnapshot(ctx, ids.account, ids.snapshotDate)
		}},
		{"UpdateSnapshotDate", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return discard(svc.UpdateSnapshotDate(ctx, ids.snapshotDate, ids.snapshotDate.Add(date.Day)))
		}},
		{"UpsertOrDeleteSnapshot/upsert", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return discard(svc.UpsertOrDeleteSnapshot(ctx, ids.account, model.AccountSnapshotInput{Date: ids.snapshotDate, Balance: newFixedValue(3000)}))
		}},
		{"UpsertOrDeleteSnapshot/delete", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return discard(svc.UpsertOrDeleteSnapshot(ctx, ids.account, model.AccountSnapshotInput{Date: ids.snapshotDate, EmptyBalance: true}))
		}},
		// special_date.go
		{"UpsertSpecialDate", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return discard(svc.UpsertSpecialDate(ctx, model.SpecialDateInput{Name: "House", Date: mustParseDate("2030-01-01")}))
		}},
		{"DeleteSpecialDate", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return svc.DeleteSpecialDate(ctx, ids.specialDate)
		}},
		// startup_shares.go
		{"UpsertStartupShareAccount", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return discard(svc.UpsertStartupShareAccount(ctx, model.StartupShareAccountInput{AccountID: ids.account, TaxRate: ptr(0.25), ValuationDiscountFactor: ptr(0.8)}))
		}},
		{"DeleteStartupShareAccount", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return svc.DeleteStartupShareAccount(ctx, ids.account)
		}},
		{"UpsertInvestmentRound", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return discard(svc.UpsertInvestmentRound(ctx, model.InvestmentRoundInput{
				AccountID: ids.account, Date: mustParseDate("2027-01-01"), Valuation: 2e6, PreMoneyShares: 1000, Investment: 2e5,
			}))
		}},
		{"DeleteInvestmentRound", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return svc.DeleteInvestmentRound(ctx, ids.investmentRound)
		}},
		{"UpsertShareChange", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return discard(svc.UpsertShareChange(ctx, model.ShareChangeInput{AccountID: ids.account, Date: mustParseDate("2027-01-01"), DeltaShares: 5, TotalPrice: 50}))
		}},
		{"DeleteShareChange", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return svc.DeleteShareChange(ctx, ids.shareChange)
		}},
		{"UpsertStartupShareOption", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return discard(svc.UpsertStartupShareOption(ctx, model.StartupShareOptionInput{
				AccountID: ids.account, SourceAccountID: ids.account, Shares: 20, StrikePricePerShare: 2, GrantDate: mustParseDate("2027-01-01"), EndDate: mustParseDate("2032-01-01"),
			}))
		}},
		{"DeleteStartupShareOption", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return svc.DeleteStartupShareOption(ctx, ids.startupShareOption)
		}},
		// swe_yearly_params.go
		{"UpsertSweYearlyParams", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return discard(svc.UpsertSweYearlyParams(ctx, model.SweYearlyParams{Amount: 80600, Prisbasbelopp: 58800, ValidFrom: mustParseDate("2026-01-01")}))
		}},
		{"DeleteSweYearlyParams", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return svc.DeleteSweYearlyParams(ctx, ids.sweYearlyParams)
		}},
		// transfer_template.go
		{"UpsertTransferTemplate", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return discard(svc.UpsertTransferTemplate(ctx, model.TransferTemplate{
				Name: "Gym", FromAccountID: ids.account, AmountType: "fixed", AmountFixed: newFixedValue(400), Recurrence: "*-*-05", StartDate: mustParseDate("2026-01-01"), Enabled: true,
			}))
		}},
		{"DuplicateTransferTemplate", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return discard(svc.DuplicateTransferTemplate(ctx, ids.transferTemplate))
		}},
		{"DeleteTransferTemplate", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return svc.DeleteTransferTemplate(ctx, ids.transferTemplate)
		}},
		// view.go
		{"ModifySnapshotDateRow", func(ctx context.Context, svc *model.Service, ids forecastInputIDs) error {
			return discard(svc.ModifySnapshotDateRow(ctx, ids.snapshotDate, ids.snapshotDate.Add(date.Day)))
		}},
	}
}

func TestForecastInputMutationsAdvanceGeneration(t *testing.T) {
	for _, m := range forecastInputMutations() {
		t.Run(m.name, func(t *testing.T) {
			svc := newTestService(t)
			ctx := t.Context()
			ids := seedForecastInputs(t, svc)

			before := svc.ForecastInputGeneration()
			if err := m.mutate(ctx, svc, ids); err != nil {
				t.Fatalf("%s: %v", m.name, err)
			}
			if after := svc.ForecastInputGeneration(); after <= before {
				t.Fatalf("%s left the Forecast Input generation at %d", m.name, after)
			}
		})
	}
}

// TestForecastInputMutationsCoverEveryServiceMutation fails when a Service method that looks like a
// write has no case in forecastInputMutations, so new Forecast Inputs get a case too.
func TestForecastInputMutationsCoverEveryServiceMutation(t *testing.T) {
	notForecastInputWrites := map[string]bool{
		"UpsertFavicon": true, // favicon_cache, see TestFaviconCacheWriteLeavesForecastInputGenerationUnchanged
	}
	covered := map[string]bool{}
	for _, m := range forecastInputMutations() {
		method, _, _ := strings.Cut(m.name, "/")
		covered[method] = true
	}
	svcType := reflect.TypeFor[*model.Service]()
	for i := range svcType.NumMethod() {
		name := svcType.Method(i).Name
		if !looksLikeWrite(name) || notForecastInputWrites[name] {
			continue
		}
		if !covered[name] {
			t.Errorf("Service.%s looks like a write but has no case in forecastInputMutations", name)
		}
	}
}

func looksLikeWrite(method string) bool {
	for _, prefix := range []string{"Upsert", "Delete", "Set", "Update", "Duplicate", "Modify", "Create", "Insert", "Add", "Remove", "Import"} {
		if strings.HasPrefix(method, prefix) {
			return true
		}
	}
	return false
}

func TestForecastInputWriteMarksDashboardForecastStale(t *testing.T) {
	svc, timers := newForecastTestService(t, nil)
	ctx := t.Context()
	seedForecastableData(t, svc)
	startForecast(svc)
	timers.fire()
	if n := timers.pending(); n != 0 {
		t.Fatalf("expected no Forecast Run scheduled after the first one, got %d", n)
	}

	if err := svc.SetForecastSettings(ctx, model.ForecastSettingsInput{Samples: ptr[int64](100)}); err != nil {
		t.Fatalf("set forecast settings: %v", err)
	}
	if n := timers.pending(); n != 1 {
		t.Fatalf("expected a Forecast Input write to schedule one Forecast Run, got %d", n)
	}

	sub := subscribe(t, svc)
	timers.fire()
	got := received(sub.Events)
	if len(got) < 3 || got[0].Type != model.ForecastEventStarted || got[len(got)-1].Type != model.ForecastEventDone {
		t.Fatalf("expected the scheduled Forecast Run to recompute the Dashboard Forecast, got %v", eventTypes(got))
	}
}

func TestForecastRunLeavesForecastInputGenerationUnchanged(t *testing.T) {
	svc, timers := newForecastTestService(t, nil)
	seedForecastableData(t, svc)
	startForecast(svc)

	before := svc.ForecastInputGeneration()
	sub := subscribe(t, svc)
	timers.fire()
	if rows := snapshotRows(received(sub.Events)); len(rows) == 0 {
		t.Fatal("expected the Forecast Run to store forecast rows")
	}
	if after := svc.ForecastInputGeneration(); after != before {
		t.Fatalf("Forecast Run changed the Forecast Input generation from %d to %d", before, after)
	}
	if n := timers.pending(); n != 0 {
		t.Fatalf("expected the Forecast Run to schedule nothing, got %d", n)
	}
}

func TestExchangeRateCacheWriteLeavesForecastInputGenerationUnchanged(t *testing.T) {
	var rateRequests atomic.Int32
	svc := newTestServiceWithCurrencyServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rateRequests.Add(1)
		fmt.Fprint(w, `{"amount":1.0,"base":"EUR","date":"2026-03-25","rates":{"SEK":11.47}}`)
	}))
	ctx := t.Context()
	ba, err := svc.UpsertBillAccount(ctx, model.BillAccount{Name: "EU Bills", Recurrence: "*-*-01", Enabled: true})
	if err != nil {
		t.Fatalf("create bill account: %v", err)
	}
	bill, err := svc.UpsertBill(ctx, model.Bill{BillAccountID: ba.ID, Name: "Spotify EU", Enabled: true})
	if err != nil {
		t.Fatalf("create bill: %v", err)
	}
	if _, err := svc.UpsertBillAmount(ctx, model.BillAmount{
		BillID: bill.ID, Amount: newFixedValue(100), Period: "monthly", Currency: "EUR", StartDate: mustParseDate("2025-01-01"),
	}); err != nil {
		t.Fatalf("create bill amount: %v", err)
	}

	before := svc.ForecastInputGeneration()
	if _, err := svc.GetTransferTemplatesPageData(ctx); err != nil {
		t.Fatalf("get transfer templates page data: %v", err)
	}
	if rateRequests.Load() == 0 {
		t.Fatal("expected an exchange rate to be fetched and cached")
	}
	if after := svc.ForecastInputGeneration(); after != before {
		t.Fatalf("exchange rate cache write changed the Forecast Input generation from %d to %d", before, after)
	}
}

func TestFaviconCacheWriteLeavesForecastInputGenerationUnchanged(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()

	before := svc.ForecastInputGeneration()
	if err := svc.UpsertFavicon(ctx, "netflix.com", []byte{0x89, 0x50, 0x4E, 0x47}, "image/png"); err != nil {
		t.Fatalf("upsert favicon: %v", err)
	}
	if after := svc.ForecastInputGeneration(); after != before {
		t.Fatalf("favicon cache write changed the Forecast Input generation from %d to %d", before, after)
	}
}

func discard[T any](_ T, err error) error {
	return err
}

// seedForecastInputs stores one row of every Forecast Input.
func seedForecastInputs(t *testing.T, svc *model.Service) forecastInputIDs {
	t.Helper()
	ctx := t.Context()
	must := func(what string, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("seed %s: %v", what, err)
		}
	}
	var ids forecastInputIDs
	var err error

	color := "#ff8800"
	cat, err := svc.UpsertCategory(ctx, model.TransferTemplateCategoryInput{Name: "Housing", Color: &color})
	must("category", err)
	ids.category = cat.ID

	at, err := svc.UpsertAccountType(ctx, model.AccountTypeInput{Name: "Savings", Color: "#00ff00"})
	must("account type", err)
	ids.accountType = at.ID

	acc, err := svc.UpsertAccountWithStartupShares(ctx, model.AccountInput{Name: "Checking", TypeID: at.ID}, &model.StartupShareAccountInput{TaxRate: ptr(0.3), ValuationDiscountFactor: ptr(1.0)})
	must("account", err)
	ids.account = acc.ID

	ids.snapshotDate = mustParseDate("2026-01-01")
	_, err = svc.UpsertAccountSnapshot(ctx, acc.ID, model.AccountSnapshotInput{Date: ids.snapshotDate, Balance: newFixedValue(1000)})
	must("snapshot", err)

	gm, err := svc.UpsertAccountGrowthModel(ctx, model.AccountGrowthModelInput{
		AccountID: acc.ID, Type: "fixed", AnnualRate: newFixedValue(0.05), AnnualVolatility: newFixedValue(0), StartDate: mustParseDate("2026-01-01"),
	})
	must("growth model", err)
	ids.growthModel = gm.ID

	sd, err := svc.UpsertSpecialDate(ctx, model.SpecialDateInput{Name: "Retirement", Date: mustParseDate("2040-01-01")})
	must("special date", err)
	ids.specialDate = sd.ID

	tt, err := svc.UpsertTransferTemplate(ctx, model.TransferTemplate{
		Name: "Rent", FromAccountID: acc.ID, AmountType: "fixed", AmountFixed: newFixedValue(1500), Recurrence: "*-*-01", StartDate: mustParseDate("2026-01-01"), Enabled: true,
	})
	must("transfer template", err)
	ids.transferTemplate = tt.ID

	sal, err := svc.UpsertSalary(ctx, model.Salary{Name: "Acme", ToAccountID: acc.ID, Recurrence: "*-*-25", Enabled: true})
	must("salary", err)
	ids.salary = sal.ID
	sa, err := svc.UpsertSalaryAmount(ctx, model.SalaryAmount{SalaryID: sal.ID, Amount: newFixedValue(40000), StartDate: mustParseDate("2026-01-01")})
	must("salary amount", err)
	ids.salaryAmount = sa.ID
	adj, err := svc.UpsertSalaryAdjustment(ctx, model.SalaryAdjustment{SalaryID: sal.ID, ValidFrom: mustParseDate("2026-01-01"), VacationDaysPerYear: 25})
	must("salary adjustment", err)
	ids.salaryAdjustment = adj.ID
	ppl, err := svc.UpsertPartialParentalLeave(ctx, model.PartialParentalLeave{
		SalaryID: sal.ID, StartDate: mustParseDate("2027-01-01"), EndDate: mustParseDate("2027-06-01"), SjukDaysPerYear: 40,
	})
	must("partial parental leave", err)
	ids.partialParentalLeave = ppl.ID
	fpl, err := svc.UpsertFullParentalLeave(ctx, model.FullParentalLeave{
		SalaryID: sal.ID, StartDate: mustParseDate("2027-06-01"), EndDate: mustParseDate("2027-09-01"), SjukDaysPerWeek: 5,
	})
	must("full parental leave", err)
	ids.fullParentalLeave = fpl.ID

	ba, err := svc.UpsertBillAccount(ctx, model.BillAccount{Name: "Bills", FromAccountID: acc.ID, Recurrence: "*-*-01", Enabled: true})
	must("bill account", err)
	ids.billAccount = ba.ID
	bill, err := svc.UpsertBill(ctx, model.Bill{BillAccountID: ba.ID, Name: "Netflix", Enabled: true})
	must("bill", err)
	ids.bill = bill.ID
	amt, err := svc.UpsertBillAmount(ctx, model.BillAmount{BillID: bill.ID, Amount: newFixedValue(100), Period: "monthly", StartDate: mustParseDate("2026-01-01")})
	must("bill amount", err)
	ids.billAmount = amt.ID

	ir, err := svc.UpsertInvestmentRound(ctx, model.InvestmentRoundInput{
		AccountID: acc.ID, Date: mustParseDate("2026-01-01"), Valuation: 1e6, PreMoneyShares: 1000, Investment: 1e5,
	})
	must("investment round", err)
	ids.investmentRound = ir.ID
	sc, err := svc.UpsertShareChange(ctx, model.ShareChangeInput{AccountID: acc.ID, Date: mustParseDate("2026-01-01"), DeltaShares: 10, TotalPrice: 100})
	must("share change", err)
	ids.shareChange = sc.ID
	opt, err := svc.UpsertStartupShareOption(ctx, model.StartupShareOptionInput{
		AccountID: acc.ID, SourceAccountID: acc.ID, Shares: 10, StrikePricePerShare: 1, GrantDate: mustParseDate("2026-01-01"), EndDate: mustParseDate("2031-01-01"),
	})
	must("startup share option", err)
	ids.startupShareOption = opt.ID

	swe, err := svc.UpsertSweYearlyParams(ctx, model.SweYearlyParams{Amount: 76200, Prisbasbelopp: 57300, ValidFrom: mustParseDate("2025-01-01")})
	must("swe yearly params", err)
	ids.sweYearlyParams = swe.ID

	ev, err := svc.UpsertEvent(ctx, model.EventInput{Name: "Apartment", Date: mustParseDate("2028-03-01")})
	must("event", err)
	ids.event = ev.ID
	ett, err := svc.UpsertTransferTemplate(ctx, model.TransferTemplate{
		Name: "Amortization", FromAccountID: acc.ID, AmountType: "fixed", AmountFixed: newFixedValue(5000), Recurrence: "*-*-25", Enabled: true, EventID: ev.ID,
	})
	must("event transfer template", err)
	ids.eventTransferTemplate = ett.ID

	return ids
}

// seedForecastableData stores the minimum a forecast run needs to write forecast ids.
func seedForecastableData(t *testing.T, svc *model.Service) {
	t.Helper()
	ctx := t.Context()
	at, err := svc.UpsertAccountType(ctx, model.AccountTypeInput{Name: "Savings", Color: "#00ff00"})
	if err != nil {
		t.Fatalf("create account type: %v", err)
	}
	acc, err := svc.UpsertAccount(ctx, model.AccountInput{Name: "Test", TypeID: at.ID})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	if _, err := svc.UpsertAccountSnapshot(ctx, acc.ID, model.AccountSnapshotInput{
		Date:    mustParseDate("2026-01-01"),
		Balance: newFixedValue(10000),
	}); err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	if _, err := svc.UpsertAccountGrowthModel(ctx, model.AccountGrowthModelInput{
		AccountID:        acc.ID,
		Type:             "fixed",
		AnnualRate:       newFixedValue(0.05),
		AnnualVolatility: newFixedValue(0),
		StartDate:        mustParseDate("2026-01-01"),
	}); err != nil {
		t.Fatalf("create growth model: %v", err)
	}
	if _, err := svc.UpsertSpecialDate(ctx, model.SpecialDateInput{
		Name: "Retirement",
		Date: date.Today().Add(2 * date.Year),
	}); err != nil {
		t.Fatalf("create special date: %v", err)
	}
}
