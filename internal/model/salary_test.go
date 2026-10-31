package model_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
	"github.com/SimonSchneider/pefigo/pkg/swe"
)

// stockholmTaxTable is Tax Table 31: 7 500 kr from 25 000 kr, 15 000 kr from 50 000 kr and, above
// 99 999 kr, 40% of the whole income.
var stockholmTaxTable = []swe.TaxTableRow{
	{From: 1, To: 24999, Tax: 0},
	{From: 25000, To: 49999, Tax: 7500},
	{From: 50000, To: 99999, Tax: 15000},
	{From: 100000, Percent: true, Tax: 40},
}

// withStockholmTaxTables gives the Service the tax data of the kommun STOCKHOLM and the församling TEST
// in each of years: stockholmTaxTable, without church membership.
func withStockholmTaxTables(years ...string) model.ServiceOption {
	data := swe.MemoryTaxData{
		TableNumbers: map[swe.TaxTableNumberKey]int{},
		Tables:       map[swe.TaxTableKey][]swe.TaxTableRow{},
	}
	for _, year := range years {
		data.TableNumbers[swe.TaxTableNumberKey{Kommun: "STOCKHOLM", Forsamling: "TEST", Year: year}] = 31
		data.Tables[swe.TaxTableKey{Number: 31, Year: year}] = stockholmTaxTable
	}
	return model.WithSweOptions(swe.WithTaxData(data))
}

func mustSalary(t *testing.T, svc *model.Service, sal model.Salary, amounts ...model.SalaryAmount) model.Salary {
	t.Helper()
	ctx := t.Context()
	sal.Enabled = true
	saved, err := svc.UpsertSalary(ctx, sal)
	if err != nil {
		t.Fatalf("create salary: %v", err)
	}
	for _, a := range amounts {
		a.SalaryID = saved.ID
		if _, err := svc.UpsertSalaryAmount(ctx, a); err != nil {
			t.Fatalf("create salary amount: %v", err)
		}
	}
	return saved
}

func mustSweYearlyParams(t *testing.T, svc *model.Service, from string, inkomstbasbelopp, prisbasbelopp float64) {
	t.Helper()
	if _, err := svc.UpsertSweYearlyParams(t.Context(), model.SweYearlyParams{Amount: inkomstbasbelopp, Prisbasbelopp: prisbasbelopp, ValidFrom: mustParseDate(from)}); err != nil {
		t.Fatalf("create swe yearly params: %v", err)
	}
}

// mustSalarySegments are the Salary Segments of the Salary id, as the Service loads it.
func mustSalarySegments(t *testing.T, svc *model.Service, id string) []model.SalarySegment {
	t.Helper()
	sal, err := svc.GetSalary(t.Context(), id)
	if err != nil {
		t.Fatalf("get salary: %v", err)
	}
	segments, err := sal.Segments()
	if err != nil {
		t.Fatalf("segment salary: %v", err)
	}
	return segments
}

func stockholmGrossSalary(name string) model.Salary {
	return model.Salary{Name: name, IsGross: true, Kommun: "STOCKHOLM", Forsamling: "TEST", Recurrence: "*-*-25"}
}

// storedWithoutKommun clears the kommun of the stored Salary id, as one saved before a Gross Salary
// required it is stored.
func storedWithoutKommun(t *testing.T, svc *model.Service, id string) {
	t.Helper()
	if _, err := svc.DB().ExecContext(t.Context(), "UPDATE salary SET kommun = '' WHERE id = ?", id); err != nil {
		t.Fatalf("clear kommun: %v", err)
	}
}

func TestSavingAGrossSalaryWithoutAKommunOrForsamlingIsInvalidInput(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()

	for name, sal := range map[string]model.Salary{
		"kommun":     {Name: "Acme", IsGross: true, Forsamling: "TEST"},
		"församling": {Name: "Acme", IsGross: true, Kommun: "STOCKHOLM"},
	} {
		_, err := svc.UpsertSalary(ctx, sal)
		if !errors.Is(err, model.ErrInvalidInput) || !strings.Contains(err.Error(), name+" is required") {
			t.Errorf("expected a Gross Salary without a %s to be invalid input, got %v", name, err)
		}
	}
	salaries, err := svc.ListSalaries(ctx)
	if err != nil || len(salaries) != 0 {
		t.Fatalf("expected no salary saved, got %+v (%v)", salaries, err)
	}
	if _, err := svc.UpsertSalary(ctx, model.Salary{Name: "Acme"}); err != nil {
		t.Fatalf("expected a net Salary without a kommun or församling to save, got %v", err)
	}
}

func TestAGrossSalaryStoredWithoutAKommunIsNeverPaidGross(t *testing.T) {
	svc := newTestService(t, withStockholmTaxTables("2025"))
	ctx := t.Context()
	sal := mustSalary(t, svc, stockholmGrossSalary("Acme"), model.SalaryAmount{Amount: newFixedValue(50000), StartDate: mustParseDate("2025-01-01")})
	storedWithoutKommun(t, svc, sal.ID)

	_, err := svc.TransferSchedule(ctx)
	if err == nil || !strings.Contains(err.Error(), `"Acme"`) || !strings.Contains(err.Error(), "kommun") {
		t.Fatalf("expected the Transfer Schedule to fail naming the Salary and its missing kommun, got %v", err)
	}

	edit, err := svc.GetSalaryEditPageData(ctx, sal.ID)
	if err != nil {
		t.Fatalf("expected the edit page to load, got %v", err)
	}
	if segments, err := edit.Salary.Segments(); err == nil || segments != nil {
		t.Errorf("expected the edit page's Salary to carry why it has no Salary Segments, got %d segments", len(segments))
	}

	salaries, err := svc.GetSalariesPageData(ctx)
	if err != nil {
		t.Fatalf("expected the Salaries list to load, got %v", err)
	}
	if _, err := salaries[0].Segments(); err == nil {
		t.Error("expected the listed Salary to carry why it has no Salary Segments")
	}
}

func TestAGrossSalaryWithoutTaxDataFailsTheTransferScheduleNamingTheYears(t *testing.T) {
	svc := newTestService(t)
	mustSalary(t, svc, stockholmGrossSalary("Acme"), model.SalaryAmount{Amount: newFixedValue(50000), StartDate: mustParseDate("2025-01-01")})

	_, err := svc.TransferSchedule(t.Context())
	if err == nil || !strings.Contains(err.Error(), `"Acme"`) || !strings.Contains(err.Error(), "2025 or 2024") {
		t.Fatalf("expected the Transfer Schedule to fail naming the Salary and the years without tax data, got %v", err)
	}
}

func TestAGrossSalaryAboveTheTaxTablesAmountsIsTaxedItsPercentage(t *testing.T) {
	svc := newTestService(t, withStockholmTaxTables("2025"))
	sal := mustSalary(t, svc, stockholmGrossSalary("Acme"), model.SalaryAmount{Amount: newFixedValue(150000), StartDate: mustParseDate("2025-01-01")})

	tts := salaryTTsFromAll(t, svc, sal.ID)
	if len(tts) != 1 {
		t.Fatalf("expected 1 Net Salary Transfer Template, got %d", len(tts))
	}
	if got := tts[0].AmountFixed.Mean(); got != 90000 {
		t.Errorf("Net Salary = %v, want 150 000 less 40%%: 90 000", got)
	}
	got, err := svc.GetSalary(t.Context(), sal.ID)
	if err != nil {
		t.Fatalf("get salary: %v", err)
	}
	segments, err := got.Segments()
	if err != nil || len(segments) != 1 {
		t.Fatalf("expected 1 Salary Segment, got %d (%v)", len(segments), err)
	}
	if bd := segments[0].Breakdown; bd.Tax != 60000 || bd.NetMonthly != 90000 {
		t.Errorf("expected the Salary Breakdown to tax 60 000 to a net of 90 000, got %v and %v", bd.Tax, bd.NetMonthly)
	}
}

func TestNetSalaryPensionContributionAndBreakdownComeFromOneSegmentation(t *testing.T) {
	svc := newTestService(t, withStockholmTaxTables("2025"))
	ctx := t.Context()
	checking := mustAccount(t, svc, "Checking")
	pension := mustAccount(t, svc, "Pension")
	gross := stockholmGrossSalary("Acme")
	gross.ToAccountID, gross.PensionAccountID = checking.ID, pension.ID
	sal := mustSalary(t, svc, gross, model.SalaryAmount{Amount: newFixedValue(40000), StartDate: mustParseDate("2025-01-01")})
	mustSweYearlyParams(t, svc, "2025-01-01", 76200, 57300)
	for _, adj := range []model.SalaryAdjustment{
		{SalaryID: sal.ID, ValidFrom: mustParseDate("2025-01-01"), VacationDaysPerYear: 25},
		{SalaryID: sal.ID, ValidFrom: mustParseDate("2025-07-01"), VacationDaysPerYear: 25, SickDaysPerOccasion: 3, SickOccasionsPerYear: 4},
	} {
		if _, err := svc.UpsertSalaryAdjustment(ctx, adj); err != nil {
			t.Fatalf("create adjustment: %v", err)
		}
	}

	var netTTs, pensionTTs []model.TransferTemplate
	for _, tt := range salaryTTsFromAll(t, svc, sal.ID) {
		if tt.ToAccountID == pension.ID {
			pensionTTs = append(pensionTTs, tt)
		} else {
			netTTs = append(netTTs, tt)
		}
	}
	split := mustParseDate("2025-07-01")
	for name, tts := range map[string][]model.TransferTemplate{"Net Salary": netTTs, "Pension Contribution": pensionTTs} {
		if len(tts) != 2 || tts[0].EndDate == nil || *tts[0].EndDate != split || tts[1].StartDate != split {
			t.Fatalf("expected the %s to split at the adjustment change, got %+v", name, tts)
		}
	}
	wantPension := swe.CalculateITP1Pension(40000, 76200)
	if pensionTTs[0].AmountFixed.Mean() != wantPension || pensionTTs[1].AmountFixed.Mean() != wantPension {
		t.Errorf("expected the same Pension Contribution %v on both sides, got %v and %v", wantPension, pensionTTs[0].AmountFixed.Mean(), pensionTTs[1].AmountFixed.Mean())
	}

	got, err := svc.GetSalary(ctx, sal.ID)
	if err != nil {
		t.Fatalf("get salary: %v", err)
	}
	segments, err := got.Segments()
	if err != nil || len(segments) != 2 {
		t.Fatalf("expected 2 Salary Segments, got %d (%v)", len(segments), err)
	}
	for i, seg := range segments {
		if seg.StartDate != netTTs[i].StartDate {
			t.Errorf("segment %d starts %v, its Transfer Template %v", i, seg.StartDate, netTTs[i].StartDate)
		}
		if !approxEqual(seg.Breakdown.NetMonthly, netTTs[i].AmountFixed.Mean(), 0.01) {
			t.Errorf("segment %d: Salary Breakdown net %v, Net Salary %v", i, seg.Breakdown.NetMonthly, netTTs[i].AmountFixed.Mean())
		}
		if seg.Breakdown.PensionContribution == nil || *seg.Breakdown.PensionContribution != wantPension {
			t.Errorf("segment %d: Salary Breakdown Pension Contribution %v, want %v", i, seg.Breakdown.PensionContribution, wantPension)
		}
	}
	if segments[1].Breakdown.SickPayDeduction <= 0 || segments[0].Breakdown.SickPayDeduction != 0 {
		t.Errorf("expected sick pay deducted from the second segment only, got %v and %v", segments[0].Breakdown.SickPayDeduction, segments[1].Breakdown.SickPayDeduction)
	}
}

func TestASegmentWithoutAnIncomeBaseAmountPaysNoPensionContribution(t *testing.T) {
	svc := newTestService(t, withStockholmTaxTables("2025"))
	pension := mustAccount(t, svc, "Pension")
	gross := stockholmGrossSalary("Acme")
	gross.PensionAccountID = pension.ID
	sal := mustSalary(t, svc, gross, model.SalaryAmount{Amount: newFixedValue(40000), StartDate: mustParseDate("2025-01-01")})
	mustSweYearlyParams(t, svc, "2025-07-01", 76200, 57300)

	var pensionTTs []model.TransferTemplate
	for _, tt := range salaryTTsFromAll(t, svc, sal.ID) {
		if tt.ToAccountID == pension.ID {
			pensionTTs = append(pensionTTs, tt)
		}
	}
	if len(pensionTTs) != 1 || pensionTTs[0].StartDate != mustParseDate("2025-07-01") {
		t.Fatalf("expected a Pension Contribution from the first income base amount only, got %+v", pensionTTs)
	}
	got, err := svc.GetSalary(t.Context(), sal.ID)
	if err != nil {
		t.Fatalf("get salary: %v", err)
	}
	segments, _ := got.Segments()
	if len(segments) != 2 || segments[0].PensionContribution != nil || segments[0].Breakdown.PensionContribution != nil {
		t.Fatalf("expected the first of 2 Salary Segments without a Pension Contribution, got %+v", segments)
	}
}

func TestTheSalariesListShowsTodaysNetSalaryAndPensionContribution(t *testing.T) {
	svc := newTestService(t, withStockholmTaxTables("2025", "2026"))
	mustSweYearlyParams(t, svc, "2025-01-01", 76200, 57300)
	mustSalary(t, svc, stockholmGrossSalary("Acme"), model.SalaryAmount{Amount: newFixedValue(50000), StartDate: mustParseDate("2025-01-01")})
	mustSalary(t, svc, stockholmGrossSalary("Globex"), model.SalaryAmount{Amount: newFixedValue(50000), StartDate: date.Today().Add(1)})
	mustSalary(t, svc, model.Salary{Name: "Initech"}, model.SalaryAmount{Amount: newFixedValue(30000), StartDate: mustParseDate("2025-01-01")})

	salaries, err := svc.GetSalariesPageData(t.Context())
	if err != nil {
		t.Fatalf("get salaries page data: %v", err)
	}
	byName := model.KeyBy(salaries, func(s model.Salary) string { return s.Name })

	today := byName["Acme"].CurrentSegment()
	if today == nil {
		t.Fatal("expected a Salary Segment today")
	}
	if today.Breakdown.NetMonthly != 35000 {
		t.Errorf("today's Net Salary = %v, want 35 000", today.Breakdown.NetMonthly)
	}
	if want := swe.CalculateITP1Pension(50000, 76200); today.Breakdown.PensionContribution == nil || *today.Breakdown.PensionContribution != want {
		t.Errorf("today's Pension Contribution = %v, want %v", today.Breakdown.PensionContribution, want)
	}
	if seg := byName["Globex"].CurrentSegment(); seg != nil {
		t.Errorf("expected no Salary Segment today for a Salary starting tomorrow, got %+v", seg)
	}
	if segments, err := byName["Initech"].Segments(); segments != nil || err != nil {
		t.Errorf("expected a net Salary without Salary Segments, got %d (%v)", len(segments), err)
	}
}
