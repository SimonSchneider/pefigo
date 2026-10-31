package swe_test

import (
	"context"
	"strings"
	"testing"

	"github.com/SimonSchneider/pefigo/pkg/swe"
)

// taxTableRows are the rows of a small Tax Table numbered tabellnr for year: an amount of tax up to
// 99 999 kr and 40% above it.
func taxTableRows(tabellnr, year string, low, high string) []map[string]string {
	return []map[string]string{
		{"inkomst fr.o.m.": "1", "inkomst t.o.m.": "24999", "kolumn 1": "0", "tabellnr": tabellnr, "år": year, "antal dgr": "30B"},
		{"inkomst fr.o.m.": "25000", "inkomst t.o.m.": "49999", "kolumn 1": low, "tabellnr": tabellnr, "år": year, "antal dgr": "30B"},
		{"inkomst fr.o.m.": "50000", "inkomst t.o.m.": "99999", "kolumn 1": high, "tabellnr": tabellnr, "år": year, "antal dgr": "30B"},
		{"inkomst fr.o.m.": "100000", "inkomst t.o.m.": "", "kolumn 1": "40", "tabellnr": tabellnr, "år": year, "antal dgr": "30%"},
	}
}

func taxRate(year, rate string) map[string]string {
	return map[string]string{
		"kommun":                   "STOCKHOLM",
		"församling":               "ADOLF FREDRIKS FÖRSAMLING",
		"summa, exkl. kyrkoavgift": rate,
		"summa, inkl. kyrkoavgift": "31.85",
		"år":                       year,
	}
}

func yearAwareClient(t *testing.T) *swe.Client {
	t.Helper()
	tables := append(taxTableRows("31", "2025", "7500", "15000"), taxTableRows("31", "2026", "8000", "16000")...)
	return swe.NewClient(newFakeCache(),
		swe.WithTaxRateURL(rowStoreServer(t, []map[string]string{taxRate("2025", "30.67"), taxRate("2026", "31.00")}).URL),
		swe.WithTaxTableURL(rowStoreServer(t, tables).URL),
	)
}

func stockholm(year string) swe.GrossSalaryInput {
	return swe.GrossSalaryInput{
		Kommun:       "STOCKHOLM",
		Forsamling:   "ADOLF FREDRIKS FÖRSAMLING",
		Year:         year,
		ChurchMember: false,
	}
}

func TestCalculateNetSalary(t *testing.T) {
	client := yearAwareClient(t)
	ctx := context.Background()

	t.Run("basic gross to net", func(t *testing.T) {
		result, err := client.CalculateNetSalary(ctx, swe.GrossSalaryInputWithAmount{GrossSalaryInput: stockholm("2025"), GrossMonthly: 40000})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.GrossMonthly != 40000 {
			t.Errorf("gross = %v, want 40000", result.GrossMonthly)
		}
		if result.Tax != 7500 {
			t.Errorf("tax = %v, want 7500", result.Tax)
		}
		if result.NetMonthly != 32500 {
			t.Errorf("net = %v, want 32500", result.NetMonthly)
		}
	})

	t.Run("higher salary bracket", func(t *testing.T) {
		result, err := client.CalculateNetSalary(ctx, swe.GrossSalaryInputWithAmount{GrossSalaryInput: stockholm("2025"), GrossMonthly: 60000})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.Tax != 15000 {
			t.Errorf("tax = %v, want 15000", result.Tax)
		}
		if result.NetMonthly != 45000 {
			t.Errorf("net = %v, want 45000", result.NetMonthly)
		}
	})

	t.Run("above the table's amounts", func(t *testing.T) {
		result, err := client.CalculateNetSalary(ctx, swe.GrossSalaryInputWithAmount{GrossSalaryInput: stockholm("2025"), GrossMonthly: 150000})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Tax != 60000 || result.NetMonthly != 90000 {
			t.Errorf("tax = %v and net = %v, want 40%%: 60000 and 90000", result.Tax, result.NetMonthly)
		}
	})

	t.Run("clamps future year to current year", func(t *testing.T) {
		// 2027 > current year (2026), clamps to 2026 which has data (tax=8000 for 40k bracket)
		calculate, err := client.NetSalaryCalculator(ctx, stockholm("2027"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res := calculate(40000); res.Tax != 8000 {
			t.Errorf("tax = %v, want 8000 (from 2026 via clamping)", res.Tax)
		}
	})

	t.Run("errors naming the years when no previous year data exists", func(t *testing.T) {
		// 2020 has no data, and no previous years have data either
		_, err := client.NetSalaryCalculator(ctx, stockholm("2020"))
		if err == nil {
			t.Fatal("expected error for year with no data and no fallback, got nil")
		}
		if !strings.Contains(err.Error(), "2020") || !strings.Contains(err.Error(), "2019") {
			t.Errorf("expected the error to name 2020 and 2019, got %q", err)
		}
	})
}

func TestNetSalaryCalculatorFallsBackToThePreviousYear(t *testing.T) {
	ctx := context.Background()

	t.Run("without a tax rate for the year", func(t *testing.T) {
		client := swe.NewClient(newFakeCache(),
			swe.WithTaxRateURL(rowStoreServer(t, []map[string]string{taxRate("2025", "30.67")}).URL),
			swe.WithTaxTableURL(rowStoreServer(t, taxTableRows("31", "2025", "7500", "15000")).URL),
		)
		// Request 2026 (current year); only 2025 exists, should fall back to 2025
		calculate, err := client.NetSalaryCalculator(ctx, stockholm("2026"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res := calculate(40000); res.Tax != 7500 {
			t.Errorf("tax = %v, want 7500 (from 2025 fallback)", res.Tax)
		}
	})

	t.Run("without a Tax Table for the year", func(t *testing.T) {
		client := swe.NewClient(newFakeCache(),
			swe.WithTaxRateURL(rowStoreServer(t, []map[string]string{taxRate("2025", "30.67"), taxRate("2026", "31.00")}).URL),
			swe.WithTaxTableURL(rowStoreServer(t, taxTableRows("31", "2025", "7500", "15000")).URL),
		)
		calculate, err := client.NetSalaryCalculator(ctx, stockholm("2026"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res := calculate(40000); res.Tax != 7500 {
			t.Errorf("tax = %v, want 7500 (from 2025 fallback)", res.Tax)
		}
	})
}

func TestMemoryTaxDataSuppliesTheClientsTaxData(t *testing.T) {
	ctx := context.Background()
	rows := []swe.TaxTableRow{
		{From: 1, To: 49999, Tax: 7500},
		{From: 50000, To: 99999, Tax: 15000},
		{From: 100000, Percent: true, Tax: 40},
	}
	client := swe.NewClient(newFakeCache(), swe.WithTaxData(swe.MemoryTaxData{
		TableNumbers: map[swe.TaxTableNumberKey]int{
			{Kommun: "STOCKHOLM", Forsamling: "ADOLF FREDRIKS FÖRSAMLING", Year: "2025"}: 31,
		},
		Tables: map[swe.TaxTableKey][]swe.TaxTableRow{{Number: 31, Year: "2025"}: rows},
	}))

	calculate, err := client.NetSalaryCalculator(ctx, stockholm("2026"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res := calculate(60000); res.Tax != 15000 || res.NetMonthly != 45000 {
		t.Errorf("got tax %v and net %v, want 15000 and 45000 from 2025's table", res.Tax, res.NetMonthly)
	}
	if res := calculate(200000); res.Tax != 80000 {
		t.Errorf("got tax %v, want 40%%: 80000", res.Tax)
	}
	if _, err := client.NetSalaryCalculator(ctx, swe.GrossSalaryInput{Kommun: "GÖTEBORG", Forsamling: "DOMKYRKO", Year: "2025"}); err == nil {
		t.Error("expected no tax data for a kommun without a table number")
	}
}
