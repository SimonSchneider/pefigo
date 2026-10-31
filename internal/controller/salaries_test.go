package controller_test

import (
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
	"github.com/SimonSchneider/pefigo/pkg/swe"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

// withStockholmTaxTable gives the Service Tax Table 31 for the kommun STOCKHOLM and the församling TEST
// in 2025: 15 000 kr from 50 000 kr and 40% above 99 999 kr.
func withStockholmTaxTable() model.ServiceOption {
	return model.WithSweOptions(swe.WithTaxData(swe.MemoryTaxData{
		TableNumbers: map[swe.TaxTableNumberKey]int{{Kommun: "STOCKHOLM", Forsamling: "TEST", Year: "2025"}: 31},
		Tables: map[swe.TaxTableKey][]swe.TaxTableRow{{Number: 31, Year: "2025"}: {
			{From: 1, To: 49999, Tax: 7500},
			{From: 50000, To: 99999, Tax: 15000},
			{From: 100000, Percent: true, Tax: 40},
		}},
	}))
}

// mustGrossSalary saves a Gross Salary of 50 000 kr a month from 2025-01-01 in STOCKHOLM, TEST, with
// the income base amount 76 200 kr.
func mustGrossSalary(t *testing.T, svc *model.Service) model.Salary {
	t.Helper()
	ctx := t.Context()
	sal, err := svc.UpsertSalary(ctx, model.Salary{Name: "Acme", IsGross: true, Kommun: "STOCKHOLM", Forsamling: "TEST", Enabled: true})
	if err != nil {
		t.Fatalf("create salary: %v", err)
	}
	start, _ := date.ParseDate("2025-01-01")
	if _, err := svc.UpsertSalaryAmount(ctx, model.SalaryAmount{SalaryID: sal.ID, Amount: uncertain.NewFixed(50000), StartDate: start}); err != nil {
		t.Fatalf("create salary amount: %v", err)
	}
	if _, err := svc.UpsertSweYearlyParams(ctx, model.SweYearlyParams{Amount: 76200, Prisbasbelopp: 57300, ValidFrom: start}); err != nil {
		t.Fatalf("create swe yearly params: %v", err)
	}
	return sal
}

func mustGet(t *testing.T, client *http.Client, url string) string {
	t.Helper()
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("get %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", url, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get %s: expected 200, got %d: %s", url, resp.StatusCode, body)
	}
	return string(body)
}

func TestSavingAGrossSalaryWithoutAKommunIsABadRequest(t *testing.T) {
	svc, srv := newSettingsServer(t)

	resp, err := srv.Client().PostForm(srv.URL+"/salaries/", url.Values{
		"name":       {"Acme"},
		"is_gross":   {"on"},
		"forsamling": {"TEST"},
	})
	if err != nil {
		t.Fatalf("save salary: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(body), "kommun is required") {
		t.Fatalf("expected 400 saying the kommun is required, got %d: %s", resp.StatusCode, body)
	}
	if salaries, err := svc.ListSalaries(t.Context()); err != nil || len(salaries) != 0 {
		t.Fatalf("expected no salary saved, got %+v (%v)", salaries, err)
	}
}

func TestTheSalaryPagesShowNetSalaryAndPensionContribution(t *testing.T) {
	svc, srv := newSettingsServer(t, withStockholmTaxTable())
	sal := mustGrossSalary(t, svc)
	pension := shown(swe.CalculateITP1Pension(50000, 76200))

	list := mustGet(t, srv.Client(), srv.URL+"/salaries")
	for _, want := range []string{"Net Salary", "Pension Contribution", "50,000", "35,000", pension} {
		if !strings.Contains(list, want) {
			t.Errorf("expected the Salaries list to show %q", want)
		}
	}

	edit := mustGet(t, srv.Client(), srv.URL+"/salaries/"+sal.ID+"/edit")
	for _, want := range []string{"Net Salary Breakdown", "Pension contribution", pension} {
		if !strings.Contains(edit, want) {
			t.Errorf("expected the Salary Breakdown to show %q", want)
		}
	}
}

func TestASalaryStoredWithoutAKommunShowsWhyItHasNoNetSalary(t *testing.T) {
	svc, srv := newSettingsServer(t, withStockholmTaxTable())
	sal := mustGrossSalary(t, svc)
	if _, err := svc.DB().ExecContext(t.Context(), "UPDATE salary SET kommun = '' WHERE id = ?", sal.ID); err != nil {
		t.Fatalf("clear kommun: %v", err)
	}

	for _, page := range []string{"/salaries", "/salaries/" + sal.ID + "/edit"} {
		if body := mustGet(t, srv.Client(), srv.URL+page); !strings.Contains(body, "no kommun and församling") {
			t.Errorf("expected %s to show why the Salary has no Net Salary", page)
		}
	}
}

// shown formats an amount as the pages show it.
func shown(amount float64) string {
	s := strconv.FormatFloat(math.Round(amount), 'f', 0, 64)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}
