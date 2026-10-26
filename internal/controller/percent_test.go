package controller_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/SimonSchneider/pefigo/internal/model"
)

// postForm posts form to path and expects the redirect a saved form answers with.
func postForm(t *testing.T, srv *httptest.Server, path string, form url.Values) {
	t.Helper()
	client := *srv.Client()
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.PostForm(srv.URL+path, form)
	if err != nil {
		t.Fatalf("post %s: %v", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther && resp.StatusCode != http.StatusFound {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected post %s to redirect, got %d: %s", path, resp.StatusCode, body)
	}
}

// getPage gets the page at path.
func getPage(t *testing.T, srv *httptest.Server, path string) string {
	t.Helper()
	resp, err := srv.Client().Get(srv.URL + path)
	if err != nil {
		t.Fatalf("get %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected get %s to succeed, got %d: %s", path, resp.StatusCode, body)
	}
	return string(body)
}

// inputNamed is the <input> tag named name on page, or "" without one.
func inputNamed(page, name string) string {
	i := strings.Index(page, `name="`+name+`"`)
	if i < 0 {
		return ""
	}
	start := strings.LastIndex(page[:i], "<input")
	end := strings.Index(page[i:], ">")
	if start < 0 || end < 0 {
		return ""
	}
	return page[start : i+end+1]
}

func mustAccountNamed(t *testing.T, svc *model.Service, name string) model.Account {
	t.Helper()
	accs, err := svc.ListAccounts(t.Context())
	if err != nil {
		t.Fatalf("list accounts: %v", err)
	}
	for _, acc := range accs {
		if acc.Name == name {
			return acc
		}
	}
	t.Fatalf("expected an account named %q, got %v", name, accs)
	return model.Account{}
}

func TestSavingAStartupShareAccountStoresItsPercentagesAsFractions(t *testing.T) {
	tests := []struct {
		name                       string
		taxRate, valuationDiscount string
		wantTaxRate, wantDiscount  float64
		showTaxRate, showDiscount  string
	}{
		{"given", "15", "40", 0.15, 0.40, "15", "40"},
		{"empty", "", "", 0.15, 0.50, "15", "50"},
		{"zero", "0", "0", 0, 0, "0", "0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, srv := newSettingsServer(t)
			postForm(t, srv, "/accounts/", url.Values{
				"name":                      {"Startup"},
				"account_form_mode":         {"startup"},
				"tax_rate":                  {tt.taxRate},
				"valuation_discount_factor": {tt.valuationDiscount},
			})

			acc := mustAccountNamed(t, svc, "Startup")
			ssa, err := svc.GetStartupShareAccount(t.Context(), acc.ID)
			if err != nil {
				t.Fatalf("get startup share account: %v", err)
			}
			if ssa.TaxRate != tt.wantTaxRate || ssa.ValuationDiscountFactor != tt.wantDiscount {
				t.Errorf("expected a Tax Rate of %v and a Valuation Discount of %v, got %v and %v", tt.wantTaxRate, tt.wantDiscount, ssa.TaxRate, ssa.ValuationDiscountFactor)
			}

			page := getPage(t, srv, "/accounts/"+acc.ID+"/edit")
			if input := inputNamed(page, "tax_rate"); !strings.Contains(input, `value="`+tt.showTaxRate+`"`) {
				t.Errorf("expected the edit page to show a Tax Rate of %s, got %s", tt.showTaxRate, input)
			}
			if input := inputNamed(page, "valuation_discount_factor"); !strings.Contains(input, `value="`+tt.showDiscount+`"`) {
				t.Errorf("expected the edit page to show a Valuation Discount of %s, got %s", tt.showDiscount, input)
			}
		})
	}
}

func TestANewAccountShowsTheDefaultTaxRateAndValuationDiscount(t *testing.T) {
	_, srv := newSettingsServer(t)

	page := getPage(t, srv, "/accounts/new")
	if input := inputNamed(page, "tax_rate"); !strings.Contains(input, `value="15"`) {
		t.Errorf("expected a new account to show a Tax Rate of 15, got %s", input)
	}
	if input := inputNamed(page, "valuation_discount_factor"); !strings.Contains(input, `value="50"`) {
		t.Errorf("expected a new account to show a Valuation Discount of 50, got %s", input)
	}
}

func TestSavingATransferTemplateStoresItsPercentageAsAFraction(t *testing.T) {
	tests := []struct {
		percentage string
		want       float64
		show       string
	}{
		{"10", 0.1, "10"},
		{"150", 1.5, "150"},
		{"-5", -0.05, "-5"},
		{"", 0, "0"},
	}
	for _, tt := range tests {
		t.Run(tt.percentage, func(t *testing.T) {
			svc, srv := newSettingsServer(t)
			from, err := svc.UpsertAccount(t.Context(), model.AccountInput{Name: "Checking"})
			if err != nil {
				t.Fatalf("create account: %v", err)
			}
			postForm(t, srv, "/transfers/", url.Values{
				"name":            {"Saving"},
				"from_account_id": {from.ID},
				"amount_type":     {"percent"},
				"amount_percent":  {tt.percentage},
				"recurrence":      {"*-*-25"},
				"start_date":      {"2025-01-01"},
				"enabled":         {"on"},
			})

			templates, err := svc.ListTransferTemplates(t.Context())
			if err != nil || len(templates) != 1 {
				t.Fatalf("expected one Transfer Template, got %v (%v)", templates, err)
			}
			if got := templates[0].AmountPercent; got != tt.want {
				t.Errorf("expected a Percentage of %v, got %v", tt.want, got)
			}

			if list := getPage(t, srv, "/transfer-templates"); !strings.Contains(list, ">"+tt.show+"%<") {
				t.Errorf("expected the list to show %s%%", tt.show)
			}
			edit := getPage(t, srv, "/transfer-templates/"+templates[0].ID+"/edit")
			if input := inputNamed(edit, "amount_percent"); !strings.Contains(input, `value="`+tt.show+`"`) {
				t.Errorf("expected the edit page to show a Percentage of %s, got %s", tt.show, input)
			}
		})
	}
}

func TestTheTransferTemplatePercentageHasNoRange(t *testing.T) {
	_, srv := newSettingsServer(t)

	input := inputNamed(getPage(t, srv, "/transfer-templates/new"), "amount_percent")
	if input == "" {
		t.Fatal("expected a Percentage field")
	}
	if strings.Contains(input, "min=") || strings.Contains(input, "max=") {
		t.Errorf("expected the Percentage field to have no min or max, got %s", input)
	}
}
