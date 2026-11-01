package controller_test

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
)

// parents are the rows the forms of upsertRoutes link to.
type parents struct {
	account, sourceAccount, salary, billAccount, bill, transferTemplate string
}

func mustParents(t *testing.T, svc *model.Service) parents {
	t.Helper()
	ctx := t.Context()
	account, err := svc.UpsertAccount(ctx, model.AccountInput{Name: "Startup"})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	sourceAccount, err := svc.UpsertAccount(ctx, model.AccountInput{Name: "Checking"})
	if err != nil {
		t.Fatalf("create source account: %v", err)
	}
	salary, err := svc.UpsertSalary(ctx, model.Salary{Name: "Acme"})
	if err != nil {
		t.Fatalf("create salary: %v", err)
	}
	billAccount, err := svc.UpsertBillAccount(ctx, model.BillAccount{Name: "Bills"})
	if err != nil {
		t.Fatalf("create bill account: %v", err)
	}
	bill, err := svc.UpsertBill(ctx, model.Bill{BillAccountID: billAccount.ID, Name: "Rent"})
	if err != nil {
		t.Fatalf("create bill: %v", err)
	}
	transferTemplate, err := svc.UpsertTransferTemplate(ctx, model.TransferTemplate{Name: "Gym", AmountType: model.TransferAmountFixed, StartDate: date.Today()})
	if err != nil {
		t.Fatalf("create transfer template: %v", err)
	}
	return parents{account.ID, sourceAccount.ID, salary.ID, billAccount.ID, bill.ID, transferTemplate.ID}
}

// upsertRoute is a route that saves an entity, and a valid form for it without next.
type upsertRoute struct {
	name, path string
	form       url.Values
}

func upsertRoutes(p parents) []upsertRoute {
	return []upsertRoute{
		{"account", "/accounts/", url.Values{"name": {"Savings"}}},
		{"account with startup shares", "/accounts/", url.Values{"name": {"Startup 2"}, "account_form_mode": {"startup"}}},
		{"growth model", "/growth-models/", url.Values{"account_id": {p.account}, "type": {"fixed"}, "start_date": {"2025-01-01"}}},
		{"snapshot", "/accounts/" + p.account + "/snapshots/2025-01-01/", url.Values{"balance": {"100"}}},
		{"startup share account", "/startup-share-accounts/", url.Values{"account_id": {p.account}}},
		{"investment round", "/investment-rounds/", url.Values{"account_id": {p.account}, "date": {"2025-01-01"}}},
		{"share change", "/share-changes/", url.Values{"account_id": {p.account}, "date": {"2025-01-01"}}},
		{"startup share option", "/startup-share-options/", url.Values{"account_id": {p.account}, "source_account_id": {p.sourceAccount}, "grant_date": {"2025-01-01"}, "end_date": {"2030-01-01"}}},
		{"salary", "/salaries/", url.Values{"name": {"Initech"}}},
		{"salary amount", "/salary-amounts/", url.Values{"salary_id": {p.salary}, "start_date": {"2025-01-01"}, "amount": {"30000"}}},
		{"salary adjustment", "/salary-adjustments/", url.Values{"salary_id": {p.salary}, "valid_from": {"2025-01-01"}}},
		{"partial parental leave", "/partial-parental-leaves/", url.Values{"salary_id": {p.salary}, "start_date": {"2025-01-01"}, "end_date": {"2025-06-01"}}},
		{"full parental leave", "/full-parental-leaves/", url.Values{"salary_id": {p.salary}, "start_date": {"2025-01-01"}, "end_date": {"2025-06-01"}}},
		{"bill account", "/bills/", url.Values{"name": {"Utilities"}}},
		{"bill", "/bill-items/", url.Values{"bill_account_id": {p.billAccount}, "name": {"Power"}}},
		{"bill amount", "/bill-amounts/", url.Values{"bill_id": {p.bill}, "start_date": {"2025-01-01"}, "amount": {"500"}}},
		{"transfer template", "/transfers/", url.Values{"name": {"Saving"}, "amount_type": {"fixed"}, "recurrence": {"*-*-25"}, "start_date": {"2025-01-01"}}},
		{"transfer template end", "/transfer-template-ends/", url.Values{"transfer_template_id": {p.transferTemplate}, "date": {"2030-01-01"}}},
		{"transfer template category", "/transfer-template-categories/", url.Values{"name": {"Food"}}},
		{"account type", "/account-types/", url.Values{"name": {"Pension"}}},
		{"special date", "/special-dates/", url.Values{"name": {"Retirement"}, "date": {"2050-01-01"}}},
		{"event", "/events/", url.Values{"name": {"Apartment"}, "date": {"2028-03-01"}}},
		{"swe yearly params", "/settings/swe-yearly-params/", url.Values{"valid_from": {"2025-01-01"}}},
	}
}

func TestUpsertingWithoutNextRedirectsToAPageThatExists(t *testing.T) {
	svc, srv := newSettingsServer(t)
	p := mustParents(t, svc)

	for _, route := range upsertRoutes(p) {
		t.Run(route.name, func(t *testing.T) {
			location := postForm(t, srv, route.path, route.form)
			getPage(t, srv, location)
		})
	}
}

func TestUpsertingWithNextRedirectsToNext(t *testing.T) {
	_, srv := newSettingsServer(t)

	if location := postForm(t, srv, "/accounts/?next=/budget", url.Values{"name": {"Savings"}}); location != "/budget" {
		t.Errorf("expected a redirect to /budget, got %q", location)
	}
}

func TestDeletingWithoutNextRedirectsToAPageThatExists(t *testing.T) {
	svc, srv := newSettingsServer(t)
	account, err := svc.UpsertAccount(t.Context(), model.AccountInput{Name: "Savings"})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}

	// Deleting a row that doesn't exist succeeds, except for an account.
	for _, path := range []string{
		"/accounts/" + account.ID + "/delete",
		"/account-types/missing/delete",
		"/special-dates/missing/delete",
		"/events/missing/delete",
		"/salaries/missing/delete",
		"/salary-amounts/missing/delete",
		"/salary-adjustments/missing/delete",
		"/partial-parental-leaves/missing/delete",
		"/full-parental-leaves/missing/delete",
		"/bills/missing/delete",
		"/bill-amounts/missing/delete",
		"/settings/swe-yearly-params/missing/delete",
		"/transfers/missing/delete",
		"/transfer-template-categories/missing/delete",
	} {
		t.Run(path, func(t *testing.T) {
			location := postForm(t, srv, path, nil)
			getPage(t, srv, location)
		})
	}
}

// lookupDelete is a delete route whose fallback is the edit page of the deleted row's parent.
type lookupDelete struct {
	name   string
	create func(t *testing.T, svc *model.Service, p parents) (path, parentEditPage string)
}

var lookupDeletes = []lookupDelete{
	{"growth model", func(t *testing.T, svc *model.Service, p parents) (string, string) {
		gm, err := svc.UpsertAccountGrowthModel(t.Context(), model.AccountGrowthModelInput{AccountID: p.account, Type: model.GrowthModelFixed, StartDate: date.Today()})
		if err != nil {
			t.Fatalf("create growth model: %v", err)
		}
		return "/growth-models/" + gm.ID + "/delete", "/accounts/" + p.account + "/edit"
	}},
	{"investment round", func(t *testing.T, svc *model.Service, p parents) (string, string) {
		r, err := svc.UpsertInvestmentRound(t.Context(), model.InvestmentRoundInput{AccountID: p.account, Date: date.Today()})
		if err != nil {
			t.Fatalf("create investment round: %v", err)
		}
		return "/investment-rounds/" + r.ID + "/delete", "/accounts/" + p.account + "/edit"
	}},
	{"share change", func(t *testing.T, svc *model.Service, p parents) (string, string) {
		sc, err := svc.UpsertShareChange(t.Context(), model.ShareChangeInput{AccountID: p.account, Date: date.Today()})
		if err != nil {
			t.Fatalf("create share change: %v", err)
		}
		return "/share-changes/" + sc.ID + "/delete", "/accounts/" + p.account + "/edit"
	}},
	{"startup share option", func(t *testing.T, svc *model.Service, p parents) (string, string) {
		o, err := svc.UpsertStartupShareOption(t.Context(), model.StartupShareOptionInput{AccountID: p.account, SourceAccountID: p.sourceAccount, GrantDate: date.Today(), EndDate: date.Today().Add(365 * date.Day)})
		if err != nil {
			t.Fatalf("create startup share option: %v", err)
		}
		return "/startup-share-options/" + o.ID + "/delete", "/accounts/" + p.account + "/edit"
	}},
	{"bill", func(t *testing.T, svc *model.Service, p parents) (string, string) {
		return "/bill-items/" + p.bill + "/delete", "/bills/" + p.billAccount + "/edit"
	}},
	{"transfer template end", func(t *testing.T, svc *model.Service, p parents) (string, string) {
		end, err := svc.UpsertTransferTemplateEnd(t.Context(), model.TransferTemplateEnd{TransferTemplateID: p.transferTemplate, Date: date.Today()})
		if err != nil {
			t.Fatalf("create transfer template end: %v", err)
		}
		return "/transfer-template-ends/" + end.ID + "/delete", "/transfer-templates/" + p.transferTemplate + "/edit"
	}},
}

func TestDeletingARowWithoutNextRedirectsToItsParentsEditPage(t *testing.T) {
	for _, d := range lookupDeletes {
		t.Run(d.name, func(t *testing.T) {
			svc, srv := newSettingsServer(t)
			path, want := d.create(t, svc, mustParents(t, svc))

			location := postForm(t, srv, path, nil)
			if location != want {
				t.Errorf("expected a redirect to %s, got %q", want, location)
			}
			getPage(t, srv, location)
		})
	}
}

func TestUpsertingWithoutARequiredNameIsABadRequest(t *testing.T) {
	_, srv := newSettingsServer(t)

	resp, err := srv.Client().PostForm(srv.URL+"/account-types/", url.Values{"name": {""}})
	if err != nil {
		t.Fatalf("save account type: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), "upserting account type") || !strings.Contains(string(body), "name is required") {
		t.Errorf("expected the response to name the entity and say why, got %q", body)
	}
}
