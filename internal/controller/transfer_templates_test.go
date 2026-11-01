package controller_test

import (
	"strings"
	"testing"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

// linkHrefs are the hrefs of every <a> tag titled title on page.
func linkHrefs(page, title string) []string {
	var hrefs []string
	for _, tag := range strings.Split(page, "<a ")[1:] {
		tag = tag[:strings.Index(tag, ">")]
		if !strings.Contains(tag, `title="`+title+`"`) {
			continue
		}
		_, href, _ := strings.Cut(tag, `href="`)
		href, _, _ = strings.Cut(href, `"`)
		hrefs = append(hrefs, href)
	}
	return hrefs
}

func TestTheTransferTemplatesPageLinksGeneratedTransfersToWhatTheyAreGeneratedFrom(t *testing.T) {
	svc, srv := newSettingsServer(t)
	ctx := t.Context()
	start, _ := date.ParseDate("2025-01-01")
	checking, err := svc.UpsertAccount(ctx, model.AccountInput{Name: "Checking"})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	salary, err := svc.UpsertSalary(ctx, model.Salary{Name: "Acme", ToAccountID: checking.ID, Recurrence: "*-*-25", Enabled: true})
	if err != nil {
		t.Fatalf("create salary: %v", err)
	}
	if _, err := svc.UpsertSalaryAmount(ctx, model.SalaryAmount{SalaryID: salary.ID, Amount: uncertain.NewFixed(30000), StartDate: start}); err != nil {
		t.Fatalf("create salary amount: %v", err)
	}
	billAccount, err := svc.UpsertBillAccount(ctx, model.BillAccount{Name: "Streaming", FromAccountID: checking.ID, Recurrence: "*-*-01", Enabled: true})
	if err != nil {
		t.Fatalf("create bill account: %v", err)
	}
	bill, err := svc.UpsertBill(ctx, model.Bill{Name: "Netflix", BillAccountID: billAccount.ID, Enabled: true})
	if err != nil {
		t.Fatalf("create bill: %v", err)
	}
	if _, err := svc.UpsertBillAmount(ctx, model.BillAmount{BillID: bill.ID, Amount: uncertain.NewFixed(149), StartDate: start}); err != nil {
		t.Fatalf("create bill amount: %v", err)
	}

	page := getPage(t, srv, "/transfer-templates")
	for title, want := range map[string]string{
		"Edit Acme":    "/salaries/" + salary.ID + "/edit",
		"Edit Netflix": "/bills/" + billAccount.ID + "/edit",
	} {
		hrefs := linkHrefs(page, title)
		if len(hrefs) == 0 {
			t.Errorf("expected a link titled %q", title)
		}
		for _, href := range hrefs {
			if href != want {
				t.Errorf("expected the link titled %q to lead to %s, got %s", title, want, href)
			}
		}
	}
}

// tableRowTags are the opening <tr> tags on page, each with the text up to the next one.
func tableRowTags(page string) []string {
	return strings.Split(page, "<tr")[1:]
}

func TestTheTransferTemplatesPageShowsEachBillAccountCollapsedWithItsBills(t *testing.T) {
	svc, srv := newSettingsServer(t)
	ctx := t.Context()
	start, _ := date.ParseDate("2025-01-01")
	checking, err := svc.UpsertAccount(ctx, model.AccountInput{Name: "Checking"})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	household, err := svc.UpsertBillAccount(ctx, model.BillAccount{Name: "Household", FromAccountID: checking.ID, Recurrence: "*-*-01", Enabled: true})
	if err != nil {
		t.Fatalf("create bill account: %v", err)
	}
	for _, name := range []string{"Electricity", "Internet"} {
		bill, err := svc.UpsertBill(ctx, model.Bill{Name: name, BillAccountID: household.ID, Enabled: true})
		if err != nil {
			t.Fatalf("create bill: %v", err)
		}
		if _, err := svc.UpsertBillAmount(ctx, model.BillAmount{BillID: bill.ID, Amount: uncertain.NewFixed(300), StartDate: start}); err != nil {
			t.Fatalf("create bill amount: %v", err)
		}
	}

	page := getPage(t, srv, "/transfer-templates")
	groupID := "bill-account-" + household.ID
	var group string
	bills := map[string]bool{}
	for _, row := range tableRowTags(page) {
		tag := row[:strings.Index(row, ">")]
		switch {
		case strings.Contains(tag, `data-template-id="`+groupID+`"`):
			group = row
			if !strings.Contains(tag, "collapsed") {
				t.Errorf("expected the Household row to be collapsed, got %s", tag)
			}
		case strings.Contains(tag, `data-parent-id="`+groupID+`"`):
			if !strings.Contains(tag, "hidden") {
				t.Errorf("expected the Household's Bills hidden until it is expanded, got %s", tag)
			}
			for _, name := range []string{"Electricity", "Internet"} {
				bills[name] = bills[name] || strings.Contains(row, name)
			}
		}
	}
	if !strings.Contains(group, "Household") || !strings.Contains(group, "600") {
		t.Fatalf("expected a Household row with the total 600 of its Bills, got %q", group)
	}
	if hrefs := linkHrefs(group, "Edit Household"); len(hrefs) != 1 || hrefs[0] != "/bills/"+household.ID+"/edit" {
		t.Errorf("expected the Household row to link to its Bill Account, got %v", hrefs)
	}
	if !bills["Electricity"] || !bills["Internet"] {
		t.Errorf("expected Electricity and Internet in the Household, got %v", bills)
	}
}
