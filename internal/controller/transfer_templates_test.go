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
