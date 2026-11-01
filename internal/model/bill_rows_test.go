package model_test

import (
	"testing"

	"github.com/SimonSchneider/pefigo/internal/model"
)

// mustBillAccount saves an enabled Bill Account named name, paid from fromAccountID on the 1st.
func mustBillAccount(t *testing.T, svc *model.Service, name, fromAccountID string) model.BillAccount {
	t.Helper()
	ba, err := svc.UpsertBillAccount(t.Context(), model.BillAccount{Name: name, FromAccountID: fromAccountID, Recurrence: "*-*-01", Priority: 1, Enabled: true})
	if err != nil {
		t.Fatalf("create bill account %q: %v", name, err)
	}
	return ba
}

// mustBillIn saves an enabled Bill named name in the Bill Account ba with amounts.
func mustBillIn(t *testing.T, svc *model.Service, ba model.BillAccount, name string, amounts ...model.BillAmount) model.Bill {
	t.Helper()
	bill, err := svc.UpsertBill(t.Context(), model.Bill{Name: name, BillAccountID: ba.ID, Enabled: true})
	if err != nil {
		t.Fatalf("create bill %q: %v", name, err)
	}
	for _, amt := range amounts {
		amt.BillID = bill.ID
		if _, err := svc.UpsertBillAmount(t.Context(), amt); err != nil {
			t.Fatalf("create bill amount: %v", err)
		}
	}
	return bill
}

func TestTheTransferTemplatesPageFoldsTheBillsOfEachBillAccountIntoOneRow(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	household := mustBillAccount(t, svc, "Household", checking.ID)
	mustBillIn(t, svc, household, "Electricity", model.BillAmount{Amount: newFixedValue(500), StartDate: mustParseDate("2025-01-01")})
	mustBillIn(t, svc, household, "Internet",
		model.BillAmount{Amount: newFixedValue(300), StartDate: mustParseDate("2025-01-01")},
		model.BillAmount{Amount: newFixedValue(400), StartDate: mustParseDate("2025-03-01")},
	)
	mustBillIn(t, svc, household, "Insurance", model.BillAmount{Amount: newFixedValue(200), StartDate: mustParseDate("2025-01-01")})
	subscriptions := mustBillAccount(t, svc, "Subscriptions", checking.ID)
	mustBillIn(t, svc, subscriptions, "Music", model.BillAmount{Amount: newFixedValue(100), StartDate: mustParseDate("2025-01-01")})
	mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Rent", FromAccountID: checking.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(1200),
		Priority: 2, Recurrence: "*-*-01", StartDate: mustParseDate("2025-01-01"), Enabled: true,
	})

	view, err := svc.GetTransferTemplatesPageData(t.Context())
	if err != nil {
		t.Fatalf("get transfer templates page: %v", err)
	}
	rows := make(map[string]model.TransferScheduleRow)
	for _, row := range view.Rows {
		rows[row.Name] = row
	}
	if len(rows) != 3 {
		t.Fatalf("expected the rows Household, Subscriptions and Rent, got %v", rows)
	}
	for _, want := range []struct {
		name        string
		billAccount string
		bills       []string
		amount      float64
	}{
		{"Household", household.ID, []string{"Electricity", "Internet", "Insurance"}, 1100},
		{"Subscriptions", subscriptions.ID, []string{"Music"}, 100},
	} {
		row := rows[want.name]
		var bills []string
		for _, m := range row.Members {
			bills = append(bills, m.Name)
		}
		if !row.IsGroup() || row.Source.BillAccountID != want.billAccount || row.Amount != want.amount || row.ActiveState() != model.TransferActive {
			t.Errorf("%s: expected an active group of its Bill Account with %f, got %+v", want.name, want.amount, row.ScheduledTransfer)
		}
		if len(bills) != len(want.bills) {
			t.Errorf("%s: expected the Bills %v, got %v", want.name, want.bills, bills)
		}
		for _, name := range want.bills {
			found := false
			for _, b := range bills {
				found = found || b == name
			}
			if !found {
				t.Errorf("%s: expected the Bill %s in %v", want.name, name, bills)
			}
		}
	}
	for _, bill := range rows["Household"].Members {
		if bill.Name == "Internet" && (len(bill.Members) != 2 || bill.Amount != 400) {
			t.Errorf("expected Internet to stay one row of its 2 amounts, charging 400, got %d members and %f", len(bill.Members), bill.Amount)
		}
	}
	if rent := rows["Rent"]; rent.IsGroup() || rent.Amount != 1200 {
		t.Errorf("expected Rent on its own row, got %+v", rent)
	}
	if view.MonthlyExpenses != -2400 {
		t.Errorf("expected folding to keep the monthly expenses at -2400, got %f", view.MonthlyExpenses)
	}
	if got := view.TemplateCount(); got != 5 {
		t.Errorf("expected 5 Transfer Templates, each Bill once, got %d", got)
	}
}
