package model_test

import (
	"testing"

	"github.com/SimonSchneider/pefigo/internal/model"
)

// rowsNamed are the rows of the Transfer Templates page named name.
func rowsNamed(t *testing.T, svc *model.Service, name string) []model.TransferScheduleRow {
	t.Helper()
	view, err := svc.GetTransferTemplatesPageData(t.Context())
	if err != nil {
		t.Fatalf("get transfer templates page: %v", err)
	}
	var rows []model.TransferScheduleRow
	for _, row := range view.Rows {
		if row.Name == name {
			rows = append(rows, row)
		}
	}
	return rows
}

func TestSameNamedTransferTemplatesInAndOutsideAnEventAreSeparateRows(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	apartment := mustEvent(t, svc, model.EventInput{Name: "Apartment", Date: mustParseDate("2025-06-01")})
	savings := model.TransferTemplate{
		Name: "Savings", FromAccountID: checking.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(1000),
		Recurrence: "*-*-25", StartDate: mustParseDate("2025-01-01"), Enabled: true,
	}
	mustTransferTemplate(t, svc, savings)
	inApartment := savings
	inApartment.AmountFixed, inApartment.StartDate, inApartment.EventID = newFixedValue(500), 0, apartment.ID
	mustTransferTemplate(t, svc, inApartment)

	rows := rowsNamed(t, svc, "Savings")
	if len(rows) != 2 || rows[0].IsGroup() || rows[1].IsGroup() || rows[0].EventID == rows[1].EventID {
		t.Fatalf("expected Savings in the Baseline and Savings in the Apartment as two rows, got %+v", rows)
	}
}

func TestSameNamedBillsOfTwoBillAccountsStayInTheirOwnBillAccount(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	household := mustBillAccount(t, svc, "Household", checking.ID)
	cabin := mustBillAccount(t, svc, "Cabin", checking.ID)
	mustBillIn(t, svc, household, "Electricity", model.BillAmount{Amount: newFixedValue(500), StartDate: mustParseDate("2025-01-01")})
	mustBillIn(t, svc, cabin, "Electricity", model.BillAmount{Amount: newFixedValue(300), StartDate: mustParseDate("2025-01-01")})

	for _, want := range []struct {
		name   string
		amount float64
	}{{"Household", 500}, {"Cabin", 300}} {
		rows := rowsNamed(t, svc, want.name)
		if len(rows) != 1 || len(rows[0].Members) != 1 || rows[0].Members[0].Name != "Electricity" || rows[0].Amount != want.amount {
			t.Errorf("expected %s to hold its own Electricity of %f, got %+v", want.name, want.amount, rows)
		}
	}
}
