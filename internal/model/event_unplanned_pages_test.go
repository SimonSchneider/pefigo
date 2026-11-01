package model_test

import (
	"testing"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
)

func TestASalaryInAnUnplannedEventShowsWhatItWouldPayWithTheEventAtItsDate(t *testing.T) {
	svc := newSalaryService(t)
	ctx := t.Context()
	checking := mustAccount(t, svc, "Checking")
	newJob := mustEvent(t, svc, model.EventInput{Name: "New job", Date: mustParseDate("2027-09-01"), Planned: ptr(false)})
	salaryB := mustSalary(t, svc, grossSalaryFrom(t, svc, "Salary B", checking.ID, newJob.ID),
		model.SalaryAmount{Amount: newFixedValue(80000), EventID: newJob.ID, StartOffset: days(0)})

	page, err := svc.GetSalaryEditPageData(ctx, salaryB.ID)
	if err != nil {
		t.Fatalf("salary page: %v", err)
	}
	if page.Salary.InBaseline() {
		t.Errorf("expected Salary B, in the unplanned New job, out of the Baseline")
	}
	segments, err := page.Salary.Segments()
	if err != nil {
		t.Fatalf("segments: %v", err)
	}
	if len(segments) == 0 || segments[0].StartDate != newJob.Date || segments[0].Breakdown.GrossMonthly != 80000 {
		t.Errorf("expected Salary B's Salary Breakdown of 80 000 from the New job's date, got %+v", segments)
	}
}

func TestABillInAnUnplannedEventShowsWhatItWouldChargeWithTheEventAtItsDate(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	checking := mustAccount(t, svc, "Checking")
	move := mustEvent(t, svc, model.EventInput{Name: "Move", Date: date.Today().Add(-30 * date.Day), Planned: ptr(false)})
	household := mustBillAccount(t, svc, "Household", checking.ID)
	rent, err := svc.UpsertBill(ctx, model.Bill{Name: "Rent", BillAccountID: household.ID, Enabled: true, EventID: move.ID})
	if err != nil {
		t.Fatalf("save bill: %v", err)
	}
	if _, err := svc.UpsertBillAmount(ctx, model.BillAmount{BillID: rent.ID, EventID: move.ID, StartOffset: days(0), Amount: newFixedValue(9000)}); err != nil {
		t.Fatalf("save bill amount: %v", err)
	}

	page, err := svc.GetBillEditPageData(ctx, rent.ID)
	if err != nil {
		t.Fatalf("bill page: %v", err)
	}
	if page.Bill.InBaseline() {
		t.Errorf("expected the Rent, in the unplanned Move, out of the Baseline")
	}
	if got := page.Bill.CurrentAmount(); got != 9000 {
		t.Errorf("expected the Rent to show the 9 000 it would charge with the Move at its date, got %v", got)
	}
}

func TestTheTransferTemplatesPageListsWhatSalariesAndBillsOfUnplannedEventsWouldMoveApart(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	checking := mustAccount(t, svc, "Checking")
	newJob := mustEvent(t, svc, model.EventInput{Name: "New job", Date: mustParseDate("2027-09-01"), Planned: ptr(false)})
	mustSalary(t, svc, model.Salary{Name: "Salary B", ToAccountID: checking.ID, Recurrence: "*-*-25", EventID: newJob.ID},
		model.SalaryAmount{Amount: newFixedValue(40000), EventID: newJob.ID, StartOffset: days(0)})
	move := mustEvent(t, svc, model.EventInput{Name: "Move", Date: mustParseDate("2027-10-01"), Planned: ptr(false)})
	rent, err := svc.UpsertBill(ctx, model.Bill{Name: "Rent", BillAccountID: mustBillAccount(t, svc, "Household", checking.ID).ID, Enabled: true, EventID: move.ID})
	if err != nil {
		t.Fatalf("save bill: %v", err)
	}
	if _, err := svc.UpsertBillAmount(ctx, model.BillAmount{BillID: rent.ID, EventID: move.ID, StartOffset: days(0), Amount: newFixedValue(9000)}); err != nil {
		t.Fatalf("save bill amount: %v", err)
	}

	page, err := svc.GetTransferTemplatesPageData(ctx)
	if err != nil {
		t.Fatalf("transfer templates page: %v", err)
	}
	want := map[string]model.Event{"Salary B": newJob, "Rent": move}
	for _, tt := range page.LeftOut {
		if ev, ok := want[tt.Name]; ok && tt.EventID == ev.ID && tt.StartDate == ev.Date {
			delete(want, tt.Name)
		}
	}
	if len(want) > 0 {
		t.Errorf("expected the page to list %v apart, from their Events' dates, got %+v", want, page.LeftOut)
	}
	for _, row := range page.Rows {
		if row.Name == "Salary B" || row.Name == "Household" {
			t.Errorf("expected nothing of the unplanned Events in the Baseline's rows, got %+v", row)
		}
	}
}

// twoUnplannedEvents are a first Event, 30 days ago, and a second, 10 days ago, both unplanned.
func twoUnplannedEvents(t *testing.T, svc *model.Service, first, second string) (model.Event, model.Event) {
	t.Helper()
	today := date.Today()
	return mustEvent(t, svc, model.EventInput{Name: first, Date: today.Add(-30 * date.Day), Planned: ptr(false)}),
		mustEvent(t, svc, model.EventInput{Name: second, Date: today.Add(-10 * date.Day), Planned: ptr(false)})
}

func TestAnUnplannedEventsSalaryShowsWhatItWouldPayWithNoOtherUnplannedEvent(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	checking := mustAccount(t, svc, "Checking")
	newJob, quit := twoUnplannedEvents(t, svc, "New job", "Quit")
	salaryB := mustSalary(t, svc, model.Salary{Name: "Salary B", ToAccountID: checking.ID, Recurrence: "*-*-25", EventID: newJob.ID},
		model.SalaryAmount{Amount: newFixedValue(40000), EventID: newJob.ID, StartOffset: days(0)})
	// The Quit, another unplanned Event, ends Salary B: only a Scenario with both would see it.
	mustSalaryEnd(t, svc, model.SalaryEnd{SalaryID: salaryB.ID, EventID: quit.ID, DateOffset: days(0)})

	page, err := svc.GetSalaryEditPageData(ctx, salaryB.ID)
	if err != nil {
		t.Fatalf("salary page: %v", err)
	}
	if got := page.Salary.CurrentAmount(); got != 40000 {
		t.Errorf("expected Salary B's page to show the 40 000 it would pay with only the New job planned, got %v", got)
	}
	list, err := svc.GetSalariesPageData(ctx)
	if err != nil {
		t.Fatalf("salaries page: %v", err)
	}
	for _, sal := range list.Salaries {
		if sal.ID == salaryB.ID && sal.CurrentAmount() != 40000 {
			t.Errorf("expected the Salaries page to show Salary B's 40 000, got %v", sal.CurrentAmount())
		}
	}
	templates, err := svc.GetTransferTemplatesPageData(ctx)
	if err != nil {
		t.Fatalf("transfer templates page: %v", err)
	}
	for _, tt := range templates.LeftOut {
		if tt.Name == "Salary B" && tt.EndDate != nil {
			t.Errorf("expected Salary B's left-out row without the Quit's end, got it ending %s", tt.EndDate)
		}
	}
}

func TestAnUnplannedEventsBillShowsWhatItWouldChargeWithNoOtherUnplannedEvent(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	checking := mustAccount(t, svc, "Checking")
	move, raise := twoUnplannedEvents(t, svc, "Move", "Raise")
	rent, err := svc.UpsertBill(ctx, model.Bill{Name: "Rent", BillAccountID: mustBillAccount(t, svc, "Household", checking.ID).ID, Enabled: true, EventID: move.ID})
	if err != nil {
		t.Fatalf("save bill: %v", err)
	}
	for _, amt := range []model.BillAmount{
		{BillID: rent.ID, EventID: move.ID, StartOffset: days(0), Amount: newFixedValue(9000)},
		// Another unplanned Event raises the Rent.
		{BillID: rent.ID, EventID: raise.ID, StartOffset: days(0), Amount: newFixedValue(12000)},
	} {
		if _, err := svc.UpsertBillAmount(ctx, amt); err != nil {
			t.Fatalf("save bill amount: %v", err)
		}
	}

	page, err := svc.GetBillEditPageData(ctx, rent.ID)
	if err != nil {
		t.Fatalf("bill page: %v", err)
	}
	if got := page.Bill.CurrentAmount(); got != 9000 {
		t.Errorf("expected the Rent's page to show the 9 000 it would charge with only the Move planned, got %v", got)
	}
	list, err := svc.GetBillAccountsPageData(ctx)
	if err != nil {
		t.Fatalf("bills page: %v", err)
	}
	for _, ba := range list.BillAccounts {
		for _, b := range ba.Bills {
			if b.ID == rent.ID && b.CurrentAmount() != 9000 {
				t.Errorf("expected the Bills page to show the Rent's 9 000, got %v", b.CurrentAmount())
			}
		}
	}
}

func TestAnUnplannedEventsStartupShareAccountShowsWhatItWouldHoldWithNoOtherUnplannedEvent(t *testing.T) {
	svc := newTestService(t)
	founding, seriesA := twoUnplannedEvents(t, svc, "Founding", "Series A")
	startup := mustEventAccount(t, svc, "Startup Co", founding.ID)
	mustStartupShareAccount(t, svc, startup.ID,
		[]model.InvestmentRoundInput{{EventID: founding.ID, DateOffset: days(0), Valuation: 1e6, PreMoneyShares: 10000}},
		[]model.ShareChangeInput{{EventID: founding.ID, DateOffset: days(0), DeltaShares: 1000, TotalPrice: 1000}},
	)
	// Another unplanned Event's round would make it 12 500 shares.
	mustInvestmentRound(t, svc, model.InvestmentRoundInput{AccountID: startup.ID, EventID: seriesA.ID, DateOffset: days(0), Valuation: 4e6, PreMoneyShares: 10000, Investment: 1e6})

	page, err := svc.GetAccountEditPageData(t.Context(), startup.ID, nil)
	if err != nil {
		t.Fatalf("account page: %v", err)
	}
	if page.DerivedStartupShareSummary == nil || page.DerivedStartupShareSummary.TotalShares != 10000 {
		t.Errorf("expected Startup Co's page to show the 10 000 shares it would have with only its own Event planned, got %+v", page.DerivedStartupShareSummary)
	}
}

func TestAnUnplannedBillWhoseExchangeRateCantBeFetchedFailsNoPage(t *testing.T) {
	rates := newRateServer(t)
	rates.failing.Store(true)
	svc := newTestService(t, rates.option())
	ctx := t.Context()
	checking := mustAccount(t, svc, "Checking")
	mustSnapshot(t, svc, checking.ID, date.Today(), 1000)
	household := mustBillAccount(t, svc, "Household", checking.ID)
	mustBillIn(t, svc, household, "Power", model.BillAmount{Amount: newFixedValue(500), StartDate: mustParseDate("2025-01-01")})
	move := mustEvent(t, svc, model.EventInput{Name: "Move abroad", Date: date.Today().Add(-30 * date.Day), Planned: ptr(false)})
	streaming, err := svc.UpsertBill(ctx, model.Bill{Name: "Streaming", BillAccountID: household.ID, Enabled: true, EventID: move.ID})
	if err != nil {
		t.Fatalf("save bill: %v", err)
	}
	if _, err := svc.UpsertBillAmount(ctx, model.BillAmount{BillID: streaming.ID, EventID: move.ID, StartOffset: days(0), Amount: newFixedValue(10), Currency: "EUR"}); err != nil {
		t.Fatalf("save bill amount: %v", err)
	}

	if _, err := svc.GetBudgetData(ctx); err != nil {
		t.Errorf("expected the Budget page without the unplanned Bill's rate, got %v", err)
	}
	if _, err := svc.GetTransferChartData(ctx, model.GroupByAccount); err != nil {
		t.Errorf("expected the transfer chart without the unplanned Bill's rate, got %v", err)
	}
	templates, err := svc.GetTransferTemplatesPageData(ctx)
	if err != nil {
		t.Fatalf("expected the Transfer Templates page without the unplanned Bill's rate, got %v", err)
	}
	if len(templates.Unavailable) != 1 || templates.Unavailable[0].Name != "Streaming" || templates.Unavailable[0].EventID != move.ID || templates.Unavailable[0].Reason == "" {
		t.Errorf("expected the Streaming listed apart as unavailable, with why, got %+v", templates.Unavailable)
	}
	bills, err := svc.GetBillAccountsPageData(ctx)
	if err != nil {
		t.Fatalf("expected the Bills page without the unplanned Bill's rate, got %v", err)
	}
	for _, ba := range bills.BillAccounts {
		for _, b := range ba.Bills {
			if (b.ID == streaming.ID) != (b.ChargeError() != nil) {
				t.Errorf("expected only the Streaming to say what it would charge is unavailable, got %q: %v", b.Name, b.ChargeError())
			}
		}
	}
	if _, err := svc.GetBillEditPageData(ctx, streaming.ID); err != nil {
		t.Errorf("expected the Streaming's page without its rate, got %v", err)
	}
}
