package model_test

import (
	"maps"
	"slices"
	"testing"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
)

// mustBill saves an enabled Bill with the given name and budget category, in its own enabled Bill
// account charging fromAccountID on recurrence.
func mustBill(t *testing.T, svc *model.Service, fromAccountID string, recurrence date.Cron, bill model.Bill, amounts ...model.BillAmount) model.Bill {
	t.Helper()
	ctx := t.Context()
	ba, err := svc.UpsertBillAccount(ctx, model.BillAccount{Name: bill.Name + " bills", FromAccountID: fromAccountID, Recurrence: recurrence, Priority: 1, Enabled: true})
	if err != nil {
		t.Fatalf("create bill account: %v", err)
	}
	bill.BillAccountID = ba.ID
	bill.Enabled = true
	bill, err = svc.UpsertBill(ctx, bill)
	if err != nil {
		t.Fatalf("create bill: %v", err)
	}
	for _, amt := range amounts {
		amt.BillID = bill.ID
		if _, err := svc.UpsertBillAmount(ctx, amt); err != nil {
			t.Fatalf("create bill amount: %v", err)
		}
	}
	return bill
}

func mustTransferSchedule(t *testing.T, svc *model.Service) *model.TransferSchedule {
	t.Helper()
	schedule, err := svc.TransferSchedule(t.Context())
	if err != nil {
		t.Fatalf("load transfer schedule: %v", err)
	}
	return schedule
}

func chargedAmounts(day *model.TransferScheduleDay) []float64 {
	var amounts []float64
	for _, t := range day.Charged() {
		amounts = append(amounts, t.Amount)
	}
	return amounts
}

func TestTransferScheduleChargesABillPriceChangeOnceOnItsRecurrenceDay(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	mustBill(t, svc, checking.ID, "*-*-01", model.Bill{Name: "Music"},
		model.BillAmount{Amount: newFixedValue(100), StartDate: mustParseDate("2025-01-01")},
		model.BillAmount{Amount: newFixedValue(200), StartDate: mustParseDate("2025-03-01")},
	)
	schedule := mustTransferSchedule(t, svc)

	for _, tc := range []struct {
		day  string
		want []float64
	}{
		{"2025-02-01", []float64{100}},
		{"2025-03-01", []float64{200}},
		{"2025-04-01", []float64{200}},
	} {
		if got := chargedAmounts(schedule.On(mustParseDate(tc.day))); !slices.Equal(got, tc.want) {
			t.Errorf("on %s: expected charges %v, got %v", tc.day, tc.want, got)
		}
	}
}

func mustTransferTemplate(t *testing.T, svc *model.Service, tt model.TransferTemplate) model.TransferTemplate {
	t.Helper()
	saved, err := svc.UpsertTransferTemplate(t.Context(), tt)
	if err != nil {
		t.Fatalf("create transfer template %q: %v", tt.Name, err)
	}
	return saved
}

func TestTransferScheduleAppliesATransferTemplateFromItsStartDateUpToButNotOnItsEndDate(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	end := mustParseDate("2025-04-01")
	mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Gym", FromAccountID: checking.ID, AmountType: "fixed", AmountFixed: newFixedValue(300),
		Recurrence: "*-*-01", StartDate: mustParseDate("2025-02-01"), EndDate: &end, Enabled: true,
	})
	schedule := mustTransferSchedule(t, svc)

	for _, tc := range []struct {
		day     string
		state   model.TransferState
		charged []float64
	}{
		{"2025-01-01", model.TransferInactive, nil},
		{"2025-02-01", model.TransferActive, []float64{300}},
		{"2025-03-01", model.TransferActive, []float64{300}},
		{"2025-03-31", model.TransferActive, nil},
		{"2025-04-01", model.TransferInactive, nil},
	} {
		day := schedule.On(mustParseDate(tc.day))
		if got := day.Transfers()[0].ActiveState(); got != tc.state {
			t.Errorf("on %s: expected %s, got %s", tc.day, tc.state, got)
		}
		if got := chargedAmounts(day); !slices.Equal(got, tc.charged) {
			t.Errorf("on %s: expected charges %v, got %v", tc.day, tc.charged, got)
		}
	}
}

func TestTransferScheduleChargesNoDisabledTransferTemplate(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Gym", FromAccountID: checking.ID, AmountType: "fixed", AmountFixed: newFixedValue(300),
		Recurrence: "*-*-01", StartDate: mustParseDate("2025-01-01"), Enabled: false,
	})

	day := mustTransferSchedule(t, svc).On(mustParseDate("2025-02-01"))

	if got := day.Transfers()[0]; got.ActiveState() != model.TransferDisabled || got.Amount != 0 {
		t.Errorf("expected Disabled with amount 0, got %s with %f", got.ActiveState(), got.Amount)
	}
	if got := day.Charged(); len(got) != 0 {
		t.Errorf("expected no charges, got %v", got)
	}
}

func amountsByID(day *model.TransferScheduleDay) map[string]float64 {
	amounts := make(map[string]float64)
	for _, t := range day.Transfers() {
		amounts[t.ID] = t.Amount
	}
	return amounts
}

func TestTransferSchedulePercentAmountsChainOffEarlierPriorityGroups(t *testing.T) {
	svc := newTestService(t)
	salaryAcc := mustAccount(t, svc, "Lön")
	savings := mustAccount(t, svc, "Gem")
	oldEnd := mustParseDate("2024-01-01")
	oldSalary := mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Lön", ToAccountID: salaryAcc.ID, AmountType: "fixed", AmountFixed: newFixedValue(4000),
		Priority: 1, Recurrence: "*-*-25", StartDate: mustParseDate("2020-01-01"), EndDate: &oldEnd, Enabled: true,
	})
	salary := mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Lön", ToAccountID: salaryAcc.ID, AmountType: "fixed", AmountFixed: newFixedValue(5000),
		Priority: 1, Recurrence: "*-*-25", StartDate: mustParseDate("2024-01-01"), Enabled: true,
	})
	samePriority := mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Spar", FromAccountID: salaryAcc.ID, ToAccountID: savings.ID, AmountType: "percent", AmountFixed: newFixedValue(0), AmountPercent: 0.1,
		Priority: 1, Recurrence: "*-*-25", StartDate: mustParseDate("2020-01-01"), Enabled: true,
	})
	half := mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Gem bidrag", FromAccountID: salaryAcc.ID, ToAccountID: savings.ID, AmountType: "percent", AmountFixed: newFixedValue(0), AmountPercent: 0.5,
		Priority: 3, Recurrence: "*-*-25", StartDate: mustParseDate("2020-01-01"), Enabled: true,
	})
	rest := mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Rest", FromAccountID: salaryAcc.ID, ToAccountID: savings.ID, AmountType: "percent", AmountFixed: newFixedValue(0), AmountPercent: 1,
		Priority: 4, Recurrence: "*-*-25", StartDate: mustParseDate("2020-01-01"), Enabled: true,
	})

	got := amountsByID(mustTransferSchedule(t, svc).On(mustParseDate("2025-06-25")))

	for _, want := range []struct {
		name   string
		id     string
		amount float64
	}{
		{"old salary (ended)", oldSalary.ID, 0},
		{"salary", salary.ID, 5000},
		{"percent in the salary's priority group", samePriority.ID, 0},
		{"50% after the salary", half.ID, 2500},
		{"100% of what is left", rest.ID, 2500},
	} {
		if got[want.id] != want.amount {
			t.Errorf("%s: expected %f, got %f", want.name, want.amount, got[want.id])
		}
	}
}

func TestTransferScheduleMonthlyViewHoldsActiveRecurringTransferTemplatesByKind(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	savings := mustAccount(t, svc, "Savings")
	start := mustParseDate("2025-01-01")
	ended := mustParseDate("2025-06-01")
	for _, tt := range []model.TransferTemplate{
		{Name: "Salary", ToAccountID: checking.ID, AmountFixed: newFixedValue(5000), Recurrence: "*-*-25", StartDate: start, Enabled: true},
		{Name: "Bonus", ToAccountID: checking.ID, AmountFixed: newFixedValue(10000), Recurrence: "2025-06-01", StartDate: start, Enabled: true},
		{Name: "Rent", FromAccountID: checking.ID, AmountFixed: newFixedValue(1200), Recurrence: "*-*-01", StartDate: start, Enabled: true},
		{Name: "Old rent", FromAccountID: checking.ID, AmountFixed: newFixedValue(900), Recurrence: "*-*-01", StartDate: start, EndDate: &ended, Enabled: true},
		{Name: "Gym", FromAccountID: checking.ID, AmountFixed: newFixedValue(300), Recurrence: "*-*-01", StartDate: start, Enabled: false},
		{Name: "Saving", FromAccountID: checking.ID, ToAccountID: savings.ID, AmountFixed: newFixedValue(1000), Recurrence: "*-*-25", StartDate: start, Enabled: true},
	} {
		tt.AmountType = "fixed"
		mustTransferTemplate(t, svc, tt)
	}

	got := make(map[string]model.TransferKind)
	for _, tt := range mustTransferSchedule(t, svc).On(mustParseDate("2025-06-15")).Monthly() {
		got[tt.Name] = tt.Kind()
	}

	want := map[string]model.TransferKind{
		"Salary": model.TransferIncome,
		"Rent":   model.TransferExpense,
		"Saving": model.TransferInternal,
	}
	if !maps.Equal(got, want) {
		t.Errorf("expected monthly transfers %v, got %v", want, got)
	}
}

func TestTransferScheduleRowsGroupTransferTemplatesAfterEvaluatingThem(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	mustBill(t, svc, checking.ID, "*-*-01", model.Bill{Name: "Music"},
		model.BillAmount{Amount: newFixedValue(100), StartDate: mustParseDate("2025-01-01")},
		model.BillAmount{Amount: newFixedValue(200), StartDate: mustParseDate("2025-03-01")},
	)
	mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Rent", FromAccountID: checking.ID, AmountType: "fixed", AmountFixed: newFixedValue(1200),
		Priority: 2, Recurrence: "*-*-01", StartDate: mustParseDate("2025-01-01"), Enabled: true,
	})
	for _, start := range []string{"2024-01-01", "2024-06-01"} {
		end := mustParseDate(start).Add(30 * date.Day)
		mustTransferTemplate(t, svc, model.TransferTemplate{
			Name: "Gym", FromAccountID: checking.ID, AmountType: "fixed", AmountFixed: newFixedValue(300),
			Priority: 3, Recurrence: "*-*-01", StartDate: mustParseDate(start), EndDate: &end, Enabled: true,
		})
	}
	day := mustTransferSchedule(t, svc).On(mustParseDate("2025-06-01"))

	rows := make(map[string]model.TransferScheduleRow)
	var rowsTotal float64
	for _, row := range day.Rows() {
		rows[row.Name] = row
		rowsTotal += row.Amount
	}
	if len(rows) != 3 {
		t.Fatalf("expected rows Music, Rent and Gym, got %v", rows)
	}
	for _, want := range []struct {
		name    string
		members int
		amount  float64
		state   model.TransferState
	}{
		{"Music", 2, 200, model.TransferActive},
		{"Rent", 0, 1200, model.TransferActive},
		{"Gym", 2, 0, model.TransferInactive},
	} {
		row := rows[want.name]
		if len(row.Members) != want.members || row.Amount != want.amount || row.ActiveState() != want.state {
			t.Errorf("%s: expected %d members, amount %f and %s, got %d members, amount %f and %s",
				want.name, want.members, want.amount, want.state, len(row.Members), row.Amount, row.ActiveState())
		}
	}
	var transfersTotal float64
	for _, tt := range day.Transfers() {
		transfersTotal += tt.Amount
	}
	if rowsTotal != transfersTotal {
		t.Errorf("expected grouping to keep the total %f, got %f", transfersTotal, rowsTotal)
	}
}

func TestTransferScheduleChargesPercentAmountsFromTheDaysTransfersOnly(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	savings := mustAccount(t, svc, "Savings")
	start := mustParseDate("2025-01-01")
	mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Side income", ToAccountID: checking.ID, AmountType: "fixed", AmountFixed: newFixedValue(1000),
		Priority: 0, Recurrence: "*-*-01", StartDate: start, Enabled: true,
	})
	mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Salary", ToAccountID: checking.ID, AmountType: "fixed", AmountFixed: newFixedValue(4000),
		Priority: 1, Recurrence: "*-*-25", StartDate: start, Enabled: true,
	})
	mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Saving", FromAccountID: checking.ID, ToAccountID: savings.ID, AmountType: "percent", AmountFixed: newFixedValue(0), AmountPercent: 0.5,
		Priority: 2, Recurrence: "*-*-25", StartDate: start, Enabled: true,
	})
	day := mustTransferSchedule(t, svc).On(mustParseDate("2025-06-25"))

	charged := make(map[string]float64)
	for _, tt := range day.Charged() {
		charged[tt.Name] = tt.Amount
	}
	if want := map[string]float64{"Salary": 4000, "Saving": 2000}; !maps.Equal(charged, want) {
		t.Errorf("expected charges %v on the 25th, got %v", want, charged)
	}
	monthly := make(map[string]float64)
	for _, tt := range day.Monthly() {
		monthly[tt.Name] = tt.Amount
	}
	if monthly["Saving"] != 2500 {
		t.Errorf("expected a month of saving to be half of all income, 2500, got %f", monthly["Saving"])
	}
}

func TestTransferScheduleChargesADaysSamePriorityTransfersTogetherAsTheForecastDoes(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	savings := mustAccount(t, svc, "Savings")
	start := mustParseDate("2025-01-01")
	mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Income", ToAccountID: checking.ID, AmountType: "fixed", AmountFixed: newFixedValue(4000),
		Priority: 1, Recurrence: "*-*-*", StartDate: start, Enabled: true,
	})
	mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Saving", FromAccountID: checking.ID, ToAccountID: savings.ID, AmountType: "percent", AmountFixed: newFixedValue(0), AmountPercent: 0.5,
		Priority: 1, Recurrence: "*-*-25", StartDate: start, Enabled: true,
	})

	charged := make(map[string]float64)
	for _, tt := range mustTransferSchedule(t, svc).On(mustParseDate("2025-06-25")).Charged() {
		charged[tt.Name] = tt.Amount
	}

	// Transfers of one priority on a day happen at the same time, so Saving sees Checking before Income.
	if want := map[string]float64{"Income": 4000, "Saving": 0}; !maps.Equal(charged, want) {
		t.Errorf("expected charges %v on the 25th, got %v", want, charged)
	}
}
