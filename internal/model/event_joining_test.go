package model_test

import (
	"testing"

	"github.com/SimonSchneider/pefigo/internal/model"
)

func TestAOneOffSalaryInAnEventIsPaidOnTheEventsDayWhereverTheEventMoves(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	bonus := mustEvent(t, svc, model.EventInput{Name: "Bonus", Date: mustParseDate("2027-06-01")})
	mustSalary(t, svc, model.Salary{Name: "Signing bonus", ToAccountID: checking.ID, Recurrence: "2027-06-01", EventID: bonus.ID},
		model.SalaryAmount{Amount: newFixedValue(20000), EventID: bonus.ID, StartOffset: days(0)})

	if got := chargedOn(t, svc, "2027-06-01")["Signing bonus"]; got != 20000 {
		t.Fatalf("expected the Signing bonus paid on the Bonus's date, got %v", got)
	}
	moveEvent(t, svc, bonus, "2027-07-01")
	if got := chargedOn(t, svc, "2027-07-01")["Signing bonus"]; got != 20000 {
		t.Errorf("expected the Signing bonus paid on the Bonus's new date, got %v", got)
	}
	if got := chargedOn(t, svc, "2027-06-01")["Signing bonus"]; got != 0 {
		t.Errorf("expected nothing paid on the Bonus's old date, got %v", got)
	}
}

func TestAOneOffSalaryJoiningOrLeavingAnEventKeepsItsDay(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	checking := mustAccount(t, svc, "Checking")
	bonus := mustEvent(t, svc, model.EventInput{Name: "Bonus", Date: mustParseDate("2027-05-01")})
	salary := mustSalary(t, svc, model.Salary{Name: "Signing bonus", ToAccountID: checking.ID, Recurrence: "2027-06-01"},
		model.SalaryAmount{Amount: newFixedValue(20000), StartDate: mustParseDate("2027-06-01")})

	salary.EventID = bonus.ID
	if _, err := svc.UpsertSalary(ctx, salary); err != nil {
		t.Fatalf("join the Bonus: %v", err)
	}
	if got := chargedOn(t, svc, "2027-06-01")["Signing bonus"]; got != 20000 {
		t.Fatalf("expected the Signing bonus still paid on 2027-06-01 in the Bonus, got %v", got)
	}
	moveEvent(t, svc, bonus, "2027-05-11")
	if got := chargedOn(t, svc, "2027-06-11")["Signing bonus"]; got != 20000 {
		t.Fatalf("expected the Signing bonus paid 10 days later once the Bonus moves 10 days, got %v", got)
	}

	mustRemoveFromEvent(t, svc, model.EventEntry{Kind: model.EventEntrySalary, ID: salary.ID})
	moveEvent(t, svc, bonus, "2027-05-01")
	if got := chargedOn(t, svc, "2027-06-11")["Signing bonus"]; got != 20000 {
		t.Errorf("expected the Signing bonus kept on 2027-06-11 once it leaves the Bonus, got %v", got)
	}
}

func TestASalaryJoiningOrLeavingAnEventBringsItsPartsWithIt(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	checking := mustAccount(t, svc, "Checking")
	newJob := mustEvent(t, svc, model.EventInput{Name: "New job", Date: mustParseDate("2027-09-01")})
	child := mustEvent(t, svc, model.EventInput{Name: "Second child", Date: mustParseDate("2027-12-01")})
	salary := mustSalary(t, svc, model.Salary{Name: "Salary B", ToAccountID: checking.ID, Recurrence: "*-*-25"},
		model.SalaryAmount{Amount: newFixedValue(40000), StartDate: mustParseDate("2027-09-01")})
	// The Second child ends Salary B: that end isn't Salary B's to bring along.
	mustSalaryEnd(t, svc, model.SalaryEnd{SalaryID: salary.ID, EventID: child.ID, DateOffset: days(0)})

	salary.EventID = newJob.ID
	if _, err := svc.UpsertSalary(ctx, salary); err != nil {
		t.Fatalf("join the New job: %v", err)
	}
	moveEvent(t, svc, newJob, "2027-10-01")
	for day, want := range map[string]float64{"2027-09-25": 0, "2027-10-25": 40000, "2027-12-25": 0} {
		if got := chargedOn(t, svc, day)["Salary B"]; got != want {
			t.Errorf("in the New job on 2027-10-01: expected Salary B to pay %v on %s, got %v", want, day, got)
		}
	}

	mustRemoveFromEvent(t, svc, model.EventEntry{Kind: model.EventEntrySalary, ID: salary.ID})
	moveEvent(t, svc, newJob, "2028-01-01")
	for day, want := range map[string]float64{"2027-10-25": 40000, "2027-12-25": 0} {
		if got := chargedOn(t, svc, day)["Salary B"]; got != want {
			t.Errorf("out of the New job: expected Salary B to pay %v on %s, got %v", want, day, got)
		}
	}
}

func TestABillJoiningOrLeavingAnEventBringsItsPartsWithIt(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	checking := mustAccount(t, svc, "Checking")
	move := mustEvent(t, svc, model.EventInput{Name: "Move", Date: mustParseDate("2027-09-01")})
	sale := mustEvent(t, svc, model.EventInput{Name: "Sale", Date: mustParseDate("2027-12-15")})
	rent := mustBillIn(t, svc, mustBillAccount(t, svc, "Household", checking.ID), "Rent",
		model.BillAmount{Amount: newFixedValue(9000), StartDate: mustParseDate("2027-09-01")})
	// The Sale ends the Rent: that end isn't the Rent's to bring along.
	mustBillEnd(t, svc, model.BillEnd{BillID: rent.ID, EventID: sale.ID, DateOffset: days(0)})

	rent.EventID = move.ID
	if _, err := svc.UpsertBill(ctx, rent); err != nil {
		t.Fatalf("join the Move: %v", err)
	}
	moveEvent(t, svc, move, "2027-10-01")
	for day, want := range map[string]float64{"2027-09-01": 0, "2027-10-01": 9000, "2028-01-01": 0} {
		if got := chargedOn(t, svc, day)["Rent"]; got != want {
			t.Errorf("in the Move on 2027-10-01: expected the Rent to charge %v on %s, got %v", want, day, got)
		}
	}

	mustRemoveFromEvent(t, svc, model.EventEntry{Kind: model.EventEntryBill, ID: rent.ID})
	moveEvent(t, svc, move, "2028-03-01")
	for day, want := range map[string]float64{"2027-10-01": 9000, "2028-01-01": 0} {
		if got := chargedOn(t, svc, day)["Rent"]; got != want {
			t.Errorf("out of the Move: expected the Rent to charge %v on %s, got %v", want, day, got)
		}
	}
}
