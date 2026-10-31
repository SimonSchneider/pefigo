package swe_test

import (
	"math"
	"testing"

	"github.com/SimonSchneider/pefigo/pkg/swe"
)

func TestCalculateSalaryBreakdown_Standard(t *testing.T) {
	gross := 50000.0
	pbb := 57300.0
	adj := swe.SalaryAdjustmentParams{
		YearlyVacationDays:   25,
		SickDaysPerOccasion:  3,
		SickOccasionsPerYear: 4,
		VABDaysPerYear:       10,
		Prisbasbelopp:        pbb,
	}
	taxFunc := func(adjusted float64) float64 {
		return adjusted * 0.30
	}

	bd := swe.CalculateSalaryBreakdown(gross, adj, taxFunc)

	if bd.GrossMonthly != gross {
		t.Errorf("GrossMonthly = %v, want %v", bd.GrossMonthly, gross)
	}
	if bd.VacationSupplement <= 0 {
		t.Error("expected positive VacationSupplement")
	}
	if bd.SickPayDeduction <= 0 {
		t.Error("expected positive SickPayDeduction")
	}
	if bd.VABDeduction <= 0 {
		t.Error("expected positive VABDeduction")
	}
	if bd.PartialParentalDeduction != 0 {
		t.Errorf("expected 0 PartialParentalDeduction, got %v", bd.PartialParentalDeduction)
	}

	wantAdj := gross + bd.VacationSupplement - bd.SickPayDeduction - bd.VABDeduction
	if math.Abs(bd.AdjustedGross-wantAdj) > 0.01 {
		t.Errorf("AdjustedGross = %v, want %v", bd.AdjustedGross, wantAdj)
	}
	if math.Abs(bd.Tax-wantAdj*0.30) > 0.01 {
		t.Errorf("Tax = %v, want %v", bd.Tax, wantAdj*0.30)
	}
	if math.Abs(bd.NetMonthly-(wantAdj-bd.Tax)) > 0.01 {
		t.Errorf("NetMonthly = %v, want %v", bd.NetMonthly, wantAdj-bd.Tax)
	}
	if bd.IsFullParentalLeave {
		t.Error("expected IsFullParentalLeave = false")
	}
}

func TestCalculateSalaryBreakdown_WithPartialParentalLeave(t *testing.T) {
	gross := 50000.0
	pbb := 57300.0
	adj := swe.SalaryAdjustmentParams{
		YearlyVacationDays:             25,
		SickDaysPerOccasion:            3,
		SickOccasionsPerYear:           4,
		VABDaysPerYear:                 10,
		ParentalSjukDaysPerYear:        40,
		ParentalLagstaDaysPerYear:      10,
		ParentalSkippedWorkDaysPerYear: 50,
		Prisbasbelopp:                  pbb,
	}
	taxFunc := func(adjusted float64) float64 {
		return adjusted * 0.30
	}

	bd := swe.CalculateSalaryBreakdown(gross, adj, taxFunc)

	wantDeduction := swe.CalculatePartialParentalLeaveDeduction(gross, 40, 10, 50, pbb)
	if bd.PartialParentalDeduction == 0 || bd.PartialParentalDeduction != wantDeduction {
		t.Errorf("PartialParentalDeduction = %v, want %v", bd.PartialParentalDeduction, wantDeduction)
	}
	wantAdj := gross + bd.VacationSupplement - bd.SickPayDeduction - bd.VABDeduction - bd.PartialParentalDeduction
	if math.Abs(bd.AdjustedGross-wantAdj) > 0.01 {
		t.Errorf("AdjustedGross = %v, want %v", bd.AdjustedGross, wantAdj)
	}
	if got := swe.AdjustGrossSalary(gross, adj); got != bd.AdjustedGross {
		t.Errorf("AdjustGrossSalary = %v, want the breakdown's adjusted gross %v", got, bd.AdjustedGross)
	}
}

func TestCalculateFullParentalLeaveBreakdown(t *testing.T) {
	gross := 50000.0
	pbb := 57300.0

	bd := swe.CalculateFullParentalLeaveBreakdown(gross, 5, pbb)

	if !bd.IsFullParentalLeave {
		t.Error("expected IsFullParentalLeave = true")
	}
	if bd.GrossMonthly != gross {
		t.Errorf("GrossMonthly = %v, want %v", bd.GrossMonthly, gross)
	}
	if bd.FKSjukCompensation <= 0 {
		t.Error("expected positive FKSjukCompensation")
	}
	if bd.NetMonthly != bd.FKSjukCompensation {
		t.Errorf("NetMonthly = %v, want FKSjukCompensation = %v", bd.NetMonthly, bd.FKSjukCompensation)
	}
}

func TestCalculateFullParentalLeaveBreakdown_ZeroDays(t *testing.T) {
	bd := swe.CalculateFullParentalLeaveBreakdown(50000, 0, 57300)

	if bd.FKSjukCompensation != 0 {
		t.Errorf("expected 0 compensation with 0 days, got %v", bd.FKSjukCompensation)
	}
	if bd.NetMonthly != 0 {
		t.Errorf("expected 0 net, got %v", bd.NetMonthly)
	}
}
