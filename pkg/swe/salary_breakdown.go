package swe

type SalaryBreakdown struct {
	GrossMonthly             float64
	VacationSupplement       float64
	SickPayDeduction         float64
	VABDeduction             float64
	PartialParentalDeduction float64
	AdjustedGross            float64
	Tax                      float64
	NetMonthly               float64
	IsFullParentalLeave      bool
	FKSjukCompensation       float64
}

// CalculateSalaryBreakdown produces an itemized breakdown of a normal (non-full-parental-leave)
// monthly salary. tax returns the monthly tax on an adjusted gross.
func CalculateSalaryBreakdown(grossMonthly float64, adj SalaryAdjustmentParams, tax func(adjustedGross float64) float64) SalaryBreakdown {
	bd := adjustGrossSalary(grossMonthly, adj)
	bd.Tax = tax(bd.AdjustedGross)
	bd.NetMonthly = bd.AdjustedGross - bd.Tax
	return bd
}

// CalculateFullParentalLeaveBreakdown produces a breakdown for a period where
// the employee is on full parental leave (no employer salary, FK compensation only).
func CalculateFullParentalLeaveBreakdown(grossMonthly, sjukDaysPerWeek, prisbasbelopp float64) SalaryBreakdown {
	sjukComp := CalculateFullParentalLeaveCompensation(grossMonthly, sjukDaysPerWeek, prisbasbelopp)
	return SalaryBreakdown{
		GrossMonthly:        grossMonthly,
		IsFullParentalLeave: true,
		FKSjukCompensation:  sjukComp,
		NetMonthly:          sjukComp,
	}
}
