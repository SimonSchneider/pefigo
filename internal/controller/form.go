package controller

import (
	"fmt"
	"net/http"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/goslu/static/shttp"
	"github.com/SimonSchneider/pefigo/internal/model"
	"github.com/SimonSchneider/pefigo/pkg/ui"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

type accountInputForm struct {
	model.AccountInput
}

func (a *accountInputForm) FromForm(r *http.Request) error {
	a.ID = r.FormValue("id")
	a.Name = r.FormValue("name")
	if err := shttp.Parse(&a.BalanceUpperLimit, ui.ParseNullableFloat, r.FormValue("balance_upper_limit"), nil); err != nil {
		return fmt.Errorf("parsing balance limit: %w", err)
	}
	a.CashFlowFrequency = r.FormValue("cash_flow_frequency")
	a.CashFlowDestinationID = r.FormValue("cash_flow_destination_id")
	a.TypeID = r.FormValue("type_id")
	budgetCategoryID := r.FormValue("budget_category_id")
	if budgetCategoryID != "" {
		a.BudgetCategoryID = &budgetCategoryID
	}
	a.IsISK = r.FormValue("is_isk") == "on"
	return nil
}

type accountTypeInputForm struct {
	model.AccountTypeInput
}

func (a *accountTypeInputForm) FromForm(r *http.Request) error {
	a.ID = r.FormValue("id")
	a.Name = r.FormValue("name")
	a.Color = r.FormValue("color")
	return nil
}

type specialDateInputForm struct {
	model.SpecialDateInput
}

func (s *specialDateInputForm) FromForm(r *http.Request) error {
	s.ID = r.FormValue("id")
	s.Name = r.FormValue("name")
	if err := shttp.Parse(&s.Date, date.ParseDate, r.FormValue("date"), date.Date(0)); err != nil {
		return fmt.Errorf("parsing start date: %w", err)
	}
	s.Color = r.FormValue("color")
	return nil
}

type accountGrowthModelInputForm struct {
	model.AccountGrowthModelInput
}

func (a *accountGrowthModelInputForm) FromForm(r *http.Request) error {
	a.ID = r.FormValue("id")
	a.AccountID = r.FormValue("account_id")
	a.Type = r.FormValue("type")
	if a.Type != "fixed" && a.Type != "lognormal" {
		return fmt.Errorf("invalid growth model type: %s", a.Type)
	}
	if err := shttp.Parse(&a.AnnualRate, ui.ParseUncertainValue, r.FormValue("annual_rate"), uncertain.NewFixed(0)); err != nil {
		return fmt.Errorf("parsing annual rate: %w", err)
	}
	if err := shttp.Parse(&a.AnnualVolatility, ui.ParseUncertainValue, r.FormValue("annual_volatility"), uncertain.NewFixed(0)); err != nil {
		return fmt.Errorf("parsing annual volatility: %w", err)
	}
	if err := shttp.Parse(&a.StartDate, date.ParseDate, r.FormValue("start_date"), date.Date(0)); err != nil {
		return fmt.Errorf("parsing start date: %w", err)
	}
	if endDateStr := r.FormValue("end_date"); endDateStr != "" {
		var endDate date.Date
		if err := shttp.Parse(&endDate, date.ParseDate, endDateStr, date.Date(0)); err != nil {
			return fmt.Errorf("parsing end date: %w", err)
		}
		if endDate.IsZero() {
			a.EndDate = nil
		} else {
			a.EndDate = &endDate
		}
	}
	return nil
}

type startupShareAccountInputForm struct {
	model.StartupShareAccountInput
}

func (s *startupShareAccountInputForm) FromForm(r *http.Request) error {
	s.AccountID = r.FormValue("account_id")
	if err := shttp.Parse(&s.TaxRate, ui.ParseNullable(ui.ParsePercent), r.FormValue("tax_rate"), nil); err != nil {
		return fmt.Errorf("parsing tax rate: %w", err)
	}
	if err := shttp.Parse(&s.ValuationDiscountFactor, ui.ParseNullable(ui.ParsePercent), r.FormValue("valuation_discount_factor"), nil); err != nil {
		return fmt.Errorf("parsing valuation discount factor: %w", err)
	}
	return nil
}

type investmentRoundInputForm struct {
	model.InvestmentRoundInput
}

func (i *investmentRoundInputForm) FromForm(r *http.Request) error {
	i.ID = r.FormValue("id")
	i.AccountID = r.FormValue("account_id")
	if err := shttp.Parse(&i.Date, date.ParseDate, r.FormValue("date"), date.Date(0)); err != nil {
		return fmt.Errorf("parsing date: %w", err)
	}
	if err := shttp.Parse(&i.Valuation, shttp.ParseFloat, r.FormValue("valuation"), 0.0); err != nil {
		return fmt.Errorf("parsing valuation: %w", err)
	}
	if err := shttp.Parse(&i.PreMoneyShares, shttp.ParseFloat, r.FormValue("pre_money_shares"), 0.0); err != nil {
		return fmt.Errorf("parsing pre_money_shares: %w", err)
	}
	if err := shttp.Parse(&i.Investment, shttp.ParseFloat, r.FormValue("investment"), 0.0); err != nil {
		return fmt.Errorf("parsing investment: %w", err)
	}
	return nil
}

type shareChangeInputForm struct {
	model.ShareChangeInput
}

func (s *shareChangeInputForm) FromForm(r *http.Request) error {
	s.ID = r.FormValue("id")
	s.AccountID = r.FormValue("account_id")
	if err := shttp.Parse(&s.Date, date.ParseDate, r.FormValue("date"), date.Date(0)); err != nil {
		return fmt.Errorf("parsing date: %w", err)
	}
	if err := shttp.Parse(&s.DeltaShares, shttp.ParseFloat, r.FormValue("delta_shares"), 0.0); err != nil {
		return fmt.Errorf("parsing delta_shares: %w", err)
	}
	if err := shttp.Parse(&s.TotalPrice, shttp.ParseFloat, r.FormValue("total_price"), 0.0); err != nil {
		return fmt.Errorf("parsing total_price: %w", err)
	}
	return nil
}

type startupShareOptionInputForm struct {
	model.StartupShareOptionInput
}

func (o *startupShareOptionInputForm) FromForm(r *http.Request) error {
	o.ID = r.FormValue("id")
	o.AccountID = r.FormValue("account_id")
	o.SourceAccountID = r.FormValue("source_account_id")
	if err := shttp.Parse(&o.Shares, shttp.ParseFloat, r.FormValue("shares"), 0.0); err != nil {
		return fmt.Errorf("parsing shares: %w", err)
	}
	if err := shttp.Parse(&o.StrikePricePerShare, shttp.ParseFloat, r.FormValue("strike_price_per_share"), 0.0); err != nil {
		return fmt.Errorf("parsing strike price per share: %w", err)
	}
	if err := shttp.Parse(&o.GrantDate, date.ParseDate, r.FormValue("grant_date"), date.Date(0)); err != nil {
		return fmt.Errorf("parsing grant date: %w", err)
	}
	if err := shttp.Parse(&o.EndDate, date.ParseDate, r.FormValue("end_date"), date.Date(0)); err != nil {
		return fmt.Errorf("parsing end date: %w", err)
	}
	return nil
}

type accountSnapshotInputForm struct {
	model.AccountSnapshotInput
}

func (a *accountSnapshotInputForm) FromForm(r *http.Request) error {
	dateStr := r.PathValue("date")
	if dateStr == "" {
		dateStr = r.FormValue("date")
	}
	if err := shttp.Parse(&a.Date, date.ParseDate, dateStr, 0); err != nil {
		return fmt.Errorf("parsing date: %w", err)
	}
	balanceStr := r.FormValue("balance")
	if balanceStr == "" {
		a.EmptyBalance = true
	} else if err := shttp.Parse(&a.Balance, ui.ParseUncertainValue, balanceStr, uncertain.NewFixed(0)); err != nil {
		return fmt.Errorf("parsing balance: %w", err)
	}
	return nil
}

type transferTemplateForm struct {
	model.TransferTemplate
}

func (t *transferTemplateForm) FromForm(r *http.Request) error {
	t.ID = r.FormValue("id")
	t.Name = r.FormValue("name")
	t.FromAccountID = r.FormValue("from_account_id")
	t.ToAccountID = r.FormValue("to_account_id")
	t.AmountType = r.FormValue("amount_type")
	if t.AmountType != "fixed" && t.AmountType != "percent" {
		return fmt.Errorf("invalid amount type: %s", t.AmountType)
	}
	if err := shttp.Parse(&t.AmountFixed, ui.ParseUncertainValue, r.FormValue("amount_fixed"), uncertain.NewFixed(0)); err != nil {
		return fmt.Errorf("parsing amount fixed: %w", err)
	}
	if err := shttp.Parse(&t.AmountPercent, shttp.ParseFloat, r.FormValue("amount_percent"), 0); err != nil {
		return fmt.Errorf("parsing amount percent: %w", err)
	}
	if err := shttp.Parse(&t.Priority, ui.ParseInt64, r.FormValue("priority"), int64(0)); err != nil {
		return fmt.Errorf("parsing priority: %w", err)
	}
	if err := shttp.Parse(&t.Recurrence, ui.ParseDateCron, r.FormValue("recurrence"), date.Cron("")); err != nil {
		return fmt.Errorf("parsing recurrence: %w", err)
	}
	if err := shttp.Parse(&t.StartDate, date.ParseDate, r.FormValue("start_date"), date.Date(0)); err != nil {
		return fmt.Errorf("parsing effective from: %w", err)
	}
	if endDateStr := r.FormValue("end_date"); endDateStr != "" {
		var endDate date.Date
		if err := shttp.Parse(&endDate, date.ParseDate, endDateStr, date.Date(0)); err != nil {
			return fmt.Errorf("parsing effective to: %w", err)
		}
		t.EndDate = &endDate
	} else {
		t.EndDate = nil
	}
	t.Enabled = r.FormValue("enabled") == "on"
	budgetCategoryID := r.FormValue("budget_category_id")
	if budgetCategoryID != "" {
		t.BudgetCategoryID = &budgetCategoryID
	} else {
		t.BudgetCategoryID = nil
	}
	return nil
}

type transferTemplateCategoryInputForm struct {
	model.TransferTemplateCategoryInput
}

func (c *transferTemplateCategoryInputForm) FromForm(r *http.Request) error {
	c.ID = r.FormValue("id")
	c.Name = r.FormValue("name")
	color := r.FormValue("color")
	if color != "" {
		c.Color = &color
	} else {
		c.Color = nil
	}
	return nil
}

type salaryInputForm struct {
	model.Salary
}

func (s *salaryInputForm) FromForm(r *http.Request) error {
	s.ID = r.FormValue("id")
	s.Name = r.FormValue("name")
	s.ToAccountID = r.FormValue("to_account_id")
	s.PensionAccountID = r.FormValue("pension_account_id")
	if err := shttp.Parse(&s.Priority, ui.ParseInt64, r.FormValue("priority"), int64(0)); err != nil {
		return fmt.Errorf("parsing priority: %w", err)
	}
	if err := shttp.Parse(&s.Recurrence, ui.ParseDateCron, r.FormValue("recurrence"), date.Cron("*-*-25")); err != nil {
		return fmt.Errorf("parsing recurrence: %w", err)
	}
	s.Enabled = r.FormValue("enabled") == "on"
	s.IsGross = r.FormValue("is_gross") == "on"
	s.Kommun = r.FormValue("kommun")
	s.Forsamling = r.FormValue("forsamling")
	s.ChurchMember = r.FormValue("church_member") == "on"
	budgetCategoryID := r.FormValue("budget_category_id")
	if budgetCategoryID != "" {
		s.BudgetCategoryID = &budgetCategoryID
	} else {
		s.BudgetCategoryID = nil
	}
	return nil
}

type salaryAmountInputForm struct {
	model.SalaryAmount
}

func (a *salaryAmountInputForm) FromForm(r *http.Request) error {
	a.ID = r.FormValue("id")
	a.SalaryID = r.FormValue("salary_id")
	if err := shttp.Parse(&a.StartDate, date.ParseDate, r.FormValue("start_date"), date.Date(0)); err != nil {
		return fmt.Errorf("parsing start date: %w", err)
	}
	if err := shttp.Parse(&a.Amount, ui.ParseUncertainValue, r.FormValue("amount"), uncertain.NewFixed(0)); err != nil {
		return fmt.Errorf("parsing amount: %w", err)
	}
	return nil
}

type salaryAdjustmentInputForm struct {
	model.SalaryAdjustment
}

func (a *salaryAdjustmentInputForm) FromForm(r *http.Request) error {
	a.ID = r.FormValue("id")
	a.SalaryID = r.FormValue("salary_id")
	if err := shttp.Parse(&a.ValidFrom, date.ParseDate, r.FormValue("valid_from"), date.Date(0)); err != nil {
		return fmt.Errorf("parsing valid_from: %w", err)
	}
	if err := shttp.Parse(&a.VacationDaysPerYear, shttp.ParseFloat, r.FormValue("vacation_days_per_year"), float64(0)); err != nil {
		return fmt.Errorf("parsing vacation_days_per_year: %w", err)
	}
	if err := shttp.Parse(&a.SickDaysPerOccasion, shttp.ParseFloat, r.FormValue("sick_days_per_occasion"), float64(0)); err != nil {
		return fmt.Errorf("parsing sick_days_per_occasion: %w", err)
	}
	if err := shttp.Parse(&a.SickOccasionsPerYear, shttp.ParseFloat, r.FormValue("sick_occasions_per_year"), float64(0)); err != nil {
		return fmt.Errorf("parsing sick_occasions_per_year: %w", err)
	}
	if err := shttp.Parse(&a.VABDaysPerYear, shttp.ParseFloat, r.FormValue("vab_days_per_year"), float64(0)); err != nil {
		return fmt.Errorf("parsing vab_days_per_year: %w", err)
	}
	return nil
}

type partialParentalLeaveInputForm struct {
	model.PartialParentalLeave
}

func (p *partialParentalLeaveInputForm) FromForm(r *http.Request) error {
	p.ID = r.FormValue("id")
	p.SalaryID = r.FormValue("salary_id")
	if err := shttp.Parse(&p.StartDate, date.ParseDate, r.FormValue("start_date"), date.Date(0)); err != nil {
		return fmt.Errorf("parsing start_date: %w", err)
	}
	if err := shttp.Parse(&p.EndDate, date.ParseDate, r.FormValue("end_date"), date.Date(0)); err != nil {
		return fmt.Errorf("parsing end_date: %w", err)
	}
	if err := shttp.Parse(&p.SjukDaysPerYear, shttp.ParseFloat, r.FormValue("sjuk_days_per_year"), float64(0)); err != nil {
		return fmt.Errorf("parsing sjuk_days_per_year: %w", err)
	}
	if err := shttp.Parse(&p.LagstaDaysPerYear, shttp.ParseFloat, r.FormValue("lagsta_days_per_year"), float64(0)); err != nil {
		return fmt.Errorf("parsing lagsta_days_per_year: %w", err)
	}
	if err := shttp.Parse(&p.SkippedWorkDaysPerYear, shttp.ParseFloat, r.FormValue("skipped_work_days_per_year"), float64(0)); err != nil {
		return fmt.Errorf("parsing skipped_work_days_per_year: %w", err)
	}
	return nil
}

type fullParentalLeaveInputForm struct {
	model.FullParentalLeave
}

func (f *fullParentalLeaveInputForm) FromForm(r *http.Request) error {
	f.ID = r.FormValue("id")
	f.SalaryID = r.FormValue("salary_id")
	if err := shttp.Parse(&f.StartDate, date.ParseDate, r.FormValue("start_date"), date.Date(0)); err != nil {
		return fmt.Errorf("parsing start_date: %w", err)
	}
	if err := shttp.Parse(&f.EndDate, date.ParseDate, r.FormValue("end_date"), date.Date(0)); err != nil {
		return fmt.Errorf("parsing end_date: %w", err)
	}
	if err := shttp.Parse(&f.SjukDaysPerWeek, shttp.ParseFloat, r.FormValue("sjuk_days_per_week"), float64(0)); err != nil {
		return fmt.Errorf("parsing sjuk_days_per_week: %w", err)
	}
	return nil
}

type predictionParamsForm struct {
	model.PredictionParams
}

func (p *predictionParamsForm) FromForm(r *http.Request) error {
	if err := shttp.Parse(&p.Duration, date.ParseDuration, r.FormValue("duration"), 365); err != nil {
		return fmt.Errorf("parsing duration: %w", err)
	}
	if err := shttp.Parse(&p.Samples, ui.ParseHumanNumber(ui.ParseInt64), r.FormValue("samples"), 2000); err != nil {
		return fmt.Errorf("parsing samples: %w", err)
	}
	if err := shttp.Parse(&p.Quantile, shttp.ParseFloat, r.FormValue("quantile"), 0.8); err != nil {
		return fmt.Errorf("parsing quantile: %w", err)
	}
	if err := shttp.Parse(&p.SnapshotInterval, ui.ParseDateCron, r.FormValue("snapshot_interval"), "*-*-28"); err != nil {
		return fmt.Errorf("parsing snapshot interval: %w", err)
	}
	if err := shttp.Parse(&p.GroupBy, model.ParseGroupBy, r.FormValue("group_by"), model.GroupByType); err != nil {
		return fmt.Errorf("parsing group by: %w", err)
	}
	return nil
}

type dateInputForm struct {
	OldDate date.Date
	NewDate date.Date
}

func (d *dateInputForm) FromForm(r *http.Request) error {
	if err := shttp.Parse(&d.OldDate, date.ParseDate, r.FormValue("old-date"), date.Date(0)); err != nil {
		return fmt.Errorf("parsing old date: %w", err)
	}
	if err := shttp.Parse(&d.NewDate, date.ParseDate, r.FormValue("new-date"), date.Date(0)); err != nil {
		return fmt.Errorf("parsing new date: %w", err)
	}
	return nil
}

type sweYearlyParamsInputForm struct {
	model.SweYearlyParams
}

func (f *sweYearlyParamsInputForm) FromForm(r *http.Request) error {
	f.ID = r.FormValue("id")
	if err := shttp.Parse(&f.Amount, ui.ParseAmount, r.FormValue("amount"), float64(0)); err != nil {
		return fmt.Errorf("parsing amount: %w", err)
	}
	if err := shttp.Parse(&f.Prisbasbelopp, ui.ParseAmount, r.FormValue("prisbasbelopp"), float64(0)); err != nil {
		return fmt.Errorf("parsing prisbasbelopp: %w", err)
	}
	if err := shttp.Parse(&f.SchablonRanta, ui.ParseAmount, r.FormValue("schablon_ranta"), float64(0)); err != nil {
		return fmt.Errorf("parsing schablon_ranta: %w", err)
	}
	if err := shttp.Parse(&f.IskFribelopp, ui.ParseAmount, r.FormValue("isk_fribelopp"), float64(0)); err != nil {
		return fmt.Errorf("parsing isk_fribelopp: %w", err)
	}
	if err := shttp.Parse(&f.ValidFrom, date.ParseDate, r.FormValue("valid_from"), date.Date(0)); err != nil {
		return fmt.Errorf("parsing valid_from: %w", err)
	}
	return nil
}

type billAccountInputForm struct {
	model.BillAccount
}

func (b *billAccountInputForm) FromForm(r *http.Request) error {
	b.ID = r.FormValue("id")
	b.Name = r.FormValue("name")
	b.FromAccountID = r.FormValue("from_account_id")
	if err := shttp.Parse(&b.Priority, ui.ParseInt64, r.FormValue("priority"), int64(0)); err != nil {
		return fmt.Errorf("parsing priority: %w", err)
	}
	if err := shttp.Parse(&b.Recurrence, ui.ParseDateCron, r.FormValue("recurrence"), date.Cron("*-*-01")); err != nil {
		return fmt.Errorf("parsing recurrence: %w", err)
	}
	b.Enabled = r.FormValue("enabled") == "on"
	return nil
}

type billInputForm struct {
	model.Bill
}

func (b *billInputForm) FromForm(r *http.Request) error {
	b.ID = r.FormValue("id")
	b.BillAccountID = r.FormValue("bill_account_id")
	b.Name = r.FormValue("name")
	b.Enabled = r.FormValue("enabled") == "on"
	b.Notes = r.FormValue("notes")
	b.URL = r.FormValue("url")
	budgetCategoryID := r.FormValue("budget_category_id")
	if budgetCategoryID != "" {
		b.BudgetCategoryID = &budgetCategoryID
	} else {
		b.BudgetCategoryID = nil
	}
	return nil
}

type billAmountInputForm struct {
	model.BillAmount
}

func (a *billAmountInputForm) FromForm(r *http.Request) error {
	a.ID = r.FormValue("id")
	a.BillID = r.FormValue("bill_id")
	if err := shttp.Parse(&a.StartDate, date.ParseDate, r.FormValue("start_date"), date.Date(0)); err != nil {
		return fmt.Errorf("parsing start date: %w", err)
	}
	if endDateStr := r.FormValue("end_date"); endDateStr != "" {
		var endDate date.Date
		if err := shttp.Parse(&endDate, date.ParseDate, endDateStr, date.Date(0)); err != nil {
			return fmt.Errorf("parsing end date: %w", err)
		}
		a.EndDate = &endDate
	} else {
		a.EndDate = nil
	}
	if err := shttp.Parse(&a.Amount, ui.ParseUncertainValue, r.FormValue("amount"), uncertain.NewFixed(0)); err != nil {
		return fmt.Errorf("parsing amount: %w", err)
	}
	if r.FormValue("amount_period") == "yearly" {
		a.Period = "yearly"
	} else {
		a.Period = "monthly"
	}
	a.Currency = r.FormValue("amount_currency")
	return nil
}

func extractExcludedTypeIDs(r *http.Request) []string {
	if err := r.ParseForm(); err != nil {
		return nil
	}
	var ids []string
	for key := range r.Form {
		if len(key) > 11 && key[:11] == "exclude_at_" && r.FormValue(key) == "on" {
			ids = append(ids, key[11:])
		}
	}
	return ids
}
