package model

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/goslu/sid"
	"github.com/SimonSchneider/pefigo/internal/pdb"
	"github.com/SimonSchneider/pefigo/pkg/swe"
	"github.com/SimonSchneider/pefigo/pkg/ui"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

type PensionSegment struct {
	StartDate date.Date
	EndDate   *date.Date
	Pension   uncertain.Value
}

type NetSalarySegment struct {
	StartDate date.Date
	EndDate   *date.Date
	Net       uncertain.Value
}

type Salary struct {
	ID                    string
	Name                  string
	ToAccountID           string
	PensionAccountID      string
	Priority              int64
	Recurrence            date.Cron
	BudgetCategoryID      *string
	Enabled               bool
	Kommun                string
	Forsamling            string
	ChurchMember          bool
	IsGross               bool
	Amounts               []SalaryAmount
	Adjustments           []SalaryAdjustment
	PartialParentalLeaves []PartialParentalLeave
	FullParentalLeaves    []FullParentalLeave
	// NetSegments is populated by the service layer when IsGross is true.
	// Segments are split at the union of salary-amount, adjustment, and PBB change dates.
	NetSegments []NetSalarySegment
	// PensionSegments is populated by the service layer when IsGross is true.
	// Segments are split at the union of salary-amount and IBB change dates.
	PensionSegments []PensionSegment
}

type SalaryAmount struct {
	ID        string
	SalaryID  string
	Amount    uncertain.Value
	StartDate date.Date
}

type SalaryAdjustment struct {
	ID                   string
	SalaryID             string
	ValidFrom            date.Date
	VacationDaysPerYear  float64
	SickDaysPerOccasion  float64
	SickOccasionsPerYear float64
	VABDaysPerYear       float64
}

type PartialParentalLeave struct {
	ID                     string
	SalaryID               string
	StartDate              date.Date
	EndDate                date.Date
	SjukDaysPerYear        float64
	LagstaDaysPerYear      float64
	SkippedWorkDaysPerYear float64
}

type FullParentalLeave struct {
	ID              string
	SalaryID        string
	StartDate       date.Date
	EndDate         date.Date
	SjukDaysPerWeek float64
}

func (s Salary) GenerateTransferTemplates() []TransferTemplate {
	source := TransferTemplateSource{
		Type:     "salary",
		EntityID: s.ID,
		Label:    s.Name,
		EditURL:  "/salaries/" + s.ID + "/edit",
	}

	var templates []TransferTemplate

	if s.IsGross && len(s.NetSegments) > 0 {
		for i, seg := range s.NetSegments {
			templates = append(templates, TransferTemplate{
				ID:               fmt.Sprintf("salary:%s:%d", s.ID, i),
				Name:             s.Name,
				FromAccountID:    "",
				ToAccountID:      s.ToAccountID,
				AmountType:       "fixed",
				AmountFixed:      seg.Net,
				Priority:         s.Priority,
				Recurrence:       s.Recurrence,
				StartDate:        seg.StartDate,
				EndDate:          seg.EndDate,
				Enabled:          s.Enabled,
				BudgetCategoryID: s.BudgetCategoryID,
				Source:           source,
			})
		}
	} else {
		sorted := make([]SalaryAmount, len(s.Amounts))
		copy(sorted, s.Amounts)
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].StartDate < sorted[j].StartDate
		})
		for i, amt := range sorted {
			var endDate *date.Date
			if i+1 < len(sorted) {
				ed := sorted[i+1].StartDate
				endDate = &ed
			}
			templates = append(templates, TransferTemplate{
				ID:               "salary:" + amt.ID,
				Name:             s.Name,
				FromAccountID:    "",
				ToAccountID:      s.ToAccountID,
				AmountType:       "fixed",
				AmountFixed:      amt.Amount,
				Priority:         s.Priority,
				Recurrence:       s.Recurrence,
				StartDate:        amt.StartDate,
				EndDate:          endDate,
				Enabled:          s.Enabled,
				BudgetCategoryID: s.BudgetCategoryID,
				Source:           source,
			})
		}
	}

	// Pension TTs: split at both salary-amount and IBB change boundaries
	if s.IsGross && s.PensionAccountID != "" {
		for i, seg := range s.PensionSegments {
			templates = append(templates, TransferTemplate{
				ID:               fmt.Sprintf("salary-pension:%s:%d", s.ID, i),
				Name:             s.Name + " (pension)",
				FromAccountID:    "",
				ToAccountID:      s.PensionAccountID,
				AmountType:       "fixed",
				AmountFixed:      seg.Pension,
				Priority:         s.Priority,
				Recurrence:       s.Recurrence,
				StartDate:        seg.StartDate,
				EndDate:          seg.EndDate,
				Enabled:          s.Enabled,
				BudgetCategoryID: s.BudgetCategoryID,
				Source:           source,
			})
		}
	}

	return templates
}

func salaryFromDB(s pdb.Salary) Salary {
	return Salary{
		ID:               s.ID,
		Name:             s.Name,
		ToAccountID:      ui.OrDefault(s.ToAccountID),
		PensionAccountID: ui.OrDefault(s.PensionAccountID),
		Priority:         s.Priority,
		Recurrence:       date.Cron(s.Recurrence),
		BudgetCategoryID: s.BudgetCategoryID,
		Enabled:          s.Enabled,
		Kommun:           s.Kommun,
		Forsamling:       s.Forsamling,
		ChurchMember:     s.ChurchMember,
		IsGross:          s.IsGross,
	}
}

func salaryAmountFromDB(a pdb.SalaryAmount) (SalaryAmount, error) {
	var amount uncertain.Value
	if err := amount.Decode(a.Amount); err != nil {
		return SalaryAmount{}, fmt.Errorf("decoding salary amount: %w", err)
	}
	return SalaryAmount{
		ID:        a.ID,
		SalaryID:  a.SalaryID,
		Amount:    amount,
		StartDate: date.Date(a.StartDate),
	}, nil
}

func (s *Service) UpsertSalary(ctx context.Context, inp Salary) (Salary, error) {
	if inp.ID == "" {
		inp.ID = sid.MustNewString(32)
	}
	now := time.Now().Unix()
	sal, err := s.q.UpsertSalary(ctx, pdb.UpsertSalaryParams{
		ID:               inp.ID,
		Name:             inp.Name,
		ToAccountID:      ui.WithDefaultNull(inp.ToAccountID),
		PensionAccountID: ui.WithDefaultNull(inp.PensionAccountID),
		Priority:         inp.Priority,
		Recurrence:       string(inp.Recurrence),
		BudgetCategoryID: inp.BudgetCategoryID,
		Enabled:          inp.Enabled,
		Kommun:           inp.Kommun,
		Forsamling:       inp.Forsamling,
		ChurchMember:     inp.ChurchMember,
		IsGross:          inp.IsGross,
		CreatedAt:        now,
		UpdatedAt:        now,
	})
	if err != nil {
		return Salary{}, fmt.Errorf("upserting salary: %w", err)
	}
	return salaryFromDB(sal), nil
}

func (s *Service) ListSalaries(ctx context.Context) ([]Salary, error) {
	rows, err := s.q.ListSalaries(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing salaries: %w", err)
	}
	salaries := make([]Salary, 0, len(rows))
	for _, r := range rows {
		salaries = append(salaries, salaryFromDB(r))
	}
	return salaries, nil
}

func (s *Service) GetSalary(ctx context.Context, id string) (Salary, error) {
	sal, err := s.q.GetSalary(ctx, id)
	if err != nil {
		return Salary{}, fmt.Errorf("getting salary: %w", err)
	}
	result := salaryFromDB(sal)
	amounts, err := s.ListSalaryAmounts(ctx, id)
	if err != nil {
		return Salary{}, fmt.Errorf("listing salary amounts: %w", err)
	}
	result.Amounts = amounts
	adjustments, err := s.ListSalaryAdjustments(ctx, id)
	if err != nil {
		return Salary{}, fmt.Errorf("listing salary adjustments: %w", err)
	}
	result.Adjustments = adjustments
	partialPLs, err := s.ListPartialParentalLeaves(ctx, id)
	if err != nil {
		return Salary{}, fmt.Errorf("listing partial parental leaves: %w", err)
	}
	result.PartialParentalLeaves = partialPLs
	fullPLs, err := s.ListFullParentalLeaves(ctx, id)
	if err != nil {
		return Salary{}, fmt.Errorf("listing full parental leaves: %w", err)
	}
	result.FullParentalLeaves = fullPLs
	return result, nil
}

func (s *Service) DeleteSalary(ctx context.Context, id string) error {
	if err := s.q.DeleteSalary(ctx, id); err != nil {
		return fmt.Errorf("deleting salary: %w", err)
	}
	return nil
}

func (s *Service) UpsertSalaryAmount(ctx context.Context, inp SalaryAmount) (SalaryAmount, error) {
	if inp.ID == "" {
		inp.ID = sid.MustNewString(32)
	}
	encoded, err := inp.Amount.Encode()
	if err != nil {
		return SalaryAmount{}, fmt.Errorf("encoding amount: %w", err)
	}
	now := time.Now().Unix()
	a, err := s.q.UpsertSalaryAmount(ctx, pdb.UpsertSalaryAmountParams{
		ID:        inp.ID,
		SalaryID:  inp.SalaryID,
		Amount:    encoded,
		StartDate: int64(inp.StartDate),
		CreatedAt: now,
		UpdatedAt: now,
	})
	if err != nil {
		return SalaryAmount{}, fmt.Errorf("upserting salary amount: %w", err)
	}
	return salaryAmountFromDB(a)
}

func (s *Service) ListSalaryAmounts(ctx context.Context, salaryID string) ([]SalaryAmount, error) {
	rows, err := s.q.ListSalaryAmounts(ctx, salaryID)
	if err != nil {
		return nil, fmt.Errorf("listing salary amounts: %w", err)
	}
	amounts := make([]SalaryAmount, 0, len(rows))
	for _, r := range rows {
		a, err := salaryAmountFromDB(r)
		if err != nil {
			return nil, err
		}
		amounts = append(amounts, a)
	}
	return amounts, nil
}

func (s *Service) DeleteSalaryAmount(ctx context.Context, id string) error {
	if err := s.q.DeleteSalaryAmount(ctx, id); err != nil {
		return fmt.Errorf("deleting salary amount: %w", err)
	}
	return nil
}

func salaryAdjustmentFromDB(a pdb.SalaryAdjustment) SalaryAdjustment {
	return SalaryAdjustment{
		ID:                   a.ID,
		SalaryID:             a.SalaryID,
		ValidFrom:            date.Date(a.ValidFrom),
		VacationDaysPerYear:  a.VacationDaysPerYear,
		SickDaysPerOccasion:  a.SickDaysPerOccasion,
		SickOccasionsPerYear: a.SickOccasionsPerYear,
		VABDaysPerYear:       a.VabDaysPerYear,
	}
}

func (s *Service) UpsertSalaryAdjustment(ctx context.Context, inp SalaryAdjustment) (SalaryAdjustment, error) {
	if inp.ID == "" {
		inp.ID = sid.MustNewString(32)
	}
	now := time.Now().Unix()
	a, err := s.q.UpsertSalaryAdjustment(ctx, pdb.UpsertSalaryAdjustmentParams{
		ID:                   inp.ID,
		SalaryID:             inp.SalaryID,
		ValidFrom:            int64(inp.ValidFrom),
		VacationDaysPerYear:  inp.VacationDaysPerYear,
		SickDaysPerOccasion:  inp.SickDaysPerOccasion,
		SickOccasionsPerYear: inp.SickOccasionsPerYear,
		VabDaysPerYear:       inp.VABDaysPerYear,
		CreatedAt:            now,
		UpdatedAt:            now,
	})
	if err != nil {
		return SalaryAdjustment{}, fmt.Errorf("upserting salary adjustment: %w", err)
	}
	return salaryAdjustmentFromDB(a), nil
}

func (s *Service) ListSalaryAdjustments(ctx context.Context, salaryID string) ([]SalaryAdjustment, error) {
	rows, err := s.q.ListSalaryAdjustments(ctx, salaryID)
	if err != nil {
		return nil, fmt.Errorf("listing salary adjustments: %w", err)
	}
	adjustments := make([]SalaryAdjustment, 0, len(rows))
	for _, r := range rows {
		adjustments = append(adjustments, salaryAdjustmentFromDB(r))
	}
	return adjustments, nil
}

func (s *Service) DeleteSalaryAdjustment(ctx context.Context, id string) error {
	if err := s.q.DeleteSalaryAdjustment(ctx, id); err != nil {
		return fmt.Errorf("deleting salary adjustment: %w", err)
	}
	return nil
}

func partialParentalLeaveFromDB(a pdb.PartialParentalLeave) PartialParentalLeave {
	return PartialParentalLeave{
		ID:                     a.ID,
		SalaryID:               a.SalaryID,
		StartDate:              date.Date(a.StartDate),
		EndDate:                date.Date(a.EndDate),
		SjukDaysPerYear:        a.SjukDaysPerYear,
		LagstaDaysPerYear:      a.LagstaDaysPerYear,
		SkippedWorkDaysPerYear: a.SkippedWorkDaysPerYear,
	}
}

func (s *Service) UpsertPartialParentalLeave(ctx context.Context, inp PartialParentalLeave) (PartialParentalLeave, error) {
	if inp.ID == "" {
		inp.ID = sid.MustNewString(32)
	}
	now := time.Now().Unix()
	a, err := s.q.UpsertPartialParentalLeave(ctx, pdb.UpsertPartialParentalLeaveParams{
		ID:                     inp.ID,
		SalaryID:               inp.SalaryID,
		StartDate:              int64(inp.StartDate),
		EndDate:                int64(inp.EndDate),
		SjukDaysPerYear:        inp.SjukDaysPerYear,
		LagstaDaysPerYear:      inp.LagstaDaysPerYear,
		SkippedWorkDaysPerYear: inp.SkippedWorkDaysPerYear,
		CreatedAt:              now,
		UpdatedAt:              now,
	})
	if err != nil {
		return PartialParentalLeave{}, fmt.Errorf("upserting partial parental leave: %w", err)
	}
	return partialParentalLeaveFromDB(a), nil
}

func (s *Service) ListPartialParentalLeaves(ctx context.Context, salaryID string) ([]PartialParentalLeave, error) {
	rows, err := s.q.ListPartialParentalLeaves(ctx, salaryID)
	if err != nil {
		return nil, fmt.Errorf("listing partial parental leaves: %w", err)
	}
	result := make([]PartialParentalLeave, len(rows))
	for i, r := range rows {
		result[i] = partialParentalLeaveFromDB(r)
	}
	return result, nil
}

func (s *Service) DeletePartialParentalLeave(ctx context.Context, id string) error {
	if err := s.q.DeletePartialParentalLeave(ctx, id); err != nil {
		return fmt.Errorf("deleting partial parental leave: %w", err)
	}
	return nil
}

func fullParentalLeaveFromDB(a pdb.FullParentalLeave) FullParentalLeave {
	return FullParentalLeave{
		ID:              a.ID,
		SalaryID:        a.SalaryID,
		StartDate:       date.Date(a.StartDate),
		EndDate:         date.Date(a.EndDate),
		SjukDaysPerWeek: a.SjukDaysPerWeek,
	}
}

func (s *Service) UpsertFullParentalLeave(ctx context.Context, inp FullParentalLeave) (FullParentalLeave, error) {
	if inp.ID == "" {
		inp.ID = sid.MustNewString(32)
	}
	now := time.Now().Unix()
	a, err := s.q.UpsertFullParentalLeave(ctx, pdb.UpsertFullParentalLeaveParams{
		ID:              inp.ID,
		SalaryID:        inp.SalaryID,
		StartDate:       int64(inp.StartDate),
		EndDate:         int64(inp.EndDate),
		SjukDaysPerWeek: inp.SjukDaysPerWeek,
		CreatedAt:       now,
		UpdatedAt:       now,
	})
	if err != nil {
		return FullParentalLeave{}, fmt.Errorf("upserting full parental leave: %w", err)
	}
	return fullParentalLeaveFromDB(a), nil
}

func (s *Service) ListFullParentalLeaves(ctx context.Context, salaryID string) ([]FullParentalLeave, error) {
	rows, err := s.q.ListFullParentalLeaves(ctx, salaryID)
	if err != nil {
		return nil, fmt.Errorf("listing full parental leaves: %w", err)
	}
	result := make([]FullParentalLeave, len(rows))
	for i, r := range rows {
		result[i] = fullParentalLeaveFromDB(r)
	}
	return result, nil
}

func (s *Service) DeleteFullParentalLeave(ctx context.Context, id string) error {
	if err := s.q.DeleteFullParentalLeave(ctx, id); err != nil {
		return fmt.Errorf("deleting full parental leave: %w", err)
	}
	return nil
}

func (ppl PartialParentalLeave) GetStartDateString() string {
	if ppl.ID == "" {
		return ""
	}
	return ppl.StartDate.String()
}

func (ppl PartialParentalLeave) GetEndDateString() string {
	if ppl.ID == "" {
		return ""
	}
	return ppl.EndDate.String()
}

func (fpl FullParentalLeave) GetStartDateString() string {
	if fpl.ID == "" {
		return ""
	}
	return fpl.StartDate.String()
}

func (fpl FullParentalLeave) GetEndDateString() string {
	if fpl.ID == "" {
		return ""
	}
	return fpl.EndDate.String()
}

func activePartialParentalLeaveAt(leaves []PartialParentalLeave, d date.Date) *PartialParentalLeave {
	for i := range leaves {
		if leaves[i].StartDate <= d && d < leaves[i].EndDate {
			return &leaves[i]
		}
	}
	return nil
}

func activeFullParentalLeaveAt(leaves []FullParentalLeave, d date.Date) *FullParentalLeave {
	for i := range leaves {
		if leaves[i].StartDate <= d && d < leaves[i].EndDate {
			return &leaves[i]
		}
	}
	return nil
}

func (adj SalaryAdjustment) GetValidFromString() string {
	if adj.ID == "" {
		return ""
	}
	return adj.ValidFrom.String()
}

// activeSalaryAdjustmentAt returns the adjustment active at a given date.
func activeSalaryAdjustmentAt(adjustments []SalaryAdjustment, d date.Date) SalaryAdjustment {
	var active SalaryAdjustment
	for _, adj := range adjustments {
		if adj.ValidFrom <= d {
			active = adj
		}
	}
	return active
}

func (sa SalaryAmount) GetStartDateString() string {
	if sa.ID == "" {
		return ""
	}
	return sa.StartDate.String()
}

func (sa SalaryAmount) GetAmountString() string {
	if sa.ID == "" {
		return ""
	}
	return sa.Amount.SimpleEncode()
}

func (s Salary) CurrentAmount() float64 {
	today := date.Today()
	var current *SalaryAmount
	for i := range s.Amounts {
		if s.Amounts[i].StartDate <= today {
			if current == nil || s.Amounts[i].StartDate > current.StartDate {
				current = &s.Amounts[i]
			}
		}
	}
	if current == nil {
		return 0
	}
	return current.Amount.Mean()
}

type SalaryEditView struct {
	Salary     Salary
	Accounts   []Account
	Categories []TransferTemplateCategory
	Breakdowns []NetSalarySegmentBreakdown
}

func (v SalaryEditView) IsEdit() bool {
	return v.Salary.ID != ""
}

func (s *Service) GetSalariesPageData(ctx context.Context) ([]Salary, error) {
	salaries, err := s.ListSalaries(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing salaries: %w", err)
	}
	allAmounts, err := s.q.ListAllSalaryAmounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing salary amounts: %w", err)
	}
	amountsBySalary := make(map[string][]SalaryAmount)
	for _, a := range allAmounts {
		parsed, err := salaryAmountFromDB(a)
		if err != nil {
			return nil, err
		}
		amountsBySalary[a.SalaryID] = append(amountsBySalary[a.SalaryID], parsed)
	}
	for i := range salaries {
		salaries[i].Amounts = amountsBySalary[salaries[i].ID]
	}
	return salaries, nil
}

func (s *Service) GetSalaryNewPageData(ctx context.Context) (*SalaryEditView, error) {
	accs, err := s.ListAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing accounts: %w", err)
	}
	categories, err := s.ListCategories(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing categories: %w", err)
	}
	return &SalaryEditView{
		Accounts:   accs,
		Categories: categories,
	}, nil
}

func (s *Service) GetSalaryEditPageData(ctx context.Context, id string) (*SalaryEditView, error) {
	sal, err := s.GetSalary(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("getting salary: %w", err)
	}
	accs, err := s.ListAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing accounts: %w", err)
	}
	categories, err := s.ListCategories(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing categories: %w", err)
	}
	return &SalaryEditView{
		Salary:     sal,
		Accounts:   accs,
		Categories: categories,
	}, nil
}

func (s *Service) generateSalaryTransferTemplates(ctx context.Context) ([]TransferTemplate, error) {
	salaries, err := s.q.ListSalaries(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing salaries: %w", err)
	}
	allAmounts, err := s.q.ListAllSalaryAmounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing salary amounts: %w", err)
	}
	amountsBySalary := make(map[string][]SalaryAmount)
	for _, a := range allAmounts {
		parsed, err := salaryAmountFromDB(a)
		if err != nil {
			return nil, err
		}
		amountsBySalary[a.SalaryID] = append(amountsBySalary[a.SalaryID], parsed)
	}

	allAdjustments, err := s.q.ListAllSalaryAdjustments(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing salary adjustments: %w", err)
	}
	adjustmentsBySalary := make(map[string][]SalaryAdjustment)
	for _, a := range allAdjustments {
		adjustmentsBySalary[a.SalaryID] = append(adjustmentsBySalary[a.SalaryID], salaryAdjustmentFromDB(a))
	}

	allPartialPLs, err := s.q.ListAllPartialParentalLeaves(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing partial parental leaves: %w", err)
	}
	partialPLsBySalary := make(map[string][]PartialParentalLeave)
	for _, a := range allPartialPLs {
		partialPLsBySalary[a.SalaryID] = append(partialPLsBySalary[a.SalaryID], partialParentalLeaveFromDB(a))
	}

	allFullPLs, err := s.q.ListAllFullParentalLeaves(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing full parental leaves: %w", err)
	}
	fullPLsBySalary := make(map[string][]FullParentalLeave)
	for _, a := range allFullPLs {
		fullPLsBySalary[a.SalaryID] = append(fullPLsBySalary[a.SalaryID], fullParentalLeaveFromDB(a))
	}

	ibbs, err := s.ListSweYearlyParams(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing swe yearly params: %w", err)
	}

	var templates []TransferTemplate
	for _, sal := range salaries {
		salary := salaryFromDB(sal)
		salary.Amounts = amountsBySalary[salary.ID]
		salary.Adjustments = adjustmentsBySalary[salary.ID]
		salary.PartialParentalLeaves = partialPLsBySalary[salary.ID]
		salary.FullParentalLeaves = fullPLsBySalary[salary.ID]

		if salary.IsGross && salary.Kommun != "" && salary.Forsamling != "" {
			netSegs, err := s.computeNetSegments(ctx, salary, ibbs)
			if err != nil {
				return nil, fmt.Errorf("computing net segments: %w", err)
			}
			salary.NetSegments = netSegs
			salary.PensionSegments = s.computePensionSegments(ctx, salary, ibbs)
		}

		templates = append(templates, salary.GenerateTransferTemplates()...)
	}
	return templates, nil
}

// computeNetSegments builds net salary segments split at the union of
// salary-amount, adjustment, and PBB change-point dates. Each segment's Net
// is a mapped uncertain.Value that derives net salary from the gross amount
// via salary adjustments and tax computation.
func (s *Service) computeNetSegments(ctx context.Context, sal Salary, ibbs []SweYearlyParams) ([]NetSalarySegment, error) {
	if len(sal.Amounts) == 0 {
		return nil, nil
	}

	sorted := make([]SalaryAmount, len(sal.Amounts))
	copy(sorted, sal.Amounts)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].StartDate < sorted[j].StartDate
	})

	dateSet := make(map[date.Date]struct{})
	for _, amt := range sorted {
		dateSet[amt.StartDate] = struct{}{}
	}
	for _, adj := range sal.Adjustments {
		if adj.ValidFrom >= sorted[0].StartDate {
			dateSet[adj.ValidFrom] = struct{}{}
		}
	}
	for _, ppl := range sal.PartialParentalLeaves {
		if ppl.StartDate >= sorted[0].StartDate {
			dateSet[ppl.StartDate] = struct{}{}
		}
		if ppl.EndDate >= sorted[0].StartDate {
			dateSet[ppl.EndDate] = struct{}{}
		}
	}
	for _, fpl := range sal.FullParentalLeaves {
		if fpl.StartDate >= sorted[0].StartDate {
			dateSet[fpl.StartDate] = struct{}{}
		}
		if fpl.EndDate >= sorted[0].StartDate {
			dateSet[fpl.EndDate] = struct{}{}
		}
	}
	for _, ibb := range ibbs {
		if ibb.ValidFrom >= sorted[0].StartDate {
			dateSet[ibb.ValidFrom] = struct{}{}
		}
	}

	dates := make([]date.Date, 0, len(dateSet))
	for d := range dateSet {
		dates = append(dates, d)
	}
	sort.Slice(dates, func(i, j int) bool { return dates[i] < dates[j] })

	var segments []NetSalarySegment
	for i, d := range dates {
		grossAmount := activeSalaryAmountAt(sorted, d)
		if grossAmount == nil {
			continue
		}

		year := strings.SplitN(d.String(), "-", 2)[0]
		calculator, err := s.sweClient.NetSalaryCalculator(ctx, swe.GrossSalaryInput{
			Kommun:       sal.Kommun,
			Forsamling:   sal.Forsamling,
			Year:         year,
			ChurchMember: sal.ChurchMember,
			Column:       1,
		})
		if err != nil {
			return nil, fmt.Errorf("creating net salary calculator: %w", err)
		}

		adj := activeSalaryAdjustmentAt(sal.Adjustments, d)
		pbb := activePBBAt(ibbs, d)
		ppl := activePartialParentalLeaveAt(sal.PartialParentalLeaves, d)
		fpl := activeFullParentalLeaveAt(sal.FullParentalLeaves, d)

		gross := *grossAmount
		calc := calculator
		adjParams := swe.SalaryAdjustmentParams{
			YearlyVacationDays:   adj.VacationDaysPerYear,
			SickDaysPerOccasion:  adj.SickDaysPerOccasion,
			SickOccasionsPerYear: adj.SickOccasionsPerYear,
			VABDaysPerYear:       adj.VABDaysPerYear,
			Prisbasbelopp:        pbb,
		}
		fplActive := fpl
		pbbVal := pbb
		var pplSjuk, pplLagsta, pplSkipped float64
		if ppl != nil {
			pplSjuk = ppl.SjukDaysPerYear
			pplLagsta = ppl.LagstaDaysPerYear
			pplSkipped = ppl.SkippedWorkDaysPerYear
		}

		var net uncertain.Value
		if fplActive != nil {
			net = uncertain.NewMapped(func(cfg *uncertain.Config) float64 {
				return swe.CalculateFullParentalLeaveCompensation(gross.Sample(cfg), fplActive.SjukDaysPerWeek, pbbVal)
			})
		} else {
			net = uncertain.NewMapped(func(cfg *uncertain.Config) float64 {
				sampled := gross.Sample(cfg)
				adjusted := swe.AdjustGrossSalary(sampled, adjParams)
				adjusted -= swe.CalculatePartialParentalLeaveDeduction(sampled, pplSjuk, pplLagsta, pplSkipped, pbbVal)
				res, err := calc(adjusted)
				if err != nil {
					return adjusted
				}
				return res.NetMonthly
			})
		}

		var endDate *date.Date
		if i+1 < len(dates) {
			ed := dates[i+1]
			endDate = &ed
		}

		segments = append(segments, NetSalarySegment{
			StartDate: d,
			EndDate:   endDate,
			Net:       net,
		})
	}
	return segments, nil
}

type NetSalarySegmentBreakdown struct {
	StartDate date.Date
	EndDate   *date.Date
	Breakdown swe.SalaryBreakdown
}

// ComputeSalaryBreakdowns returns an itemized breakdown for each net salary
// segment of the given gross salary. Returns nil for non-gross salaries.
func (s *Service) ComputeSalaryBreakdowns(ctx context.Context, salaryID string) ([]NetSalarySegmentBreakdown, error) {
	sal, err := s.GetSalary(ctx, salaryID)
	if err != nil {
		return nil, fmt.Errorf("getting salary: %w", err)
	}
	if !sal.IsGross || sal.Kommun == "" || sal.Forsamling == "" || len(sal.Amounts) == 0 {
		return nil, nil
	}

	ibbs, err := s.ListSweYearlyParams(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing swe yearly params: %w", err)
	}

	return s.computeBreakdownSegments(ctx, sal, ibbs)
}

func (s *Service) computeBreakdownSegments(ctx context.Context, sal Salary, ibbs []SweYearlyParams) ([]NetSalarySegmentBreakdown, error) {
	sorted := make([]SalaryAmount, len(sal.Amounts))
	copy(sorted, sal.Amounts)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].StartDate < sorted[j].StartDate
	})

	dateSet := make(map[date.Date]struct{})
	for _, amt := range sorted {
		dateSet[amt.StartDate] = struct{}{}
	}
	for _, adj := range sal.Adjustments {
		if adj.ValidFrom >= sorted[0].StartDate {
			dateSet[adj.ValidFrom] = struct{}{}
		}
	}
	for _, ppl := range sal.PartialParentalLeaves {
		if ppl.StartDate >= sorted[0].StartDate {
			dateSet[ppl.StartDate] = struct{}{}
		}
		if ppl.EndDate >= sorted[0].StartDate {
			dateSet[ppl.EndDate] = struct{}{}
		}
	}
	for _, fpl := range sal.FullParentalLeaves {
		if fpl.StartDate >= sorted[0].StartDate {
			dateSet[fpl.StartDate] = struct{}{}
		}
		if fpl.EndDate >= sorted[0].StartDate {
			dateSet[fpl.EndDate] = struct{}{}
		}
	}
	for _, ibb := range ibbs {
		if ibb.ValidFrom >= sorted[0].StartDate {
			dateSet[ibb.ValidFrom] = struct{}{}
		}
	}

	dates := make([]date.Date, 0, len(dateSet))
	for d := range dateSet {
		dates = append(dates, d)
	}
	sort.Slice(dates, func(i, j int) bool { return dates[i] < dates[j] })

	var segments []NetSalarySegmentBreakdown
	for i, d := range dates {
		grossAmount := activeSalaryAmountAt(sorted, d)
		if grossAmount == nil {
			continue
		}
		grossMean := grossAmount.Mean()

		fpl := activeFullParentalLeaveAt(sal.FullParentalLeaves, d)
		pbb := activePBBAt(ibbs, d)

		var bd swe.SalaryBreakdown
		if fpl != nil {
			bd = swe.CalculateFullParentalLeaveBreakdown(grossMean, fpl.SjukDaysPerWeek, pbb)
		} else {
			adj := activeSalaryAdjustmentAt(sal.Adjustments, d)
			adjParams := swe.SalaryAdjustmentParams{
				YearlyVacationDays:   adj.VacationDaysPerYear,
				SickDaysPerOccasion:  adj.SickDaysPerOccasion,
				SickOccasionsPerYear: adj.SickOccasionsPerYear,
				VABDaysPerYear:       adj.VABDaysPerYear,
				Prisbasbelopp:        pbb,
			}
			ppl := activePartialParentalLeaveAt(sal.PartialParentalLeaves, d)
			var pplSjuk, pplLagsta, pplSkipped float64
			if ppl != nil {
				pplSjuk = ppl.SjukDaysPerYear
				pplLagsta = ppl.LagstaDaysPerYear
				pplSkipped = ppl.SkippedWorkDaysPerYear
			}

			year := strings.SplitN(d.String(), "-", 2)[0]
			calculator, err := s.sweClient.NetSalaryCalculator(ctx, swe.GrossSalaryInput{
				Kommun:       sal.Kommun,
				Forsamling:   sal.Forsamling,
				Year:         year,
				ChurchMember: sal.ChurchMember,
				Column:       1,
			})
			if err != nil {
				return nil, fmt.Errorf("creating net salary calculator: %w", err)
			}
			taxFunc := func(adjustedGross float64) (float64, error) {
				res, err := calculator(adjustedGross)
				if err != nil {
					return 0, err
				}
				return res.Tax, nil
			}
			bd = swe.CalculateSalaryBreakdown(grossMean, adjParams, pplSjuk, pplLagsta, pplSkipped, pbb, taxFunc)
		}

		var endDate *date.Date
		if i+1 < len(dates) {
			ed := dates[i+1]
			endDate = &ed
		}

		segments = append(segments, NetSalarySegmentBreakdown{
			StartDate: d,
			EndDate:   endDate,
			Breakdown: bd,
		})
	}
	return segments, nil
}

// computePensionSegments builds pension segments split at the union of
// salary-amount and IBB change-point dates. Each segment's Pension is a
// mapped uncertain.Value that derives pension from the gross amount.
func (s *Service) computePensionSegments(_ context.Context, sal Salary, ibbs []SweYearlyParams) []PensionSegment {
	if len(sal.Amounts) == 0 {
		return nil
	}

	sorted := make([]SalaryAmount, len(sal.Amounts))
	copy(sorted, sal.Amounts)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].StartDate < sorted[j].StartDate
	})

	dateSet := make(map[date.Date]struct{})
	for _, amt := range sorted {
		dateSet[amt.StartDate] = struct{}{}
	}
	for _, ibb := range ibbs {
		if ibb.ValidFrom >= sorted[0].StartDate {
			dateSet[ibb.ValidFrom] = struct{}{}
		}
	}

	dates := make([]date.Date, 0, len(dateSet))
	for d := range dateSet {
		dates = append(dates, d)
	}
	sort.Slice(dates, func(i, j int) bool { return dates[i] < dates[j] })

	var segments []PensionSegment
	for i, d := range dates {
		grossAmount := activeSalaryAmountAt(sorted, d)
		if grossAmount == nil {
			continue
		}
		ibb := activeIBBAt(ibbs, d)
		if ibb == 0 {
			continue
		}

		gross := *grossAmount
		ibbVal := ibb
		pension := uncertain.NewMapped(func(cfg *uncertain.Config) float64 {
			return swe.CalculateITP1Pension(gross.Sample(cfg), ibbVal)
		})

		var endDate *date.Date
		if i+1 < len(dates) {
			ed := dates[i+1]
			endDate = &ed
		}

		segments = append(segments, PensionSegment{
			StartDate: d,
			EndDate:   endDate,
			Pension:   pension,
		})
	}
	return segments
}

// activeSalaryAmountAt returns the uncertain.Value gross salary active at a given date.
func activeSalaryAmountAt(sorted []SalaryAmount, d date.Date) *uncertain.Value {
	var active *uncertain.Value
	for i := range sorted {
		if sorted[i].StartDate <= d {
			active = &sorted[i].Amount
		}
	}
	return active
}
