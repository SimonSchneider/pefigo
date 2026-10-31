package model

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/goslu/sid"
	"github.com/SimonSchneider/pefigo/internal/pdb"
	"github.com/SimonSchneider/pefigo/pkg/swe"
	"github.com/SimonSchneider/pefigo/pkg/ui"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

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

	// segments are a loaded Gross Salary's Salary Segments, unless segmentsErr kept them from being
	// computed. See Service.loadSalaries.
	segments    []SalarySegment
	segmentsErr error
}

// SalarySegment is a Salary Segment of a Gross Salary: from its Start Date up to its End Date, the
// Gross Salary pays its Net Salary and its Pension Contribution on each recurrence.
type SalarySegment struct {
	StartDate date.Date
	EndDate   *date.Date
	NetSalary uncertain.Value
	// PensionContribution is nil in a segment without an income base amount: it pays none.
	PensionContribution *uncertain.Value
	// Breakdown is the Salary Breakdown of the segment's mean Salary Amount.
	Breakdown SalaryBreakdown
}

// SalaryBreakdown itemizes a Salary Segment's mean Salary Amount down to its Net Salary, and gives its
// Pension Contribution: nil when the segment pays none.
type SalaryBreakdown struct {
	swe.SalaryBreakdown
	PensionContribution *float64
}

// Segments returns a loaded Gross Salary's Salary Segments, or the error that kept them from being
// computed. A net Salary has none.
func (s Salary) Segments() ([]SalarySegment, error) {
	return s.segments, s.segmentsErr
}

// CurrentSegment is the Salary Segment of a loaded Gross Salary that today is in, or nil without one.
func (s Salary) CurrentSegment() *SalarySegment {
	today := date.Today()
	for i, seg := range s.segments {
		if seg.StartDate <= today && (seg.EndDate == nil || today < *seg.EndDate) {
			return &s.segments[i]
		}
	}
	return nil
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

// GenerateTransferTemplates returns what the Salary pays: each Salary Amount of a net Salary, and each
// Salary Segment's Net Salary and Pension Contribution of a Gross Salary, which only a Salary the
// Service loaded has. It fails on a Gross Salary whose Net Salary is unknown.
func (s Salary) GenerateTransferTemplates() ([]TransferTemplate, error) {
	source := TransferTemplateSource{
		Type:          "salary",
		EntityID:      s.ID,
		Label:         s.Name,
		EditURL:       "/salaries/" + s.ID + "/edit",
		recurrenceRow: "salary " + s.ID,
	}
	template := func(id, name, toAccountID string, amount uncertain.Value, startDate date.Date, endDate *date.Date) TransferTemplate {
		return TransferTemplate{
			ID:               id,
			Name:             name,
			FromAccountID:    "",
			ToAccountID:      toAccountID,
			AmountType:       TransferAmountFixed,
			AmountFixed:      amount,
			Priority:         s.Priority,
			Recurrence:       s.Recurrence,
			StartDate:        startDate,
			EndDate:          endDate,
			Enabled:          s.Enabled,
			BudgetCategoryID: s.BudgetCategoryID,
			Source:           source,
		}
	}

	var templates []TransferTemplate
	if !s.IsGross {
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
			templates = append(templates, template("salary:"+amt.ID, s.Name, s.ToAccountID, amt.Amount, amt.StartDate, endDate))
		}
		return templates, nil
	}

	if s.segmentsErr != nil {
		return nil, fmt.Errorf("salary %q: %w", s.Name, s.segmentsErr)
	}
	// A loaded Gross Salary with a Salary Amount has a Salary Segment from its Start Date.
	if len(s.segments) == 0 && len(s.Amounts) > 0 {
		return nil, fmt.Errorf("salary %q is not loaded: it has no Salary Segments", s.Name)
	}
	for i, seg := range s.segments {
		templates = append(templates, template(fmt.Sprintf("salary:%s:%d", s.ID, i), s.Name, s.ToAccountID, seg.NetSalary, seg.StartDate, seg.EndDate))
	}
	if s.PensionAccountID != "" {
		for i, seg := range s.segments {
			if seg.PensionContribution != nil {
				templates = append(templates, template(fmt.Sprintf("salary-pension:%s:%d", s.ID, i), s.Name+" (pension)", s.PensionAccountID, *seg.PensionContribution, seg.StartDate, seg.EndDate))
			}
		}
	}
	return templates, nil
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

// DefaultSalaryRecurrence is the recurrence of a Salary saved without one: the 25th of every month.
const DefaultSalaryRecurrence date.Cron = "*-*-25"

func (s *Service) UpsertSalary(ctx context.Context, inp Salary) (Salary, error) {
	if inp.Recurrence == "" {
		inp.Recurrence = DefaultSalaryRecurrence
	}
	var taxedBy error
	if inp.IsGross {
		taxedBy = errors.Join(checkRequired("kommun", inp.Kommun), checkRequired("församling", inp.Forsamling))
	}
	if err := errors.Join(checkRequired("name", inp.Name), checkDatePattern("salary recurrence", inp.Recurrence), taxedBy); err != nil {
		return Salary{}, err
	}
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

// ListSalaries returns every Salary, loaded as loadSalaries loads it.
func (s *Service) ListSalaries(ctx context.Context) ([]Salary, error) {
	return s.loadSalaries(ctx, "")
}

// GetSalary returns the Salary id, loaded as loadSalaries loads it.
func (s *Service) GetSalary(ctx context.Context, id string) (Salary, error) {
	salaries, err := s.loadSalaries(ctx, id)
	if err != nil {
		return Salary{}, err
	}
	return salaries[0], nil
}

// loadSalaries returns every Salary, or only the Salary id when id isn't empty, with everything it is
// paid by: its Salary Amounts, adjustments and parental leaves, and a Gross Salary its Salary Segments,
// segmented once, or the error that kept them from being computed.
func (s *Service) loadSalaries(ctx context.Context, id string) ([]Salary, error) {
	var rows []pdb.Salary
	if id == "" {
		var err error
		if rows, err = s.q.ListSalaries(ctx); err != nil {
			return nil, fmt.Errorf("listing salaries: %w", err)
		}
	} else {
		row, err := s.q.GetSalary(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("getting salary: %w", err)
		}
		rows = []pdb.Salary{row}
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

	params, err := s.ListSweYearlyParams(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing swe yearly params: %w", err)
	}

	salaries := make([]Salary, 0, len(rows))
	for _, row := range rows {
		salary := salaryFromDB(row)
		salary.Amounts = amountsBySalary[salary.ID]
		salary.Adjustments = adjustmentsBySalary[salary.ID]
		salary.PartialParentalLeaves = partialPLsBySalary[salary.ID]
		salary.FullParentalLeaves = fullPLsBySalary[salary.ID]
		if salary.IsGross {
			salary.segments, salary.segmentsErr = s.segment(ctx, salary, params)
		}
		salaries = append(salaries, salary)
	}
	return salaries, nil
}

func (s *Service) DeleteSalary(ctx context.Context, id string) error {
	if err := s.q.DeleteSalary(ctx, id); err != nil {
		return fmt.Errorf("deleting salary: %w", err)
	}
	return nil
}

func (s *Service) UpsertSalaryAmount(ctx context.Context, inp SalaryAmount) (SalaryAmount, error) {
	if err := errors.Join(checkRequired("salary", inp.SalaryID), checkRequired("start date", inp.StartDate)); err != nil {
		return SalaryAmount{}, err
	}
	if inp.ID == "" {
		inp.ID = sid.MustNewString(32)
	}
	encoded, err := orZero(inp.Amount).Encode()
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
	if err := errors.Join(checkRequired("salary", inp.SalaryID), checkRequired("valid from", inp.ValidFrom)); err != nil {
		return SalaryAdjustment{}, err
	}
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

// checkParentalLeave rejects a parental leave without a salary, a Start Date or an End Date after it.
func checkParentalLeave(salaryID string, start, end date.Date) error {
	if err := errors.Join(checkRequired("salary", salaryID), checkRequired("start date", start), checkRequired("end date", end)); err != nil {
		return err
	}
	return checkEndDate(start, &end)
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
	if err := checkParentalLeave(inp.SalaryID, inp.StartDate, inp.EndDate); err != nil {
		return PartialParentalLeave{}, err
	}
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
	if err := checkParentalLeave(inp.SalaryID, inp.StartDate, inp.EndDate); err != nil {
		return FullParentalLeave{}, err
	}
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
}

func (v SalaryEditView) IsEdit() bool {
	return v.Salary.ID != ""
}

func (s *Service) GetSalariesPageData(ctx context.Context) ([]Salary, error) {
	return s.ListSalaries(ctx)
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
	salaries, err := s.ListSalaries(ctx)
	if err != nil {
		return nil, err
	}
	var templates []TransferTemplate
	for _, salary := range salaries {
		salaryTemplates, err := salary.GenerateTransferTemplates()
		if err != nil {
			return nil, err
		}
		templates = append(templates, salaryTemplates...)
	}
	return templates, nil
}

// segment splits a Gross Salary into its Salary Segments, from its first Salary Amount's Start Date.
// Each segment's Net Salary, Pension Contribution and Salary Breakdown come from the same Salary
// Amount, adjustment, parental leave, Swedish yearly parameters and Tax Table. It fails without a kommun
// and församling to look up the tax in, or without tax data for a segment's year or the year before.
func (s *Service) segment(ctx context.Context, sal Salary, params []SweYearlyParams) ([]SalarySegment, error) {
	dates := sal.segmentStartDates(params)
	if len(dates) == 0 {
		return nil, nil
	}
	if sal.Kommun == "" || sal.Forsamling == "" {
		return nil, errors.New("no kommun and församling to look up its tax in")
	}

	sorted := make([]SalaryAmount, len(sal.Amounts))
	copy(sorted, sal.Amounts)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].StartDate < sorted[j].StartDate
	})

	calculators := make(map[int]func(float64) swe.NetSalaryResult)
	segments := make([]SalarySegment, 0, len(dates))
	for i, d := range dates {
		calculate, ok := calculators[d.Year()]
		if !ok {
			var err error
			calculate, err = s.sweClient.NetSalaryCalculator(ctx, swe.GrossSalaryInput{
				Kommun:       sal.Kommun,
				Forsamling:   sal.Forsamling,
				Year:         strconv.Itoa(d.Year()),
				ChurchMember: sal.ChurchMember,
			})
			if err != nil {
				return nil, err
			}
			calculators[d.Year()] = calculate
		}
		breakdown := sal.breakdownOn(d, params, func(adjustedGross float64) float64 { return calculate(adjustedGross).Tax })
		gross := *activeSalaryAmountAt(sorted, d)

		seg := SalarySegment{
			StartDate: d,
			NetSalary: uncertain.NewMapped(func(cfg *uncertain.Config) float64 {
				return breakdown(gross.Sample(cfg)).NetMonthly
			}),
			Breakdown: SalaryBreakdown{SalaryBreakdown: breakdown(gross.Mean())},
		}
		if i+1 < len(dates) {
			ed := dates[i+1]
			seg.EndDate = &ed
		}
		if ibb := activeIBBAt(params, d); ibb != 0 {
			pension := uncertain.NewMapped(func(cfg *uncertain.Config) float64 {
				return swe.CalculateITP1Pension(gross.Sample(cfg), ibb)
			})
			meanPension := swe.CalculateITP1Pension(gross.Mean(), ibb)
			seg.PensionContribution = &pension
			seg.Breakdown.PensionContribution = &meanPension
		}
		segments = append(segments, seg)
	}
	return segments, nil
}

// segmentStartDates are the days the Salary's Salary Segments start on: its first Salary Amount's
// Start Date and every later day its Salary Amount, adjustment, a parental leave or the Swedish yearly
// parameters change.
func (s Salary) segmentStartDates(params []SweYearlyParams) []date.Date {
	if len(s.Amounts) == 0 {
		return nil
	}
	first := s.Amounts[0].StartDate
	for _, amt := range s.Amounts {
		first = min(first, amt.StartDate)
	}
	dateSet := make(map[date.Date]struct{})
	add := func(d date.Date) {
		if d >= first {
			dateSet[d] = struct{}{}
		}
	}
	for _, amt := range s.Amounts {
		add(amt.StartDate)
	}
	for _, adj := range s.Adjustments {
		add(adj.ValidFrom)
	}
	for _, ppl := range s.PartialParentalLeaves {
		add(ppl.StartDate)
		add(ppl.EndDate)
	}
	for _, fpl := range s.FullParentalLeaves {
		add(fpl.StartDate)
		add(fpl.EndDate)
	}
	for _, p := range params {
		add(p.ValidFrom)
	}

	dates := make([]date.Date, 0, len(dateSet))
	for d := range dateSet {
		dates = append(dates, d)
	}
	slices.Sort(dates)
	return dates
}

// breakdownOn returns the Salary Breakdown of a monthly gross in the Salary Segment starting on d, taxed
// by tax: the compensation of a full parental leave, or otherwise the gross adjusted and taxed.
func (s Salary) breakdownOn(d date.Date, params []SweYearlyParams, tax func(adjustedGross float64) float64) func(gross float64) swe.SalaryBreakdown {
	pbb := activePBBAt(params, d)
	if fpl := activeFullParentalLeaveAt(s.FullParentalLeaves, d); fpl != nil {
		sjukDaysPerWeek := fpl.SjukDaysPerWeek
		return func(gross float64) swe.SalaryBreakdown {
			return swe.CalculateFullParentalLeaveBreakdown(gross, sjukDaysPerWeek, pbb)
		}
	}
	adj := activeSalaryAdjustmentAt(s.Adjustments, d)
	adjParams := swe.SalaryAdjustmentParams{
		YearlyVacationDays:   adj.VacationDaysPerYear,
		SickDaysPerOccasion:  adj.SickDaysPerOccasion,
		SickOccasionsPerYear: adj.SickOccasionsPerYear,
		VABDaysPerYear:       adj.VABDaysPerYear,
		Prisbasbelopp:        pbb,
	}
	if ppl := activePartialParentalLeaveAt(s.PartialParentalLeaves, d); ppl != nil {
		adjParams.ParentalSjukDaysPerYear = ppl.SjukDaysPerYear
		adjParams.ParentalLagstaDaysPerYear = ppl.LagstaDaysPerYear
		adjParams.ParentalSkippedWorkDaysPerYear = ppl.SkippedWorkDaysPerYear
	}
	return func(gross float64) swe.SalaryBreakdown {
		return swe.CalculateSalaryBreakdown(gross, adjParams, tax)
	}
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
