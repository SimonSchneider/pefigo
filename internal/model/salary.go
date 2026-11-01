package model

import (
	"cmp"
	"context"
	"database/sql"
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
	ID               string
	Name             string
	ToAccountID      string
	PensionAccountID string
	Priority         int64
	Recurrence       date.Cron
	BudgetCategoryID *string
	Enabled          bool
	Kommun           string
	Forsamling       string
	ChurchMember     bool
	IsGross          bool
	// EventID is the Event the Salary is in, or "" in the Baseline. A Salary has no dates of its own:
	// when its Event is left out, it is left out with every part of it.
	EventID               string
	Amounts               []SalaryAmount
	Adjustments           []SalaryAdjustment
	PartialParentalLeaves []PartialParentalLeave
	FullParentalLeaves    []FullParentalLeave
	// Ends are the days the Salary may end on: it pays nothing from the earliest of them.
	Ends []SalaryEnd

	// segments are a loaded Gross Salary's Salary Segments, unless segmentsErr kept them from being
	// computed. See Service.loadSalaries.
	segments    []SalarySegment
	segmentsErr error
	// paid is how a Salary loaded for its pages, whose parts are each at their own Event's date, is
	// paid: as the Baseline pays it, or, when the Baseline leaves it out with its Event, as it would be
	// paid with its Event at its own date. It is nil for a Salary as it is paid. See Salary.payment.
	paid *Salary
	// leftOut is whether the Baseline leaves the loaded Salary out with its unplanned Event.
	leftOut bool
}

// SalaryEnd is a day a Salary may end on: from it, no Salary Amount applies and nothing of the Salary
// is paid. In an Event its date is stored as days after the Event's date.
type SalaryEnd struct {
	ID       string
	SalaryID string
	Date     date.Date
	EventID  string
	// DateOffset, when set, gives the date in an Event as days after the date of OffsetEventID, the
	// Event the form showed it for, or of EventID when that is blank.
	DateOffset    *date.Duration
	OffsetEventID string
}

// payment is the Salary as it is paid: as loaded for its pages, how the Baseline pays it, or how it
// would be paid with its unplanned Event at its date. It is the Salary itself otherwise.
func (s Salary) payment() Salary {
	if s.paid != nil {
		return *s.paid
	}
	return s
}

// InBaseline reports whether the Baseline pays the Salary: not when its Event is unplanned. Its pages
// then show what it would pay with its Event at its date.
func (s Salary) InBaseline() bool {
	return !s.leftOut
}

// End is the first day the Salary pays nothing: the earliest of its Ends, or nil without one.
func (s Salary) End() *date.Date {
	return firstEnd(s.Ends, func(e SalaryEnd) date.Date { return e.Date })
}

// hasEndedBy reports whether the Salary has ended by day.
func (s Salary) hasEndedBy(day date.Date) bool {
	return endedBy(s.End(), day)
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

// Segments returns the Salary Segments a loaded Gross Salary is paid in, or the error that kept them
// from being computed. A net Salary has none.
func (s Salary) Segments() ([]SalarySegment, error) {
	paid := s.payment()
	return paid.segments, paid.segmentsErr
}

// CurrentSegment is the Salary Segment of a loaded Gross Salary that today is in, or nil without one.
func (s Salary) CurrentSegment() *SalarySegment {
	today := date.Today()
	segments, _ := s.Segments()
	for i, seg := range segments {
		if seg.StartDate <= today && (seg.EndDate == nil || today < *seg.EndDate) {
			return &segments[i]
		}
	}
	return nil
}

// The dated parts of a Salary each belong to at most one Event, which may differ from the Salary's:
// an Event may add a part to a Salary outside it. In an Event a part's dates are stored as days after
// the Event's date, and its form may give them so, after the date of OffsetEventID, the Event the form
// showed them for, or of EventID when that is blank.

type SalaryAmount struct {
	ID            string
	SalaryID      string
	Amount        uncertain.Value
	StartDate     date.Date
	EventID       string
	StartOffset   *date.Duration
	OffsetEventID string
}

type SalaryAdjustment struct {
	ID                   string
	SalaryID             string
	ValidFrom            date.Date
	VacationDaysPerYear  float64
	SickDaysPerOccasion  float64
	SickOccasionsPerYear float64
	VABDaysPerYear       float64
	EventID              string
	ValidFromOffset      *date.Duration
	OffsetEventID        string
}

type PartialParentalLeave struct {
	ID                     string
	SalaryID               string
	StartDate              date.Date
	EndDate                date.Date
	SjukDaysPerYear        float64
	LagstaDaysPerYear      float64
	SkippedWorkDaysPerYear float64
	EventID                string
	StartOffset            *date.Duration
	EndOffset              *date.Duration
	OffsetEventID          string
}

type FullParentalLeave struct {
	ID              string
	SalaryID        string
	StartDate       date.Date
	EndDate         date.Date
	SjukDaysPerWeek float64
	EventID         string
	StartOffset     *date.Duration
	EndOffset       *date.Duration
	OffsetEventID   string
}

// GenerateTransferTemplates returns what the Salary pays: each Salary Amount of a net Salary, and each
// Salary Segment's Net Salary and Pension Contribution of a Gross Salary, which only a Salary the
// Service loaded has. It fails on a Gross Salary whose Net Salary is unknown.
func (s Salary) GenerateTransferTemplates() ([]TransferTemplate, error) {
	source := TransferTemplateSource{
		Type:          TransferSourceSalary,
		EntityID:      s.ID,
		Label:         s.Name,
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
			if s.hasEndedBy(amt.StartDate) {
				break
			}
			endDate := s.End()
			if i+1 < len(sorted) {
				endDate = earliest(endDate, &sorted[i+1].StartDate)
			}
			templates = append(templates, template("salary:"+amt.ID, s.Name, s.ToAccountID, amt.Amount, amt.StartDate, endDate))
		}
		return templates, nil
	}

	if s.segmentsErr != nil {
		return nil, fmt.Errorf("salary %q: %w", s.Name, s.segmentsErr)
	}
	// A loaded Gross Salary with a Salary Amount before its End has a Salary Segment from its Start Date.
	if len(s.segments) == 0 && len(s.segmentStartDates(nil)) > 0 {
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

// salaryFromDB is the stored Salary s with its Event at at: a one-off's recurrence is a date of its
// Event, as a one-off Transfer Template's is.
func salaryFromDB(s pdb.Salary, at date.Date) Salary {
	return Salary{
		ID:               s.ID,
		Name:             s.Name,
		ToAccountID:      ui.OrDefault(s.ToAccountID),
		PensionAccountID: ui.OrDefault(s.PensionAccountID),
		Priority:         s.Priority,
		Recurrence:       placeRecurrence(s.Recurrence, s.EventID != nil, at),
		BudgetCategoryID: s.BudgetCategoryID,
		Enabled:          s.Enabled,
		Kommun:           s.Kommun,
		Forsamling:       s.Forsamling,
		ChurchMember:     s.ChurchMember,
		IsGross:          s.IsGross,
		EventID:          ui.OrDefault(s.EventID),
	}
}

// salaryAmountFromDB is the stored Salary Amount a with its Event at at.
func salaryAmountFromDB(a pdb.SalaryAmount, at date.Date) (SalaryAmount, error) {
	var amount uncertain.Value
	if err := amount.Decode(a.Amount); err != nil {
		return SalaryAmount{}, fmt.Errorf("decoding salary amount: %w", err)
	}
	return SalaryAmount{
		ID:        a.ID,
		SalaryID:  a.SalaryID,
		Amount:    amount,
		StartDate: placeDate(a.StartDate, at),
		EventID:   ui.OrDefault(a.EventID),
	}, nil
}

// salaryEndFromDB is the stored end e with its Event at at.
func salaryEndFromDB(e pdb.SalaryEnd, at date.Date) SalaryEnd {
	return SalaryEnd{ID: e.ID, SalaryID: e.SalaryID, Date: placeDate(e.Date, at), EventID: ui.OrDefault(e.EventID)}
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
	at, err := s.eventDate(ctx, inp.EventID)
	if err != nil {
		return Salary{}, err
	}
	previous, err := storedEventOf(ctx, inp.ID, s.q.GetSalary, func(sal pdb.Salary) *string { return sal.EventID })
	if err != nil {
		return Salary{}, fmt.Errorf("getting salary: %w", err)
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
		Recurrence:       storeRecurrence(inp.Recurrence, inp.EventID != "", at),
		BudgetCategoryID: inp.BudgetCategoryID,
		Enabled:          inp.Enabled,
		Kommun:           inp.Kommun,
		Forsamling:       inp.Forsamling,
		ChurchMember:     inp.ChurchMember,
		IsGross:          inp.IsGross,
		EventID:          ui.WithDefaultNull(inp.EventID),
		CreatedAt:        now,
		UpdatedAt:        now,
	})
	if err != nil {
		return Salary{}, fmt.Errorf("upserting salary: %w", err)
	}
	if previous != nil && *previous != inp.EventID {
		if err := s.bringParts(ctx, EventEntrySalary, inp.ID, *previous, inp.EventID); err != nil {
			return Salary{}, err
		}
	}
	return salaryFromDB(sal, at), nil
}

// ListSalaries returns every Salary, loaded as loadSalaries loads it.
func (s *Service) ListSalaries(ctx context.Context) ([]Salary, error) {
	loaded, err := s.loadSalaries(ctx, "")
	return loaded.salaries, err
}

// GetSalary returns the Salary id, loaded as loadSalaries loads it.
func (s *Service) GetSalary(ctx context.Context, id string) (Salary, error) {
	loaded, err := s.loadSalaries(ctx, id)
	if err != nil {
		return Salary{}, err
	}
	return loaded.salaries[0], nil
}

// loadSalaries returns every Salary, or only the Salary id when id isn't empty, with every part of it
// at its own Event's date, as its forms edit it, and with the Events it shows. What it pays is how the
// Baseline pays it: its Salary Segments and current amount leave out what unplanned Events add or end.
// A Salary in an unplanned Event pays what it would with every Event at its own date. A Gross Salary is
// segmented once, or carries the error that kept it from being segmented.
func (s *Service) loadSalaries(ctx context.Context, id string) (loadedSalaries, error) {
	events, err := s.ListEvents(ctx)
	if err != nil {
		return loadedSalaries{}, err
	}
	stored, err := s.loadStoredEntries(ctx)
	if err != nil {
		return loadedSalaries{}, err
	}
	own, err := placeEntries(events, stored, asStored(events))
	if err != nil {
		return loadedSalaries{}, err
	}
	baseline, err := placeEntries(events, stored, nil)
	if err != nil {
		return loadedSalaries{}, err
	}
	inBaseline := make(map[string]bool, len(baseline.salaries))
	for _, sal := range baseline.salaries {
		inBaseline[sal.ID] = true
	}
	paying := slices.Clone(baseline.salaries)
	for _, sal := range own.salaries {
		if !inBaseline[sal.ID] {
			paying = append(paying, sal)
		}
	}
	if err := s.segmentSalaries(ctx, paying); err != nil {
		return loadedSalaries{}, err
	}
	paid := make(map[string]*Salary, len(paying))
	for i := range paying {
		paid[paying[i].ID] = &paying[i]
	}
	var salaries []Salary
	for _, sal := range own.salaries {
		if id != "" && sal.ID != id {
			continue
		}
		sal.paid, sal.leftOut = paid[sal.ID], !inBaseline[sal.ID]
		salaries = append(salaries, sal)
	}
	if id != "" && len(salaries) == 0 {
		return loadedSalaries{}, fmt.Errorf("getting salary: %w", sql.ErrNoRows)
	}
	return loadedSalaries{salaries: salaries, events: events, accounts: accountsListFromDB(stored.accounts)}, nil
}

// loadedSalaries are Salaries loaded for their pages, with the Events and accounts they were loaded
// with, which their pages show.
type loadedSalaries struct {
	salaries []Salary
	events   []Event
	accounts []Account
}

// segmentSalaries segments each Gross Salary of salaries, or keeps the error that kept it from being
// segmented on it.
func (s *Service) segmentSalaries(ctx context.Context, salaries []Salary) error {
	params, err := s.ListSweYearlyParams(ctx)
	if err != nil {
		return fmt.Errorf("listing swe yearly params: %w", err)
	}
	for i := range salaries {
		if salaries[i].IsGross {
			salaries[i].segments, salaries[i].segmentsErr = s.segment(ctx, salaries[i], params)
		}
	}
	return nil
}

func (s *Service) DeleteSalary(ctx context.Context, id string) error {
	if err := s.q.DeleteSalary(ctx, id); err != nil {
		return fmt.Errorf("deleting salary: %w", err)
	}
	return nil
}

func (s *Service) UpsertSalaryAmount(ctx context.Context, inp SalaryAmount) (SalaryAmount, error) {
	dating, err := s.entryDating(ctx, inp.EventID, inp.OffsetEventID)
	if err != nil {
		return SalaryAmount{}, err
	}
	if inp.StartDate, err = dating.firstDay("start", inp.StartDate, inp.StartOffset); err != nil {
		return SalaryAmount{}, err
	}
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
		StartDate: dating.store(inp.StartDate),
		EventID:   ui.WithDefaultNull(inp.EventID),
		CreatedAt: now,
		UpdatedAt: now,
	})
	if err != nil {
		return SalaryAmount{}, fmt.Errorf("upserting salary amount: %w", err)
	}
	return salaryAmountFromDB(a, dating.at)
}

// getSalaryAmount returns the Salary Amount id, in an Event at the Event's date.
func (s *Service) getSalaryAmount(ctx context.Context, id string) (SalaryAmount, error) {
	a, err := s.q.GetSalaryAmount(ctx, id)
	if err != nil {
		return SalaryAmount{}, fmt.Errorf("getting salary amount: %w", err)
	}
	at, err := s.eventDate(ctx, ui.OrDefault(a.EventID))
	if err != nil {
		return SalaryAmount{}, err
	}
	return salaryAmountFromDB(a, at)
}

// ListSalaryAmounts returns the Salary's Salary Amounts by Start Date, each in an Event at the Event's
// date.
func (s *Service) ListSalaryAmounts(ctx context.Context, salaryID string) ([]SalaryAmount, error) {
	rows, err := s.q.ListSalaryAmounts(ctx, salaryID)
	if err != nil {
		return nil, fmt.Errorf("listing salary amounts: %w", err)
	}
	dates, err := s.eventDates(ctx)
	if err != nil {
		return nil, err
	}
	amounts := make([]SalaryAmount, 0, len(rows))
	for _, r := range rows {
		a, err := salaryAmountFromDB(r, ownDate(dates, r.EventID))
		if err != nil {
			return nil, err
		}
		amounts = append(amounts, a)
	}
	sortByDayAndID(amounts, SalaryAmount.dayAndID)
	return amounts, nil
}

func (s *Service) DeleteSalaryAmount(ctx context.Context, id string) error {
	if err := s.q.DeleteSalaryAmount(ctx, id); err != nil {
		return fmt.Errorf("deleting salary amount: %w", err)
	}
	return nil
}

// salaryAdjustmentFromDB is the stored salary adjustment a with its Event at at.
func salaryAdjustmentFromDB(a pdb.SalaryAdjustment, at date.Date) SalaryAdjustment {
	return SalaryAdjustment{
		ID:                   a.ID,
		SalaryID:             a.SalaryID,
		ValidFrom:            placeDate(a.ValidFrom, at),
		VacationDaysPerYear:  a.VacationDaysPerYear,
		SickDaysPerOccasion:  a.SickDaysPerOccasion,
		SickOccasionsPerYear: a.SickOccasionsPerYear,
		VABDaysPerYear:       a.VabDaysPerYear,
		EventID:              ui.OrDefault(a.EventID),
	}
}

func (s *Service) UpsertSalaryAdjustment(ctx context.Context, inp SalaryAdjustment) (SalaryAdjustment, error) {
	dating, err := s.entryDating(ctx, inp.EventID, inp.OffsetEventID)
	if err != nil {
		return SalaryAdjustment{}, err
	}
	if inp.ValidFrom, err = dating.firstDay("valid from", inp.ValidFrom, inp.ValidFromOffset); err != nil {
		return SalaryAdjustment{}, err
	}
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
		ValidFrom:            dating.store(inp.ValidFrom),
		VacationDaysPerYear:  inp.VacationDaysPerYear,
		SickDaysPerOccasion:  inp.SickDaysPerOccasion,
		SickOccasionsPerYear: inp.SickOccasionsPerYear,
		VabDaysPerYear:       inp.VABDaysPerYear,
		EventID:              ui.WithDefaultNull(inp.EventID),
		CreatedAt:            now,
		UpdatedAt:            now,
	})
	if err != nil {
		return SalaryAdjustment{}, fmt.Errorf("upserting salary adjustment: %w", err)
	}
	return salaryAdjustmentFromDB(a, dating.at), nil
}

// getSalaryAdjustment returns the salary adjustment id, in an Event at the Event's date.
func (s *Service) getSalaryAdjustment(ctx context.Context, id string) (SalaryAdjustment, error) {
	a, err := s.q.GetSalaryAdjustment(ctx, id)
	if err != nil {
		return SalaryAdjustment{}, fmt.Errorf("getting salary adjustment: %w", err)
	}
	at, err := s.eventDate(ctx, ui.OrDefault(a.EventID))
	if err != nil {
		return SalaryAdjustment{}, err
	}
	return salaryAdjustmentFromDB(a, at), nil
}

// ListSalaryAdjustments returns the Salary's adjustments by the day they are valid from, each in an
// Event at the Event's date.
func (s *Service) ListSalaryAdjustments(ctx context.Context, salaryID string) ([]SalaryAdjustment, error) {
	rows, err := s.q.ListSalaryAdjustments(ctx, salaryID)
	if err != nil {
		return nil, fmt.Errorf("listing salary adjustments: %w", err)
	}
	dates, err := s.eventDates(ctx)
	if err != nil {
		return nil, err
	}
	adjustments := make([]SalaryAdjustment, 0, len(rows))
	for _, r := range rows {
		adjustments = append(adjustments, salaryAdjustmentFromDB(r, ownDate(dates, r.EventID)))
	}
	sortByDayAndID(adjustments, SalaryAdjustment.dayAndID)
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

// leaveDates are the Start and End Dates of a parental leave input, as dating gives them.
func leaveDates(dating entryDating, start, end date.Date, startOffset, endOffset *date.Duration) (date.Date, date.Date, error) {
	start, startErr := dating.firstDay("start", start, startOffset)
	end, endErr := dating.date("end", end, endOffset)
	return start, end, errors.Join(startErr, endErr)
}

// partialParentalLeaveFromDB is the stored partial parental leave a with its Event at at.
func partialParentalLeaveFromDB(a pdb.PartialParentalLeave, at date.Date) PartialParentalLeave {
	return PartialParentalLeave{
		ID:                     a.ID,
		SalaryID:               a.SalaryID,
		StartDate:              placeDate(a.StartDate, at),
		EndDate:                placeDate(a.EndDate, at),
		SjukDaysPerYear:        a.SjukDaysPerYear,
		LagstaDaysPerYear:      a.LagstaDaysPerYear,
		SkippedWorkDaysPerYear: a.SkippedWorkDaysPerYear,
		EventID:                ui.OrDefault(a.EventID),
	}
}

func (s *Service) UpsertPartialParentalLeave(ctx context.Context, inp PartialParentalLeave) (PartialParentalLeave, error) {
	dating, err := s.entryDating(ctx, inp.EventID, inp.OffsetEventID)
	if err != nil {
		return PartialParentalLeave{}, err
	}
	if inp.StartDate, inp.EndDate, err = leaveDates(dating, inp.StartDate, inp.EndDate, inp.StartOffset, inp.EndOffset); err != nil {
		return PartialParentalLeave{}, err
	}
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
		StartDate:              dating.store(inp.StartDate),
		EndDate:                dating.store(inp.EndDate),
		SjukDaysPerYear:        inp.SjukDaysPerYear,
		LagstaDaysPerYear:      inp.LagstaDaysPerYear,
		SkippedWorkDaysPerYear: inp.SkippedWorkDaysPerYear,
		EventID:                ui.WithDefaultNull(inp.EventID),
		CreatedAt:              now,
		UpdatedAt:              now,
	})
	if err != nil {
		return PartialParentalLeave{}, fmt.Errorf("upserting partial parental leave: %w", err)
	}
	return partialParentalLeaveFromDB(a, dating.at), nil
}

// getPartialParentalLeave returns the partial parental leave id, in an Event at the Event's date.
func (s *Service) getPartialParentalLeave(ctx context.Context, id string) (PartialParentalLeave, error) {
	a, err := s.q.GetPartialParentalLeave(ctx, id)
	if err != nil {
		return PartialParentalLeave{}, fmt.Errorf("getting partial parental leave: %w", err)
	}
	at, err := s.eventDate(ctx, ui.OrDefault(a.EventID))
	if err != nil {
		return PartialParentalLeave{}, err
	}
	return partialParentalLeaveFromDB(a, at), nil
}

// ListPartialParentalLeaves returns the Salary's partial parental leaves by Start Date, each in an
// Event at the Event's date.
func (s *Service) ListPartialParentalLeaves(ctx context.Context, salaryID string) ([]PartialParentalLeave, error) {
	rows, err := s.q.ListPartialParentalLeaves(ctx, salaryID)
	if err != nil {
		return nil, fmt.Errorf("listing partial parental leaves: %w", err)
	}
	dates, err := s.eventDates(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]PartialParentalLeave, len(rows))
	for i, r := range rows {
		result[i] = partialParentalLeaveFromDB(r, ownDate(dates, r.EventID))
	}
	sortByDayAndID(result, PartialParentalLeave.dayAndID)
	return result, nil
}

func (s *Service) DeletePartialParentalLeave(ctx context.Context, id string) error {
	if err := s.q.DeletePartialParentalLeave(ctx, id); err != nil {
		return fmt.Errorf("deleting partial parental leave: %w", err)
	}
	return nil
}

// fullParentalLeaveFromDB is the stored full parental leave a with its Event at at.
func fullParentalLeaveFromDB(a pdb.FullParentalLeave, at date.Date) FullParentalLeave {
	return FullParentalLeave{
		ID:              a.ID,
		SalaryID:        a.SalaryID,
		StartDate:       placeDate(a.StartDate, at),
		EndDate:         placeDate(a.EndDate, at),
		SjukDaysPerWeek: a.SjukDaysPerWeek,
		EventID:         ui.OrDefault(a.EventID),
	}
}

func (s *Service) UpsertFullParentalLeave(ctx context.Context, inp FullParentalLeave) (FullParentalLeave, error) {
	dating, err := s.entryDating(ctx, inp.EventID, inp.OffsetEventID)
	if err != nil {
		return FullParentalLeave{}, err
	}
	if inp.StartDate, inp.EndDate, err = leaveDates(dating, inp.StartDate, inp.EndDate, inp.StartOffset, inp.EndOffset); err != nil {
		return FullParentalLeave{}, err
	}
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
		StartDate:       dating.store(inp.StartDate),
		EndDate:         dating.store(inp.EndDate),
		SjukDaysPerWeek: inp.SjukDaysPerWeek,
		EventID:         ui.WithDefaultNull(inp.EventID),
		CreatedAt:       now,
		UpdatedAt:       now,
	})
	if err != nil {
		return FullParentalLeave{}, fmt.Errorf("upserting full parental leave: %w", err)
	}
	return fullParentalLeaveFromDB(a, dating.at), nil
}

// getFullParentalLeave returns the full parental leave id, in an Event at the Event's date.
func (s *Service) getFullParentalLeave(ctx context.Context, id string) (FullParentalLeave, error) {
	a, err := s.q.GetFullParentalLeave(ctx, id)
	if err != nil {
		return FullParentalLeave{}, fmt.Errorf("getting full parental leave: %w", err)
	}
	at, err := s.eventDate(ctx, ui.OrDefault(a.EventID))
	if err != nil {
		return FullParentalLeave{}, err
	}
	return fullParentalLeaveFromDB(a, at), nil
}

// ListFullParentalLeaves returns the Salary's full parental leaves by Start Date, each in an Event at
// the Event's date.
func (s *Service) ListFullParentalLeaves(ctx context.Context, salaryID string) ([]FullParentalLeave, error) {
	rows, err := s.q.ListFullParentalLeaves(ctx, salaryID)
	if err != nil {
		return nil, fmt.Errorf("listing full parental leaves: %w", err)
	}
	dates, err := s.eventDates(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]FullParentalLeave, len(rows))
	for i, r := range rows {
		result[i] = fullParentalLeaveFromDB(r, ownDate(dates, r.EventID))
	}
	sortByDayAndID(result, FullParentalLeave.dayAndID)
	return result, nil
}

func (s *Service) DeleteFullParentalLeave(ctx context.Context, id string) error {
	if err := s.q.DeleteFullParentalLeave(ctx, id); err != nil {
		return fmt.Errorf("deleting full parental leave: %w", err)
	}
	return nil
}

func (s *Service) UpsertSalaryEnd(ctx context.Context, inp SalaryEnd) (SalaryEnd, error) {
	dating, err := s.entryDating(ctx, inp.EventID, inp.OffsetEventID)
	if err != nil {
		return SalaryEnd{}, err
	}
	if inp.Date, err = dating.firstDay("end", inp.Date, inp.DateOffset); err != nil {
		return SalaryEnd{}, err
	}
	if err := errors.Join(checkRequired("salary", inp.SalaryID), checkRequired("end date", inp.Date)); err != nil {
		return SalaryEnd{}, err
	}
	if inp.ID == "" {
		inp.ID = sid.MustNewString(32)
	}
	now := time.Now().Unix()
	e, err := s.q.UpsertSalaryEnd(ctx, pdb.UpsertSalaryEndParams{
		ID:        inp.ID,
		SalaryID:  inp.SalaryID,
		Date:      dating.store(inp.Date),
		EventID:   ui.WithDefaultNull(inp.EventID),
		CreatedAt: now,
		UpdatedAt: now,
	})
	if err != nil {
		return SalaryEnd{}, fmt.Errorf("upserting salary end: %w", err)
	}
	return salaryEndFromDB(e, dating.at), nil
}

// GetSalaryEnd returns the Salary's end id, in an Event at the Event's date.
func (s *Service) GetSalaryEnd(ctx context.Context, id string) (SalaryEnd, error) {
	e, err := s.q.GetSalaryEnd(ctx, id)
	if err != nil {
		return SalaryEnd{}, fmt.Errorf("getting salary end: %w", err)
	}
	at, err := s.eventDate(ctx, ui.OrDefault(e.EventID))
	if err != nil {
		return SalaryEnd{}, err
	}
	return salaryEndFromDB(e, at), nil
}

func (s *Service) DeleteSalaryEnd(ctx context.Context, id string) error {
	if err := s.q.DeleteSalaryEnd(ctx, id); err != nil {
		return fmt.Errorf("deleting salary end: %w", err)
	}
	return nil
}

// sortSalaryParts orders each of the Salary's parts by date and ID. Of two parts on the same day, the
// one with the later ID applies.
func sortSalaryParts(sal *Salary) {
	sortByDayAndID(sal.Amounts, SalaryAmount.dayAndID)
	sortByDayAndID(sal.Adjustments, SalaryAdjustment.dayAndID)
	sortByDayAndID(sal.PartialParentalLeaves, PartialParentalLeave.dayAndID)
	sortByDayAndID(sal.FullParentalLeaves, FullParentalLeave.dayAndID)
	sortByDayAndID(sal.Ends, SalaryEnd.dayAndID)
}

// The first day and ID of each part of a Salary, which order it among the others.
func (a SalaryAmount) dayAndID() (date.Date, string)         { return a.StartDate, a.ID }
func (a SalaryAdjustment) dayAndID() (date.Date, string)     { return a.ValidFrom, a.ID }
func (l PartialParentalLeave) dayAndID() (date.Date, string) { return l.StartDate, l.ID }
func (l FullParentalLeave) dayAndID() (date.Date, string)    { return l.StartDate, l.ID }
func (e SalaryEnd) dayAndID() (date.Date, string)            { return e.Date, e.ID }

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

// CurrentAmount is the Salary Amount the Salary is paid by today: 0 before its first or after its End.
func (s Salary) CurrentAmount() float64 {
	paid := s.payment()
	today := date.Today()
	if paid.hasEndedBy(today) {
		return 0
	}
	var current *SalaryAmount
	for i := range paid.Amounts {
		if paid.Amounts[i].StartDate <= today {
			if current == nil || paid.Amounts[i].StartDate >= current.StartDate {
				current = &paid.Amounts[i]
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
	Events     []Event
}

func (v SalaryEditView) IsEdit() bool {
	return v.Salary.ID != ""
}

// PartEventID is the Event a new part of the Salary is in, unless its form picks another: the
// Salary's.
func (v SalaryEditView) PartEventID() string {
	return v.Salary.EventID
}

// SalariesView is the Salaries page: every Salary, each with what the Baseline pays it, and the Events
// they and their parts are in.
type SalariesView struct {
	Salaries []Salary
	Events   map[string]Event
}

// GetEvent is the Event id, or nil for "" or an Event that doesn't exist.
func (v *SalariesView) GetEvent(id string) *Event {
	return eventByID(v.Events, id)
}

func (s *Service) GetSalariesPageData(ctx context.Context) (*SalariesView, error) {
	loaded, err := s.loadSalaries(ctx, "")
	if err != nil {
		return nil, err
	}
	return &SalariesView{Salaries: loaded.salaries, Events: KeyBy(loaded.events, func(e Event) string { return e.ID })}, nil
}

// GetSalaryNewPageData is the page of a new Salary in the Event eventID, or in the Baseline for "".
func (s *Service) GetSalaryNewPageData(ctx context.Context, eventID string) (*SalaryEditView, error) {
	accs, err := s.ListAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing accounts: %w", err)
	}
	categories, err := s.ListCategories(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing categories: %w", err)
	}
	events, err := s.ListEvents(ctx)
	if err != nil {
		return nil, err
	}
	return &SalaryEditView{
		Salary:     Salary{EventID: eventID},
		Accounts:   accs,
		Categories: categories,
		Events:     events,
	}, nil
}

func (s *Service) GetSalaryEditPageData(ctx context.Context, id string) (*SalaryEditView, error) {
	loaded, err := s.loadSalaries(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("getting salary: %w", err)
	}
	categories, err := s.ListCategories(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing categories: %w", err)
	}
	return &SalaryEditView{
		Salary:     loaded.salaries[0],
		Accounts:   loaded.accounts,
		Categories: categories,
		Events:     loaded.events,
	}, nil
}

// salaryTransferTemplates are what salaries, with the Events placed, pay: each Gross Salary segmented.
func (s *Service) salaryTransferTemplates(ctx context.Context, salaries []Salary) ([]TransferTemplate, error) {
	salaries = slices.Clone(salaries)
	if err := s.segmentSalaries(ctx, salaries); err != nil {
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

	sorted := slices.Clone(sal.Amounts)
	slices.SortStableFunc(sorted, func(a, b SalaryAmount) int { return cmp.Compare(a.StartDate, b.StartDate) })

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
		} else {
			seg.EndDate = sal.End()
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
		if d >= first && !s.hasEndedBy(d) {
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
