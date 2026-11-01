package model

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/goslu/sid"
	"github.com/SimonSchneider/pefigo/internal/pdb"
	"github.com/SimonSchneider/pefigo/pkg/finance"
	"github.com/SimonSchneider/pefigo/pkg/ui"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

// The types of what a generated Transfer Template is generated from (TransferTemplateSource.Type).
const (
	TransferSourceSalary = "salary"
	TransferSourceBill   = "bill"
)

// TransferTemplateSource is what a Transfer Template is generated from: a Salary or a Bill, or nothing
// for a stored Transfer Template.
type TransferTemplateSource struct {
	Type     string // "" for a stored Transfer Template, TransferSourceSalary or TransferSourceBill
	EntityID string // the ID of the Salary or Bill
	Label    string
	// BillAccountID is the Bill Account of a Bill source, which lists the Bill, and BillAccountName
	// its name.
	BillAccountID   string
	BillAccountName string

	// recurrenceRow names the stored row a generated Transfer Template's recurrence is from, such as
	// its Bill account, for an error about it.
	recurrenceRow string
}

func (s TransferTemplateSource) IsGenerated() bool {
	return s.Type != ""
}

// recurrenceRow names the stored row t's recurrence is from: t itself, or the Salary or Bill account
// it is generated from.
func (t TransferTemplate) recurrenceRow() string {
	if t.Source.recurrenceRow != "" {
		return t.Source.recurrenceRow
	}
	return "transfer template " + t.ID
}

// The Transfer Template amount types (TransferTemplate.AmountType).
const (
	TransferAmountFixed   = "fixed"
	TransferAmountPercent = "percent"
)

type TransferTemplate struct {
	ID            string
	Name          string
	FromAccountID string
	ToAccountID   string
	AmountType    string
	AmountFixed   uncertain.Value
	AmountPercent float64
	Priority      int64
	Recurrence    date.Cron

	StartDate        date.Date
	EndDate          *date.Date
	Enabled          bool
	BudgetCategoryID *string
	// EventID is the Event the Transfer Template is in, or "" in the Baseline. In an Event its Start and
	// End Dates, and the date of a one-off, are stored as offsets from the Event's date.
	EventID string
	// StartOffset and EndOffset, when set, give the Start and End Dates of a Transfer Template in an
	// Event as days after the Event's date, in place of StartDate and EndDate. Only saving reads them,
	// so a loaded Transfer Template has none.
	StartOffset *date.Duration
	EndOffset   *date.Duration
	// OffsetEventID is the Event StartOffset and EndOffset are days after: the Event the form showed
	// them for, which an entry changing its Event leaves. It is EventID when blank.
	OffsetEventID string

	Source TransferTemplateSource
}

func (t *TransferTemplate) ToFinanceTransferTemplate() finance.TransferTemplate {
	return finance.TransferTemplate{
		ID:            t.ID,
		Name:          t.Name,
		FromAccountID: t.FromAccountID,
		ToAccountID:   t.ToAccountID,
		AmountType:    finance.TransferAmountType(t.AmountType),
		AmountFixed: finance.TransferFixed{
			Amount: t.AmountFixed,
		},
		AmountPercent: finance.TransferPercent{
			Percent: t.AmountPercent,
		},
		Priority:      t.Priority,
		EffectiveFrom: t.StartDate,
		EffectiveTo:   t.EndDate,
		Recurrence:    t.Recurrence,
		Enabled:       t.Enabled,
	}
}

// transferTemplateFromDB is the stored Transfer Template t with its Event at at: its stored dates are
// offsets from at.
func transferTemplateFromDB(t pdb.TransferTemplate, at date.Date) (TransferTemplate, error) {
	var amountFixed uncertain.Value
	if err := amountFixed.Decode(t.AmountFixed); err != nil {
		return TransferTemplate{}, fmt.Errorf("decoding amount fixed: %w", err)
	}
	return TransferTemplate{
		ID:               t.ID,
		Name:             t.Name,
		FromAccountID:    ui.OrDefault(t.FromAccountID),
		ToAccountID:      ui.OrDefault(t.ToAccountID),
		AmountType:       t.AmountType,
		AmountFixed:      amountFixed,
		AmountPercent:    t.AmountPercent,
		Priority:         t.Priority,
		Recurrence:       placeRecurrence(t.Recurrence, t.EventID != nil, at),
		StartDate:        placeDate(t.StartDate, at),
		EndDate:          placeEndDate(t.EndDate, at),
		Enabled:          t.Enabled,
		BudgetCategoryID: t.BudgetCategoryID,
		EventID:          ui.OrDefault(t.EventID),
	}, nil
}

type TransferTemplateInput struct {
	ID               string
	Name             string
	FromAccountID    string
	ToAccountID      string
	AmountType       string
	AmountFixed      uncertain.Value
	AmountPercent    float64
	Priority         int64
	Recurrence       date.Cron
	StartDate        date.Date
	EndDate          *date.Date
	Enabled          bool
	BudgetCategoryID *string
}

// UpsertTransferTemplate saves the Transfer Template. In an Event its dates may be given as days after
// the Event's date, its Start Date is the Event's date when it isn't given, and its dates are saved as
// offsets from the Event's date. Without a recurrence it is a one-off on its Start Date.
func (s *Service) UpsertTransferTemplate(ctx context.Context, inp TransferTemplate) (TransferTemplate, error) {
	at, err := s.eventDate(ctx, inp.EventID)
	if err != nil {
		return TransferTemplate{}, err
	}
	offsetEventID, offsetAt, err := s.offsetEvent(ctx, inp.OffsetEventID, inp.EventID, at)
	if err != nil {
		return TransferTemplate{}, err
	}
	startDate, startErr := givenDate("start", inp.StartDate, inp.StartOffset, offsetEventID, offsetAt)
	endDate, endErr := givenEndDate(inp.EndDate, inp.EndOffset, offsetEventID, offsetAt)
	if err := errors.Join(startErr, endErr); err != nil {
		return TransferTemplate{}, err
	}
	inp.StartDate, inp.EndDate = startDate, endDate
	if inp.EventID != "" && inp.StartDate.IsZero() {
		inp.StartDate = at
	}
	if err := errors.Join(checkRequired("name", inp.Name), checkRequired("start date", inp.StartDate)); err != nil {
		return TransferTemplate{}, err
	}
	if inp.Recurrence == "" {
		inp.Recurrence = date.Cron(inp.StartDate.String())
	}
	if err := checkOneOf("transfer template amount type", inp.AmountType, TransferAmountFixed, TransferAmountPercent); err != nil {
		return TransferTemplate{}, err
	}
	if err := checkDatePattern("transfer template recurrence", inp.Recurrence); err != nil {
		return TransferTemplate{}, err
	}
	if err := checkEndDate(inp.StartDate, inp.EndDate); err != nil {
		return TransferTemplate{}, err
	}
	amountFixed, err := orZero(inp.AmountFixed).Encode()
	if err != nil {
		return TransferTemplate{}, fmt.Errorf("encoding amount fixed: %w", err)
	}
	if inp.ID == "" {
		inp.ID = sid.MustNewString(32)
	}
	t, err := s.q.UpsertTransferTemplate(ctx, pdb.UpsertTransferTemplateParams{
		ID:               inp.ID,
		Name:             inp.Name,
		FromAccountID:    ui.WithDefaultNull(inp.FromAccountID),
		ToAccountID:      ui.WithDefaultNull(inp.ToAccountID),
		AmountType:       inp.AmountType,
		AmountFixed:      amountFixed,
		AmountPercent:    inp.AmountPercent,
		Priority:         inp.Priority,
		Recurrence:       storeRecurrence(inp.Recurrence, inp.EventID != "", at),
		StartDate:        storeDate(inp.StartDate, at),
		EndDate:          storeEndDate(inp.EndDate, at),
		Enabled:          inp.Enabled,
		BudgetCategoryID: inp.BudgetCategoryID,
		EventID:          ui.WithDefaultNull(inp.EventID),
		CreatedAt:        time.Now().Unix(),
		UpdatedAt:        time.Now().Unix(),
	})
	if err != nil {
		return TransferTemplate{}, fmt.Errorf("failed to upsert template: %w", err)
	}
	return transferTemplateFromDB(t, at)
}

func (s *Service) DuplicateTransferTemplate(ctx context.Context, id string) (TransferTemplate, error) {
	t, err := s.GetTransferTemplate(ctx, id)
	if err != nil {
		return TransferTemplate{}, fmt.Errorf("failed to get transfer template: %w", err)
	}
	t.ID = ""
	return s.UpsertTransferTemplate(ctx, t)
}

// ListTransferTemplates returns every stored Transfer Template, each in an Event at the Event's date.
func (s *Service) ListTransferTemplates(ctx context.Context) ([]TransferTemplate, error) {
	templates, err := s.q.GetTransferTemplates(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list templates: %w", err)
	}
	dates, err := s.eventDates(ctx)
	if err != nil {
		return nil, err
	}
	var parsedTemplates []TransferTemplate
	for _, t := range templates {
		template, err := transferTemplateFromDB(t, ownDate(dates, t.EventID))
		if err != nil {
			return nil, fmt.Errorf("converting transfer template from DB: %w", err)
		}
		parsedTemplates = append(parsedTemplates, template)
	}
	return parsedTemplates, nil
}

func sortTransferTemplates(ts []TransferTemplate) {
	sort.SliceStable(ts, func(i, j int) bool {
		ri, rj := string(ts[i].Recurrence), string(ts[j].Recurrence)
		if ri != rj {
			return ri < rj
		}
		if ts[i].Priority != ts[j].Priority {
			return ts[i].Priority < ts[j].Priority
		}
		if ts[i].Name != ts[j].Name {
			return ts[i].Name < ts[j].Name
		}
		if ts[i].StartDate != ts[j].StartDate {
			return ts[i].StartDate < ts[j].StartDate
		}
		return false
	})
}

// GetTransferTemplate returns the Transfer Template, in an Event at the Event's date.
func (s *Service) GetTransferTemplate(ctx context.Context, id string) (TransferTemplate, error) {
	t, err := s.q.GetTransferTemplate(ctx, id)
	if err != nil {
		return TransferTemplate{}, fmt.Errorf("failed to get transfer template: %w", err)
	}
	at, err := s.eventDate(ctx, ui.OrDefault(t.EventID))
	if err != nil {
		return TransferTemplate{}, err
	}
	return transferTemplateFromDB(t, at)
}

func (s *Service) DeleteTransferTemplate(ctx context.Context, id string) error {
	if err := s.q.DeleteTransferTemplate(ctx, id); err != nil {
		return fmt.Errorf("failed to delete template: %w", err)
	}
	return nil
}

type TransferTemplateEditView struct {
	TransferTemplate TransferTemplate
	Accounts         []Account
	Categories       []TransferTemplateCategory
	Events           []Event
}

func (c TransferTemplateEditView) IsEdit() bool {
	return c.TransferTemplate.ID != ""
}

// Event is the Event the Transfer Template is in, or nil in the Baseline.
func (c TransferTemplateEditView) Event() *Event {
	return findEvent(c.Events, c.TransferTemplate.EventID)
}

// StartInput is the Start Date as the form gives it: in an Event the days after its date. A new
// Transfer Template has none.
func (c TransferTemplateEditView) StartInput() string {
	if !c.IsEdit() {
		return ""
	}
	return DateInput(c.Events, c.TransferTemplate.EventID, &c.TransferTemplate.StartDate)
}

// EndInput is the End Date as the form gives it: in an Event the days after its date.
func (c TransferTemplateEditView) EndInput() string {
	return DateInput(c.Events, c.TransferTemplate.EventID, c.TransferTemplate.EndDate)
}

// RecurrenceInput is the recurrence as the form gives it: blank for a one-off on its Start Date, which
// saving without a recurrence makes, so the one-off follows the Start.
func (c TransferTemplateEditView) RecurrenceInput() string {
	t := c.TransferTemplate
	if d, ok := fixedDate(t.Recurrence); ok && d == t.StartDate {
		return ""
	}
	return string(t.Recurrence)
}

type TransferTemplatesView struct {
	Rows []TransferScheduleRow
	// LeftOut are the Transfer Templates the Events leave out of the Baseline, at their Events' own
	// dates. They count in nothing.
	LeftOut         []TransferTemplate
	Accounts        map[string]Account
	Categories      map[string]TransferTemplateCategory
	Events          map[string]Event
	MonthlyIncome   float64
	MonthlyExpenses float64
}

func newTransferTemplatesView(schedule *TransferSchedule, day date.Date, accounts []Account, categories []TransferTemplateCategory, events []Event) *TransferTemplatesView {
	on := schedule.On(day)
	v := &TransferTemplatesView{
		Rows:       on.Rows(),
		LeftOut:    schedule.LeftOut(),
		Accounts:   KeyBy(accounts, func(a Account) string { return a.ID }),
		Categories: KeyBy(categories, func(c TransferTemplateCategory) string { return c.ID }),
		Events:     KeyBy(events, func(e Event) string { return e.ID }),
	}
	for _, t := range on.Monthly() {
		switch t.Kind() {
		case TransferIncome:
			v.MonthlyIncome += t.Amount
		case TransferExpense:
			v.MonthlyExpenses -= t.Amount
		}
	}
	return v
}

// GetEvent is the Event id, or nil for "" or an Event that doesn't exist.
func (v *TransferTemplatesView) GetEvent(id string) *Event {
	ev, ok := v.Events[id]
	if !ok {
		return nil
	}
	return &ev
}

// TemplateCount is the number of Transfer Templates the table lists: a Bill, or the Transfer Templates
// of one row, count once.
func (v *TransferTemplatesView) TemplateCount() int {
	n := 0
	for _, row := range v.Rows {
		if row.isBillAccount() {
			n += len(row.Members)
		} else {
			n++
		}
	}
	return n
}

func (v *TransferTemplatesView) GetBudgetCategory(id *string) *TransferTemplateCategory {
	if id == nil {
		return nil
	}
	cat, ok := v.Categories[*id]
	if !ok {
		return nil
	}
	return &cat
}

func (v *TransferTemplatesView) GetAccount(id string) *Account {
	a := v.Accounts[id]
	return &a
}

func (s *Service) GetTransferTemplatesPageData(ctx context.Context) (*TransferTemplatesView, error) {
	schedule, err := s.TransferSchedule(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading transfer schedule: %w", err)
	}
	accounts, err := s.ListAccounts(ctx)
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
	return newTransferTemplatesView(schedule, date.Today(), accounts, categories, events), nil
}

// GetTransferTemplateNewPageData is the page of a new Transfer Template in the Event eventID, or in the
// Baseline for "".
func (s *Service) GetTransferTemplateNewPageData(ctx context.Context, eventID string) (*TransferTemplateEditView, error) {
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
	return &TransferTemplateEditView{
		TransferTemplate: TransferTemplate{EventID: eventID},
		Accounts:         accs,
		Categories:       categories,
		Events:           events,
	}, nil
}

func (s *Service) GetTransferTemplateEditPageData(ctx context.Context, id string) (*TransferTemplateEditView, error) {
	t, err := s.GetTransferTemplate(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("getting transfer template: %w", err)
	}
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
	return &TransferTemplateEditView{
		Accounts:         accs,
		TransferTemplate: t,
		Categories:       categories,
		Events:           events,
	}, nil
}
