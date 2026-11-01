package model

import (
	"context"
	"errors"
	"fmt"
	"slices"
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

	StartDate date.Date
	// EndDate is the first day the Transfer Template no longer applies, or nil. A stored Transfer
	// Template's is the earliest of the Ends it is loaded with: saving it doesn't read it, as its Ends
	// are saved on their own. See ADR 0009.
	EndDate          *date.Date
	Enabled          bool
	BudgetCategoryID *string
	// EventID is the Event the Transfer Template is in, or "" in the Baseline. In an Event its Start
	// Date, and the date of a one-off, are stored as offsets from the Event's date.
	EventID string
	// Ends are the days a stored Transfer Template may end on, each in an Event or in none: it applies
	// up to the earliest of them. Where it is placed, only the placed Ends are loaded.
	Ends []TransferTemplateEnd
	// StartOffset, when set, gives the Start Date of a Transfer Template in an Event as days after the
	// Event's date, in place of StartDate. Only saving reads it, so a loaded Transfer Template has none.
	StartOffset *date.Duration
	// OffsetEventID is the Event StartOffset is days after: the Event the form showed it for, which an
	// entry changing its Event leaves. It is EventID when blank.
	OffsetEventID string

	Source TransferTemplateSource
}

// TransferTemplateEnd is a day a Transfer Template may end on: from it, the Transfer Template no
// longer applies. In an Event its date is stored as days after the Event's date, whatever Event the
// Transfer Template itself is in.
type TransferTemplateEnd struct {
	ID                 string
	TransferTemplateID string
	Date               date.Date
	EventID            string
	// DateOffset, when set, gives the date in an Event as days after the date of OffsetEventID, the
	// Event the form showed it for, or of EventID when that is blank.
	DateOffset    *date.Duration
	OffsetEventID string
}

// transferTemplateEndFromDB is the stored end e with its Event at at.
func transferTemplateEndFromDB(e pdb.TransferTemplateEnd, at date.Date) TransferTemplateEnd {
	return TransferTemplateEnd{ID: e.ID, TransferTemplateID: e.TransferTemplateID, Date: placeDate(e.Date, at), EventID: ui.OrDefault(e.EventID)}
}

func (e TransferTemplateEnd) dayAndID() (date.Date, string) { return e.Date, e.ID }

// withEnds is t with ends, ordered by date and ID, as its Ends, ending at the earliest of them.
func (t TransferTemplate) withEnds(ends []TransferTemplateEnd) TransferTemplate {
	t.Ends = slices.Clone(ends)
	sortByDayAndID(t.Ends, TransferTemplateEnd.dayAndID)
	t.EndDate = firstEnd(t.Ends, func(e TransferTemplateEnd) date.Date { return e.Date })
	return t
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
		Enabled:          t.Enabled,
		BudgetCategoryID: t.BudgetCategoryID,
		EventID:          ui.OrDefault(t.EventID),
	}, nil
}

// UpsertTransferTemplate saves the Transfer Template, but not its Ends, which are saved on their own. In
// an Event its Start Date may be given as days after the Event's date, is the Event's date when it isn't
// given, and is saved as an offset from the Event's date. Without a recurrence it is a one-off on its
// Start Date.
func (s *Service) UpsertTransferTemplate(ctx context.Context, inp TransferTemplate) (TransferTemplate, error) {
	at, err := s.eventDate(ctx, inp.EventID)
	if err != nil {
		return TransferTemplate{}, err
	}
	offsetEventID, offsetAt, err := s.offsetEvent(ctx, inp.OffsetEventID, inp.EventID, at)
	if err != nil {
		return TransferTemplate{}, err
	}
	if inp.StartDate, err = givenDate("start", inp.StartDate, inp.StartOffset, offsetEventID, offsetAt); err != nil {
		return TransferTemplate{}, err
	}
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
	amountFixed, err := orZero(inp.AmountFixed).Encode()
	if err != nil {
		return TransferTemplate{}, fmt.Errorf("encoding amount fixed: %w", err)
	}
	previous, err := storedEventOf(ctx, inp.ID, s.q.GetTransferTemplate, func(t pdb.TransferTemplate) *string { return t.EventID })
	if err != nil {
		return TransferTemplate{}, fmt.Errorf("getting transfer template: %w", err)
	}
	if inp.ID == "" {
		inp.ID = sid.MustNewString(32)
	}
	// A Transfer Template changing Event brings its Ends along, or stays where it was.
	err = s.inTx(ctx, func(tx *Service) error {
		if err := tx.storeTransferTemplate(ctx, inp, at, amountFixed); err != nil {
			return err
		}
		if previous != nil && *previous != inp.EventID {
			return tx.bringParts(ctx, EventEntryTransferTemplate, inp.ID, *previous, inp.EventID)
		}
		return nil
	})
	if err != nil {
		return TransferTemplate{}, err
	}
	return s.GetTransferTemplate(ctx, inp.ID)
}

// storeTransferTemplate writes inp, in an Event at at, with its fixed amount encoded as amountFixed.
func (s *Service) storeTransferTemplate(ctx context.Context, inp TransferTemplate, at date.Date, amountFixed string) error {
	_, err := s.q.UpsertTransferTemplate(ctx, pdb.UpsertTransferTemplateParams{
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
		Enabled:          inp.Enabled,
		BudgetCategoryID: inp.BudgetCategoryID,
		EventID:          ui.WithDefaultNull(inp.EventID),
		CreatedAt:        time.Now().Unix(),
		UpdatedAt:        time.Now().Unix(),
	})
	if err != nil {
		return fmt.Errorf("failed to upsert template: %w", err)
	}
	return nil
}

// DuplicateTransferTemplate saves a copy of the Transfer Template id with a copy of each of its Ends, each
// in the same Event, so the copy ends wherever the original does.
func (s *Service) DuplicateTransferTemplate(ctx context.Context, id string) (TransferTemplate, error) {
	t, err := s.GetTransferTemplate(ctx, id)
	if err != nil {
		return TransferTemplate{}, fmt.Errorf("failed to get transfer template: %w", err)
	}
	var copied TransferTemplate
	err = s.inTx(ctx, func(tx *Service) error {
		t.ID = ""
		if copied, err = tx.UpsertTransferTemplate(ctx, t); err != nil {
			return err
		}
		for _, end := range t.Ends {
			end.ID, end.TransferTemplateID = "", copied.ID
			if _, err := tx.UpsertTransferTemplateEnd(ctx, end); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return TransferTemplate{}, err
	}
	return s.GetTransferTemplate(ctx, copied.ID)
}

// ListTransferTemplates returns every stored Transfer Template with every End of it, each in an Event at
// its own Event's date.
func (s *Service) ListTransferTemplates(ctx context.Context) ([]TransferTemplate, error) {
	rows, err := s.q.GetTransferTemplates(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list templates: %w", err)
	}
	ends, err := s.q.ListAllTransferTemplateEnds(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing transfer template ends: %w", err)
	}
	return s.atOwnDates(ctx, rows, ends)
}

// atOwnDates are the stored Transfer Templates rows, each with its stored ends, every one at its own
// Event's date: the dates their forms edit. Each ends at the earliest of them.
func (s *Service) atOwnDates(ctx context.Context, rows []pdb.TransferTemplate, endRows []pdb.TransferTemplateEnd) ([]TransferTemplate, error) {
	dates, err := s.eventDates(ctx)
	if err != nil {
		return nil, err
	}
	ends := make(map[string][]TransferTemplateEnd)
	for _, row := range endRows {
		ends[row.TransferTemplateID] = append(ends[row.TransferTemplateID], transferTemplateEndFromDB(row, ownDate(dates, row.EventID)))
	}
	templates := make([]TransferTemplate, len(rows))
	for i, row := range rows {
		tt, err := transferTemplateFromDB(row, ownDate(dates, row.EventID))
		if err != nil {
			return nil, fmt.Errorf("converting transfer template from DB: %w", err)
		}
		templates[i] = tt.withEnds(ends[tt.ID])
	}
	return templates, nil
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

// GetTransferTemplate returns the Transfer Template with every End of it, each in an Event at its own
// Event's date: the dates its forms edit. Its End Date is the earliest of them.
func (s *Service) GetTransferTemplate(ctx context.Context, id string) (TransferTemplate, error) {
	t, err := s.q.GetTransferTemplate(ctx, id)
	if err != nil {
		return TransferTemplate{}, fmt.Errorf("failed to get transfer template: %w", err)
	}
	ends, err := s.q.ListTransferTemplateEnds(ctx, id)
	if err != nil {
		return TransferTemplate{}, fmt.Errorf("listing transfer template ends: %w", err)
	}
	templates, err := s.atOwnDates(ctx, []pdb.TransferTemplate{t}, ends)
	if err != nil {
		return TransferTemplate{}, err
	}
	return templates[0], nil
}

func (s *Service) DeleteTransferTemplate(ctx context.Context, id string) error {
	if err := s.q.DeleteTransferTemplate(ctx, id); err != nil {
		return fmt.Errorf("failed to delete template: %w", err)
	}
	return nil
}

// UpsertTransferTemplateEnd saves a day the Transfer Template may end on, in an Event or in none. In an
// Event its date may be given as days after the Event's date, is the Event's date when it isn't given,
// and is saved as an offset from the Event's date, whatever Event the Transfer Template is in.
func (s *Service) UpsertTransferTemplateEnd(ctx context.Context, inp TransferTemplateEnd) (TransferTemplateEnd, error) {
	dating, err := s.entryDating(ctx, inp.EventID, inp.OffsetEventID)
	if err != nil {
		return TransferTemplateEnd{}, err
	}
	if inp.Date, err = dating.firstDay("end", inp.Date, inp.DateOffset); err != nil {
		return TransferTemplateEnd{}, err
	}
	if err := errors.Join(checkRequired("transfer template", inp.TransferTemplateID), checkRequired("end date", inp.Date)); err != nil {
		return TransferTemplateEnd{}, err
	}
	if inp.ID == "" {
		inp.ID = sid.MustNewString(32)
	}
	now := time.Now().Unix()
	e, err := s.q.UpsertTransferTemplateEnd(ctx, pdb.UpsertTransferTemplateEndParams{
		ID:                 inp.ID,
		TransferTemplateID: inp.TransferTemplateID,
		Date:               dating.store(inp.Date),
		EventID:            ui.WithDefaultNull(inp.EventID),
		CreatedAt:          now,
		UpdatedAt:          now,
	})
	if err != nil {
		return TransferTemplateEnd{}, fmt.Errorf("upserting transfer template end: %w", err)
	}
	return transferTemplateEndFromDB(e, dating.at), nil
}

// GetTransferTemplateEnd returns the Transfer Template's end id, in an Event at the Event's date.
func (s *Service) GetTransferTemplateEnd(ctx context.Context, id string) (TransferTemplateEnd, error) {
	e, err := s.q.GetTransferTemplateEnd(ctx, id)
	if err != nil {
		return TransferTemplateEnd{}, fmt.Errorf("getting transfer template end: %w", err)
	}
	at, err := s.eventDate(ctx, ui.OrDefault(e.EventID))
	if err != nil {
		return TransferTemplateEnd{}, err
	}
	return transferTemplateEndFromDB(e, at), nil
}

func (s *Service) DeleteTransferTemplateEnd(ctx context.Context, id string) error {
	if err := s.q.DeleteTransferTemplateEnd(ctx, id); err != nil {
		return fmt.Errorf("deleting transfer template end: %w", err)
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

// PartEventID is the Event a new End of the Transfer Template is in, unless its form picks another: the
// Transfer Template's.
func (c TransferTemplateEditView) PartEventID() string {
	return c.TransferTemplate.EventID
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
	LeftOut []TransferTemplate
	// Unavailable are the Salaries and Bills of unplanned Events whose payments can't be worked out.
	Unavailable     []UnavailablePayment
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
	return eventByID(v.Events, id)
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

// GetTransferTemplatesPageData is the Baseline's Transfer Templates, and apart from them, what the
// Events leave out: their stored Transfer Templates and those using their accounts, and what their
// Salaries and Bills would pay and charge, or why that can't be worked out.
func (s *Service) GetTransferTemplatesPageData(ctx context.Context) (*TransferTemplatesView, error) {
	pl, err := s.loadPlacings(ctx)
	if err != nil {
		return nil, err
	}
	baseline, err := pl.inBaseline()
	if err != nil {
		return nil, fmt.Errorf("placing the events: %w", err)
	}
	schedule, err := s.transferSchedule(ctx, baseline)
	if err != nil {
		return nil, fmt.Errorf("loading transfer schedule: %w", err)
	}
	unplanned, unavailable, err := s.unplannedPayments(ctx, pl)
	if err != nil {
		return nil, err
	}
	categories, err := s.ListCategories(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing categories: %w", err)
	}
	v := newTransferTemplatesView(schedule, date.Today(), accountsListFromDB(pl.stored.accounts), categories, pl.events)
	v.LeftOut = slices.Concat(v.LeftOut, unplanned)
	sortTransferTemplates(v.LeftOut)
	v.Unavailable = unavailable
	return v, nil
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
