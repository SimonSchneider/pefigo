package model

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/goslu/sid"
	"github.com/SimonSchneider/pefigo/internal/pdb"
	"github.com/SimonSchneider/pefigo/pkg/ui"
)

// Event is a named group of related changes to the plans for the future that happen around one date:
// the Accounts, growth models, Transfer Templates, Salaries and Bills it adds, and the dated parts it
// adds to Salaries, Bills and Startup Share accounts, in or outside it. Every date of its changes is an
// offset from its date, so moving it moves them all. It is planned when the Baseline holds it at its
// date, and unplanned when only Scenarios use it.
type Event struct {
	ID      string
	Name    string
	Date    date.Date
	Planned bool
}

type EventInput struct {
	ID   string
	Name string
	Date date.Date
	// Planned is whether the Baseline holds the Event at its date. Without it, the Event is planned.
	Planned *bool
}

func eventFromDB(e pdb.Event) Event {
	return Event{ID: e.ID, Name: e.Name, Date: date.Date(e.Date), Planned: e.Planned}
}

func (s *Service) UpsertEvent(ctx context.Context, inp EventInput) (Event, error) {
	if err := errors.Join(checkRequired("name", inp.Name), checkRequired("date", inp.Date)); err != nil {
		return Event{}, err
	}
	if inp.ID == "" {
		inp.ID = sid.MustNewString(15)
	}
	planned := inp.Planned == nil || *inp.Planned
	now := time.Now().UnixMilli()
	ev, err := s.q.UpsertEvent(ctx, pdb.UpsertEventParams{
		ID:        inp.ID,
		Name:      inp.Name,
		Date:      int64(inp.Date),
		Planned:   planned,
		CreatedAt: now,
		UpdatedAt: now,
	})
	if err != nil {
		return Event{}, fmt.Errorf("saving event: %w", err)
	}
	return eventFromDB(ev), nil
}

func (s *Service) GetEvent(ctx context.Context, id string) (Event, error) {
	ev, err := s.q.GetEvent(ctx, id)
	if err != nil {
		return Event{}, fmt.Errorf("getting event: %w", err)
	}
	return eventFromDB(ev), nil
}

// ListEvents returns every Event by date and name.
func (s *Service) ListEvents(ctx context.Context) ([]Event, error) {
	rows, err := s.q.ListEvents(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing events: %w", err)
	}
	events := make([]Event, len(rows))
	for i, row := range rows {
		events[i] = eventFromDB(row)
	}
	return events, nil
}

// DeleteEvent deletes the Event with the entries it adds, and everything deleted with its Accounts.
func (s *Service) DeleteEvent(ctx context.Context, id string) error {
	if err := s.q.DeleteEvent(ctx, id); err != nil {
		return fmt.Errorf("deleting event: %w", err)
	}
	return nil
}

// EventEntryKind is a type of entry an Event adds.
type EventEntryKind string

const (
	EventEntryAccount              EventEntryKind = "account"
	EventEntryGrowthModel          EventEntryKind = "growth-model"
	EventEntryTransferTemplate     EventEntryKind = "transfer-template"
	EventEntrySalary               EventEntryKind = "salary"
	EventEntrySalaryAmount         EventEntryKind = "salary-amount"
	EventEntrySalaryAdjustment     EventEntryKind = "salary-adjustment"
	EventEntryPartialParentalLeave EventEntryKind = "partial-parental-leave"
	EventEntryFullParentalLeave    EventEntryKind = "full-parental-leave"
	EventEntrySalaryEnd            EventEntryKind = "salary-end"
	EventEntryBill                 EventEntryKind = "bill"
	EventEntryBillAmount           EventEntryKind = "bill-amount"
	EventEntryBillEnd              EventEntryKind = "bill-end"
	EventEntryInvestmentRound      EventEntryKind = "investment-round"
	EventEntryShareChange          EventEntryKind = "share-change"
)

// eventEntryKinds are every kind of entry an Event can add.
var eventEntryKinds = []string{
	string(EventEntryAccount), string(EventEntryGrowthModel), string(EventEntryTransferTemplate),
	string(EventEntrySalary), string(EventEntrySalaryAmount), string(EventEntrySalaryAdjustment),
	string(EventEntryPartialParentalLeave), string(EventEntryFullParentalLeave), string(EventEntrySalaryEnd),
	string(EventEntryBill), string(EventEntryBillAmount), string(EventEntryBillEnd),
	string(EventEntryInvestmentRound), string(EventEntryShareChange),
}

// EventEntry names an entry an Event can add or end.
type EventEntry struct {
	Kind EventEntryKind
	ID   string
}

// String is the entry as ParseEventEntry reads it: its kind and ID, such as "account:abc".
func (e EventEntry) String() string {
	return string(e.Kind) + ":" + e.ID
}

// ParseEventEntry reads an EventEntry from its String.
func ParseEventEntry(s string) (EventEntry, error) {
	kind, id, ok := strings.Cut(s, ":")
	entry := EventEntry{Kind: EventEntryKind(kind), ID: id}
	if !ok || id == "" {
		return EventEntry{}, fmt.Errorf("%w: %q is not an entry", ErrInvalidInput, s)
	}
	if err := checkOneOf("entry kind", kind, eventEntryKinds...); err != nil {
		return EventEntry{}, err
	}
	return entry, nil
}

// An entry's stored dates are the days after its Event's date, or after day 0 without an Event, so
// placing its Event at a date adds that date to them and moving the Event moves them.

// placeDate is the date stored days after at.
func placeDate(stored int64, at date.Date) date.Date {
	return at.Add(date.Duration(stored))
}

// placeEndDate is the End Date stored days after at, or none.
func placeEndDate(stored *int64, at date.Date) *date.Date {
	if stored == nil {
		return nil
	}
	return ptr(placeDate(*stored, at))
}

// storeDate is d as the days after at.
func storeDate(d date.Date, at date.Date) int64 {
	return int64(d.Sub(at))
}

func storeEndDate(d *date.Date, at date.Date) *int64 {
	if d == nil {
		return nil
	}
	return ptr(storeDate(*d, at))
}

// givenDate is the date an entry's input gives: offset days after at, the date of the Event eventID,
// when offset is set, and d otherwise. Days after an Event's date need an Event; what names the date
// in that error.
func givenDate(what string, d date.Date, offset *date.Duration, eventID string, at date.Date) (date.Date, error) {
	if offset == nil {
		return d, nil
	}
	if eventID == "" {
		return 0, fmt.Errorf("%w: a %s of %s days after an Event's date needs an Event", ErrInvalidInput, what, ui.FormatDays(*offset))
	}
	return at.Add(*offset), nil
}

// offsetEvent is the Event an input's offsets are days after, with its date: offsetEventID, or eventID,
// whose date is at, when offsetEventID is blank. A form gives the offsets of the Event it showed them
// for, so an entry changing its Event in the form keeps the dates it applies on.
func (s *Service) offsetEvent(ctx context.Context, offsetEventID, eventID string, at date.Date) (string, date.Date, error) {
	if offsetEventID == "" || offsetEventID == eventID {
		return eventID, at, nil
	}
	d, err := s.eventDate(ctx, offsetEventID)
	return offsetEventID, d, err
}

// entryDating is how an entry input in an Event gives its dates and how they are stored: offsets in its
// form are days after the date of the Event its form showed them for, and its dates are stored as days
// after its own Event's date.
type entryDating struct {
	eventID       string
	at            date.Date // the date its dates are stored after: its Event's, or day 0
	offsetEventID string
	offsetAt      date.Date
}

// entryDating is how an input in the Event eventID gives and stores its dates, with offsets given
// after offsetEventID, or eventID when blank. An Event that doesn't exist is invalid input.
func (s *Service) entryDating(ctx context.Context, eventID, offsetEventID string) (entryDating, error) {
	at, err := s.eventDate(ctx, eventID)
	if err != nil {
		return entryDating{}, err
	}
	offsetEventID, offsetAt, err := s.offsetEvent(ctx, offsetEventID, eventID, at)
	if err != nil {
		return entryDating{}, err
	}
	return entryDating{eventID: eventID, at: at, offsetEventID: offsetEventID, offsetAt: offsetAt}, nil
}

// date is the date d an input gives, or offset days after the date its form showed them for. what
// names the date in an error.
func (d entryDating) date(what string, given date.Date, offset *date.Duration) (date.Date, error) {
	return givenDate(what, given, offset, d.offsetEventID, d.offsetAt)
}

// firstDay is the date an input gives for its first day: the first day a Salary Amount, adjustment,
// leave, Bill Amount, round or share change applies, or the first day an end stops its Salary or Bill.
// In an Event it is the Event's date when none is given.
func (d entryDating) firstDay(what string, given date.Date, offset *date.Duration) (date.Date, error) {
	day, err := d.date(what, given, offset)
	if err == nil && d.eventID != "" && day.IsZero() {
		day = d.at
	}
	return day, err
}

// endDate is the End Date an input gives, which may be none.
func (d entryDating) endDate(given *date.Date, offset *date.Duration) (*date.Date, error) {
	return givenEndDate(given, offset, d.offsetEventID, d.offsetAt)
}

// store is day as stored: days after the entry's Event's date.
func (d entryDating) store(day date.Date) int64 {
	return storeDate(day, d.at)
}

// givenEndDate is givenDate for an End Date, which may be none.
func givenEndDate(d *date.Date, offset *date.Duration, eventID string, at date.Date) (*date.Date, error) {
	if offset == nil {
		return d, nil
	}
	end, err := givenDate("end", 0, offset, eventID, at)
	if err != nil {
		return nil, err
	}
	return &end, nil
}

// DateInput is the date d of an entry in the Event eventID as its form gives it: in an Event the days
// after the Event's date, such as +20, and otherwise the date. No date is blank.
func DateInput(events []Event, eventID string, d *date.Date) string {
	if d == nil {
		return ""
	}
	if ev := findEvent(events, eventID); ev != nil {
		return ui.FormatDays(d.Sub(ev.Date))
	}
	return d.String()
}

// eventByID is the Event id of a page's Events, or nil for "" or an Event that doesn't exist: the
// GetEvent of each page that shows entries' Events.
func eventByID(events map[string]Event, id string) *Event {
	if ev, ok := events[id]; ok {
		return &ev
	}
	return nil
}

// findEvent is the Event eventID of events, or nil.
func findEvent(events []Event, eventID string) *Event {
	for i := range events {
		if events[i].ID == eventID {
			return &events[i]
		}
	}
	return nil
}

// fixedDate is the one day recurrence matches when it is a date, such as 2028-03-01: the recurrence
// of a one-off Transfer Template.
func fixedDate(recurrence date.Cron) (date.Date, bool) {
	parts := strings.Split(string(recurrence), "-")
	if len(parts) != 3 {
		return 0, false
	}
	var n [3]int
	for i, part := range parts {
		v, ok := unsignedInt(part)
		if !ok {
			return 0, false
		}
		n[i] = v
	}
	t := time.Date(n[0], time.Month(n[1]), n[2], 0, 0, 0, 0, time.UTC)
	if t.Year() != n[0] || int(t.Month()) != n[1] || t.Day() != n[2] {
		return 0, false
	}
	return date.FromTime(t), true
}

// storeRecurrence is the recurrence of a Transfer Template or Salary placed at at, as stored. The date
// of a one-off in an Event is one of the Event's dates, so it is stored as the date that many days
// after day 0. A recurrence that repeats isn't a date of the Event and is stored as it is.
func storeRecurrence(recurrence date.Cron, inEvent bool, at date.Date) string {
	if d, ok := fixedDate(recurrence); ok && inEvent {
		return date.Date(storeDate(d, at)).String()
	}
	return string(recurrence)
}

// placeRecurrence is the stored recurrence of a Transfer Template or Salary placed at at.
func placeRecurrence(stored string, inEvent bool, at date.Date) date.Cron {
	if d, ok := fixedDate(date.Cron(stored)); ok && inEvent {
		return date.Cron(placeDate(int64(d), at).String())
	}
	return date.Cron(stored)
}

// eventDate is the date the entries of the Event eventID are stored relative to: its date, or day 0
// without an Event. An Event that doesn't exist is invalid input.
func (s *Service) eventDate(ctx context.Context, eventID string) (date.Date, error) {
	if eventID == "" {
		return 0, nil
	}
	ev, err := s.q.GetEvent(ctx, eventID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("%w: event %q doesn't exist", ErrInvalidInput, eventID)
	}
	if err != nil {
		return 0, fmt.Errorf("getting event: %w", err)
	}
	return date.Date(ev.Date), nil
}

// eventDates are the dates of every Event, by ID.
func (s *Service) eventDates(ctx context.Context) (map[string]date.Date, error) {
	events, err := s.ListEvents(ctx)
	if err != nil {
		return nil, err
	}
	dates := make(map[string]date.Date, len(events))
	for _, ev := range events {
		dates[ev.ID] = ev.Date
	}
	return dates, nil
}

// ownDate is the date an entry of eventID is at in the Baseline's stored form: its Event's own date,
// or day 0 without an Event.
func ownDate(dates map[string]date.Date, eventID *string) date.Date {
	if eventID == nil {
		return 0
	}
	return dates[*eventID]
}

// RemoveFromEvent puts entry in the Baseline, out of its Event. Its dates become the absolute dates
// they are at with the Event at its current date. An end leaves its Event only from its Salary's or
// Bill's page: removing one from the Event's page is invalid input.
func (s *Service) RemoveFromEvent(ctx context.Context, entry EventEntry) error {
	if !entry.Kind.RemovableFromEvent() {
		return fmt.Errorf("%w: a %s is edited on its own page, not removed from its Event", ErrInvalidInput, strings.ToLower(entry.Kind.Label()))
	}
	return s.setEntryEvent(ctx, entry, "")
}

// RemovableFromEvent reports whether the Event's page removes an entry of the kind from it: every
// kind but an end, which is edited on its Salary's or Bill's page.
func (k EventEntryKind) RemovableFromEvent() bool {
	return k != EventEntrySalaryEnd && k != EventEntryBillEnd
}

func (s *Service) setEntryEvent(ctx context.Context, entry EventEntry, eventID string) error {
	switch entry.Kind {
	case EventEntryAccount:
		if _, err := s.GetAccount(ctx, entry.ID); err != nil {
			return err
		}
		if err := s.q.SetAccountEvent(ctx, pdb.SetAccountEventParams{EventID: ui.WithDefaultNull(eventID), ID: entry.ID}); err != nil {
			return fmt.Errorf("moving account: %w", err)
		}
	case EventEntryGrowthModel:
		gm, err := s.GetGrowthModel(ctx, entry.ID)
		if err != nil {
			return err
		}
		gm.EventID = eventID
		if _, err := s.UpsertAccountGrowthModel(ctx, gm.input()); err != nil {
			return err
		}
	case EventEntryTransferTemplate:
		tt, err := s.GetTransferTemplate(ctx, entry.ID)
		if err != nil {
			return err
		}
		tt.EventID = eventID
		if _, err := s.UpsertTransferTemplate(ctx, tt); err != nil {
			return err
		}
	case EventEntrySalary:
		return rejoin(ctx, entry.ID, eventID, s.GetSalary, s.UpsertSalary, func(v *Salary, id string) { v.EventID = id })
	case EventEntrySalaryAmount:
		return rejoin(ctx, entry.ID, eventID, s.getSalaryAmount, s.UpsertSalaryAmount, func(v *SalaryAmount, id string) { v.EventID = id })
	case EventEntrySalaryAdjustment:
		return rejoin(ctx, entry.ID, eventID, s.getSalaryAdjustment, s.UpsertSalaryAdjustment, func(v *SalaryAdjustment, id string) { v.EventID = id })
	case EventEntryPartialParentalLeave:
		return rejoin(ctx, entry.ID, eventID, s.getPartialParentalLeave, s.UpsertPartialParentalLeave, func(v *PartialParentalLeave, id string) { v.EventID = id })
	case EventEntryFullParentalLeave:
		return rejoin(ctx, entry.ID, eventID, s.getFullParentalLeave, s.UpsertFullParentalLeave, func(v *FullParentalLeave, id string) { v.EventID = id })
	case EventEntrySalaryEnd:
		return rejoin(ctx, entry.ID, eventID, s.GetSalaryEnd, s.UpsertSalaryEnd, func(v *SalaryEnd, id string) { v.EventID = id })
	case EventEntryBill:
		return rejoin(ctx, entry.ID, eventID, s.GetBill, s.UpsertBill, func(v *Bill, id string) { v.EventID = id })
	case EventEntryBillAmount:
		return rejoin(ctx, entry.ID, eventID, s.getBillAmount, s.UpsertBillAmount, func(v *BillAmount, id string) { v.EventID = id })
	case EventEntryBillEnd:
		return rejoin(ctx, entry.ID, eventID, s.GetBillEnd, s.UpsertBillEnd, func(v *BillEnd, id string) { v.EventID = id })
	case EventEntryInvestmentRound:
		get := func(ctx context.Context, id string) (InvestmentRoundInput, error) {
			ir, err := s.GetInvestmentRound(ctx, id)
			return ir.input(), err
		}
		return rejoin(ctx, entry.ID, eventID, get, s.UpsertInvestmentRound, func(v *InvestmentRoundInput, id string) { v.EventID = id })
	case EventEntryShareChange:
		get := func(ctx context.Context, id string) (ShareChangeInput, error) {
			sc, err := s.GetShareChange(ctx, id)
			return sc.input(), err
		}
		return rejoin(ctx, entry.ID, eventID, get, s.UpsertShareChange, func(v *ShareChangeInput, id string) { v.EventID = id })
	default:
		return fmt.Errorf("%w: %s can't be in an event", ErrInvalidInput, entry)
	}
	return nil
}

// rejoin saves the entry id, as get reads it at its dates, in the Event eventID that setEvent puts it
// in. Its dates are kept: save stores them again, as days after the new Event's date.
func rejoin[T, R any](ctx context.Context, id, eventID string, get func(context.Context, string) (T, error), save func(context.Context, T) (R, error), setEvent func(*T, string)) error {
	v, err := get(ctx, id)
	if err != nil {
		return err
	}
	setEvent(&v, eventID)
	_, err = save(ctx, v)
	return err
}

// storedEntries are every stored entry an Event can add, as stored: the dates of an Event's entries
// are offsets from its date.
type storedEntries struct {
	accounts              []pdb.Account
	growthModels          []pdb.GrowthModel
	transferTemplates     []pdb.TransferTemplate
	salaries              []pdb.Salary
	salaryAmounts         []pdb.SalaryAmount
	salaryAdjustments     []pdb.SalaryAdjustment
	partialParentalLeaves []pdb.PartialParentalLeave
	fullParentalLeaves    []pdb.FullParentalLeave
	salaryEnds            []pdb.SalaryEnd
	bills                 []pdb.Bill
	billAmounts           []pdb.BillAmount
	billEnds              []pdb.BillEnd
	startupShareAccounts  []pdb.StartupShareAccount
	investmentRounds      []pdb.InvestmentRound
	shareChanges          []pdb.ShareChange
	startupShareOptions   []pdb.StartupShareOption
}

func (s *Service) loadStoredEntries(ctx context.Context) (storedEntries, error) {
	var st storedEntries
	for _, load := range []func() error{
		func() (err error) { st.accounts, err = s.q.ListAccounts(ctx); return wrap(err, "listing accounts") },
		func() (err error) {
			st.growthModels, err = s.q.ListGrowthModels(ctx)
			return wrap(err, "listing growth models")
		},
		func() (err error) {
			st.transferTemplates, err = s.q.GetTransferTemplates(ctx)
			return wrap(err, "listing transfer templates")
		},
		func() (err error) { st.salaries, err = s.q.ListSalaries(ctx); return wrap(err, "listing salaries") },
		func() (err error) {
			st.salaryAmounts, err = s.q.ListAllSalaryAmounts(ctx)
			return wrap(err, "listing salary amounts")
		},
		func() (err error) {
			st.salaryAdjustments, err = s.q.ListAllSalaryAdjustments(ctx)
			return wrap(err, "listing salary adjustments")
		},
		func() (err error) {
			st.partialParentalLeaves, err = s.q.ListAllPartialParentalLeaves(ctx)
			return wrap(err, "listing partial parental leaves")
		},
		func() (err error) {
			st.fullParentalLeaves, err = s.q.ListAllFullParentalLeaves(ctx)
			return wrap(err, "listing full parental leaves")
		},
		func() (err error) {
			st.salaryEnds, err = s.q.ListAllSalaryEnds(ctx)
			return wrap(err, "listing salary ends")
		},
		func() (err error) { st.bills, err = s.q.ListAllBills(ctx); return wrap(err, "listing bills") },
		func() (err error) {
			st.billAmounts, err = s.q.ListAllBillAmounts(ctx)
			return wrap(err, "listing bill amounts")
		},
		func() (err error) { st.billEnds, err = s.q.ListAllBillEnds(ctx); return wrap(err, "listing bill ends") },
		func() (err error) {
			st.startupShareAccounts, err = s.q.ListStartupShareAccounts(ctx)
			return wrap(err, "listing startup share accounts")
		},
		func() (err error) {
			st.investmentRounds, err = s.q.ListAllInvestmentRounds(ctx)
			return wrap(err, "listing investment rounds")
		},
		func() (err error) {
			st.shareChanges, err = s.q.ListAllShareChanges(ctx)
			return wrap(err, "listing share changes")
		},
		func() (err error) {
			st.startupShareOptions, err = s.q.ListAllStartupShareOptions(ctx)
			return wrap(err, "listing startup share options")
		},
	} {
		if err := load(); err != nil {
			return st, err
		}
	}
	return st, nil
}

// wrap is err, if any, with what was being done.
func wrap(err error, doing string) error {
	if err != nil {
		return fmt.Errorf("%s: %w", doing, err)
	}
	return nil
}

// EventGrowthModel is a growth model of an Event, at the Event's date.
type EventGrowthModel struct {
	GrowthModel
	AccountName string
	eventDate   date.Date
}

// StartOffset is how long after the Event's date the growth model starts.
func (g EventGrowthModel) StartOffset() date.Duration {
	return g.StartDate.Sub(g.eventDate)
}

// EndOffset is how long after the Event's date the growth model ends, or nil without an End Date.
func (g EventGrowthModel) EndOffset() *date.Duration {
	if g.EndDate == nil {
		return nil
	}
	return ptr(g.EndDate.Sub(g.eventDate))
}

// EventTransferTemplate is a Transfer Template of an Event, at the Event's date.
type EventTransferTemplate struct {
	TransferTemplate
	eventDate date.Date
}

// StartOffset is how long after the Event's date the Transfer Template starts.
func (t EventTransferTemplate) StartOffset() date.Duration {
	return t.StartDate.Sub(t.eventDate)
}

// EndOffset is how long after the Event's date the Transfer Template ends, or nil without an End Date.
func (t EventTransferTemplate) EndOffset() *date.Duration {
	if t.EndDate == nil {
		return nil
	}
	return ptr(t.EndDate.Sub(t.eventDate))
}

// NamedEventEntry is an entry with a name to show it by, and the Event it is in, if any.
type NamedEventEntry struct {
	EventEntry
	Name      string
	EventName string
}

// EventView is an Event at its date with the entries it adds.
type EventView struct {
	Event
	Accounts          []Account
	GrowthModels      []EventGrowthModel
	TransferTemplates []EventTransferTemplate
	// Parts are the Salaries and Bills it adds, and what it adds to Salaries, Bills and Startup Share
	// accounts, in or outside it: by date, a Salary or Bill first, then by kind.
	Parts []EventPart
}

// EventPart is an entry an Event adds that makes up a Salary, Bill or Startup Share account: a Salary
// or Bill, a Salary Amount, adjustment, parental leave or end of a Salary, a Bill Amount or end of a
// Bill, or an investment round or share change.
type EventPart struct {
	EventEntry
	// OwnerID and OwnerName are the Salary, Bill or account the part makes up, or the Salary or Bill
	// itself.
	OwnerID   string
	OwnerName string
	// StartDate is the part's first day, and EndDate its End of a parental leave or Bill Amount. A
	// Salary or Bill has neither.
	StartDate *date.Date
	EndDate   *date.Date
	eventDate date.Date
}

// StartOffset is how long after the Event's date the part starts.
func (p EventPart) StartOffset() date.Duration {
	if p.StartDate == nil {
		return 0
	}
	return p.StartDate.Sub(p.eventDate)
}

// EndOffset is how long after the Event's date the part ends, or 0 without an End Date.
func (p EventPart) EndOffset() date.Duration {
	if p.EndDate == nil {
		return 0
	}
	return p.EndDate.Sub(p.eventDate)
}

// newEventPart is the part id of the kind kind that makes up the Salary, Bill or account ownerID, in
// the Event eventID, from start to end.
func newEventPart(eventID string, kind EventEntryKind, id, ownerID, ownerName string, start, end *date.Date) eventPartIn {
	return eventPartIn{eventID: eventID, part: EventPart{
		EventEntry: EventEntry{Kind: kind, ID: id}, OwnerID: ownerID, OwnerName: ownerName, StartDate: start, EndDate: end,
	}}
}

// salaryParts are the Salary and every part of it, each with the Event it is in.
func salaryParts(sal Salary) []eventPartIn {
	parts := []eventPartIn{newEventPart(sal.EventID, EventEntrySalary, sal.ID, sal.ID, sal.Name, nil, nil)}
	for _, a := range sal.Amounts {
		parts = append(parts, newEventPart(a.EventID, EventEntrySalaryAmount, a.ID, sal.ID, sal.Name, &a.StartDate, nil))
	}
	for _, a := range sal.Adjustments {
		parts = append(parts, newEventPart(a.EventID, EventEntrySalaryAdjustment, a.ID, sal.ID, sal.Name, &a.ValidFrom, nil))
	}
	for _, l := range sal.PartialParentalLeaves {
		parts = append(parts, newEventPart(l.EventID, EventEntryPartialParentalLeave, l.ID, sal.ID, sal.Name, &l.StartDate, &l.EndDate))
	}
	for _, l := range sal.FullParentalLeaves {
		parts = append(parts, newEventPart(l.EventID, EventEntryFullParentalLeave, l.ID, sal.ID, sal.Name, &l.StartDate, &l.EndDate))
	}
	for _, e := range sal.Ends {
		parts = append(parts, newEventPart(e.EventID, EventEntrySalaryEnd, e.ID, sal.ID, sal.Name, &e.Date, nil))
	}
	return parts
}

// billParts are the Bill and every Bill Amount and end of it, each with the Event it is in.
func billParts(b Bill) []eventPartIn {
	parts := []eventPartIn{newEventPart(b.EventID, EventEntryBill, b.ID, b.ID, b.Name, nil, nil)}
	for _, a := range b.Amounts {
		parts = append(parts, newEventPart(a.EventID, EventEntryBillAmount, a.ID, b.ID, b.Name, &a.StartDate, a.EndDate))
	}
	for _, e := range b.Ends {
		parts = append(parts, newEventPart(e.EventID, EventEntryBillEnd, e.ID, b.ID, b.Name, &e.Date, nil))
	}
	return parts
}

// startupShareParts are every stored investment round and share change, each at its own Event's date
// with the Event it is in. They are listed on whatever account holds them, a Startup Share account or
// one that no longer is: deleting their Event deletes them either way.
func startupShareParts(stored storedEntries, dates map[string]date.Date, accountNames map[string]string) []eventPartIn {
	var parts []eventPartIn
	for _, row := range stored.investmentRounds {
		r := investmentRoundFromDB(row, ownDate(dates, row.EventID))
		parts = append(parts, newEventPart(r.EventID, EventEntryInvestmentRound, r.ID, r.AccountID, accountNames[r.AccountID], &r.Date, nil))
	}
	for _, row := range stored.shareChanges {
		c := shareChangeFromDB(row, ownDate(dates, row.EventID))
		parts = append(parts, newEventPart(c.EventID, EventEntryShareChange, c.ID, c.AccountID, accountNames[c.AccountID], &c.Date, nil))
	}
	return parts
}

// ownPlans are the plans with every Event at its own date, planned or not: every entry at the dates it
// is stored after.
func (s *Service) ownPlans(ctx context.Context) (*plans, error) {
	events, err := s.ListEvents(ctx)
	if err != nil {
		return nil, err
	}
	stored, err := s.loadStoredEntries(ctx)
	if err != nil {
		return nil, err
	}
	return placeEntries(events, stored, asStored(events))
}

// storedEventOf is the Event the stored row id is in, "" for none, or nil when there is no row id yet:
// the entry is new.
func storedEventOf[R any](ctx context.Context, id string, get func(context.Context, string) (R, error), eventID func(R) *string) (*string, error) {
	if id == "" {
		return nil, nil
	}
	row, err := get(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return ptr(ui.OrDefault(eventID(row))), nil
}

// bringParts puts the parts of the Salary or Bill ownerID that are in from, the Event it was in or ""
// for none, in to, the Event it is in now, keeping the days they apply on: a Salary or Bill joining,
// leaving or changing Event brings its parts along. Its parts in other Events, which they add to it,
// stay where they are.
func (s *Service) bringParts(ctx context.Context, kind EventEntryKind, ownerID, from, to string) error {
	own, err := s.ownPlans(ctx)
	if err != nil {
		return err
	}
	var parts []eventPartIn
	switch kind {
	case EventEntrySalary:
		if i := slices.IndexFunc(own.salaries, func(sal Salary) bool { return sal.ID == ownerID }); i >= 0 {
			parts = salaryParts(own.salaries[i])
		}
	case EventEntryBill:
		if i := slices.IndexFunc(own.bills, func(b Bill) bool { return b.ID == ownerID }); i >= 0 {
			parts = billParts(own.bills[i])
		}
	}
	for _, p := range parts {
		if p.eventID == from && p.part.ID != ownerID {
			if err := s.setEntryEvent(ctx, p.part.EventEntry, to); err != nil {
				return err
			}
		}
	}
	return nil
}

// Label names the kind of entry, such as "Full parental leave".
func (k EventEntryKind) Label() string {
	switch k {
	case EventEntryAccount:
		return "Account"
	case EventEntryGrowthModel:
		return "Growth model"
	case EventEntryTransferTemplate:
		return "Transfer Template"
	case EventEntrySalary:
		return "Salary"
	case EventEntrySalaryAmount:
		return "Salary Amount"
	case EventEntrySalaryAdjustment:
		return "Salary adjustment"
	case EventEntryPartialParentalLeave:
		return "Partial parental leave"
	case EventEntryFullParentalLeave:
		return "Full parental leave"
	case EventEntrySalaryEnd:
		return "Salary end"
	case EventEntryBill:
		return "Bill"
	case EventEntryBillAmount:
		return "Bill Amount"
	case EventEntryBillEnd:
		return "Bill end"
	case EventEntryInvestmentRound:
		return "Investment round"
	case EventEntryShareChange:
		return "Share change"
	}
	return string(k)
}

// Name names the part by its kind, owner and first day, such as "Full parental leave of Salary A from
// 2027-06-01".
func (p EventPart) Name() string {
	if p.StartDate == nil {
		return p.OwnerName
	}
	return fmt.Sprintf("%s of %s from %s", p.Kind.Label(), p.OwnerName, p.StartDate)
}

// eventPartIn is a part with the Event it is in, or "".
type eventPartIn struct {
	eventID string
	part    EventPart
}

// growthModelName names a growth model of the account accountName.
func growthModelName(gm GrowthModel, accountName string) string {
	return fmt.Sprintf("%s growth from %s", accountName, gm.StartDate)
}

// eventEntries are every stored entry at its Event's own date, with the Events.
type eventEntries struct {
	events            []Event
	accounts          []Account
	growthModels      []GrowthModel
	transferTemplates []TransferTemplate
	parts             []eventPartIn
	stored            storedEntries
}

func (s *Service) loadEventEntries(ctx context.Context) (*eventEntries, error) {
	events, err := s.ListEvents(ctx)
	if err != nil {
		return nil, err
	}
	stored, err := s.loadStoredEntries(ctx)
	if err != nil {
		return nil, err
	}
	dates := make(map[string]date.Date, len(events))
	for _, ev := range events {
		dates[ev.ID] = ev.Date
	}
	e := &eventEntries{events: events, stored: stored, accounts: accountsListFromDB(stored.accounts)}
	for _, row := range stored.growthModels {
		gm, err := growthModelFromDB(row, ownDate(dates, row.EventID))
		if err != nil {
			return nil, err
		}
		e.growthModels = append(e.growthModels, gm)
	}
	for _, row := range stored.transferTemplates {
		tt, err := transferTemplateFromDB(row, ownDate(dates, row.EventID))
		if err != nil {
			return nil, err
		}
		e.transferTemplates = append(e.transferTemplates, tt)
	}
	own, err := placeEntries(events, stored, asStored(events))
	if err != nil {
		return nil, err
	}
	for _, sal := range own.salaries {
		e.parts = append(e.parts, salaryParts(sal)...)
	}
	for _, b := range own.bills {
		e.parts = append(e.parts, billParts(b)...)
	}
	e.parts = append(e.parts, startupShareParts(stored, dates, e.accountNames())...)
	return e, nil
}

func (e *eventEntries) accountNames() map[string]string {
	names := make(map[string]string, len(e.accounts))
	for _, acc := range e.accounts {
		names[acc.ID] = acc.Name
	}
	return names
}

func (e *eventEntries) eventNames() map[string]string {
	names := make(map[string]string, len(e.events))
	for _, ev := range e.events {
		names[ev.ID] = ev.Name
	}
	return names
}

// view is ev with its entries.
func (e *eventEntries) view(ev Event) EventView {
	accountNames := e.accountNames()
	v := EventView{Event: ev}
	for _, acc := range e.accounts {
		if acc.EventID == ev.ID {
			v.Accounts = append(v.Accounts, acc)
		}
	}
	for _, gm := range e.growthModels {
		if gm.EventID == ev.ID {
			v.GrowthModels = append(v.GrowthModels, EventGrowthModel{GrowthModel: gm, AccountName: accountNames[gm.AccountID], eventDate: ev.Date})
		}
	}
	for _, tt := range e.transferTemplates {
		if tt.EventID == ev.ID {
			v.TransferTemplates = append(v.TransferTemplates, EventTransferTemplate{TransferTemplate: tt, eventDate: ev.Date})
		}
	}
	for _, p := range e.parts {
		if p.eventID == ev.ID {
			part := p.part
			part.eventDate = ev.Date
			v.Parts = append(v.Parts, part)
		}
	}
	slices.SortStableFunc(v.Parts, func(a, b EventPart) int {
		return cmp.Or(
			cmp.Compare(ui.OrDefault(a.StartDate), ui.OrDefault(b.StartDate)),
			cmp.Compare(slices.Index(eventEntryKinds, string(a.Kind)), slices.Index(eventEntryKinds, string(b.Kind))),
			cmp.Compare(a.OwnerName, b.OwnerName),
		)
	})
	slices.SortStableFunc(v.GrowthModels, func(a, b EventGrowthModel) int {
		return cmp.Or(cmp.Compare(a.StartDate, b.StartDate), cmp.Compare(a.AccountName, b.AccountName))
	})
	slices.SortStableFunc(v.TransferTemplates, func(a, b EventTransferTemplate) int {
		return cmp.Or(cmp.Compare(a.StartDate, b.StartDate), cmp.Compare(a.Name, b.Name))
	})
	return v
}

func (e *eventEntries) event(id string) (Event, bool) {
	i := slices.IndexFunc(e.events, func(ev Event) bool { return ev.ID == id })
	if i < 0 {
		return Event{}, false
	}
	return e.events[i], true
}

// EventsView is the Events page: every Event by date with its entries.
type EventsView struct {
	Events []EventView
}

func (s *Service) GetEventsPageData(ctx context.Context) (*EventsView, error) {
	e, err := s.loadEventEntries(ctx)
	if err != nil {
		return nil, err
	}
	v := &EventsView{Events: make([]EventView, len(e.events))}
	for i, ev := range e.events {
		v.Events[i] = e.view(ev)
	}
	return v, nil
}

// EventEditView is an Event's edit page: the Event with its entries. An entry joins an Event from its
// own form.
type EventEditView struct {
	Event EventView
}

func (v *EventEditView) IsEdit() bool {
	return v.Event.ID != ""
}

func (s *Service) GetEventNewPageData(context.Context) (*EventEditView, error) {
	return &EventEditView{Event: EventView{Event: Event{Planned: true}}}, nil
}

func (s *Service) GetEventEditPageData(ctx context.Context, id string) (*EventEditView, error) {
	e, err := s.loadEventEntries(ctx)
	if err != nil {
		return nil, err
	}
	ev, ok := e.event(id)
	if !ok {
		return nil, fmt.Errorf("getting event: %w", sql.ErrNoRows)
	}
	return &EventEditView{Event: e.view(ev)}, nil
}

// EventDeleteView confirms deleting an Event: it lists the entries the Event adds, and the entries
// and Options outside it deleted with its Accounts.
type EventDeleteView struct {
	Event       EventView
	AlsoDeleted []NamedEventEntry
	// AlsoDeletedOptions are the Options on Startup Share accounts outside the Event that one of its
	// Accounts funds: they are deleted with it.
	AlsoDeletedOptions []DeletedOption
}

// DeletedOption is an Option deleted with the Account that funds it.
type DeletedOption struct {
	AccountName       string // the Startup Share account the Option is on
	SourceAccountName string // the Account that funds it
	Shares            float64
}

func (s *Service) GetEventDeletePageData(ctx context.Context, id string) (*EventDeleteView, error) {
	e, err := s.loadEventEntries(ctx)
	if err != nil {
		return nil, err
	}
	ev, ok := e.event(id)
	if !ok {
		return nil, fmt.Errorf("getting event: %w", sql.ErrNoRows)
	}
	v := &EventDeleteView{Event: e.view(ev)}
	accounts := make(map[string]bool, len(v.Event.Accounts))
	for _, acc := range v.Event.Accounts {
		accounts[acc.ID] = true
	}
	eventNames := e.eventNames()
	accountNames := e.accountNames()
	for _, gm := range e.growthModels {
		if gm.EventID != ev.ID && accounts[gm.AccountID] {
			v.AlsoDeleted = append(v.AlsoDeleted, NamedEventEntry{
				EventEntry: EventEntry{Kind: EventEntryGrowthModel, ID: gm.ID}, Name: growthModelName(gm, accountNames[gm.AccountID]), EventName: eventNames[gm.EventID],
			})
		}
	}
	for _, tt := range e.transferTemplates {
		if tt.EventID != ev.ID && (accounts[tt.FromAccountID] || accounts[tt.ToAccountID]) {
			v.AlsoDeleted = append(v.AlsoDeleted, NamedEventEntry{
				EventEntry: EventEntry{Kind: EventEntryTransferTemplate, ID: tt.ID}, Name: tt.Name, EventName: eventNames[tt.EventID],
			})
		}
	}
	// The parts in other Events, or none, of the Salaries, Bills and accounts it adds are deleted with
	// them.
	owners := maps.Clone(accounts)
	for _, p := range v.Event.Parts {
		if p.Kind == EventEntrySalary || p.Kind == EventEntryBill {
			owners[p.OwnerID] = true
		}
	}
	for _, p := range e.parts {
		if p.eventID != ev.ID && owners[p.part.OwnerID] && p.part.Kind != EventEntrySalary && p.part.Kind != EventEntryBill {
			v.AlsoDeleted = append(v.AlsoDeleted, NamedEventEntry{EventEntry: p.part.EventEntry, Name: p.part.Name(), EventName: eventNames[p.eventID]})
		}
	}
	options, err := s.q.ListAllStartupShareOptions(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing startup share options: %w", err)
	}
	for _, row := range options {
		if o := startupShareOptionFromDB(row); accounts[o.SourceAccountID] && !accounts[o.AccountID] {
			v.AlsoDeletedOptions = append(v.AlsoDeletedOptions, DeletedOption{
				AccountName: accountNames[o.AccountID], SourceAccountName: accountNames[o.SourceAccountID], Shares: o.Shares,
			})
		}
	}
	return v, nil
}
