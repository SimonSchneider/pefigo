package model

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/goslu/sid"
	"github.com/SimonSchneider/pefigo/internal/pdb"
	"github.com/SimonSchneider/pefigo/pkg/ui"
)

// Event is a named group of related changes to the plans for the future that happen around one date:
// the Accounts, growth models and Transfer Templates it adds, and the Transfer Templates and growth
// models outside it that it ends. Every date of its changes is an offset from its date, so moving it
// moves them all. It is planned when the Baseline holds it at its date, and unplanned when only
// Scenarios use it.
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

// DeleteEvent deletes the Event with the entries it adds, and everything deleted with its Accounts,
// and lifts the ends it declares.
func (s *Service) DeleteEvent(ctx context.Context, id string) error {
	if err := s.q.DeleteEvent(ctx, id); err != nil {
		return fmt.Errorf("deleting event: %w", err)
	}
	return nil
}

// EventEntryKind is a type of entry an Event adds or ends.
type EventEntryKind string

const (
	EventEntryAccount          EventEntryKind = "account"
	EventEntryGrowthModel      EventEntryKind = "growth-model"
	EventEntryTransferTemplate EventEntryKind = "transfer-template"
)

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
	if err := checkOneOf("entry kind", kind, string(EventEntryAccount), string(EventEntryGrowthModel), string(EventEntryTransferTemplate)); err != nil {
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

// storeRecurrence is the recurrence of a Transfer Template placed at at, as stored. The date of a
// one-off in an Event is one of the Event's dates, so it is stored as the date that many days after
// day 0. A recurrence that repeats isn't a date of the Event and is stored as it is.
func storeRecurrence(recurrence date.Cron, inEvent bool, at date.Date) string {
	if d, ok := fixedDate(recurrence); ok && inEvent {
		return date.Date(storeDate(d, at)).String()
	}
	return string(recurrence)
}

// placeRecurrence is the stored recurrence of a Transfer Template placed at at.
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
// they are at with the Event at its current date.
func (s *Service) RemoveFromEvent(ctx context.Context, entry EventEntry) error {
	return s.setEntryEvent(ctx, entry, "")
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
	default:
		return fmt.Errorf("%w: %s can't be in an event", ErrInvalidInput, entry)
	}
	return nil
}

// EventEndInput ends a Transfer Template or growth model outside the Event on EndDate, which is the
// Event's date when it isn't given. It is stored as days after the Event's date.
type EventEndInput struct {
	EventID string
	Entry   EventEntry
	EndDate date.Date
	// EndOffset, when set, gives the End Date as days after the Event's date, in place of EndDate.
	EndOffset *date.Duration
}

// UpsertEventEnd makes the Event end the entry. An Event only ends Transfer Templates and growth
// models outside it.
func (s *Service) UpsertEventEnd(ctx context.Context, inp EventEndInput) error {
	at, err := s.eventDate(ctx, inp.EventID)
	if err != nil {
		return err
	}
	if inp.EventID == "" {
		return fmt.Errorf("%w: no event to end %s in", ErrInvalidInput, inp.Entry)
	}
	end, err := givenDate("end", inp.EndDate, inp.EndOffset, inp.EventID, at)
	if err != nil {
		return err
	}
	if end.IsZero() {
		end = at
	}
	offset := storeDate(end, at)
	switch inp.Entry.Kind {
	case EventEntryTransferTemplate:
		tt, err := s.GetTransferTemplate(ctx, inp.Entry.ID)
		if err != nil {
			return err
		}
		if tt.EventID == inp.EventID {
			return fmt.Errorf("%w: an event only ends entries outside it, and %q is in it", ErrInvalidInput, tt.Name)
		}
		err = s.q.UpsertTransferTemplateEnd(ctx, pdb.UpsertTransferTemplateEndParams{EventID: inp.EventID, TransferTemplateID: tt.ID, EndOffset: offset})
		if err != nil {
			return fmt.Errorf("ending transfer template: %w", err)
		}
	case EventEntryGrowthModel:
		gm, err := s.GetGrowthModel(ctx, inp.Entry.ID)
		if err != nil {
			return err
		}
		if gm.EventID == inp.EventID {
			return fmt.Errorf("%w: an event only ends entries outside it, and this growth model is in it", ErrInvalidInput)
		}
		err = s.q.UpsertGrowthModelEnd(ctx, pdb.UpsertGrowthModelEndParams{EventID: inp.EventID, GrowthModelID: gm.ID, EndOffset: offset})
		if err != nil {
			return fmt.Errorf("ending growth model: %w", err)
		}
	default:
		return fmt.Errorf("%w: an event only ends Transfer Templates and growth models, not %s", ErrInvalidInput, inp.Entry)
	}
	return nil
}

// DeleteEventEnd lifts the end the Event declares for entry, if any.
func (s *Service) DeleteEventEnd(ctx context.Context, eventID string, entry EventEntry) error {
	var err error
	switch entry.Kind {
	case EventEntryTransferTemplate:
		err = s.q.DeleteTransferTemplateEnd(ctx, pdb.DeleteTransferTemplateEndParams{EventID: eventID, TransferTemplateID: entry.ID})
	case EventEntryGrowthModel:
		err = s.q.DeleteGrowthModelEnd(ctx, pdb.DeleteGrowthModelEndParams{EventID: eventID, GrowthModelID: entry.ID})
	}
	if err != nil {
		return fmt.Errorf("lifting the end of %s: %w", entry, err)
	}
	return nil
}

// storedEntries are every stored entry an Event can add or end, as stored: the dates of an Event's
// entries are offsets from its date.
type storedEntries struct {
	accounts          []pdb.Account
	growthModels      []pdb.GrowthModel
	transferTemplates []pdb.TransferTemplate
	transferEnds      []pdb.EventTransferTemplateEnd
	growthEnds        []pdb.EventGrowthModelEnd
}

func (s *Service) loadStoredEntries(ctx context.Context) (storedEntries, error) {
	var st storedEntries
	var err error
	if st.accounts, err = s.q.ListAccounts(ctx); err != nil {
		return st, fmt.Errorf("listing accounts: %w", err)
	}
	if st.growthModels, err = s.q.ListGrowthModels(ctx); err != nil {
		return st, fmt.Errorf("listing growth models: %w", err)
	}
	if st.transferTemplates, err = s.q.GetTransferTemplates(ctx); err != nil {
		return st, fmt.Errorf("listing transfer templates: %w", err)
	}
	if st.transferEnds, err = s.q.ListTransferTemplateEnds(ctx); err != nil {
		return st, fmt.Errorf("listing transfer template ends: %w", err)
	}
	if st.growthEnds, err = s.q.ListGrowthModelEnds(ctx); err != nil {
		return st, fmt.Errorf("listing growth model ends: %w", err)
	}
	return st, nil
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

// EventEnd is an entry outside an Event that the Event ends, and when.
type EventEnd struct {
	Entry   EventEntry
	Name    string
	EndDate date.Date
	Offset  date.Duration
}

// NamedEventEntry is an entry with a name to show it by, and the Event it is in, if any.
type NamedEventEntry struct {
	EventEntry
	Name      string
	EventName string
}

// EventView is an Event at its date with the entries it adds and the entries it ends.
type EventView struct {
	Event
	Accounts          []Account
	GrowthModels      []EventGrowthModel
	TransferTemplates []EventTransferTemplate
	Ends              []EventEnd
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

// view is ev with its entries and the entries it ends.
func (e *eventEntries) view(ev Event) EventView {
	accountNames := e.accountNames()
	v := EventView{Event: ev}
	for _, acc := range e.accounts {
		if acc.EventID == ev.ID {
			v.Accounts = append(v.Accounts, acc)
		}
	}
	growthByID := make(map[string]GrowthModel, len(e.growthModels))
	for _, gm := range e.growthModels {
		growthByID[gm.ID] = gm
		if gm.EventID == ev.ID {
			v.GrowthModels = append(v.GrowthModels, EventGrowthModel{GrowthModel: gm, AccountName: accountNames[gm.AccountID], eventDate: ev.Date})
		}
	}
	transferByID := make(map[string]TransferTemplate, len(e.transferTemplates))
	for _, tt := range e.transferTemplates {
		transferByID[tt.ID] = tt
		if tt.EventID == ev.ID {
			v.TransferTemplates = append(v.TransferTemplates, EventTransferTemplate{TransferTemplate: tt, eventDate: ev.Date})
		}
	}
	for _, end := range e.stored.transferEnds {
		if end.EventID == ev.ID {
			v.Ends = append(v.Ends, EventEnd{
				Entry:   EventEntry{Kind: EventEntryTransferTemplate, ID: end.TransferTemplateID},
				Name:    transferByID[end.TransferTemplateID].Name,
				EndDate: placeDate(end.EndOffset, ev.Date),
				Offset:  date.Duration(end.EndOffset),
			})
		}
	}
	for _, end := range e.stored.growthEnds {
		if end.EventID == ev.ID {
			gm := growthByID[end.GrowthModelID]
			v.Ends = append(v.Ends, EventEnd{
				Entry:   EventEntry{Kind: EventEntryGrowthModel, ID: end.GrowthModelID},
				Name:    growthModelName(gm, accountNames[gm.AccountID]),
				EndDate: placeDate(end.EndOffset, ev.Date),
				Offset:  date.Duration(end.EndOffset),
			})
		}
	}
	slices.SortStableFunc(v.GrowthModels, func(a, b EventGrowthModel) int {
		return cmp.Or(cmp.Compare(a.StartDate, b.StartDate), cmp.Compare(a.AccountName, b.AccountName))
	})
	slices.SortStableFunc(v.TransferTemplates, func(a, b EventTransferTemplate) int {
		return cmp.Or(cmp.Compare(a.StartDate, b.StartDate), cmp.Compare(a.Name, b.Name))
	})
	slices.SortStableFunc(v.Ends, func(a, b EventEnd) int { return cmp.Compare(a.Name, b.Name) })
	return v
}

func (e *eventEntries) event(id string) (Event, bool) {
	i := slices.IndexFunc(e.events, func(ev Event) bool { return ev.ID == id })
	if i < 0 {
		return Event{}, false
	}
	return e.events[i], true
}

// EventsView is the Events page: every Event by date with its entries and the entries it ends.
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

// EventEditView is an Event's edit page: the Event with its entries, and the entries it can end. An
// entry joins an Event from its own form.
type EventEditView struct {
	Event EventView
	// Endable are the Transfer Templates and growth models outside the Event that it doesn't end yet.
	Endable []NamedEventEntry
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
	v := &EventEditView{Event: e.view(ev)}
	eventNames := e.eventNames()
	accountNames := e.accountNames()
	ended := make(map[EventEntry]bool, len(v.Event.Ends))
	for _, end := range v.Event.Ends {
		ended[end.Entry] = true
	}
	endable := func(kind EventEntryKind, id, name, eventID string) {
		entry := NamedEventEntry{EventEntry: EventEntry{Kind: kind, ID: id}, Name: name, EventName: eventNames[eventID]}
		if eventID != ev.ID && !ended[entry.EventEntry] {
			v.Endable = append(v.Endable, entry)
		}
	}
	for _, gm := range e.growthModels {
		endable(EventEntryGrowthModel, gm.ID, growthModelName(gm, accountNames[gm.AccountID]), gm.EventID)
	}
	for _, tt := range e.transferTemplates {
		endable(EventEntryTransferTemplate, tt.ID, tt.Name, tt.EventID)
	}
	return v, nil
}

// EventDeleteView confirms deleting an Event: it lists the entries the Event adds and the ends it
// declares, and the entries outside it deleted with its Accounts.
type EventDeleteView struct {
	Event       EventView
	AlsoDeleted []NamedEventEntry
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
	return v, nil
}
