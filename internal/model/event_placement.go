package model

import (
	"cmp"
	"context"
	"slices"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/pdb"
	"github.com/SimonSchneider/pefigo/pkg/ui"
)

// EventPlacement puts an Event at a date, or leaves it out.
type EventPlacement struct {
	Date    date.Date
	LeftOut bool
}

// EventPlacements place Events differently from the Baseline, by Event ID, as a Scenario does. Every
// Event without one is as in the Baseline: at its own date when it is planned, and left out when it is
// unplanned. No placements is the Baseline.
type EventPlacements map[string]EventPlacement

// placementOf is where ev is placed: as placements say, or as in the Baseline.
func (placements EventPlacements) placementOf(ev Event) EventPlacement {
	if p, ok := placements[ev.ID]; ok {
		return p
	}
	return EventPlacement{Date: ev.Date, LeftOut: !ev.Planned}
}

// asStored places every Event at its own date, planned or not: every entry is at the dates it is
// stored relative to, and none is left out. The pages that edit entries show them so.
func asStored(events []Event) EventPlacements {
	placements := make(EventPlacements, len(events))
	for _, ev := range events {
		placements[ev.ID] = EventPlacement{Date: ev.Date}
	}
	return placements
}

// placedEvents are the Events placements don't leave out, each at the date it is placed at.
func placedEvents(events []Event, placements EventPlacements) []Event {
	var placed []Event
	for _, ev := range events {
		if p := placements.placementOf(ev); !p.LeftOut {
			ev.Date = p.Date
			placed = append(placed, ev)
		}
	}
	return placed
}

// plans are the entries of the plans for the future with the Events placed: the Accounts, growth
// models and stored Transfer Templates outside Events and in the placed Events, each at absolute
// dates. The entries of a left-out Event are left out, and so is every entry that uses one of its
// Accounts.
type plans struct {
	events            []Event   // the placed Events, at the dates they are placed at
	accounts          []Account // by name and ID
	growthModels      []GrowthModel
	transferTemplates []TransferTemplate
	// leftOutTransferTemplates are the stored Transfer Templates that are left out, each at its Event's
	// own date.
	leftOutTransferTemplates []TransferTemplate
	leftOutAccounts          map[string]bool
	// salaries are the placed Salaries, by name and ID, each with its placed parts by date and ID. A
	// Gross Salary isn't segmented yet.
	salaries []Salary
	// bills are the placed Bills, by Bill account, name and ID, each with its placed Bill Amounts and
	// ends by date and ID. Their Bill Amounts aren't converted to the Default Currency yet.
	bills []Bill
	// startupShareAccounts are every Startup Share account, placed or not, by account ID.
	startupShareAccounts map[string]StartupShareAccount
	// startupShares are the placed Startup Share accounts, by account ID, with their placed investment
	// rounds and share changes, and their Options but those funded by a left-out Account.
	startupShares map[string]*startupShares
}

// sortByDayAndID orders entries by the day and ID key gives each: stored offsets don't sort as the days
// they place at. Of two entries on the same day, the one with the later ID comes last.
func sortByDayAndID[T any](entries []T, key func(T) (date.Date, string)) {
	slices.SortStableFunc(entries, func(a, b T) int {
		aDay, aID := key(a)
		bDay, bID := key(b)
		return cmp.Or(cmp.Compare(aDay, bDay), cmp.Compare(aID, bID))
	})
}

// usesLeftOutAccount reports whether t moves money from or to an Account that is left out, which
// leaves it out too.
func (p *plans) usesLeftOutAccount(t TransferTemplate) bool {
	return p.leftOutAccounts[t.FromAccountID] || p.leftOutAccounts[t.ToAccountID]
}

// placeEvents is the one path every Forecast Input that Events change is loaded through: the
// Baseline's with no placements, and a Scenario's with its overrides.
func (s *Service) placeEvents(ctx context.Context, placements EventPlacements) (*plans, error) {
	pl, err := s.loadPlacings(ctx)
	if err != nil {
		return nil, err
	}
	return placeEntries(pl.events, pl.stored, placements)
}

// placings are the Events and stored entries one page load reads, placed each way the page asks for,
// once each: as the Baseline places them, with every Event at its own date, and with one unplanned
// Event planned.
type placings struct {
	events    []Event
	stored    storedEntries
	baseline  *plans
	ownDates  *plans
	unplanned map[string]*plans // by the ID of the unplanned Event they plan
}

// loadPlacings reads the Events and every stored entry, for one page load to place.
func (s *Service) loadPlacings(ctx context.Context) (*placings, error) {
	events, err := s.ListEvents(ctx)
	if err != nil {
		return nil, err
	}
	stored, err := s.loadStoredEntries(ctx)
	if err != nil {
		return nil, err
	}
	return &placings{events: events, stored: stored, unplanned: make(map[string]*plans)}, nil
}

// inBaseline are the plans as the Baseline places them.
func (pl *placings) inBaseline() (*plans, error) {
	return pl.placeOnce(&pl.baseline, nil)
}

// atOwnDates are the plans with every Event at its own date, planned or not: every entry at the dates
// it is stored after, as its forms edit it, and none left out.
func (pl *placings) atOwnDates() (*plans, error) {
	return pl.placeOnce(&pl.ownDates, asStored(pl.events))
}

func (pl *placings) placeOnce(placed **plans, placements EventPlacements) (*plans, error) {
	if *placed == nil {
		p, err := placeEntries(pl.events, pl.stored, placements)
		if err != nil {
			return nil, err
		}
		*placed = p
	}
	return *placed, nil
}

// shownBy are the plans an entry of the Event eventID, or of none for "", shows what it does by on its
// pages: the Baseline's, unless the Baseline leaves the unplanned Event out. Then they are the Baseline
// with that Event alone planned, at its own date, as a Scenario that plans only it would place them, and
// inBaseline is false. What other unplanned Events add to or end of the entry never shows.
func (pl *placings) shownBy(eventID string) (p *plans, inBaseline bool, err error) {
	i := slices.IndexFunc(pl.events, func(ev Event) bool { return ev.ID == eventID })
	if i < 0 || pl.events[i].Planned {
		p, err := pl.inBaseline()
		return p, true, err
	}
	planned := pl.unplanned[eventID]
	p, err = pl.placeOnce(&planned, EventPlacements{eventID: {Date: pl.events[i].Date}})
	pl.unplanned[eventID] = planned
	return p, false, err
}

// eventDates are the dates of every Event, by ID.
func (pl *placings) eventDates() map[string]date.Date {
	dates := make(map[string]date.Date, len(pl.events))
	for _, ev := range pl.events {
		dates[ev.ID] = ev.Date
	}
	return dates
}

// salary is the Salary id as p places it, and whether p places it.
func (p *plans) salary(id string) (Salary, bool) {
	i := slices.IndexFunc(p.salaries, func(sal Salary) bool { return sal.ID == id })
	if i < 0 {
		return Salary{}, false
	}
	return p.salaries[i], true
}

// bill is the Bill id as p places it, with its own copy of its Bill Amounts, and whether p places it.
func (p *plans) bill(id string) (Bill, bool) {
	i := slices.IndexFunc(p.bills, func(b Bill) bool { return b.ID == id })
	if i < 0 {
		return Bill{}, false
	}
	b := p.bills[i]
	b.Amounts = slices.Clone(b.Amounts)
	return b, true
}

// placeEntries places stored as placements say. It knows how to place each type of entry, and
// nothing about why the Events are placed where they are.
func placeEntries(events []Event, stored storedEntries, placements EventPlacements) (*plans, error) {
	p := &plans{leftOutAccounts: make(map[string]bool)}
	own := make(map[string]date.Date, len(events))
	at := make(map[string]date.Date, len(events))
	for _, ev := range events {
		own[ev.ID] = ev.Date
	}
	for _, ev := range placedEvents(events, placements) {
		at[ev.ID] = ev.Date
		p.events = append(p.events, ev)
	}
	// placed is where the entries of eventID are placed: at day 0 without an Event. ok is false when
	// the Event is left out.
	placed := func(eventID *string) (day date.Date, ok bool) {
		if eventID == nil {
			return 0, true
		}
		day, ok = at[*eventID]
		return day, ok
	}

	for _, row := range stored.accounts {
		if _, ok := placed(row.EventID); !ok {
			p.leftOutAccounts[row.ID] = true
			continue
		}
		p.accounts = append(p.accounts, accountFromDB(row))
	}
	// Interest paid out to a left-out Account stays in its own Account, as without a destination.
	for i, acc := range p.accounts {
		if p.leftOutAccounts[acc.CashFlowDestinationID] {
			p.accounts[i].CashFlowDestinationID = ""
		}
	}

	for _, row := range stored.growthModels {
		day, ok := placed(row.EventID)
		if !ok || p.leftOutAccounts[row.AccountID] {
			continue
		}
		gm, err := growthModelFromDB(row, day)
		if err != nil {
			return nil, err
		}
		p.growthModels = append(p.growthModels, gm)
	}
	// Stored offsets don't sort as the dates they place at, so the placed growth models are ordered by
	// their dates: of two starting on the same day, the one with the later ID applies, in an Event or
	// not.
	slices.SortStableFunc(p.growthModels, compareGrowthModels)

	if err := placeTransferTemplates(p, stored, placed, own); err != nil {
		return nil, err
	}

	placeStartupShares(p, stored, placed)
	if err := placeSalaries(p, stored, placed); err != nil {
		return nil, err
	}
	if err := placeBills(p, stored, placed); err != nil {
		return nil, err
	}
	return p, nil
}

// placeTransferTemplates places the stored Transfer Templates of stored whose Events placed places, and
// leaves out the others and those that use a left-out Account, each at its own Event's date. Each ends
// at the earliest of its Ends whose own Events placed places. A left-out Transfer Template shows what it
// would do with its Event planned: the Ends in its own Event count too, at that Event's own date.
func placeTransferTemplates(p *plans, stored storedEntries, placed func(eventID *string) (date.Date, bool), own map[string]date.Date) error {
	leftOut := make(map[string]*string) // the Event of each left-out Transfer Template, by ID
	for _, row := range stored.transferTemplates {
		day, ok := placed(row.EventID)
		out := !ok || p.leftOutAccounts[ui.OrDefault(row.FromAccountID)] || p.leftOutAccounts[ui.OrDefault(row.ToAccountID)]
		if out {
			leftOut[row.ID], day = row.EventID, ownDate(own, row.EventID)
		}
		tt, err := transferTemplateFromDB(row, day)
		if err != nil {
			return err
		}
		if out {
			p.leftOutTransferTemplates = append(p.leftOutTransferTemplates, tt)
		} else {
			p.transferTemplates = append(p.transferTemplates, tt)
		}
	}
	ends := make(map[string][]TransferTemplateEnd)
	for _, row := range stored.transferTemplateEnds {
		day, ok := placed(row.EventID)
		if eventID, out := leftOut[row.TransferTemplateID]; out && eventID != nil && row.EventID != nil && *row.EventID == *eventID {
			day, ok = own[*eventID], true
		}
		if ok {
			ends[row.TransferTemplateID] = append(ends[row.TransferTemplateID], transferTemplateEndFromDB(row, day))
		}
	}
	for _, templates := range [][]TransferTemplate{p.transferTemplates, p.leftOutTransferTemplates} {
		for i, tt := range templates {
			templates[i] = tt.withEnds(ends[tt.ID])
		}
	}
	return nil
}

// placeOwners places the Salaries or Bills of rows, each with the ID and Event idOf gives it, whose
// Events placed places, as fromDB makes them at their Event's date. part is how one of their parts finds
// the placed Salary or Bill it belongs to: given its owner's ID and its own Event, it is its owner with
// the date the part's dates are stored after. ok is false when either is left out.
func placeOwners[R, O any](rows []R, placed func(eventID *string) (date.Date, bool), idOf func(R) (id string, eventID *string), fromDB func(R, date.Date) O) (owners []O, part func(ownerID string, eventID *string) (owner *O, day date.Date, ok bool)) {
	index := make(map[string]int, len(rows))
	for _, row := range rows {
		id, eventID := idOf(row)
		if day, ok := placed(eventID); ok {
			index[id] = len(owners)
			owners = append(owners, fromDB(row, day))
		}
	}
	return owners, func(ownerID string, eventID *string) (*O, date.Date, bool) {
		i, found := index[ownerID]
		day, ok := placed(eventID)
		if !ok || !found {
			return nil, 0, false
		}
		return &owners[i], day, true
	}
}

// placeBills places the Bills of stored whose Events placed places, each with the Bill Amounts and ends
// whose own Events it places, as placeSalaries places Salaries.
func placeBills(p *plans, stored storedEntries, placed func(eventID *string) (date.Date, bool)) error {
	var part func(string, *string) (*Bill, date.Date, bool)
	p.bills, part = placeOwners(stored.bills, placed,
		func(b pdb.Bill) (string, *string) { return b.ID, b.EventID },
		func(b pdb.Bill, _ date.Date) Bill { return billFromDB(b) })
	for _, row := range stored.billAmounts {
		if b, day, ok := part(row.BillID, row.EventID); ok {
			amt, err := billAmountFromDB(row, day)
			if err != nil {
				return err
			}
			b.Amounts = append(b.Amounts, amt)
		}
	}
	for _, row := range stored.billEnds {
		if b, day, ok := part(row.BillID, row.EventID); ok {
			b.Ends = append(b.Ends, billEndFromDB(row, day))
		}
	}
	for i := range p.bills {
		sortBillParts(&p.bills[i])
	}
	return nil
}

// placeSalaries places the Salaries of stored whose Events placed places, each with the parts whose
// own Events it places: a part an Event adds to a Salary outside it is left out with that Event alone.
func placeSalaries(p *plans, stored storedEntries, placed func(eventID *string) (date.Date, bool)) error {
	var part func(string, *string) (*Salary, date.Date, bool)
	p.salaries, part = placeOwners(stored.salaries, placed,
		func(s pdb.Salary) (string, *string) { return s.ID, s.EventID },
		salaryFromDB)
	for _, row := range stored.salaryAmounts {
		if sal, day, ok := part(row.SalaryID, row.EventID); ok {
			amt, err := salaryAmountFromDB(row, day)
			if err != nil {
				return err
			}
			sal.Amounts = append(sal.Amounts, amt)
		}
	}
	for _, row := range stored.salaryAdjustments {
		if sal, day, ok := part(row.SalaryID, row.EventID); ok {
			sal.Adjustments = append(sal.Adjustments, salaryAdjustmentFromDB(row, day))
		}
	}
	for _, row := range stored.partialParentalLeaves {
		if sal, day, ok := part(row.SalaryID, row.EventID); ok {
			sal.PartialParentalLeaves = append(sal.PartialParentalLeaves, partialParentalLeaveFromDB(row, day))
		}
	}
	for _, row := range stored.fullParentalLeaves {
		if sal, day, ok := part(row.SalaryID, row.EventID); ok {
			sal.FullParentalLeaves = append(sal.FullParentalLeaves, fullParentalLeaveFromDB(row, day))
		}
	}
	for _, row := range stored.salaryEnds {
		if sal, day, ok := part(row.SalaryID, row.EventID); ok {
			sal.Ends = append(sal.Ends, salaryEndFromDB(row, day))
		}
	}
	for i := range p.salaries {
		sortSalaryParts(&p.salaries[i])
	}
	return nil
}

// placeStartupShares places the Startup Share accounts of stored that p keeps, with the investment
// rounds and share changes whose Events placed places. An Option funded by a left-out Account is left
// out with it, as a Transfer Template from it is.
func placeStartupShares(p *plans, stored storedEntries, placed func(eventID *string) (date.Date, bool)) {
	p.startupShareAccounts = make(map[string]StartupShareAccount, len(stored.startupShareAccounts))
	p.startupShares = make(map[string]*startupShares, len(stored.startupShareAccounts))
	for _, row := range stored.startupShareAccounts {
		ssa := startupShareAccountFromDB(row)
		p.startupShareAccounts[ssa.AccountID] = ssa
		if !p.leftOutAccounts[ssa.AccountID] {
			p.startupShares[ssa.AccountID] = &startupShares{StartupShareAccount: ssa}
		}
	}
	for _, row := range stored.investmentRounds {
		day, ok := placed(row.EventID)
		if ss := p.startupShares[row.AccountID]; ok && ss != nil {
			ss.rounds = append(ss.rounds, investmentRoundFromDB(row, day))
		}
	}
	for _, row := range stored.shareChanges {
		day, ok := placed(row.EventID)
		if ss := p.startupShares[row.AccountID]; ok && ss != nil {
			ss.shareChanges = append(ss.shareChanges, shareChangeFromDB(row, day))
		}
	}
	for _, row := range stored.startupShareOptions {
		if ss := p.startupShares[row.AccountID]; ss != nil && !p.leftOutAccounts[row.SourceAccountID] {
			ss.options = append(ss.options, startupShareOptionFromDB(row))
		}
	}
	// Stored offsets don't sort as the dates they place at.
	for _, ss := range p.startupShares {
		slices.SortStableFunc(ss.rounds, compareInvestmentRounds)
		sortByDayAndID(ss.shareChanges, ShareChange.dayAndID)
	}
}
