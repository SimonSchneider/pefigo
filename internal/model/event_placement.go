package model

import (
	"context"
	"slices"

	"github.com/SimonSchneider/goslu/date"
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

// usesLeftOutAccount reports whether t moves money from or to an Account that is left out, which
// leaves it out too.
func (p *plans) usesLeftOutAccount(t TransferTemplate) bool {
	return p.leftOutAccounts[t.FromAccountID] || p.leftOutAccounts[t.ToAccountID]
}

// placeEvents is the one path every Forecast Input that Events change is loaded through: the
// Baseline's with no placements, and a Scenario's with its overrides.
func (s *Service) placeEvents(ctx context.Context, placements EventPlacements) (*plans, error) {
	events, err := s.ListEvents(ctx)
	if err != nil {
		return nil, err
	}
	stored, err := s.loadStoredEntries(ctx)
	if err != nil {
		return nil, err
	}
	return placeEntries(events, stored, placements)
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

	for _, row := range stored.transferTemplates {
		day, ok := placed(row.EventID)
		if !ok || p.leftOutAccounts[ui.OrDefault(row.FromAccountID)] || p.leftOutAccounts[ui.OrDefault(row.ToAccountID)] {
			tt, err := transferTemplateFromDB(row, ownDate(own, row.EventID))
			if err != nil {
				return nil, err
			}
			p.leftOutTransferTemplates = append(p.leftOutTransferTemplates, tt)
			continue
		}
		tt, err := transferTemplateFromDB(row, day)
		if err != nil {
			return nil, err
		}
		p.transferTemplates = append(p.transferTemplates, tt)
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

// placeBills places the Bills of stored whose Events placed places, each with the Bill Amounts and ends
// whose own Events it places, as placeSalaries places Salaries.
func placeBills(p *plans, stored storedEntries, placed func(eventID *string) (date.Date, bool)) error {
	index := make(map[string]int, len(stored.bills))
	for _, row := range stored.bills {
		if _, ok := placed(row.EventID); ok {
			index[row.ID] = len(p.bills)
			p.bills = append(p.bills, billFromDB(row))
		}
	}
	part := func(billID string, eventID *string) (b *Bill, day date.Date, ok bool) {
		i, found := index[billID]
		if day, ok = placed(eventID); !ok || !found {
			return nil, 0, false
		}
		return &p.bills[i], day, true
	}
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
	index := make(map[string]int, len(stored.salaries))
	for _, row := range stored.salaries {
		if day, ok := placed(row.EventID); ok {
			index[row.ID] = len(p.salaries)
			p.salaries = append(p.salaries, salaryFromDB(row, day))
		}
	}
	// part is the placed Salary salaryID a part in the Event eventID belongs to, with the date the part's
	// dates are stored after. ok is false when either is left out.
	part := func(salaryID string, eventID *string) (sal *Salary, day date.Date, ok bool) {
		i, found := index[salaryID]
		if day, ok = placed(eventID); !ok || !found {
			return nil, 0, false
		}
		return &p.salaries[i], day, true
	}
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
		slices.SortStableFunc(ss.shareChanges, compareShareChanges)
	}
}
