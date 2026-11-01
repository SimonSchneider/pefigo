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

	return p, nil
}
