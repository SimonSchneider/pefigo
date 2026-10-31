package model

import (
	"context"
	"fmt"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/pkg/finance"
	"github.com/SimonSchneider/pefigo/pkg/swe"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

// forecast holds the Forecast Inputs every account of a Forecast is built from, with the Events
// placed. They are loaded up front, so building the accounts runs no queries. It is the only place
// that builds them, for the Forecasts and for the Transfers page alike.
type forecast struct {
	events        []Event                        // the placed Events, at the dates they are placed at
	accounts      []Account                      // by name and ID; none of a left-out Event
	snapshots     map[string][]AccountSnapshot   // by account ID, each by date; none of a Startup Share account
	growthModels  map[string]finance.GrowthModel // every account's, by account ID
	startupShares map[string]*startupShares      // the Startup Share accounts, by account ID
	sweParams     []SweYearlyParams              // by ValidFrom
	schedule      *TransferSchedule

	// start is the Forecast Start, the first of the Forecast Start Days on or after the latest
	// Snapshot. Without a Snapshot, hasStart is false and there is no Forecast. Events never change
	// it: the Snapshots of every account count, whether its Event is placed or not.
	start    date.Date
	hasStart bool
}

// forecastStartDaysSearched is how long after the latest Snapshot the Forecast Start Days are looked
// for: four years hold every day of the year, February 29 included.
const forecastStartDaysSearched = 4*date.Year + date.Day

// loadForecast loads the Forecast Inputs with the Events placed as placements say: the Baseline's
// without placements.
func (s *Service) loadForecast(ctx context.Context, placements EventPlacements) (*forecast, error) {
	p, err := s.placeEvents(ctx, placements)
	if err != nil {
		return nil, fmt.Errorf("placing the events: %w", err)
	}
	allStartupShares, err := s.listStartupShares(ctx)
	if err != nil {
		return nil, err
	}
	snapshotRows, err := s.q.ListSnapshots(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing snapshots: %w", err)
	}
	sweParams, err := s.ListSweYearlyParams(ctx)
	if err != nil {
		return nil, err
	}
	schedule, err := s.transferSchedule(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("loading transfer schedule: %w", err)
	}
	if err := schedule.checkRecurrences(); err != nil {
		return nil, err
	}
	settings, err := s.GetForecastSettings(ctx)
	if err != nil {
		return nil, err
	}
	if err := settings.checkStartDays(); err != nil {
		return nil, fmt.Errorf("the stored Forecast Settings: %w", err)
	}
	f := &forecast{
		events:        p.events,
		accounts:      p.accounts,
		snapshots:     make(map[string][]AccountSnapshot),
		growthModels:  make(map[string]finance.GrowthModel, len(p.accounts)),
		startupShares: make(map[string]*startupShares, len(allStartupShares)),
		sweParams:     sweParams,
		schedule:      schedule,
	}
	for accountID, ss := range allStartupShares {
		if !p.leftOutAccounts[accountID] {
			f.startupShares[accountID] = ss
		}
	}
	// A Startup Share account's balance is derived, so its Snapshots are ignored.
	for _, row := range snapshotRows {
		if _, ok := allStartupShares[row.AccountID]; ok {
			continue
		}
		snap := accountSnapshotFromDB(row)
		if !f.hasStart || snap.Date.After(f.start) {
			f.start, f.hasStart = snap.Date, true
		}
		if !p.leftOutAccounts[row.AccountID] {
			f.snapshots[row.AccountID] = append(f.snapshots[row.AccountID], snap)
		}
	}
	if f.hasStart {
		f.start = firstForecastStartDay(f.start, settings.StartDays)
	}
	growthModels := make(map[string]GrowthModels)
	for _, gm := range p.growthModels {
		growthModels[gm.AccountID] = append(growthModels[gm.AccountID], gm)
	}
	for _, acc := range p.accounts {
		if err := cashFlowError(acc.CashFlowFrequency, acc.CashFlowDestinationID); err != nil {
			return nil, fmt.Errorf("account %s has invalid cash-flow frequency %q: %w", acc.ID, acc.CashFlowFrequency, err)
		}
		growth, err := growthModels[acc.ID].ToFinance()
		if err != nil {
			return nil, fmt.Errorf("loading the growth models of %s: %w", acc.Name, err)
		}
		f.growthModels[acc.ID] = growth
	}
	return f, nil
}

// firstForecastStartDay is the first of startDays on or after latest, the latest Snapshot's day. The
// Transfers before it are taken to be in the Snapshots. Without one within the searched years, it is
// latest.
func firstForecastStartDay(latest date.Date, startDays date.Cron) date.Date {
	for day := range date.Iter(latest, latest.Add(forecastStartDaysSearched), date.Day) {
		if startDays.Matches(day) {
			return day
		}
	}
	return latest
}

// entities builds every account for a Forecast that starts on day, each at its Starting Balance on
// day. Entities keep state while a Forecast runs, so every run needs its own.
func (f *forecast) entities(ucfg *uncertain.Config, day date.Date) []finance.Entity {
	entities := make([]finance.Entity, 0, len(f.accounts))
	for _, acc := range f.accounts {
		entity := finance.Entity{ID: acc.ID, Name: acc.Name}
		if acc.BalanceUpperLimit != nil {
			entity.BalanceLimit = finance.BalanceLimit{Upper: uncertain.NewFixed(*acc.BalanceUpperLimit)}
		}
		if acc.hasCashFlow() {
			entity.CashFlow = &finance.CashFlowModel{
				Frequency:     date.Cron(acc.CashFlowFrequency),
				DestinationID: acc.CashFlowDestinationID,
			}
		}
		var startingBalance uncertain.Value
		if ss, ok := f.startupShares[acc.ID]; ok {
			growth := ss.growthOn(day)
			entity.GrowthModel = growth
			startingBalance = growth.Balance(ucfg)
		} else {
			entity.GrowthModel = f.growthModels[acc.ID]
			startingBalance = latestSnapshotBalance(f.snapshots[acc.ID], day)
		}
		entity.Snapshots = []finance.BalanceSnapshot{{Date: day, Balance: startingBalance}}
		if acc.IsISK {
			entity.TaxModel = &swe.ISKTax{
				ParamsFunc: func(d date.Date) swe.ISKParams {
					return swe.ISKParams{
						SchablonRanta: activeSchablonRantaAt(f.sweParams, d),
						Fribelopp:     activeISKFribeloppAt(f.sweParams, d),
					}
				},
			}
		}
		entities = append(entities, entity)
	}
	return entities
}

// latestSnapshotBalance is the balance of the latest of snapshots on or before day as it was
// recorded, or 0 without one.
func latestSnapshotBalance(snapshots []AccountSnapshot, day date.Date) uncertain.Value {
	balance := uncertain.NewFixed(0)
	for _, snap := range snapshots {
		if snap.Date.After(day) {
			break
		}
		balance = snap.Balance
	}
	return balance
}

// history is what a Forecast's setup shows before its points, by account ID: the Snapshots of every
// account but the Startup Share accounts, and the derived history of each Startup Share account.
func (f *forecast) history(ucfg *uncertain.Config) map[string][]finance.BalanceSnapshot {
	history := make(map[string][]finance.BalanceSnapshot, len(f.snapshots)+len(f.startupShares))
	for accountID, snaps := range f.snapshots {
		for _, snap := range snaps {
			history[accountID] = append(history[accountID], snap.ToFinance())
		}
	}
	for accountID, ss := range f.startupShares {
		history[accountID] = ss.history(ucfg, f.start, f.hasStart)
	}
	return history
}
