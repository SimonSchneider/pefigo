package finance

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sort"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

type BalanceSnapshot struct {
	Date    date.Date
	Balance uncertain.Value
}

type CashFlowModel struct {
	Frequency     date.Cron
	DestinationID string
}

type BalanceLimit struct {
	Upper uncertain.Value // Optional upper limit, if not set, no limit is applied
}

type TaxModel interface {
	Apply(ucfg *uncertain.Config, day date.Date, balance uncertain.Value, dayDeposits uncertain.Value) uncertain.Value
}

type Entity struct {
	ID   string
	Name string

	BalanceLimit BalanceLimit // Optional balance limit, if not set, no limit is applied

	// Balance snapshots for this entity, sorted by date
	Snapshots []BalanceSnapshot

	GrowthModel GrowthModel
	CashFlow    *CashFlowModel // Optional cash flow model, if not set, no cash flow is applied
	TaxModel    TaxModel       // Optional tax model, if not set, no tax is applied
}

func (fe *Entity) GetLatestSnapshot(day date.Date) BalanceSnapshot {
	foundSnapshot := sort.Search(len(fe.Snapshots), func(i int) bool {
		return fe.Snapshots[i].Date.After(day)
	})
	if foundSnapshot == 0 {
		// No snapshot before the given date, return zero balance
		return BalanceSnapshot{Balance: uncertain.NewFixed(0.0), Date: day}
	}
	return fe.Snapshots[foundSnapshot-1]
}

// ModeledEntity is an Entity while a Forecast runs.
type ModeledEntity struct {
	Entity

	balance             uncertain.Value
	accruedAppreciation uncertain.Value
	dayDeposits         uncertain.Value // deposits received today, reset each day
}

func (fe *ModeledEntity) ApplyGrowth(ucfg *uncertain.Config, entities map[string]*ModeledEntity, date date.Date) {
	if fe.GrowthModel == nil || !fe.GrowthModel.IsActiveOn(date) {
		return // No growth model or not active on this date
	}
	if fe.accruedAppreciation.Distribution == "" {
		fe.accruedAppreciation = uncertain.NewFixed(0.0) // Initialize if not set
	}
	totalBalance := fe.balance.Add(ucfg, fe.accruedAppreciation)
	dailyGrowth := fe.GrowthModel.Apply(ucfg, date, entities, totalBalance)
	fe.accruedAppreciation = fe.accruedAppreciation.Add(ucfg, dailyGrowth)
}

func (fe *ModeledEntity) ApplyAppreciation(ucfg *uncertain.Config, entities map[string]*ModeledEntity, day date.Date) {
	if fe.accruedAppreciation.Zero() {
		return
	}
	if fe.CashFlow == nil || (fe.CashFlow.Frequency.Matches(day) && fe.CashFlow.DestinationID == "") {
		fe.balance = fe.balance.Add(ucfg, fe.accruedAppreciation)
		fe.accruedAppreciation = uncertain.NewFixed(0.0)
	} else if fe.CashFlow.Frequency.Matches(day) {
		// If a destination account is specified, add interest to that account
		if destAccount, ok := entities[fe.CashFlow.DestinationID]; ok {
			destAccount.balance = destAccount.balance.Add(ucfg, fe.accruedAppreciation)
		} else {
			panic("Could not find account with ID " + fe.CashFlow.DestinationID)
		}
		fe.accruedAppreciation = uncertain.NewFixed(0.0)
	}
}

// RunForecast runs a Forecast from start, the Forecast Start, up to but not on end. Every entity starts
// on start at its Starting Balance. Each day charges its Transfers, then applies growth, interest and
// tax, and on every Forecast Interval day the recorder receives every entity's balance.
func RunForecast(ctx context.Context, ucfg *uncertain.Config, start, end date.Date, interval date.Cron, entities []Entity, transfers []TransferTemplate, recorder Recorder) error {
	// A day's transfers run in priority order, and those of one priority together.
	transfers = slices.Clone(transfers)
	slices.SortStableFunc(transfers, func(a, b TransferTemplate) int { return cmp.Compare(a.Priority, b.Priority) })
	dailyTransfers := make([]TransferTemplate, 0)
	fes := make(map[string]*ModeledEntity, len(entities))
	for _, fe := range entities {
		fes[fe.ID] = &ModeledEntity{
			Entity:  fe,
			balance: fe.GetLatestSnapshot(start).Balance,
		}
	}
	for day := range date.Iter(start, end, date.Day) {
		for _, fe := range fes {
			fe.dayDeposits = uncertain.NewFixed(0.0)
		}
		for _, transfer := range transfers {
			if transfer.ChargedOn(day) {
				dailyTransfers = append(dailyTransfers, transfer)
			}
		}
		if len(dailyTransfers) > 0 {
			if err := applyDailyTransfers(ucfg, fes, dailyTransfers, day, recorder); err != nil {
				return fmt.Errorf("failed to apply daily transfers: %w", err)
			}
		}
		for _, fe := range fes {
			fe.ApplyGrowth(ucfg, fes, day)
		}
		for _, fe := range fes {
			fe.ApplyAppreciation(ucfg, fes, day)
		}
		for _, fe := range fes {
			if fe.TaxModel != nil {
				tax := fe.TaxModel.Apply(ucfg, day, fe.balance, fe.dayDeposits)
				if !tax.Zero() {
					fe.balance = fe.balance.Sub(ucfg, tax)
				}
			}
		}
		if interval.Matches(day) {
			balances := make(map[string]uncertain.Value, len(fes))
			for id, fe := range fes {
				balances[id] = fe.balance
			}
			if err := recorder.OnReport(day, balances); err != nil {
				return fmt.Errorf("failed to record report on %s: %w", day, err)
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		dailyTransfers = dailyTransfers[:0]
	}
	return nil
}
