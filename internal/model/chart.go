package model

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/SimonSchneider/goslu/date"
	finance2 "github.com/SimonSchneider/pefigo/pkg/finance"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

// PredictionParams are the parameters of a Forecast. A parameter that isn't given (nil, or "") gets
// its default, the one the chart page shows: see WithDefaults.
type PredictionParams struct {
	// Duration is how long the Forecast runs from the Forecast Start.
	Duration *date.Duration
	// Through, when set, is the last day of the Forecast instead of Duration. The Forecast is empty
	// when it is on or before the Forecast Start.
	Through date.Date
	// Samples is how many outcomes of every uncertain value the Forecast draws.
	Samples *int64
	// Quantile is the share of outcomes between each point's lower and upper bound. At 0, no bounds
	// are calculated.
	Quantile *float64
	// SnapshotInterval is the Forecast Interval: the days with a point.
	SnapshotInterval date.Cron
	GroupBy          GroupBy
}

type GroupBy string

const (
	GroupByNone  GroupBy = "none"
	GroupByType  GroupBy = "type"
	GroupByTotal GroupBy = "total"
)

// The prediction parameters a Forecast runs with when they aren't given.
const (
	defaultPredictionDuration       = date.Year
	defaultPredictionSamples  int64 = 2000
	defaultPredictionQuantile       = 0.8
	defaultPredictionInterval       = date.Cron("*-*-28")
	defaultPredictionGroupBy        = GroupByType
)

// WithDefaults is p with every parameter that isn't given set to its default: a year, 2000 samples,
// bounds around 80% of outcomes, a point on the 28th of every month, grouped by Account Type. It is
// what RunPrediction runs and the chart page shows. It rejects a Forecast Interval that isn't a date
// pattern and an unknown GroupBy.
func (p PredictionParams) WithDefaults() (PredictionParams, error) {
	if p.Duration == nil {
		p.Duration = ptr(defaultPredictionDuration)
	}
	if p.Samples == nil {
		p.Samples = ptr(defaultPredictionSamples)
	}
	if p.Quantile == nil {
		p.Quantile = ptr(defaultPredictionQuantile)
	}
	if p.SnapshotInterval == "" {
		p.SnapshotInterval = defaultPredictionInterval
	}
	if p.GroupBy == "" {
		p.GroupBy = defaultPredictionGroupBy
	}
	if err := errors.Join(
		checkDatePattern("forecast interval", p.SnapshotInterval),
		checkOneOf("group by", string(p.GroupBy), string(GroupByNone), string(GroupByType), string(GroupByTotal)),
	); err != nil {
		return PredictionParams{}, err
	}
	return p, nil
}

type PredictionBalanceSnapshot struct {
	ID         string    `json:"id"`
	Day        date.Date `json:"day"`
	Balance    float64   `json:"balance"`
	LowerBound float64   `json:"lowerBound"`
	UpperBound float64   `json:"upperBound"`
}

type PredictionFinancialEntity struct {
	ID        string                      `json:"id"`
	Name      string                      `json:"name"`
	Color     string                      `json:"color"`
	Snapshots []PredictionBalanceSnapshot `json:"snapshots"`
}

// Markline is a date line on a forecast chart.
type Markline struct {
	Date  date.Date `json:"date"`
	Color string    `json:"color"`
	Name  string    `json:"name"`
}

// dateLines are the date lines of a forecast chart: today and every Special Date, by date.
func dateLines(specialDates []SpecialDate, today date.Date) []Markline {
	lines := make([]Markline, 0, len(specialDates)+1)
	lines = append(lines, Markline{Date: today, Name: "Today"})
	for _, sd := range specialDates {
		lines = append(lines, Markline{Date: sd.Date, Color: sd.Color, Name: sd.Name})
	}
	slices.SortStableFunc(lines, func(a, b Markline) int { return cmp.Compare(a.Date, b.Date) })
	return lines
}

// PredictionSetupEvent comes before the points of a Forecast, with each series and its history. Max
// is where the chart ends: the day after the Forecast's last day, or the Forecast Start when the
// Forecast is empty because Through is on or before it. Without a Forecast it is the last date line,
// so every date line is on the chart.
type PredictionSetupEvent struct {
	Max       date.Date                   `json:"max"`
	Entities  []PredictionFinancialEntity `json:"entities"`
	Marklines []Markline                  `json:"marklines"`
}

// PredictionEventHandler receives a Forecast: its setup, then one point per series on every Forecast
// Interval day, then Close.
type PredictionEventHandler interface {
	Setup(PredictionSetupEvent) error
	Snapshot(PredictionBalanceSnapshot) error
	Close() error
}

// RunPrediction runs the Forecast from the Forecast Start and sends it to eventHandler, grouped by
// params.GroupBy. Without a Forecast Start it sends the setup and closes without points. Parameters
// that aren't given get their defaults, and invalid ones are rejected before anything is sent.
func (s *Service) RunPrediction(ctx context.Context, eventHandler PredictionEventHandler, params PredictionParams) error {
	params, err := params.WithDefaults()
	if err != nil {
		return err
	}
	f, err := s.loadForecast(ctx)
	if err != nil {
		return fmt.Errorf("loading the Forecast: %w", err)
	}
	accountTypes, err := s.ListAccountTypes(ctx)
	if err != nil {
		return fmt.Errorf("listing account types for Prediction: %w", err)
	}
	specialDates, err := s.ListSpecialDates(ctx)
	if err != nil {
		return fmt.Errorf("listing special dates for Prediction: %w", err)
	}
	ucfg := uncertain.NewConfig(time.Now().UnixMilli(), *params.Samples)
	quantile := *params.Quantile
	fold := &forecastFold{
		eventHandler: eventHandler,
		ucfg:         ucfg,
		groups:       forecastGroups(params.GroupBy, f.accounts, accountTypes),
		bounds:       quantile != 0,
		q1:           (1 - quantile) / 2,
		q2:           (1 + quantile) / 2,
	}
	// The Forecast runs from the Forecast Start up to, but not on, end.
	var end date.Date
	switch {
	case !f.hasStart:
	case params.Through.IsZero():
		end = f.start.Add(*params.Duration)
	case params.Through.After(f.start):
		end = params.Through.Add(date.Day)
	default:
		end = f.start
	}
	today := date.Today()
	marklines := dateLines(specialDates, today)
	chartEnd := end
	if !f.hasStart {
		chartEnd = marklines[len(marklines)-1].Date
	}
	entities, err := s.setupEntities(ctx, fold, params.GroupBy, f, ucfg, today)
	if err != nil {
		return err
	}
	if err := eventHandler.Setup(PredictionSetupEvent{
		Max:       chartEnd,
		Entities:  entities,
		Marklines: marklines,
	}); err != nil {
		return fmt.Errorf("sending the setup: %w", err)
	}
	if f.hasStart && f.start.Before(end) {
		if err := finance2.RunForecast(ctx, ucfg, f.start, end, params.SnapshotInterval, f.entities(ucfg, f.start), f.schedule.ForecastTransfers(), finance2.CompositeRecorder{ReportRecorder: fold}); err != nil {
			return fmt.Errorf("running the Forecast: %w", err)
		}
	}
	return eventHandler.Close()
}

// totalGroupID is the ID of the one series of a Forecast in total.
const totalGroupID = "total"

// forecastGroup is a series of a grouped Forecast, adding up its accounts.
type forecastGroup struct {
	id, name, color string
	accountIDs      []string
}

// forecastGroups are the series of a Forecast of accounts grouped by groupBy, in the order they are
// sent: every Account Type with an account and Uncategorized last, every account, or their total.
func forecastGroups(groupBy GroupBy, accounts []Account, accountTypes []AccountType) []forecastGroup {
	switch groupBy {
	case GroupByType:
		known := KeyBy(accountTypes, func(at AccountType) string { return at.ID })
		byType := make(map[string][]string)
		for _, acc := range accounts {
			typeID := acc.TypeID
			if _, ok := known[typeID]; !ok {
				typeID = uncategorized.ID
			}
			byType[typeID] = append(byType[typeID], acc.ID)
		}
		groups := make([]forecastGroup, 0, len(byType))
		for _, at := range append(slices.Clone(accountTypes), uncategorized) {
			if ids, ok := byType[at.ID]; ok {
				groups = append(groups, forecastGroup{id: at.ID, name: at.Name, color: at.Color, accountIDs: ids})
			}
		}
		return groups
	case GroupByTotal:
		total := forecastGroup{id: totalGroupID, name: "total"}
		for _, acc := range accounts {
			total.accountIDs = append(total.accountIDs, acc.ID)
		}
		return []forecastGroup{total}
	default:
		groups := make([]forecastGroup, 0, len(accounts))
		for _, acc := range accounts {
			groups = append(groups, forecastGroup{id: acc.ID, name: acc.Name, accountIDs: []string{acc.ID}})
		}
		return groups
	}
}

// forecastFold sends a Forecast grouped: each group's point on a reported day is the sum of its
// accounts on that day.
type forecastFold struct {
	eventHandler PredictionEventHandler
	ucfg         *uncertain.Config
	groups       []forecastGroup
	bounds       bool // whether to calculate the lower and upper bound between q1 and q2
	q1           float64
	q2           float64
}

func (f *forecastFold) OnReport(day date.Date, balances map[string]uncertain.Value) error {
	for _, g := range f.groups {
		values := make([]uncertain.Value, len(g.accountIDs))
		for i, id := range g.accountIDs {
			values[i] = balances[id]
		}
		if err := f.eventHandler.Snapshot(f.point(g.id, day, sumInOrder(f.ucfg, values))); err != nil {
			return err
		}
	}
	return nil
}

// setupEntities are the groups of the setup with their history: per account, each account's own
// history; per Account Type or in total, the Balance History.
func (s *Service) setupEntities(ctx context.Context, fold *forecastFold, groupBy GroupBy, f *forecast, ucfg *uncertain.Config, today date.Date) ([]PredictionFinancialEntity, error) {
	if groupBy == GroupByNone {
		return fold.entities(f.history(ucfg)), nil
	}
	history, err := s.balanceHistory(ctx, today)
	if err != nil {
		return nil, fmt.Errorf("building balance history: %w", err)
	}
	if groupBy == GroupByTotal {
		history = map[string][]BalancePoint{totalGroupID: totalBalanceHistory(history)}
	}
	return fold.balanceHistoryEntities(history), nil
}

// balanceHistoryEntities are the groups of the setup, each with the Balance History under its ID.
// Balance History holds averages, so when bounds are asked for, a point's bounds are its balance.
func (f *forecastFold) balanceHistoryEntities(history map[string][]BalancePoint) []PredictionFinancialEntity {
	entities := make([]PredictionFinancialEntity, 0, len(f.groups))
	for _, g := range f.groups {
		points := make([]PredictionBalanceSnapshot, 0, len(history[g.id]))
		for _, p := range history[g.id] {
			points = append(points, f.point(g.id, p.Date, uncertain.NewFixed(p.Balance)))
		}
		entities = append(entities, PredictionFinancialEntity{ID: g.id, Name: g.name, Color: g.color, Snapshots: points})
	}
	return entities
}

// entities are the groups of the setup, each with the sum of its accounts' history on each day.
func (f *forecastFold) entities(history map[string][]finance2.BalanceSnapshot) []PredictionFinancialEntity {
	entities := make([]PredictionFinancialEntity, 0, len(f.groups))
	for _, g := range f.groups {
		byDay := make(map[date.Date][]uncertain.Value)
		for _, id := range g.accountIDs {
			for _, snap := range history[id] {
				byDay[snap.Date] = append(byDay[snap.Date], snap.Balance)
			}
		}
		points := make([]PredictionBalanceSnapshot, 0, len(byDay))
		for _, day := range slices.Sorted(maps.Keys(byDay)) {
			points = append(points, f.point(g.id, day, sumInOrder(f.ucfg, byDay[day])))
		}
		entities = append(entities, PredictionFinancialEntity{ID: g.id, Name: g.name, Color: g.color, Snapshots: points})
	}
	return entities
}

// sumInOrder adds up values in their order, so the same values always add up the same way. The first
// value is kept as it is: adding it to a fixed 0 would draw new samples from it.
func sumInOrder(ucfg *uncertain.Config, values []uncertain.Value) uncertain.Value {
	if len(values) == 0 {
		return uncertain.NewFixed(0)
	}
	sum := values[0]
	for _, v := range values[1:] {
		sum = sum.Add(ucfg, v)
	}
	return sum
}

func (f *forecastFold) point(id string, day date.Date, balance uncertain.Value) PredictionBalanceSnapshot {
	snap := PredictionBalanceSnapshot{ID: id, Day: day, Balance: balance.Mean()}
	if f.bounds {
		q := balance.Quantiles()
		snap.LowerBound, snap.UpperBound = q(f.q1), q(f.q2)
	}
	return snap
}
