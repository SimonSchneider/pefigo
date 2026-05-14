package model

import (
	"context"
	"fmt"
	"sort"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/pdb"
)

func (s *Service) ListForecastCache(ctx context.Context) ([]ForecastCacheRow, error) {
	rows, err := s.q.ListForecastCache(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing forecast cache: %w", err)
	}
	result := make([]ForecastCacheRow, len(rows))
	for i, r := range rows {
		result[i] = ForecastCacheRow{
			Date:          r.Date,
			AccountTypeID: r.AccountTypeID,
			Median:        r.Median,
			LowerBound:    r.LowerBound,
			UpperBound:    r.UpperBound,
		}
	}
	return result, nil
}

func (s *Service) RunForecastCache(ctx context.Context) error {
	specialDates, err := s.ListSpecialDates(ctx)
	if err != nil {
		return fmt.Errorf("listing special dates: %w", err)
	}
	if len(specialDates) == 0 {
		return nil
	}

	confidence, err := s.GetForecastConfidence(ctx)
	if err != nil {
		return fmt.Errorf("getting forecast confidence: %w", err)
	}
	samples, err := s.GetForecastSamples(ctx)
	if err != nil {
		return fmt.Errorf("getting forecast samples: %w", err)
	}
	snapshotInterval, err := s.GetForecastSnapshotInterval(ctx)
	if err != nil {
		return fmt.Errorf("getting forecast snapshot interval: %w", err)
	}

	// Find last special date as end date
	sort.Slice(specialDates, func(i, j int) bool {
		return specialDates[i].Date.Before(specialDates[j].Date)
	})
	endDate := specialDates[len(specialDates)-1].Date

	today := date.Today()
	if !endDate.After(today) {
		return nil
	}

	duration := endDate.Sub(today)

	// Clear old cache before starting the prediction
	if err := s.q.DeleteAllForecastCache(ctx); err != nil {
		return fmt.Errorf("deleting old forecast cache: %w", err)
	}

	handler := &forecastCacheEventHandler{
		ctx:    ctx,
		q:      s.q,
		runner: s.forecastRunner,
	}

	params := PredictionParams{
		Duration:         duration,
		Samples:          samples,
		Quantile:         confidence,
		SnapshotInterval: date.Cron(snapshotInterval),
		GroupBy:          GroupByType,
	}

	if err := s.RunPrediction(ctx, handler, params); err != nil {
		return fmt.Errorf("running prediction for forecast cache: %w", err)
	}

	if s.forecastRunner != nil {
		s.forecastRunner.Broadcast(ForecastEvent{Type: ForecastEventDone})
	}

	return nil
}

type ForecastDashboardData struct {
	Entities  []PredictionFinancialEntity `json:"entities"`
	Marklines []Markline                  `json:"marklines"`
}

func (s *Service) GetForecastCacheForDashboard(ctx context.Context) (*ForecastDashboardData, error) {
	accountTypes, err := s.ListAccountTypes(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing account types: %w", err)
	}
	typesByID := make(map[string]AccountType)
	for _, at := range accountTypes {
		typesByID[at.ID] = at
	}

	// Build entities from historic snapshot data
	history, err := s.buildSnapshotHistoryChart(ctx, accountTypes)
	if err != nil {
		return nil, fmt.Errorf("building snapshot history: %w", err)
	}
	entitiesByName := make(map[string]*PredictionFinancialEntity)
	for _, series := range history.Series {
		entity := &PredictionFinancialEntity{
			Name:  series.Name,
			Color: series.Color,
		}
		for i, dateStr := range history.Dates {
			d, err := date.ParseDate(dateStr)
			if err != nil {
				continue
			}
			entity.Snapshots = append(entity.Snapshots, PredictionBalanceSnapshot{
				Day:     d.ToStdTime().UnixMilli(),
				Balance: series.Data[i],
			})
		}
		entitiesByName[series.Name] = entity
	}

	// Append forecast cache rows
	rows, err := s.ListForecastCache(ctx)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		at, exists := typesByID[row.AccountTypeID]
		if !exists {
			continue
		}
		entity, ok := entitiesByName[at.Name]
		if !ok {
			entity = &PredictionFinancialEntity{
				ID:    row.AccountTypeID,
				Name:  at.Name,
				Color: at.Color,
			}
			entitiesByName[at.Name] = entity
		}
		if entity.ID == "" {
			entity.ID = row.AccountTypeID
		}
		entity.Snapshots = append(entity.Snapshots, PredictionBalanceSnapshot{
			ID:      row.AccountTypeID,
			Day:     row.Date,
			Balance: row.Median,
		})
	}

	entities := make([]PredictionFinancialEntity, 0, len(entitiesByName))
	for _, e := range entitiesByName {
		entities = append(entities, *e)
	}

	// Build marklines from special dates
	specialDates, err := s.ListSpecialDates(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing special dates: %w", err)
	}
	marklines := make([]Markline, 0, len(specialDates)+1)
	marklines = append(marklines, Markline{
		Date: date.Today().ToStdTime().UnixMilli(),
		Name: "Today",
	})
	for _, sd := range specialDates {
		marklines = append(marklines, Markline{
			Date:  sd.Date.ToStdTime().UnixMilli(),
			Color: sd.Color,
			Name:  sd.Name,
		})
	}

	return &ForecastDashboardData{
		Entities:  entities,
		Marklines: marklines,
	}, nil
}

// forecastCacheEventHandler writes each snapshot to the DB and broadcasts to subscribers as it arrives.
type forecastCacheEventHandler struct {
	q      *pdb.Queries
	runner *ForecastRunner
	ctx    context.Context
}

func (h *forecastCacheEventHandler) Setup(e PredictionSetupEvent) error {
	return nil
}

func (h *forecastCacheEventHandler) Snapshot(snap PredictionBalanceSnapshot) error {
	row := ForecastCacheRow{
		Date:          snap.Day,
		AccountTypeID: snap.ID,
		Median:        snap.Balance,
		LowerBound:    snap.LowerBound,
		UpperBound:    snap.UpperBound,
	}
	if err := h.q.InsertForecastCache(h.ctx, pdb.InsertForecastCacheParams{
		Date:          row.Date,
		AccountTypeID: row.AccountTypeID,
		Median:        row.Median,
		LowerBound:    row.LowerBound,
		UpperBound:    row.UpperBound,
	}); err != nil {
		return fmt.Errorf("inserting forecast cache row: %w", err)
	}
	if h.runner != nil {
		h.runner.Broadcast(ForecastEvent{
			Type:     ForecastEventSnapshot,
			Snapshot: &row,
		})
	}
	return nil
}

func (h *forecastCacheEventHandler) Close() error {
	return nil
}
