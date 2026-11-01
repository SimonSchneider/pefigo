package model

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/goslu/sid"
	"github.com/SimonSchneider/pefigo/internal/pdb"
	"github.com/SimonSchneider/pefigo/pkg/finance"
	"github.com/SimonSchneider/pefigo/pkg/ui"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

// The Growth Model types (GrowthModel.Type).
const (
	GrowthModelFixed     = "fixed"
	GrowthModelLogNormal = "lognormal"
)

type GrowthModel struct {
	ID               string
	AccountID        string
	Type             string
	AnnualRate       uncertain.Value
	AnnualVolatility uncertain.Value
	StartDate        date.Date
	EndDate          *date.Date
	// EventID is the Event the growth model is in, or "" in the Baseline. In an Event its Start and
	// End Dates are stored as offsets from the Event's date.
	EventID string
}

func (gm GrowthModel) GetEndDateString() string {
	if gm.ID == "" || gm.EndDate == nil {
		return ""
	}
	return gm.EndDate.String()
}

// AppliesOn reports whether the growth model applies on day, by the rule the forecast uses.
func (gm GrowthModel) AppliesOn(day date.Date) bool {
	return (&finance.TimeFrameGrowth{StartDate: gm.StartDate, EndDate: gm.EndDate}).IsActiveOn(day)
}

// AccountGrowthModelInput is a growth model to save. In an Event its dates may be given as days after
// the Event's date, its Start Date is the Event's date when it isn't given, and its dates are saved as
// offsets from the Event's date.
type AccountGrowthModelInput struct {
	ID               string
	AccountID        string
	Type             string
	AnnualRate       uncertain.Value
	AnnualVolatility uncertain.Value
	StartDate        date.Date
	EndDate          *date.Date
	EventID          string
	// StartOffset and EndOffset, when set, give the Start and End Dates in an Event as days after the
	// Event's date, in place of StartDate and EndDate.
	StartOffset *date.Duration
	EndOffset   *date.Duration
	// OffsetEventID is the Event StartOffset and EndOffset are days after, as for a Transfer Template.
	// It is EventID when blank.
	OffsetEventID string
}

// input is the growth model as an input that saves it unchanged.
func (gm GrowthModel) input() AccountGrowthModelInput {
	return AccountGrowthModelInput{
		ID: gm.ID, AccountID: gm.AccountID, Type: gm.Type, AnnualRate: gm.AnnualRate, AnnualVolatility: gm.AnnualVolatility,
		StartDate: gm.StartDate, EndDate: gm.EndDate, EventID: gm.EventID,
	}
}

type GrowthModels []GrowthModel

// ToFinance is the growth the forecast applies. A stored growth model of an unknown type is an error
// naming it.
func (gms GrowthModels) ToFinance() (finance.GrowthModel, error) {
	fgms := make([]finance.GrowthModel, 0, len(gms))
	for _, gm := range gms {
		switch gm.Type {
		case GrowthModelFixed:
			fgms = append(fgms, &finance.FixedGrowth{
				TimeFrameGrowth: finance.TimeFrameGrowth{
					StartDate: gm.StartDate,
					EndDate:   gm.EndDate,
				},
				AnnualRate: gm.AnnualRate,
			})
		case GrowthModelLogNormal:
			fgms = append(fgms, &finance.LogNormalGrowth{
				TimeFrameGrowth: finance.TimeFrameGrowth{
					StartDate: gm.StartDate,
					EndDate:   gm.EndDate,
				},
				AnnualRate:       gm.AnnualRate,
				AnnualVolatility: gm.AnnualVolatility,
			})
		default:
			return nil, fmt.Errorf("growth model %s has unknown type %q", gm.ID, gm.Type)
		}
	}
	return finance.NewGrowthCombined(fgms...), nil
}

// growthModelFromDB is the stored growth model g with its Event at at: its stored dates are offsets
// from at.
func growthModelFromDB(g pdb.GrowthModel, at date.Date) (GrowthModel, error) {
	var annualRate, annualVolatility uncertain.Value
	if err := annualRate.Decode(g.AnnualGrowthRate); err != nil {
		return GrowthModel{}, fmt.Errorf("decoding annual growth rate: %w", err)
	}
	if err := annualVolatility.Decode(g.AnnualVolatility); err != nil {
		return GrowthModel{}, fmt.Errorf("decoding annual volatility: %w", err)
	}
	return GrowthModel{
		ID:               g.ID,
		AccountID:        g.AccountID,
		Type:             g.ModelType,
		AnnualRate:       annualRate,
		AnnualVolatility: annualVolatility,
		StartDate:        placeDate(g.StartDate, at),
		EndDate:          placeEndDate(g.EndDate, at),
		EventID:          ui.OrDefault(g.EventID),
	}, nil
}

func (s *Service) UpsertAccountGrowthModel(ctx context.Context, inp AccountGrowthModelInput) (GrowthModel, error) {
	at, err := s.eventDate(ctx, inp.EventID)
	if err != nil {
		return GrowthModel{}, err
	}
	offsetEventID, offsetAt, err := s.offsetEvent(ctx, inp.OffsetEventID, inp.EventID, at)
	if err != nil {
		return GrowthModel{}, err
	}
	startDate, startErr := givenDate("start", inp.StartDate, inp.StartOffset, offsetEventID, offsetAt)
	endDate, endErr := givenEndDate(inp.EndDate, inp.EndOffset, offsetEventID, offsetAt)
	if err := errors.Join(startErr, endErr); err != nil {
		return GrowthModel{}, err
	}
	inp.StartDate, inp.EndDate = startDate, endDate
	if inp.EventID != "" && inp.StartDate.IsZero() {
		inp.StartDate = at
	}
	if err := errors.Join(checkRequired("account", inp.AccountID), checkRequired("start date", inp.StartDate)); err != nil {
		return GrowthModel{}, err
	}
	if err := checkOneOf("growth model type", inp.Type, GrowthModelFixed, GrowthModelLogNormal); err != nil {
		return GrowthModel{}, err
	}
	if err := checkEndDate(inp.StartDate, inp.EndDate); err != nil {
		return GrowthModel{}, err
	}
	if inp.ID == "" {
		inp.ID = sid.MustNewString(32)
	}
	annualRate, err := orZero(inp.AnnualRate).Encode()
	if err != nil {
		return GrowthModel{}, fmt.Errorf("encoding annual rate: %w", err)
	}
	annualVolatility, err := orZero(inp.AnnualVolatility).Encode()
	if err != nil {
		return GrowthModel{}, fmt.Errorf("encoding annual volatility: %w", err)
	}
	gm, err := s.q.UpsertGrowthModel(ctx, pdb.UpsertGrowthModelParams{
		ID:               inp.ID,
		AccountID:        inp.AccountID,
		ModelType:        inp.Type,
		AnnualGrowthRate: annualRate,
		AnnualVolatility: annualVolatility,
		StartDate:        storeDate(inp.StartDate, at),
		EndDate:          storeEndDate(inp.EndDate, at),
		EventID:          ui.WithDefaultNull(inp.EventID),
		CreatedAt:        time.Now().UnixMilli(),
		UpdatedAt:        time.Now().UnixMilli(),
	})
	if err != nil {
		return GrowthModel{}, fmt.Errorf("failed to upsert growth model: %w", err)
	}
	return growthModelFromDB(gm, at)
}

// ListAccountGrowthModels returns the account's growth models, each in an Event at the Event's date.
func (s *Service) ListAccountGrowthModels(ctx context.Context, accountID string) ([]GrowthModel, error) {
	gms, err := s.q.GetGrowthModelsByAccount(ctx, accountID)
	if err != nil {
		return nil, fmt.Errorf("failed to list account growth models: %w", err)
	}
	dates, err := s.eventDates(ctx)
	if err != nil {
		return nil, err
	}
	gmList := make([]GrowthModel, len(gms))
	for i, g := range gms {
		gm, err := growthModelFromDB(g, ownDate(dates, g.EventID))
		if err != nil {
			return nil, fmt.Errorf("failed to convert growth model from db: %w", err)
		}
		gmList[i] = gm
	}
	slices.SortStableFunc(gmList, compareGrowthModels)
	return gmList, nil
}

// compareGrowthModels orders growth models by Start Date, then ID: the order the forecast applies them
// in, where the last that applies on a day wins.
func compareGrowthModels(a, b GrowthModel) int {
	return cmp.Or(cmp.Compare(a.StartDate, b.StartDate), cmp.Compare(a.ID, b.ID))
}

// growthModelsAppliedOn returns, per Account, the growth model of p the forecast applies on day: the
// latest-starting one that applies, as finance.GrowthCombined picks it.
func growthModelsAppliedOn(p *plans, day date.Date) map[string]GrowthModel {
	applied := make(map[string]GrowthModel)
	for _, gm := range p.growthModels {
		current, ok := applied[gm.AccountID]
		if gm.AppliesOn(day) && (!ok || !gm.StartDate.Before(current.StartDate)) {
			applied[gm.AccountID] = gm
		}
	}
	return applied
}

func (s *Service) DeleteAccountGrowthModel(ctx context.Context, id string) error {
	if err := s.q.DeleteGrowthModel(ctx, id); err != nil {
		return fmt.Errorf("failed to delete account growth model: %w", err)
	}
	return nil
}

// GetGrowthModel returns the growth model, in an Event at the Event's date.
func (s *Service) GetGrowthModel(ctx context.Context, id string) (GrowthModel, error) {
	g, err := s.q.GetGrowthModel(ctx, id)
	if err != nil {
		return GrowthModel{}, fmt.Errorf("failed to get account growth model: %w", err)
	}
	at, err := s.eventDate(ctx, ui.OrDefault(g.EventID))
	if err != nil {
		return GrowthModel{}, err
	}
	gm, err := growthModelFromDB(g, at)
	if err != nil {
		return GrowthModel{}, fmt.Errorf("failed to convert growth model from db: %w", err)
	}
	return gm, nil
}
