package model

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/goslu/sid"
	"github.com/SimonSchneider/pefigo/internal/pdb"
	"github.com/SimonSchneider/pefigo/pkg/finance"
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

type AccountGrowthModelInput struct {
	ID               string
	AccountID        string
	Type             string
	AnnualRate       uncertain.Value
	AnnualVolatility uncertain.Value
	StartDate        date.Date
	EndDate          *date.Date
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

func growthModelFromDB(g pdb.GrowthModel) (GrowthModel, error) {
	var annualRate, annualVolatility uncertain.Value
	if err := annualRate.Decode(g.AnnualGrowthRate); err != nil {
		return GrowthModel{}, fmt.Errorf("decoding annual growth rate: %w", err)
	}
	if err := annualVolatility.Decode(g.AnnualVolatility); err != nil {
		return GrowthModel{}, fmt.Errorf("decoding annual volatility: %w", err)
	}
	var endDate *date.Date
	if g.EndDate != nil {
		d := date.Date(*g.EndDate)
		endDate = &d
	}
	return GrowthModel{
		ID:               g.ID,
		AccountID:        g.AccountID,
		Type:             g.ModelType,
		AnnualRate:       annualRate,
		AnnualVolatility: annualVolatility,
		StartDate:        date.Date(g.StartDate),
		EndDate:          endDate,
	}, nil
}

func (s *Service) UpsertAccountGrowthModel(ctx context.Context, inp AccountGrowthModelInput) (GrowthModel, error) {
	if err := errors.Join(checkRequired("account", inp.AccountID), checkRequired("start date", inp.StartDate)); err != nil {
		return GrowthModel{}, err
	}
	if err := checkOneOf("growth model type", inp.Type, GrowthModelFixed, GrowthModelLogNormal); err != nil {
		return GrowthModel{}, err
	}
	if err := checkEndDate(inp.StartDate, inp.EndDate); err != nil {
		return GrowthModel{}, err
	}
	var endDate *int64
	if inp.EndDate != nil {
		endDate = ptr(int64(*inp.EndDate))
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
		StartDate:        int64(inp.StartDate),
		EndDate:          endDate,
		CreatedAt:        time.Now().UnixMilli(),
		UpdatedAt:        time.Now().UnixMilli(),
	})
	if err != nil {
		return GrowthModel{}, fmt.Errorf("failed to upsert growth model: %w", err)
	}
	return growthModelFromDB(gm)
}

func (s *Service) ListAccountGrowthModels(ctx context.Context, accountID string) ([]GrowthModel, error) {
	gms, err := s.q.GetGrowthModelsByAccount(ctx, accountID)
	if err != nil {
		return nil, fmt.Errorf("failed to list account growth models: %w", err)
	}
	gmList := make([]GrowthModel, len(gms))
	for i, g := range gms {
		gm, err := growthModelFromDB(g)
		if err != nil {
			return nil, fmt.Errorf("failed to convert growth model from db: %w", err)
		}
		gmList[i] = gm
	}
	return gmList, nil
}

// growthModelsAppliedOn returns, per Account, the growth model the forecast applies on day: the
// latest-starting one that applies, as finance.GrowthCombined picks it.
func (s *Service) growthModelsAppliedOn(ctx context.Context, day date.Date) (map[string]pdb.GrowthModel, error) {
	gms, err := s.q.ListGrowthModels(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing growth models: %w", err)
	}
	applied := make(map[string]pdb.GrowthModel)
	for _, g := range gms {
		gm, err := growthModelFromDB(g)
		if err != nil {
			return nil, fmt.Errorf("converting growth model from db: %w", err)
		}
		if gm.AppliesOn(day) {
			applied[g.AccountID] = g
		}
	}
	return applied, nil
}

func (s *Service) DeleteAccountGrowthModel(ctx context.Context, id string) error {
	if err := s.q.DeleteGrowthModel(ctx, id); err != nil {
		return fmt.Errorf("failed to delete account growth model: %w", err)
	}
	return nil
}

func (s *Service) GetGrowthModel(ctx context.Context, id string) (GrowthModel, error) {
	g, err := s.q.GetGrowthModel(ctx, id)
	if err != nil {
		return GrowthModel{}, fmt.Errorf("failed to get account growth model: %w", err)
	}
	gm, err := growthModelFromDB(g)
	if err != nil {
		return GrowthModel{}, fmt.Errorf("failed to convert growth model from db: %w", err)
	}
	return gm, nil
}
