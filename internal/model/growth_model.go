package model

import (
	"context"
	"fmt"
	"time"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/goslu/sid"
	"github.com/SimonSchneider/pefigo/internal/pdb"
	"github.com/SimonSchneider/pefigo/pkg/finance"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
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

func (gms GrowthModels) ToFinance() finance.GrowthModel {
	fgms := make([]finance.GrowthModel, 0, len(gms))
	for _, gm := range gms {
		switch gm.Type {
		case "fixed":
			fgms = append(fgms, &finance.FixedGrowth{
				TimeFrameGrowth: finance.TimeFrameGrowth{
					StartDate: gm.StartDate,
					EndDate:   gm.EndDate,
				},
				AnnualRate: gm.AnnualRate,
			})
		case "lognormal":
			fgms = append(fgms, &finance.LogNormalGrowth{
				TimeFrameGrowth: finance.TimeFrameGrowth{
					StartDate: gm.StartDate,
					EndDate:   gm.EndDate,
				},
				AnnualRate:       gm.AnnualRate,
				AnnualVolatility: gm.AnnualVolatility,
			})
		}
	}
	return finance.NewGrowthCombined(fgms...)
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
	var endDate *int64
	if inp.EndDate != nil {
		endDate = ptr(int64(*inp.EndDate))
	}
	if inp.ID == "" {
		inp.ID = sid.MustNewString(32)
	}
	annualRate, err := inp.AnnualRate.Encode()
	if err != nil {
		return GrowthModel{}, fmt.Errorf("encoding annual rate: %w", err)
	}
	annualVolatility, err := inp.AnnualVolatility.Encode()
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
