package model

import (
	"context"
	"fmt"
	"time"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/goslu/sid"
	"github.com/SimonSchneider/pefigo/internal/pdb"
)

type SweYearlyParams struct {
	ID            string
	Amount        float64
	Prisbasbelopp float64
	SchablonRanta float64
	IskFribelopp  float64
	ValidFrom     date.Date
}

func sweYearlyParamsFromDB(row pdb.SweYearlyParam) SweYearlyParams {
	return SweYearlyParams{
		ID:            row.ID,
		Amount:        row.Amount,
		Prisbasbelopp: row.Prisbasbelopp,
		SchablonRanta: row.SchablonRanta,
		IskFribelopp:  row.IskFribelopp,
		ValidFrom:     date.Date(row.ValidFrom),
	}
}

func (s *Service) ListSweYearlyParams(ctx context.Context) ([]SweYearlyParams, error) {
	rows, err := s.q.ListSweYearlyParams(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing swe yearly params: %w", err)
	}
	result := make([]SweYearlyParams, len(rows))
	for i, r := range rows {
		result[i] = sweYearlyParamsFromDB(r)
	}
	return result, nil
}

func (s *Service) GetSweYearlyParams(ctx context.Context, id string) (SweYearlyParams, error) {
	row, err := s.q.GetSweYearlyParams(ctx, id)
	if err != nil {
		return SweYearlyParams{}, fmt.Errorf("getting swe yearly params: %w", err)
	}
	return sweYearlyParamsFromDB(row), nil
}

func (s *Service) UpsertSweYearlyParams(ctx context.Context, inp SweYearlyParams) (SweYearlyParams, error) {
	if err := checkRequired("valid from", inp.ValidFrom); err != nil {
		return SweYearlyParams{}, err
	}
	if inp.ID == "" {
		inp.ID = sid.MustNewString(32)
	}
	now := time.Now().Unix()
	row, err := s.q.UpsertSweYearlyParams(ctx, pdb.UpsertSweYearlyParamsParams{
		ID:            inp.ID,
		Amount:        inp.Amount,
		Prisbasbelopp: inp.Prisbasbelopp,
		SchablonRanta: inp.SchablonRanta,
		IskFribelopp:  inp.IskFribelopp,
		ValidFrom:     int64(inp.ValidFrom),
		CreatedAt:     now,
		UpdatedAt:     now,
	})
	if err != nil {
		return SweYearlyParams{}, fmt.Errorf("upserting swe yearly params: %w", err)
	}
	return sweYearlyParamsFromDB(row), nil
}

func (s *Service) DeleteSweYearlyParams(ctx context.Context, id string) error {
	if err := s.q.DeleteSweYearlyParams(ctx, id); err != nil {
		return fmt.Errorf("deleting swe yearly params: %w", err)
	}
	return nil
}

// activeIBBAt returns the IBB value active at a given date (the latest one with ValidFrom <= d).
func activeIBBAt(params []SweYearlyParams, d date.Date) float64 {
	var active float64
	for _, p := range params {
		if p.ValidFrom <= d {
			active = p.Amount
		}
	}
	return active
}

// activePBBAt returns the PBB (prisbasbelopp) value active at a given date.
func activePBBAt(params []SweYearlyParams, d date.Date) float64 {
	var active float64
	for _, p := range params {
		if p.ValidFrom <= d {
			active = p.Prisbasbelopp
		}
	}
	return active
}

// activeSchablonRantaAt returns the schablonränta active at a given date.
func activeSchablonRantaAt(params []SweYearlyParams, d date.Date) float64 {
	var active float64
	for _, p := range params {
		if p.ValidFrom <= d {
			active = p.SchablonRanta
		}
	}
	return active
}

// activeISKFribeloppAt returns the ISK fribelopp active at a given date.
func activeISKFribeloppAt(params []SweYearlyParams, d date.Date) float64 {
	var active float64
	for _, p := range params {
		if p.ValidFrom <= d {
			active = p.IskFribelopp
		}
	}
	return active
}
