package model

import (
	"context"
	"fmt"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/goslu/sid"
	"github.com/SimonSchneider/pefigo/internal/pdb"
	"github.com/SimonSchneider/pefigo/pkg/ui"
)

type SpecialDate struct {
	ID    string
	Name  string
	Date  date.Date
	Color string
}

type SpecialDateInput struct {
	ID    string
	Name  string
	Date  date.Date
	Color string
}

func specialDateFromDB(sd pdb.SpecialDate) SpecialDate {
	day, err := date.ParseDate(sd.Date)
	if err != nil {
		panic(fmt.Errorf("parsing date: %w", err))
	}
	return SpecialDate{
		ID:    sd.ID,
		Name:  sd.Name,
		Date:  day,
		Color: ui.OrDefault(sd.Color),
	}
}

func (s *Service) GetSpecialDate(ctx context.Context, id string) (SpecialDate, error) {
	sd, err := s.q.GetSpecialDate(ctx, id)
	if err != nil {
		return SpecialDate{}, fmt.Errorf("failed to get special date: %w", err)
	}
	return specialDateFromDB(sd), nil
}

func (s *Service) UpsertSpecialDate(ctx context.Context, inp SpecialDateInput) (SpecialDate, error) {
	id := inp.ID
	if id == "" {
		id = sid.MustNewString(15)
	}
	sd, err := s.q.UpsertSpecialDate(ctx, pdb.UpsertSpecialDateParams{
		ID:    id,
		Name:  inp.Name,
		Date:  inp.Date.String(),
		Color: ui.WithDefaultNull(inp.Color),
	})
	if err != nil {
		return SpecialDate{}, fmt.Errorf("failed to upsert special date: %w", err)
	}
	return specialDateFromDB(sd), nil
}

func (s *Service) DeleteSpecialDate(ctx context.Context, id string) error {
	err := s.q.DeleteSpecialDate(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to delete special date: %w", err)
	}
	return nil
}

func (s *Service) ListSpecialDates(ctx context.Context) ([]SpecialDate, error) {
	sds, err := s.q.GetSpecialDates(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]SpecialDate, len(sds))
	for i := range sds {
		result[i] = specialDateFromDB(sds[i])
	}
	return result, nil
}
