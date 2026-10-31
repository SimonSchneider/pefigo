package model

import (
	"context"
	"fmt"
	"time"

	"github.com/SimonSchneider/goslu/sid"
	"github.com/SimonSchneider/pefigo/internal/pdb"
)

type TransferTemplateCategory struct {
	ID        string
	Name      string
	Color     *string
	CreatedAt int64
	UpdatedAt int64
}

type TransferTemplateCategoryInput struct {
	ID    string
	Name  string
	Color *string
}

func categoryFromDB(c pdb.TransferTemplateCategory) TransferTemplateCategory {
	return TransferTemplateCategory{
		ID:        c.ID,
		Name:      c.Name,
		Color:     c.Color,
		CreatedAt: c.CreatedAt,
		UpdatedAt: c.UpdatedAt,
	}
}

func (s *Service) ListCategories(ctx context.Context) ([]TransferTemplateCategory, error) {
	categories, err := s.q.ListTransferTemplateCategories(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list categories: %w", err)
	}
	var result []TransferTemplateCategory
	for _, c := range categories {
		result = append(result, categoryFromDB(c))
	}
	return result, nil
}

func (s *Service) GetCategory(ctx context.Context, id string) (TransferTemplateCategory, error) {
	c, err := s.q.GetTransferTemplateCategory(ctx, id)
	if err != nil {
		return TransferTemplateCategory{}, fmt.Errorf("failed to get category: %w", err)
	}
	return categoryFromDB(c), nil
}

func (s *Service) UpsertCategory(ctx context.Context, inp TransferTemplateCategoryInput) (TransferTemplateCategory, error) {
	if err := checkRequired("name", inp.Name); err != nil {
		return TransferTemplateCategory{}, err
	}
	now := time.Now().Unix()
	id := inp.ID
	createdAt := now
	if id == "" {
		id = sid.MustNewString(32)
	}

	c, err := s.q.UpsertTransferTemplateCategory(ctx, pdb.UpsertTransferTemplateCategoryParams{
		ID:        id,
		Name:      inp.Name,
		Color:     inp.Color,
		CreatedAt: createdAt,
		UpdatedAt: now,
	})
	if err != nil {
		return TransferTemplateCategory{}, fmt.Errorf("failed to upsert category: %w", err)
	}
	return categoryFromDB(c), nil
}

func (s *Service) DeleteCategory(ctx context.Context, id string) error {
	if err := s.q.DeleteTransferTemplateCategory(ctx, id); err != nil {
		return fmt.Errorf("failed to delete category: %w", err)
	}
	return nil
}

type TransferTemplateCategoryEditView struct {
	Category TransferTemplateCategory
}

func (v TransferTemplateCategoryEditView) IsEdit() bool {
	return v.Category.ID != ""
}
