package model

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/goslu/sid"
	"github.com/SimonSchneider/pefigo/internal/pdb"
	"github.com/SimonSchneider/pefigo/pkg/finance"
	"github.com/SimonSchneider/pefigo/pkg/ui"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

type TransferTemplateSource struct {
	Type     string // "", "salary" (empty = DB)
	EntityID string
	Label    string
	EditURL  string
}

func (s TransferTemplateSource) IsGenerated() bool {
	return s.Type != ""
}

type TransferTemplate struct {
	ID            string
	Name          string
	FromAccountID string
	ToAccountID   string
	AmountType    string
	AmountFixed   uncertain.Value
	AmountPercent float64
	Priority      int64
	Recurrence    date.Cron

	StartDate        date.Date
	EndDate          *date.Date
	Enabled          bool
	BudgetCategoryID *string
	GroupMembers     []TransferTemplate

	Source TransferTemplateSource
}

func (t *TransferTemplate) ToFinanceTransferTemplate() finance.TransferTemplate {
	return finance.TransferTemplate{
		ID:            t.ID,
		Name:          t.Name,
		FromAccountID: t.FromAccountID,
		ToAccountID:   t.ToAccountID,
		AmountType:    finance.TransferAmountType(t.AmountType),
		AmountFixed: finance.TransferFixed{
			Amount: t.AmountFixed,
		},
		AmountPercent: finance.TransferPercent{
			Percent: t.AmountPercent,
		},
		Priority:      t.Priority,
		EffectiveFrom: t.StartDate,
		EffectiveTo:   t.EndDate,
		Recurrence:    t.Recurrence,
		Enabled:       t.Enabled,
	}
}

func transferTemplateFromDB(t pdb.TransferTemplate) (TransferTemplate, error) {
	var endDate *date.Date
	if t.EndDate != nil {
		d := date.Date(*t.EndDate)
		endDate = &d
	}
	var amountFixed uncertain.Value
	if err := amountFixed.Decode(t.AmountFixed); err != nil {
		return TransferTemplate{}, fmt.Errorf("decoding amount fixed: %w", err)
	}
	return TransferTemplate{
		ID:               t.ID,
		Name:             t.Name,
		FromAccountID:    ui.OrDefault(t.FromAccountID),
		ToAccountID:      ui.OrDefault(t.ToAccountID),
		AmountType:       t.AmountType,
		AmountFixed:      amountFixed,
		AmountPercent:    t.AmountPercent,
		Priority:         t.Priority,
		Recurrence:       date.Cron(t.Recurrence),
		StartDate:        date.Date(t.StartDate),
		EndDate:          endDate,
		Enabled:          t.Enabled,
		BudgetCategoryID: t.BudgetCategoryID,
	}, nil
}

type TransferTemplateInput struct {
	ID               string
	Name             string
	FromAccountID    string
	ToAccountID      string
	AmountType       string
	AmountFixed      uncertain.Value
	AmountPercent    float64
	Priority         int64
	Recurrence       date.Cron
	StartDate        date.Date
	EndDate          *date.Date
	Enabled          bool
	BudgetCategoryID *string
}

func (s *Service) UpsertTransferTemplate(ctx context.Context, inp TransferTemplate) (TransferTemplate, error) {
	var endDate *int64
	if inp.EndDate != nil {
		d := int64(*inp.EndDate)
		endDate = &d
	}
	amountFixed, err := inp.AmountFixed.Encode()
	if err != nil {
		return TransferTemplate{}, fmt.Errorf("encoding amount fixed: %w", err)
	}
	if inp.ID == "" {
		inp.ID = sid.MustNewString(32)
	}
	t, err := s.q.UpsertTransferTemplate(ctx, pdb.UpsertTransferTemplateParams{
		ID:               inp.ID,
		Name:             inp.Name,
		FromAccountID:    ui.WithDefaultNull(inp.FromAccountID),
		ToAccountID:      ui.WithDefaultNull(inp.ToAccountID),
		AmountType:       inp.AmountType,
		AmountFixed:      amountFixed,
		AmountPercent:    inp.AmountPercent,
		Priority:         inp.Priority,
		Recurrence:       string(inp.Recurrence),
		StartDate:        int64(inp.StartDate),
		EndDate:          endDate,
		Enabled:          inp.Enabled,
		BudgetCategoryID: inp.BudgetCategoryID,
		CreatedAt:        time.Now().Unix(),
		UpdatedAt:        time.Now().Unix(),
	})
	if err != nil {
		return TransferTemplate{}, fmt.Errorf("failed to upsert template: %w", err)
	}
	return transferTemplateFromDB(t)
}

func (s *Service) DuplicateTransferTemplate(ctx context.Context, id string) (TransferTemplate, error) {
	t, err := s.GetTransferTemplate(ctx, id)
	if err != nil {
		return TransferTemplate{}, fmt.Errorf("failed to get transfer template: %w", err)
	}
	t.ID = ""
	return s.UpsertTransferTemplate(ctx, t)
}

func (s *Service) ListTransferTemplates(ctx context.Context) ([]TransferTemplate, error) {
	templates, err := s.q.GetTransferTemplates(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list templates: %w", err)
	}
	var parsedTemplates []TransferTemplate
	for _, t := range templates {
		template, err := transferTemplateFromDB(t)
		if err != nil {
			return nil, fmt.Errorf("converting transfer template from DB: %w", err)
		}
		parsedTemplates = append(parsedTemplates, template)
	}
	return parsedTemplates, nil
}

func (s *Service) ListAllTransferTemplates(ctx context.Context) ([]TransferTemplate, error) {
	templates, err := s.ListTransferTemplates(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing DB transfer templates: %w", err)
	}
	salaryTemplates, err := s.generateSalaryTransferTemplates(ctx)
	if err != nil {
		return nil, fmt.Errorf("generating salary transfer templates: %w", err)
	}
	billTemplates, err := s.generateBillTransferTemplates(ctx)
	if err != nil {
		return nil, fmt.Errorf("generating bill transfer templates: %w", err)
	}
	all := append(templates, salaryTemplates...)
	all = append(all, billTemplates...)
	sortTransferTemplates(all)
	return all, nil
}

func sortTransferTemplates(ts []TransferTemplate) {
	sort.SliceStable(ts, func(i, j int) bool {
		ri, rj := string(ts[i].Recurrence), string(ts[j].Recurrence)
		if ri != rj {
			return ri < rj
		}
		if ts[i].Priority != ts[j].Priority {
			return ts[i].Priority < ts[j].Priority
		}
		if ts[i].Name != ts[j].Name {
			return ts[i].Name < ts[j].Name
		}
		if ts[i].StartDate != ts[j].StartDate {
			return ts[i].StartDate < ts[j].StartDate
		}
		return false
	})
}

func autoGroupTransferTemplates(templates []TransferTemplate) []TransferTemplate {
	type groupKey struct {
		Name          string
		Priority      int64
		Recurrence    string
		FromAccountID string
		ToAccountID   string
	}

	type groupEntry struct {
		indices []int
	}

	groups := make(map[groupKey]*groupEntry)
	var keyOrder []groupKey

	for i, t := range templates {
		k := groupKey{
			Name:          t.Name,
			Priority:      t.Priority,
			Recurrence:    string(t.Recurrence),
			FromAccountID: t.FromAccountID,
			ToAccountID:   t.ToAccountID,
		}
		if entry, ok := groups[k]; ok {
			entry.indices = append(entry.indices, i)
		} else {
			groups[k] = &groupEntry{indices: []int{i}}
			keyOrder = append(keyOrder, k)
		}
	}

	result := make([]TransferTemplate, 0, len(keyOrder))
	for _, k := range keyOrder {
		indices := groups[k].indices
		if len(indices) == 1 {
			result = append(result, templates[indices[0]])
		} else {
			// Build a virtual group entry: all real members go into GroupMembers,
			// the group row itself has no ID and spans the full date range.
			first := templates[indices[0]]
			minStart := first.StartDate
			maxEnd := first.EndDate
			members := make([]TransferTemplate, 0, len(indices))
			for _, idx := range indices {
				m := templates[idx]
				members = append(members, m)
				if m.StartDate < minStart {
					minStart = m.StartDate
				}
				if maxEnd != nil {
					if m.EndDate == nil {
						maxEnd = nil
					} else if *m.EndDate > *maxEnd {
						maxEnd = m.EndDate
					}
				}
			}
			virtual := TransferTemplate{
				ID:               "group-" + sid.MustNewString(16),
				Name:             k.Name,
				FromAccountID:    k.FromAccountID,
				ToAccountID:      k.ToAccountID,
				Priority:         k.Priority,
				Recurrence:       date.Cron(k.Recurrence),
				BudgetCategoryID: first.BudgetCategoryID,
				Source:           first.Source,
				Enabled:          true,
				StartDate:        minStart,
				EndDate:          maxEnd,
				GroupMembers:     members,
			}
			result = append(result, virtual)
		}
	}
	return result
}

func (s *Service) ListAllTransferTemplatesWithChildren(ctx context.Context) ([]TransferTemplate, error) {
	templates, err := s.ListAllTransferTemplates(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list all templates: %w", err)
	}
	return autoGroupTransferTemplates(templates), nil
}

func (s *Service) GetTransferTemplate(ctx context.Context, id string) (TransferTemplate, error) {
	t, err := s.q.GetTransferTemplate(ctx, id)
	if err != nil {
		return TransferTemplate{}, fmt.Errorf("failed to get transfer template: %w", err)
	}
	return transferTemplateFromDB(t)
}

func (s *Service) DeleteTransferTemplate(ctx context.Context, id string) error {
	if err := s.q.DeleteTransferTemplate(ctx, id); err != nil {
		return fmt.Errorf("failed to delete template: %w", err)
	}
	return nil
}
