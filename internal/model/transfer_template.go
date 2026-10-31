package model

import (
	"context"
	"errors"
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

// The types of what a generated Transfer Template is generated from (TransferTemplateSource.Type).
const (
	TransferSourceSalary = "salary"
	TransferSourceBill   = "bill"
)

// TransferTemplateSource is what a Transfer Template is generated from: a Salary or a Bill, or nothing
// for a stored Transfer Template.
type TransferTemplateSource struct {
	Type     string // "" for a stored Transfer Template, TransferSourceSalary or TransferSourceBill
	EntityID string // the ID of the Salary or Bill
	Label    string
	// BillAccountID is the Bill Account of a Bill source, which lists the Bill.
	BillAccountID string

	// recurrenceRow names the stored row a generated Transfer Template's recurrence is from, such as
	// its Bill account, for an error about it.
	recurrenceRow string
}

func (s TransferTemplateSource) IsGenerated() bool {
	return s.Type != ""
}

// recurrenceRow names the stored row t's recurrence is from: t itself, or the Salary or Bill account
// it is generated from.
func (t TransferTemplate) recurrenceRow() string {
	if t.Source.recurrenceRow != "" {
		return t.Source.recurrenceRow
	}
	return "transfer template " + t.ID
}

// The Transfer Template amount types (TransferTemplate.AmountType).
const (
	TransferAmountFixed   = "fixed"
	TransferAmountPercent = "percent"
)

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
	if err := errors.Join(checkRequired("name", inp.Name), checkRequired("start date", inp.StartDate)); err != nil {
		return TransferTemplate{}, err
	}
	if err := checkOneOf("transfer template amount type", inp.AmountType, TransferAmountFixed, TransferAmountPercent); err != nil {
		return TransferTemplate{}, err
	}
	if err := checkDatePattern("transfer template recurrence", inp.Recurrence); err != nil {
		return TransferTemplate{}, err
	}
	if err := checkEndDate(inp.StartDate, inp.EndDate); err != nil {
		return TransferTemplate{}, err
	}
	var endDate *int64
	if inp.EndDate != nil {
		d := int64(*inp.EndDate)
		endDate = &d
	}
	amountFixed, err := orZero(inp.AmountFixed).Encode()
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

type TransferTemplateEditView struct {
	TransferTemplate TransferTemplate
	Accounts         []Account
	Categories       []TransferTemplateCategory
}

func (c TransferTemplateEditView) IsEdit() bool {
	return c.TransferTemplate.ID != ""
}

type TransferTemplatesView struct {
	Rows            []TransferScheduleRow
	Accounts        map[string]Account
	Categories      map[string]TransferTemplateCategory
	MonthlyIncome   float64
	MonthlyExpenses float64
}

func newTransferTemplatesView(day *TransferScheduleDay, accounts []Account, categories []TransferTemplateCategory) *TransferTemplatesView {
	v := &TransferTemplatesView{
		Rows:       day.Rows(),
		Accounts:   KeyBy(accounts, func(a Account) string { return a.ID }),
		Categories: KeyBy(categories, func(c TransferTemplateCategory) string { return c.ID }),
	}
	for _, t := range day.Monthly() {
		switch t.Kind() {
		case TransferIncome:
			v.MonthlyIncome += t.Amount
		case TransferExpense:
			v.MonthlyExpenses -= t.Amount
		}
	}
	return v
}

func (v *TransferTemplatesView) GetBudgetCategory(id *string) *TransferTemplateCategory {
	if id == nil {
		return nil
	}
	cat, ok := v.Categories[*id]
	if !ok {
		return nil
	}
	return &cat
}

func (v *TransferTemplatesView) GetAccount(id string) *Account {
	a := v.Accounts[id]
	return &a
}

func (s *Service) GetTransferTemplatesPageData(ctx context.Context) (*TransferTemplatesView, error) {
	schedule, err := s.TransferSchedule(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading transfer schedule: %w", err)
	}
	accounts, err := s.ListAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing accounts: %w", err)
	}
	categories, err := s.ListCategories(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing categories: %w", err)
	}
	return newTransferTemplatesView(schedule.On(date.Today()), accounts, categories), nil
}

func (s *Service) GetTransferTemplateNewPageData(ctx context.Context) (*TransferTemplateEditView, error) {
	accs, err := s.ListAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing accounts: %w", err)
	}
	categories, err := s.ListCategories(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing categories: %w", err)
	}
	return &TransferTemplateEditView{
		Accounts:   accs,
		Categories: categories,
	}, nil
}

func (s *Service) GetTransferTemplateEditPageData(ctx context.Context, id string) (*TransferTemplateEditView, error) {
	t, err := s.GetTransferTemplate(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("getting transfer template: %w", err)
	}
	accs, err := s.ListAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing accounts: %w", err)
	}
	categories, err := s.ListCategories(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing categories: %w", err)
	}
	return &TransferTemplateEditView{
		Accounts:         accs,
		TransferTemplate: t,
		Categories:       categories,
	}, nil
}
