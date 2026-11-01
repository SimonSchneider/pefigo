package model

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/goslu/sid"
	"github.com/SimonSchneider/pefigo/internal/pdb"
	"github.com/SimonSchneider/pefigo/pkg/ui"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

type BillAccount struct {
	ID            string
	Name          string
	FromAccountID string
	Recurrence    date.Cron
	Priority      int64
	Enabled       bool
	Bills         []Bill
}

type Bill struct {
	ID               string
	BillAccountID    string
	Name             string
	BudgetCategoryID *string
	Enabled          bool
	Notes            string
	URL              string
	Amounts          []BillAmount
}

// The Bill Amount periods (BillAmount.Period).
const (
	BillPeriodMonthly = "monthly"
	BillPeriodYearly  = "yearly"
)

type BillAmount struct {
	ID        string
	BillID    string
	Amount    uncertain.Value
	Period    string // BillPeriodMonthly or BillPeriodYearly; saving it empty saves BillPeriodMonthly
	Currency  string // a supported ISO 4217 code; empty means default currency
	StartDate date.Date
	EndDate   *date.Date

	// rate converts Amount to the Default Currency: the Exchange Rate the Service loaded the Bill Amount
	// at, or 0 for one in the Default Currency. See Service.loadBillAccounts.
	rate float64
}

// inDefaultCurrency is v, an amount in the Bill Amount's currency, in the Default Currency.
func (ba BillAmount) inDefaultCurrency(v uncertain.Value) uncertain.Value {
	if ba.rate == 0 {
		return v
	}
	rate := ba.rate
	return uncertain.NewMapped(func(cfg *uncertain.Config) float64 {
		return v.Sample(cfg) * rate
	})
}

// monthlyAmount is the Bill Amount a month, in the Default Currency.
func (ba BillAmount) monthlyAmount() uncertain.Value {
	if ba.Period == BillPeriodYearly {
		v := ba.Amount
		return ba.inDefaultCurrency(uncertain.NewMapped(func(cfg *uncertain.Config) float64 {
			return v.Sample(cfg) / 12
		}))
	}
	return ba.inDefaultCurrency(ba.Amount)
}

// yearlyAmount is the Bill Amount a year, in the Default Currency.
func (ba BillAmount) yearlyAmount() uncertain.Value {
	if ba.Period == BillPeriodYearly {
		return ba.inDefaultCurrency(ba.Amount)
	}
	v := ba.Amount
	return ba.inDefaultCurrency(uncertain.NewMapped(func(cfg *uncertain.Config) float64 {
		return v.Sample(cfg) * 12
	}))
}

func (b Bill) currentBillAmount() *BillAmount {
	today := date.Today()
	var current *BillAmount
	for i := range b.Amounts {
		if b.Amounts[i].StartDate <= today {
			if b.Amounts[i].EndDate != nil && *b.Amounts[i].EndDate <= today {
				continue
			}
			if current == nil || b.Amounts[i].StartDate > current.StartDate {
				current = &b.Amounts[i]
			}
		}
	}
	return current
}

func (b Bill) CurrentPeriod() string {
	amt := b.currentBillAmount()
	if amt == nil {
		return BillPeriodMonthly
	}
	return amt.Period
}

// MonthlyAmountValue is the current Bill Amount a month, in the Default Currency.
func (b Bill) MonthlyAmountValue() uncertain.Value {
	amt := b.currentBillAmount()
	if amt == nil {
		return uncertain.NewFixed(0)
	}
	return amt.monthlyAmount()
}

func (b Bill) CurrentAmount() float64 {
	return b.MonthlyAmountValue().Mean()
}

// YearlyAmountValue is the current Bill Amount a year, in the Default Currency.
func (b Bill) YearlyAmountValue() uncertain.Value {
	amt := b.currentBillAmount()
	if amt == nil {
		return uncertain.NewFixed(0)
	}
	return amt.yearlyAmount()
}

func (b Bill) YearlyAmount() float64 {
	return b.YearlyAmountValue().Mean()
}

func SortBillsByAmount(bills []Bill) {
	sort.SliceStable(bills, func(i, j int) bool {
		return bills[i].CurrentAmount() > bills[j].CurrentAmount()
	})
}

func (ba BillAmount) GetStartDateString() string {
	if ba.ID == "" {
		return ""
	}
	return ba.StartDate.String()
}

func (ba BillAmount) GetEndDateString() string {
	if ba.ID == "" || ba.EndDate == nil {
		return ""
	}
	return ba.EndDate.String()
}

func (ba BillAmount) GetAmountString() string {
	if ba.ID == "" {
		return ""
	}
	return ba.Amount.SimpleEncode()
}

func (b Bill) GenerateTransferTemplates(ba BillAccount) []TransferTemplate {
	if len(b.Amounts) == 0 {
		return nil
	}

	source := TransferTemplateSource{
		Type:            TransferSourceBill,
		EntityID:        b.ID,
		Label:           b.Name,
		BillAccountID:   ba.ID,
		BillAccountName: ba.Name,
		recurrenceRow:   "bill account " + ba.ID,
	}

	enabled := ba.Enabled && b.Enabled

	sorted := make([]BillAmount, len(b.Amounts))
	copy(sorted, b.Amounts)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].StartDate < sorted[j].StartDate
	})

	var templates []TransferTemplate
	for i, amt := range sorted {
		endDate := amt.EndDate
		if i+1 < len(sorted) {
			nextStart := sorted[i+1].StartDate
			if endDate == nil || *endDate > nextStart {
				endDate = &nextStart
			}
		}
		templates = append(templates, TransferTemplate{
			ID:               fmt.Sprintf("bill:%s:%d", b.ID, i),
			Name:             b.Name,
			FromAccountID:    ba.FromAccountID,
			ToAccountID:      "",
			AmountType:       TransferAmountFixed,
			AmountFixed:      amt.monthlyAmount(),
			Priority:         ba.Priority,
			Recurrence:       ba.Recurrence,
			StartDate:        amt.StartDate,
			EndDate:          endDate,
			Enabled:          enabled,
			BudgetCategoryID: b.BudgetCategoryID,
			Source:           source,
		})
	}

	return templates
}

func billAccountFromDB(ba pdb.BillAccount) BillAccount {
	return BillAccount{
		ID:            ba.ID,
		Name:          ba.Name,
		FromAccountID: ui.OrDefault(ba.FromAccountID),
		Recurrence:    date.Cron(ba.Recurrence),
		Priority:      ba.Priority,
		Enabled:       ba.Enabled,
	}
}

func billFromDB(b pdb.Bill) Bill {
	return Bill{
		ID:               b.ID,
		BillAccountID:    b.BillAccountID,
		Name:             b.Name,
		BudgetCategoryID: b.BudgetCategoryID,
		Enabled:          b.Enabled,
		Notes:            b.Notes,
		URL:              b.Url,
	}
}

func billAmountFromDB(a pdb.BillAmount) (BillAmount, error) {
	var amount uncertain.Value
	if err := amount.Decode(a.Amount); err != nil {
		return BillAmount{}, fmt.Errorf("decoding bill amount: %w", err)
	}
	var endDate *date.Date
	if a.EndDate != nil {
		d := date.Date(*a.EndDate)
		endDate = &d
	}
	return BillAmount{
		ID:        a.ID,
		BillID:    a.BillID,
		Amount:    amount,
		Period:    a.Period,
		Currency:  a.Currency,
		StartDate: date.Date(a.StartDate),
		EndDate:   endDate,
	}, nil
}

// loadBillAccounts returns every Bill account with its Bills, and every Bill Amount with its amount in
// the Default Currency: one in another currency converted at the latest Exchange Rate. It fails when
// the Default Currency can't be read, or when no Exchange Rate needed was ever fetched.
func (s *Service) loadBillAccounts(ctx context.Context) ([]BillAccount, error) {
	defaultCurrency, err := s.GetDefaultCurrency(ctx)
	if err != nil {
		return nil, err
	}
	accounts, err := s.ListBillAccounts(ctx)
	if err != nil {
		return nil, err
	}
	allBills, err := s.q.ListAllBills(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing all bills: %w", err)
	}
	allAmounts, err := s.q.ListAllBillAmounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing all bill amounts: %w", err)
	}

	rates := make(map[string]float64)
	amountsByBill := make(map[string][]BillAmount)
	for _, a := range allAmounts {
		amount, err := billAmountFromDB(a)
		if err != nil {
			return nil, err
		}
		if amount.Currency != "" && amount.Currency != defaultCurrency {
			rate, ok := rates[amount.Currency]
			if !ok {
				if rate, err = s.currencyClient.GetRate(ctx, amount.Currency, defaultCurrency); err != nil {
					return nil, fmt.Errorf("converting Bill Amounts in %s: %w", amount.Currency, err)
				}
				rates[amount.Currency] = rate
			}
			amount.rate = rate
		}
		amountsByBill[a.BillID] = append(amountsByBill[a.BillID], amount)
	}
	billsByAccount := make(map[string][]Bill)
	for _, b := range allBills {
		bill := billFromDB(b)
		bill.Amounts = amountsByBill[b.ID]
		billsByAccount[b.BillAccountID] = append(billsByAccount[b.BillAccountID], bill)
	}
	for i := range accounts {
		accounts[i].Bills = billsByAccount[accounts[i].ID]
	}
	return accounts, nil
}

// DefaultBillAccountRecurrence is the recurrence of a Bill account saved without one: the 1st of every
// month.
const DefaultBillAccountRecurrence date.Cron = "*-*-01"

func (s *Service) UpsertBillAccount(ctx context.Context, inp BillAccount) (BillAccount, error) {
	if inp.Recurrence == "" {
		inp.Recurrence = DefaultBillAccountRecurrence
	}
	if err := errors.Join(checkRequired("name", inp.Name), checkDatePattern("bill account recurrence", inp.Recurrence)); err != nil {
		return BillAccount{}, err
	}
	if inp.ID == "" {
		inp.ID = sid.MustNewString(32)
	}
	now := time.Now().Unix()
	ba, err := s.q.UpsertBillAccount(ctx, pdb.UpsertBillAccountParams{
		ID:            inp.ID,
		Name:          inp.Name,
		FromAccountID: ui.WithDefaultNull(inp.FromAccountID),
		Recurrence:    string(inp.Recurrence),
		Priority:      inp.Priority,
		Enabled:       inp.Enabled,
		CreatedAt:     now,
		UpdatedAt:     now,
	})
	if err != nil {
		return BillAccount{}, fmt.Errorf("upserting bill account: %w", err)
	}
	return billAccountFromDB(ba), nil
}

func (s *Service) GetBillAccount(ctx context.Context, id string) (BillAccount, error) {
	ba, err := s.q.GetBillAccount(ctx, id)
	if err != nil {
		return BillAccount{}, fmt.Errorf("getting bill account: %w", err)
	}
	return billAccountFromDB(ba), nil
}

func (s *Service) ListBillAccounts(ctx context.Context) ([]BillAccount, error) {
	rows, err := s.q.ListBillAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing bill accounts: %w", err)
	}
	result := make([]BillAccount, len(rows))
	for i, r := range rows {
		result[i] = billAccountFromDB(r)
	}
	return result, nil
}

func (s *Service) DeleteBillAccount(ctx context.Context, id string) error {
	if err := s.q.DeleteBillAccount(ctx, id); err != nil {
		return fmt.Errorf("deleting bill account: %w", err)
	}
	return nil
}

func (s *Service) UpsertBill(ctx context.Context, inp Bill) (Bill, error) {
	if err := errors.Join(checkRequired("bill account", inp.BillAccountID), checkRequired("name", inp.Name)); err != nil {
		return Bill{}, err
	}
	if inp.ID == "" {
		inp.ID = sid.MustNewString(32)
	}
	now := time.Now().Unix()
	b, err := s.q.UpsertBill(ctx, pdb.UpsertBillParams{
		ID:               inp.ID,
		BillAccountID:    inp.BillAccountID,
		Name:             inp.Name,
		BudgetCategoryID: inp.BudgetCategoryID,
		Enabled:          inp.Enabled,
		Notes:            inp.Notes,
		Url:              inp.URL,
		CreatedAt:        now,
		UpdatedAt:        now,
	})
	if err != nil {
		return Bill{}, fmt.Errorf("upserting bill: %w", err)
	}
	return billFromDB(b), nil
}

func (s *Service) GetBill(ctx context.Context, id string) (Bill, error) {
	b, err := s.q.GetBill(ctx, id)
	if err != nil {
		return Bill{}, fmt.Errorf("getting bill: %w", err)
	}
	return billFromDB(b), nil
}

func (s *Service) ListBills(ctx context.Context, billAccountID string) ([]Bill, error) {
	rows, err := s.q.ListBills(ctx, billAccountID)
	if err != nil {
		return nil, fmt.Errorf("listing bills: %w", err)
	}
	result := make([]Bill, len(rows))
	for i, r := range rows {
		result[i] = billFromDB(r)
	}
	return result, nil
}

func (s *Service) DeleteBill(ctx context.Context, id string) error {
	if err := s.q.DeleteBill(ctx, id); err != nil {
		return fmt.Errorf("deleting bill: %w", err)
	}
	return nil
}

func (s *Service) UpsertBillAmount(ctx context.Context, inp BillAmount) (BillAmount, error) {
	if err := errors.Join(checkRequired("bill", inp.BillID), checkRequired("start date", inp.StartDate)); err != nil {
		return BillAmount{}, err
	}
	if inp.Period == "" {
		inp.Period = BillPeriodMonthly
	}
	if err := checkOneOf("bill amount period", inp.Period, BillPeriodMonthly, BillPeriodYearly); err != nil {
		return BillAmount{}, err
	}
	if inp.Currency != "" {
		if err := checkSupportedCurrency("bill amount currency", inp.Currency); err != nil {
			return BillAmount{}, err
		}
	}
	if err := checkEndDate(inp.StartDate, inp.EndDate); err != nil {
		return BillAmount{}, err
	}
	if inp.ID == "" {
		inp.ID = sid.MustNewString(32)
	}
	encoded, err := orZero(inp.Amount).Encode()
	if err != nil {
		return BillAmount{}, fmt.Errorf("encoding bill amount: %w", err)
	}
	var endDate *int64
	if inp.EndDate != nil {
		d := int64(*inp.EndDate)
		endDate = &d
	}
	now := time.Now().Unix()
	a, err := s.q.UpsertBillAmount(ctx, pdb.UpsertBillAmountParams{
		ID:        inp.ID,
		BillID:    inp.BillID,
		Amount:    encoded,
		Period:    inp.Period,
		Currency:  inp.Currency,
		StartDate: int64(inp.StartDate),
		EndDate:   endDate,
		CreatedAt: now,
		UpdatedAt: now,
	})
	if err != nil {
		return BillAmount{}, fmt.Errorf("upserting bill amount: %w", err)
	}
	return billAmountFromDB(a)
}

func (s *Service) ListBillAmounts(ctx context.Context, billID string) ([]BillAmount, error) {
	rows, err := s.q.ListBillAmounts(ctx, billID)
	if err != nil {
		return nil, fmt.Errorf("listing bill amounts: %w", err)
	}
	var result []BillAmount
	for _, r := range rows {
		a, err := billAmountFromDB(r)
		if err != nil {
			return nil, err
		}
		result = append(result, a)
	}
	return result, nil
}

func (s *Service) DeleteBillAmount(ctx context.Context, id string) error {
	if err := s.q.DeleteBillAmount(ctx, id); err != nil {
		return fmt.Errorf("deleting bill amount: %w", err)
	}
	return nil
}

type BillsView struct {
	BillAccounts []BillAccount
	Categories   map[string]TransferTemplateCategory
}

func (v *BillsView) GetBudgetCategory(id *string) *TransferTemplateCategory {
	if id == nil {
		return nil
	}
	cat, ok := v.Categories[*id]
	if !ok {
		return nil
	}
	return &cat
}

type BillAccountEditView struct {
	BillAccount BillAccount
	Accounts    []Account
	Categories  []TransferTemplateCategory
}

func (v BillAccountEditView) IsEdit() bool {
	return v.BillAccount.ID != ""
}

func (v *BillAccountEditView) BillsView() *BillsView {
	return &BillsView{
		Categories: KeyBy(v.Categories, func(c TransferTemplateCategory) string { return c.ID }),
	}
}

type BillEditView struct {
	Bill         Bill
	BillAccount  BillAccount
	BillAccounts []BillAccount
	Categories   []TransferTemplateCategory
}

func (v BillEditView) IsEdit() bool {
	return v.Bill.ID != ""
}

func (s *Service) GetBillAccountsPageData(ctx context.Context) (*BillsView, error) {
	accounts, err := s.loadBillAccounts(ctx)
	if err != nil {
		return nil, err
	}
	categories, err := s.ListCategories(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing categories: %w", err)
	}
	for i := range accounts {
		SortBillsByAmount(accounts[i].Bills)
	}
	return &BillsView{
		BillAccounts: accounts,
		Categories:   KeyBy(categories, func(c TransferTemplateCategory) string { return c.ID }),
	}, nil
}

func (s *Service) GetBillAccountNewPageData(ctx context.Context) (*BillAccountEditView, error) {
	accs, err := s.ListAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing accounts: %w", err)
	}
	categories, err := s.ListCategories(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing categories: %w", err)
	}
	return &BillAccountEditView{
		Accounts:   accs,
		Categories: categories,
	}, nil
}

func (s *Service) GetBillAccountEditPageData(ctx context.Context, id string) (*BillAccountEditView, error) {
	accounts, err := s.loadBillAccounts(ctx)
	if err != nil {
		return nil, err
	}
	i := slices.IndexFunc(accounts, func(ba BillAccount) bool { return ba.ID == id })
	if i < 0 {
		return nil, fmt.Errorf("getting bill account: %w", sql.ErrNoRows)
	}
	accs, err := s.ListAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing accounts: %w", err)
	}
	categories, err := s.ListCategories(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing categories: %w", err)
	}
	return &BillAccountEditView{
		BillAccount: accounts[i],
		Accounts:    accs,
		Categories:  categories,
	}, nil
}

func (s *Service) GetBillEditPageData(ctx context.Context, billID string) (*BillEditView, error) {
	accounts, err := s.loadBillAccounts(ctx)
	if err != nil {
		return nil, err
	}
	for _, ba := range accounts {
		for _, bill := range ba.Bills {
			if bill.ID != billID {
				continue
			}
			categories, err := s.ListCategories(ctx)
			if err != nil {
				return nil, fmt.Errorf("listing categories: %w", err)
			}
			return &BillEditView{
				Bill:         bill,
				BillAccount:  ba,
				BillAccounts: accounts,
				Categories:   categories,
			}, nil
		}
	}
	return nil, fmt.Errorf("getting bill: %w", sql.ErrNoRows)
}

func (s *Service) generateBillTransferTemplates(ctx context.Context) ([]TransferTemplate, error) {
	accounts, err := s.loadBillAccounts(ctx)
	if err != nil {
		return nil, err
	}
	var templates []TransferTemplate
	for _, ba := range accounts {
		for _, bill := range ba.Bills {
			templates = append(templates, bill.GenerateTransferTemplates(ba)...)
		}
	}
	return templates, nil
}
