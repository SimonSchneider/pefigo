package model

import (
	"context"
	"fmt"
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

	defaultCurrency string
	getRate         func(from, to string) float64
}

func (b *Bill) SetCurrencyConverter(defaultCurrency string, getRate func(from, to string) float64) {
	b.defaultCurrency = defaultCurrency
	b.getRate = getRate
}

type BillAmount struct {
	ID        string
	BillID    string
	Amount    uncertain.Value
	Period    string // "monthly" or "yearly"
	Currency  string // ISO 4217 code; empty means default currency
	StartDate date.Date
	EndDate   *date.Date
}

func (ba BillAmount) MonthlyAmountValue() uncertain.Value {
	if ba.Period == "yearly" {
		v := ba.Amount
		return uncertain.NewMapped(func(cfg *uncertain.Config) float64 {
			return v.Sample(cfg) / 12
		})
	}
	return ba.Amount
}

func (ba BillAmount) YearlyAmountValue() uncertain.Value {
	if ba.Period == "yearly" {
		return ba.Amount
	}
	v := ba.Amount
	return uncertain.NewMapped(func(cfg *uncertain.Config) float64 {
		return v.Sample(cfg) * 12
	})
}

func (ba BillAmount) needsConversion(defaultCurrency string) bool {
	return ba.Currency != "" && ba.Currency != defaultCurrency
}

func (ba BillAmount) ConvertedMonthlyAmountValue(defaultCurrency string, getRate func(from, to string) float64) uncertain.Value {
	monthly := ba.MonthlyAmountValue()
	if !ba.needsConversion(defaultCurrency) {
		return monthly
	}
	rate := getRate(ba.Currency, defaultCurrency)
	return uncertain.NewMapped(func(cfg *uncertain.Config) float64 {
		return monthly.Sample(cfg) * rate
	})
}

func (ba BillAmount) ConvertedYearlyAmountValue(defaultCurrency string, getRate func(from, to string) float64) uncertain.Value {
	yearly := ba.YearlyAmountValue()
	if !ba.needsConversion(defaultCurrency) {
		return yearly
	}
	rate := getRate(ba.Currency, defaultCurrency)
	return uncertain.NewMapped(func(cfg *uncertain.Config) float64 {
		return yearly.Sample(cfg) * rate
	})
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
	if amt == nil || amt.Period == "" {
		return "monthly"
	}
	return amt.Period
}

func (b Bill) MonthlyAmountValue() uncertain.Value {
	amt := b.currentBillAmount()
	if amt == nil {
		return uncertain.NewFixed(0)
	}
	if b.getRate != nil {
		return amt.ConvertedMonthlyAmountValue(b.defaultCurrency, b.getRate)
	}
	return amt.MonthlyAmountValue()
}

func (b Bill) CurrentAmount() float64 {
	return b.MonthlyAmountValue().Mean()
}

func (b Bill) YearlyAmountValue() uncertain.Value {
	amt := b.currentBillAmount()
	if amt == nil {
		return uncertain.NewFixed(0)
	}
	if b.getRate != nil {
		return amt.ConvertedYearlyAmountValue(b.defaultCurrency, b.getRate)
	}
	return amt.YearlyAmountValue()
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
		Type:     "bill",
		EntityID: b.ID,
		Label:    b.Name,
		EditURL:  "/bills/" + ba.ID + "/edit",
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
		monthlyAmt := amt.MonthlyAmountValue()
		if b.getRate != nil {
			monthlyAmt = amt.ConvertedMonthlyAmountValue(b.defaultCurrency, b.getRate)
		}
		templates = append(templates, TransferTemplate{
			ID:               fmt.Sprintf("bill:%s:%d", b.ID, i),
			Name:             b.Name,
			FromAccountID:    ba.FromAccountID,
			ToAccountID:      "",
			AmountType:       "fixed",
			AmountFixed:      monthlyAmt,
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
	period := a.Period
	if period == "" {
		period = "monthly"
	}
	return BillAmount{
		ID:        a.ID,
		BillID:    a.BillID,
		Amount:    amount,
		Period:    period,
		Currency:  a.Currency,
		StartDate: date.Date(a.StartDate),
		EndDate:   endDate,
	}, nil
}

func (s *Service) applyCurrencyConverter(ctx context.Context, bills []Bill) {
	defaultCurrency, err := s.GetDefaultCurrency(ctx)
	if err != nil || s.currencyClient == nil {
		return
	}
	getRate := func(from, to string) float64 {
		rate, err := s.currencyClient.GetRate(ctx, from, to)
		if err != nil {
			return 1
		}
		return rate
	}
	for i := range bills {
		bills[i].SetCurrencyConverter(defaultCurrency, getRate)
	}
}

func (s *Service) UpsertBillAccount(ctx context.Context, inp BillAccount) (BillAccount, error) {
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
	if err := checkEndDate(inp.StartDate, inp.EndDate); err != nil {
		return BillAmount{}, err
	}
	if inp.ID == "" {
		inp.ID = sid.MustNewString(32)
	}
	encoded, err := inp.Amount.Encode()
	if err != nil {
		return BillAmount{}, fmt.Errorf("encoding bill amount: %w", err)
	}
	var endDate *int64
	if inp.EndDate != nil {
		d := int64(*inp.EndDate)
		endDate = &d
	}
	now := time.Now().Unix()
	period := inp.Period
	if period == "" {
		period = "monthly"
	}
	a, err := s.q.UpsertBillAmount(ctx, pdb.UpsertBillAmountParams{
		ID:        inp.ID,
		BillID:    inp.BillID,
		Amount:    encoded,
		Period:    period,
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

type BillsPageData struct {
	BillAccounts []BillAccount
	Categories   map[string]TransferTemplateCategory
}

func (v *BillsPageData) GetBudgetCategory(id *string) *TransferTemplateCategory {
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

func (v *BillAccountEditView) BillsPageView() *BillsPageData {
	return &BillsPageData{
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

func (s *Service) GetBillAccountsPageData(ctx context.Context) (*BillsPageData, error) {
	accounts, err := s.ListBillAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing bill accounts: %w", err)
	}
	allBills, err := s.q.ListAllBills(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing all bills: %w", err)
	}
	allAmounts, err := s.q.ListAllBillAmounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing all bill amounts: %w", err)
	}
	categories, err := s.ListCategories(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing categories: %w", err)
	}
	amountsByBill := make(map[string][]BillAmount)
	for _, a := range allAmounts {
		parsed, err := billAmountFromDB(a)
		if err != nil {
			return nil, err
		}
		amountsByBill[a.BillID] = append(amountsByBill[a.BillID], parsed)
	}
	billsByAccount := make(map[string][]Bill)
	for _, b := range allBills {
		bill := billFromDB(b)
		bill.Amounts = amountsByBill[b.ID]
		billsByAccount[b.BillAccountID] = append(billsByAccount[b.BillAccountID], bill)
	}
	for i := range accounts {
		accounts[i].Bills = billsByAccount[accounts[i].ID]
		s.applyCurrencyConverter(ctx, accounts[i].Bills)
		SortBillsByAmount(accounts[i].Bills)
	}
	return &BillsPageData{
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
	ba, err := s.GetBillAccount(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("getting bill account: %w", err)
	}
	bills, err := s.ListBills(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("listing bills: %w", err)
	}
	allAmounts, err := s.q.ListAllBillAmounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing bill amounts: %w", err)
	}
	amountsByBill := make(map[string][]BillAmount)
	for _, a := range allAmounts {
		parsed, err := billAmountFromDB(a)
		if err != nil {
			return nil, err
		}
		amountsByBill[a.BillID] = append(amountsByBill[a.BillID], parsed)
	}
	for i := range bills {
		bills[i].Amounts = amountsByBill[bills[i].ID]
	}
	s.applyCurrencyConverter(ctx, bills)
	ba.Bills = bills
	accs, err := s.ListAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing accounts: %w", err)
	}
	categories, err := s.ListCategories(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing categories: %w", err)
	}
	return &BillAccountEditView{
		BillAccount: ba,
		Accounts:    accs,
		Categories:  categories,
	}, nil
}

func (s *Service) GetBillEditPageData(ctx context.Context, billID string) (*BillEditView, error) {
	bill, err := s.GetBill(ctx, billID)
	if err != nil {
		return nil, fmt.Errorf("getting bill: %w", err)
	}
	amounts, err := s.ListBillAmounts(ctx, billID)
	if err != nil {
		return nil, fmt.Errorf("listing bill amounts: %w", err)
	}
	bill.Amounts = amounts
	s.applyCurrencyConverter(ctx, []Bill{bill})
	ba, err := s.GetBillAccount(ctx, bill.BillAccountID)
	if err != nil {
		return nil, fmt.Errorf("getting bill account: %w", err)
	}
	billAccounts, err := s.ListBillAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing bill accounts: %w", err)
	}
	categories, err := s.ListCategories(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing categories: %w", err)
	}
	return &BillEditView{
		Bill:         bill,
		BillAccount:  ba,
		BillAccounts: billAccounts,
		Categories:   categories,
	}, nil
}

func (s *Service) generateBillTransferTemplates(ctx context.Context) ([]TransferTemplate, error) {
	billAccounts, err := s.q.ListBillAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing bill accounts: %w", err)
	}
	allBills, err := s.q.ListAllBills(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing all bills: %w", err)
	}
	allAmounts, err := s.q.ListAllBillAmounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing all bill amounts: %w", err)
	}

	amountsByBill := make(map[string][]BillAmount)
	for _, a := range allAmounts {
		parsed, err := billAmountFromDB(a)
		if err != nil {
			return nil, err
		}
		amountsByBill[a.BillID] = append(amountsByBill[a.BillID], parsed)
	}

	billsByAccount := make(map[string][]Bill)
	for _, b := range allBills {
		bill := billFromDB(b)
		bill.Amounts = amountsByBill[b.ID]
		billsByAccount[b.BillAccountID] = append(billsByAccount[b.BillAccountID], bill)
	}

	for accountID, bills := range billsByAccount {
		s.applyCurrencyConverter(ctx, bills)
		billsByAccount[accountID] = bills
	}

	var templates []TransferTemplate
	for _, ba := range billAccounts {
		account := billAccountFromDB(ba)
		for _, bill := range billsByAccount[ba.ID] {
			templates = append(templates, bill.GenerateTransferTemplates(account)...)
		}
	}
	return templates, nil
}
