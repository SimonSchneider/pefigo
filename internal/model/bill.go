package model

import (
	"cmp"
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
	// EventID is the Event the Bill is in, or "" in the Baseline. A Bill has no dates of its own: when
	// its Event is left out, it is left out with every Bill Amount and end of it.
	EventID string
	Amounts []BillAmount
	// Ends are the days the Bill may end on: no Bill Amount applies from the earliest of them.
	Ends []BillEnd
	// charges is how a Bill loaded for its pages, whose Bill Amounts and ends are each at their own
	// Event's date, is charged: as the Baseline charges it, or, when the Baseline leaves it out with its
	// unplanned Event, as it would be charged with that Event planned. It is nil for a Bill as it is
	// charged. See placings.shownBy and Bill.charged.
	charges *Bill
	// chargeErr is why what a Bill of an unplanned Event would charge can't be worked out.
	chargeErr error
	// leftOut is whether the Baseline leaves the loaded Bill out with its unplanned Event.
	leftOut bool
}

// BillEnd is a day a Bill may end on: from it, no Bill Amount applies. In an Event its date is stored
// as days after the Event's date.
type BillEnd struct {
	ID      string
	BillID  string
	Date    date.Date
	EventID string
	// DateOffset, when set, gives the date in an Event as days after the date of OffsetEventID, the
	// Event the form showed it for, or of EventID when that is blank.
	DateOffset    *date.Duration
	OffsetEventID string
}

// charged is the Bill as it is charged: as loaded for its pages, how the Baseline charges it, or how it
// would be charged with its unplanned Event planned. It is the Bill itself otherwise.
func (b Bill) charged() Bill {
	if b.charges != nil {
		return *b.charges
	}
	return b
}

// InBaseline reports whether the Baseline charges the Bill: not when its Event is unplanned. Its pages
// then show what it would charge with that Event planned.
func (b Bill) InBaseline() bool {
	return !b.leftOut
}

// ChargeError is why what a Bill of an unplanned Event would charge can't be worked out, such as a Bill
// Amount in a currency whose Exchange Rate can't be fetched, or nil. Such a Bill shows nothing charged.
// What the Baseline charges always can be: a page that can't work it out fails.
func (b Bill) ChargeError() error {
	return b.chargeErr
}

// End is the first day no Bill Amount of the Bill applies: the earliest of its Ends, or nil without
// one.
func (b Bill) End() *date.Date {
	return firstEnd(b.Ends, func(e BillEnd) date.Date { return e.Date })
}

// hasEndedBy reports whether the Bill has ended by day.
func (b Bill) hasEndedBy(day date.Date) bool {
	return endedBy(b.End(), day)
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
	// EventID is the Event the Bill Amount is in, or "" in the Baseline, which may differ from its Bill's:
	// an Event may add a Bill Amount to a Bill outside it. In an Event its dates are stored as days after
	// the Event's date, and its form may give them so, after the date of OffsetEventID, the Event the
	// form showed them for, or of EventID when that is blank.
	EventID       string
	StartOffset   *date.Duration
	EndOffset     *date.Duration
	OffsetEventID string

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

// currentBillAmount is the Bill Amount the Bill is charged by today, or nil without one.
func (b Bill) currentBillAmount() *BillAmount {
	charged := b.charged()
	today := date.Today()
	if charged.hasEndedBy(today) {
		return nil
	}
	var current *BillAmount
	for i := range charged.Amounts {
		if charged.Amounts[i].StartDate <= today {
			if charged.Amounts[i].EndDate != nil && *charged.Amounts[i].EndDate <= today {
				continue
			}
			if current == nil || charged.Amounts[i].StartDate >= current.StartDate {
				current = &charged.Amounts[i]
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
	b = b.charged()
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

	sorted := slices.Clone(b.Amounts)
	slices.SortStableFunc(sorted, func(a, b BillAmount) int { return cmp.Compare(a.StartDate, b.StartDate) })

	var templates []TransferTemplate
	for i, amt := range sorted {
		if b.hasEndedBy(amt.StartDate) {
			break
		}
		endDate := earliest(amt.EndDate, b.End())
		if i+1 < len(sorted) {
			endDate = earliest(endDate, &sorted[i+1].StartDate)
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
		EventID:          ui.OrDefault(b.EventID),
	}
}

// billAmountFromDB is the stored Bill Amount a with its Event at at.
func billAmountFromDB(a pdb.BillAmount, at date.Date) (BillAmount, error) {
	var amount uncertain.Value
	if err := amount.Decode(a.Amount); err != nil {
		return BillAmount{}, fmt.Errorf("decoding bill amount: %w", err)
	}
	return BillAmount{
		ID:        a.ID,
		BillID:    a.BillID,
		Amount:    amount,
		Period:    a.Period,
		Currency:  a.Currency,
		StartDate: placeDate(a.StartDate, at),
		EndDate:   placeEndDate(a.EndDate, at),
		EventID:   ui.OrDefault(a.EventID),
	}, nil
}

// billEndFromDB is the stored end e with its Event at at.
func billEndFromDB(e pdb.BillEnd, at date.Date) BillEnd {
	return BillEnd{ID: e.ID, BillID: e.BillID, Date: placeDate(e.Date, at), EventID: ui.OrDefault(e.EventID)}
}

// sortBillParts orders the Bill's Bill Amounts and ends by date and ID.
func sortBillParts(b *Bill) {
	sortByDayAndID(b.Amounts, BillAmount.dayAndID)
	sortByDayAndID(b.Ends, BillEnd.dayAndID)
}

// The first day and ID of each part of a Bill, which order it among the others.
func (a BillAmount) dayAndID() (date.Date, string) { return a.StartDate, a.ID }
func (e BillEnd) dayAndID() (date.Date, string)    { return e.Date, e.ID }

// loadBillAccounts returns every Bill account with its Bills, each with its Bill Amounts and ends at
// their own Event's dates, as its forms edit them, with the Events and accounts its pages show. What a
// Bill charges is what the plans its Event shows it by charge (see placings.shownBy): the Baseline's,
// which leave out what unplanned Events add or end, or for a Bill in an unplanned Event, the Baseline
// with that Event planned. It is in the Default Currency: a Bill Amount in another currency converted
// at the latest Exchange Rate. It fails when the Default Currency can't be read, or when no Exchange
// Rate the Baseline needs was ever fetched. A Bill of an unplanned Event whose rate wasn't charges
// nothing, and says why.
func (s *Service) loadBillAccounts(ctx context.Context) (loadedBillAccounts, error) {
	pl, err := s.loadPlacings(ctx)
	if err != nil {
		return loadedBillAccounts{}, err
	}
	own, err := pl.atOwnDates()
	if err != nil {
		return loadedBillAccounts{}, err
	}
	convert, err := s.billAmountConverter(ctx)
	if err != nil {
		return loadedBillAccounts{}, err
	}
	bills := slices.Clone(own.bills)
	for i, b := range bills {
		p, inBaseline, err := pl.shownBy(b.EventID)
		if err != nil {
			return loadedBillAccounts{}, err
		}
		// Its Event's plans place every Bill shownBy shows by them.
		charged, _ := p.bill(b.ID)
		if err := convert(&charged); err != nil {
			if inBaseline {
				return loadedBillAccounts{}, err
			}
			charged.Amounts, bills[i].chargeErr = nil, err
		}
		bills[i].charges, bills[i].leftOut = &charged, !inBaseline
	}
	accounts, err := s.billAccountsOf(ctx, bills)
	if err != nil {
		return loadedBillAccounts{}, err
	}
	return loadedBillAccounts{billAccounts: accounts, events: pl.events, accounts: accountsListFromDB(pl.stored.accounts)}, nil
}

// loadedBillAccounts are Bill accounts loaded for their pages, with the Events and accounts they were
// loaded with, which their pages show.
type loadedBillAccounts struct {
	billAccounts []BillAccount
	events       []Event
	accounts     []Account
}

// billAccountsOf is every Bill account with the bills of it, in their order.
func (s *Service) billAccountsOf(ctx context.Context, bills []Bill) ([]BillAccount, error) {
	accounts, err := s.ListBillAccounts(ctx)
	if err != nil {
		return nil, err
	}
	billsByAccount := make(map[string][]Bill)
	for _, b := range bills {
		billsByAccount[b.BillAccountID] = append(billsByAccount[b.BillAccountID], b)
	}
	for i := range accounts {
		accounts[i].Bills = billsByAccount[accounts[i].ID]
	}
	return accounts, nil
}

// billAmountConverter gives every Bill Amount of the Bill it is called with its amount in the Default
// Currency: one in another currency converted at the latest Exchange Rate, fetched once per currency.
// The Bill's Bill Amounts must be its own: they are changed in place.
func (s *Service) billAmountConverter(ctx context.Context) (func(bill *Bill) error, error) {
	defaultCurrency, err := s.GetDefaultCurrency(ctx)
	if err != nil {
		return nil, err
	}
	rates := make(map[string]float64)
	return func(bill *Bill) error {
		for j := range bill.Amounts {
			amount := &bill.Amounts[j]
			if amount.Currency == "" || amount.Currency == defaultCurrency {
				continue
			}
			rate, ok := rates[amount.Currency]
			if !ok {
				var err error
				if rate, err = s.currencyClient.GetRate(ctx, amount.Currency, defaultCurrency); err != nil {
					return fmt.Errorf("converting Bill Amounts in %s: %w", amount.Currency, err)
				}
				rates[amount.Currency] = rate
			}
			amount.rate = rate
		}
		return nil
	}, nil
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
	if _, err := s.eventDate(ctx, inp.EventID); err != nil {
		return Bill{}, err
	}
	previous, err := storedEventOf(ctx, inp.ID, s.q.GetBill, func(b pdb.Bill) *string { return b.EventID })
	if err != nil {
		return Bill{}, fmt.Errorf("getting bill: %w", err)
	}
	if inp.ID == "" {
		inp.ID = sid.MustNewString(32)
	}
	now := time.Now().Unix()
	var b pdb.Bill
	// A Bill changing Event brings its parts along, or stays where it was.
	err = s.inTx(ctx, func(tx *Service) (err error) {
		b, err = tx.q.UpsertBill(ctx, pdb.UpsertBillParams{
			ID:               inp.ID,
			BillAccountID:    inp.BillAccountID,
			Name:             inp.Name,
			BudgetCategoryID: inp.BudgetCategoryID,
			Enabled:          inp.Enabled,
			Notes:            inp.Notes,
			Url:              inp.URL,
			EventID:          ui.WithDefaultNull(inp.EventID),
			CreatedAt:        now,
			UpdatedAt:        now,
		})
		if err != nil {
			return fmt.Errorf("upserting bill: %w", err)
		}
		if previous != nil && *previous != inp.EventID {
			return tx.bringParts(ctx, EventEntryBill, inp.ID, *previous, inp.EventID)
		}
		return nil
	})
	if err != nil {
		return Bill{}, err
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
	dating, err := s.entryDating(ctx, inp.EventID, inp.OffsetEventID)
	if err != nil {
		return BillAmount{}, err
	}
	start, startErr := dating.firstDay("start", inp.StartDate, inp.StartOffset)
	end, endErr := dating.endDate(inp.EndDate, inp.EndOffset)
	if err := errors.Join(startErr, endErr); err != nil {
		return BillAmount{}, err
	}
	inp.StartDate, inp.EndDate = start, end
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
	now := time.Now().Unix()
	a, err := s.q.UpsertBillAmount(ctx, pdb.UpsertBillAmountParams{
		ID:        inp.ID,
		BillID:    inp.BillID,
		Amount:    encoded,
		Period:    inp.Period,
		Currency:  inp.Currency,
		StartDate: dating.store(inp.StartDate),
		EndDate:   storeEndDate(inp.EndDate, dating.at),
		EventID:   ui.WithDefaultNull(inp.EventID),
		CreatedAt: now,
		UpdatedAt: now,
	})
	if err != nil {
		return BillAmount{}, fmt.Errorf("upserting bill amount: %w", err)
	}
	return billAmountFromDB(a, dating.at)
}

// getBillAmount returns the Bill Amount id, in an Event at the Event's date.
func (s *Service) getBillAmount(ctx context.Context, id string) (BillAmount, error) {
	a, err := s.q.GetBillAmount(ctx, id)
	if err != nil {
		return BillAmount{}, fmt.Errorf("getting bill amount: %w", err)
	}
	at, err := s.eventDate(ctx, ui.OrDefault(a.EventID))
	if err != nil {
		return BillAmount{}, err
	}
	return billAmountFromDB(a, at)
}

// ListBillAmounts returns the Bill's Bill Amounts by Start Date, each in an Event at the Event's date.
func (s *Service) ListBillAmounts(ctx context.Context, billID string) ([]BillAmount, error) {
	rows, err := s.q.ListBillAmounts(ctx, billID)
	if err != nil {
		return nil, fmt.Errorf("listing bill amounts: %w", err)
	}
	dates, err := s.eventDates(ctx)
	if err != nil {
		return nil, err
	}
	var result []BillAmount
	for _, r := range rows {
		a, err := billAmountFromDB(r, ownDate(dates, r.EventID))
		if err != nil {
			return nil, err
		}
		result = append(result, a)
	}
	sortByDayAndID(result, BillAmount.dayAndID)
	return result, nil
}

func (s *Service) DeleteBillAmount(ctx context.Context, id string) error {
	if err := s.q.DeleteBillAmount(ctx, id); err != nil {
		return fmt.Errorf("deleting bill amount: %w", err)
	}
	return nil
}

func (s *Service) UpsertBillEnd(ctx context.Context, inp BillEnd) (BillEnd, error) {
	dating, err := s.entryDating(ctx, inp.EventID, inp.OffsetEventID)
	if err != nil {
		return BillEnd{}, err
	}
	if inp.Date, err = dating.firstDay("end", inp.Date, inp.DateOffset); err != nil {
		return BillEnd{}, err
	}
	if err := errors.Join(checkRequired("bill", inp.BillID), checkRequired("end date", inp.Date)); err != nil {
		return BillEnd{}, err
	}
	if inp.ID == "" {
		inp.ID = sid.MustNewString(32)
	}
	now := time.Now().Unix()
	e, err := s.q.UpsertBillEnd(ctx, pdb.UpsertBillEndParams{
		ID:        inp.ID,
		BillID:    inp.BillID,
		Date:      dating.store(inp.Date),
		EventID:   ui.WithDefaultNull(inp.EventID),
		CreatedAt: now,
		UpdatedAt: now,
	})
	if err != nil {
		return BillEnd{}, fmt.Errorf("upserting bill end: %w", err)
	}
	return billEndFromDB(e, dating.at), nil
}

// GetBillEnd returns the Bill's end id, in an Event at the Event's date.
func (s *Service) GetBillEnd(ctx context.Context, id string) (BillEnd, error) {
	e, err := s.q.GetBillEnd(ctx, id)
	if err != nil {
		return BillEnd{}, fmt.Errorf("getting bill end: %w", err)
	}
	at, err := s.eventDate(ctx, ui.OrDefault(e.EventID))
	if err != nil {
		return BillEnd{}, err
	}
	return billEndFromDB(e, at), nil
}

func (s *Service) DeleteBillEnd(ctx context.Context, id string) error {
	if err := s.q.DeleteBillEnd(ctx, id); err != nil {
		return fmt.Errorf("deleting bill end: %w", err)
	}
	return nil
}

type BillsView struct {
	BillAccounts []BillAccount
	Categories   map[string]TransferTemplateCategory
	Events       map[string]Event
}

// GetEvent is the Event id, or nil for "" or an Event that doesn't exist.
func (v *BillsView) GetEvent(id string) *Event {
	return eventByID(v.Events, id)
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
	Events      []Event
}

func (v BillAccountEditView) IsEdit() bool {
	return v.BillAccount.ID != ""
}

func (v *BillAccountEditView) BillsView() *BillsView {
	return &BillsView{
		Categories: KeyBy(v.Categories, func(c TransferTemplateCategory) string { return c.ID }),
		Events:     KeyBy(v.Events, func(e Event) string { return e.ID }),
	}
}

type BillEditView struct {
	Bill         Bill
	BillAccount  BillAccount
	BillAccounts []BillAccount
	Categories   []TransferTemplateCategory
	Events       []Event
}

func (v BillEditView) IsEdit() bool {
	return v.Bill.ID != ""
}

// PartEventID is the Event a new Bill Amount or end of the Bill is in, unless its form picks another:
// the Bill's.
func (v BillEditView) PartEventID() string {
	return v.Bill.EventID
}

func (s *Service) GetBillAccountsPageData(ctx context.Context) (*BillsView, error) {
	loaded, err := s.loadBillAccounts(ctx)
	if err != nil {
		return nil, err
	}
	categories, err := s.ListCategories(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing categories: %w", err)
	}
	for i := range loaded.billAccounts {
		SortBillsByAmount(loaded.billAccounts[i].Bills)
	}
	return &BillsView{
		BillAccounts: loaded.billAccounts,
		Categories:   KeyBy(categories, func(c TransferTemplateCategory) string { return c.ID }),
		Events:       KeyBy(loaded.events, func(e Event) string { return e.ID }),
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
	loaded, err := s.loadBillAccounts(ctx)
	if err != nil {
		return nil, err
	}
	i := slices.IndexFunc(loaded.billAccounts, func(ba BillAccount) bool { return ba.ID == id })
	if i < 0 {
		return nil, fmt.Errorf("getting bill account: %w", sql.ErrNoRows)
	}
	categories, err := s.ListCategories(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing categories: %w", err)
	}
	return &BillAccountEditView{
		BillAccount: loaded.billAccounts[i],
		Accounts:    loaded.accounts,
		Categories:  categories,
		Events:      loaded.events,
	}, nil
}

func (s *Service) GetBillEditPageData(ctx context.Context, billID string) (*BillEditView, error) {
	loaded, err := s.loadBillAccounts(ctx)
	if err != nil {
		return nil, err
	}
	for _, ba := range loaded.billAccounts {
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
				BillAccounts: loaded.billAccounts,
				Categories:   categories,
				Events:       loaded.events,
			}, nil
		}
	}
	return nil, fmt.Errorf("getting bill: %w", sql.ErrNoRows)
}

// billTransferTemplates are what bills, with the Events placed, charge, in the Default Currency. It
// fails on the first Bill whose charges can't be worked out.
func (s *Service) billTransferTemplates(ctx context.Context, bills []Bill) ([]TransferTemplate, error) {
	payments, err := s.billPayments(ctx, bills)
	if err != nil {
		return nil, err
	}
	return transferTemplatesOf(payments)
}

// billPayments are what each of bills, with the Events placed, charges, in the Default Currency, or why
// it can't be worked out, such as a Bill Amount in a currency whose Exchange Rate can't be fetched. A
// Bill of a Bill account that doesn't exist charges nothing.
func (s *Service) billPayments(ctx context.Context, bills []Bill) ([]payment, error) {
	convert, err := s.billAmountConverter(ctx)
	if err != nil {
		return nil, err
	}
	accounts, err := s.billAccountsOf(ctx, bills)
	if err != nil {
		return nil, err
	}
	var payments []payment
	for _, ba := range accounts {
		for _, bill := range ba.Bills {
			pay := payment{name: bill.Name, eventID: bill.EventID, source: TransferTemplateSource{Type: TransferSourceBill, EntityID: bill.ID, Label: bill.Name, BillAccountID: ba.ID, BillAccountName: ba.Name}}
			bill.Amounts = slices.Clone(bill.Amounts)
			if pay.err = convert(&bill); pay.err == nil {
				pay.templates = bill.GenerateTransferTemplates(ba)
			}
			payments = append(payments, pay)
		}
	}
	return payments, nil
}
