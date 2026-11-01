package model

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/goslu/sid"
	"github.com/SimonSchneider/pefigo/internal/pdb"
	"github.com/SimonSchneider/pefigo/pkg/finance"
	"github.com/SimonSchneider/pefigo/pkg/ui"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

type StartupShareAccount struct {
	AccountID               string
	TaxRate                 float64
	ValuationDiscountFactor float64
}

type InvestmentRound struct {
	ID             string
	AccountID      string
	Date           date.Date
	Valuation      float64
	PreMoneyShares float64
	Investment     float64
	// EventID is the Event the investment round is in, or "" in the Baseline. In an Event its date is
	// stored as days after the Event's date.
	EventID   string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// hasSharesAndPrice reports whether the investment round values shares: it has shares and a price per
// share.
func (ir InvestmentRound) hasSharesAndPrice() bool {
	return ir.PreMoneyShares > 0 && ir.Valuation > 0
}

func (ir InvestmentRound) GetDateString() string {
	if ir.ID == "" {
		return ""
	}
	return ir.Date.String()
}

func (ir InvestmentRound) GetValuationString() string {
	if ir.ID == "" {
		return ""
	}
	return fmt.Sprintf("%.2f", ir.Valuation)
}

func (ir InvestmentRound) GetPreMoneySharesString() string {
	if ir.ID == "" {
		return ""
	}
	return fmt.Sprintf("%.2f", ir.PreMoneyShares)
}

func (ir InvestmentRound) GetInvestmentString() string {
	if ir.ID == "" {
		return ""
	}
	return fmt.Sprintf("%.2f", ir.Investment)
}

type ShareChange struct {
	ID          string
	AccountID   string
	Date        date.Date
	DeltaShares float64
	TotalPrice  float64
	// EventID is the Event the share change is in, or "" in the Baseline. In an Event its date is
	// stored as days after the Event's date.
	EventID   string
	CreatedAt time.Time
	UpdatedAt time.Time
}

func (sc ShareChange) GetDateString() string {
	if sc.ID == "" {
		return ""
	}
	return sc.Date.String()
}

func (sc ShareChange) GetDeltaSharesString() string {
	if sc.ID == "" {
		return ""
	}
	return fmt.Sprintf("%.2f", sc.DeltaShares)
}

func (sc ShareChange) GetTotalPriceString() string {
	if sc.ID == "" {
		return ""
	}
	return fmt.Sprintf("%.2f", sc.TotalPrice)
}

type StartupShareOption struct {
	ID                  string
	AccountID           string
	SourceAccountID     string
	Shares              float64
	StrikePricePerShare float64
	GrantDate           date.Date
	EndDate             date.Date
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

func (opt StartupShareOption) GetSharesString() string {
	if opt.ID == "" {
		return ""
	}
	return fmt.Sprintf("%.2f", opt.Shares)
}

func (opt StartupShareOption) GetStrikePriceString() string {
	if opt.ID == "" {
		return ""
	}
	return fmt.Sprintf("%.2f", opt.StrikePricePerShare)
}

func (opt StartupShareOption) GetGrantDateString() string {
	if opt.ID == "" {
		return ""
	}
	return opt.GrantDate.String()
}

func (opt StartupShareOption) GetEndDateString() string {
	if opt.ID == "" {
		return ""
	}
	return opt.EndDate.String()
}

// The Tax Rate and Valuation Discount a Startup Share Account gets when they aren't given.
const (
	DefaultStartupShareTaxRate = 0.15
	DefaultValuationDiscount   = 0.50
)

// StartupShareAccountInput holds the Tax Rate and Valuation Discount as fractions: 0.15 is 15%. A nil
// one is not given, and gets its default.
type StartupShareAccountInput struct {
	AccountID               string
	TaxRate                 *float64
	ValuationDiscountFactor *float64
}

// startupShareAccount is the Startup Share Account inp saves, with its defaults applied. It rejects a
// Tax Rate or Valuation Discount outside 0–100%.
func (inp StartupShareAccountInput) startupShareAccount() (StartupShareAccount, error) {
	taxRate, err := fractionOrDefault("tax rate", inp.TaxRate, DefaultStartupShareTaxRate)
	if err != nil {
		return StartupShareAccount{}, err
	}
	discount, err := fractionOrDefault("valuation discount", inp.ValuationDiscountFactor, DefaultValuationDiscount)
	if err != nil {
		return StartupShareAccount{}, err
	}
	return StartupShareAccount{AccountID: inp.AccountID, TaxRate: taxRate, ValuationDiscountFactor: discount}, nil
}

// fractionOrDefault is the given fraction, or def when it isn't given. It rejects a given fraction
// outside 0–100%, naming field.
func fractionOrDefault(field string, given *float64, def float64) (float64, error) {
	if given == nil {
		return def, nil
	}
	if !(*given >= 0 && *given <= 1) {
		return 0, fmt.Errorf("%w: %s %s%% is not within 0–100%%", ErrInvalidInput, field, ui.FormatPercent(*given))
	}
	return *given, nil
}

// InvestmentRoundInput is an investment round to save. In an Event its date may be given as days
// after the Event's date, it is the Event's date when it isn't given, and it is saved as an offset from
// the Event's date.
type InvestmentRoundInput struct {
	ID             string
	AccountID      string
	Date           date.Date
	Valuation      float64
	PreMoneyShares float64
	Investment     float64
	EventID        string
	// DateOffset, when set, gives the date in an Event as days after the date of OffsetEventID, the
	// Event the form showed it for, or of EventID when that is blank.
	DateOffset    *date.Duration
	OffsetEventID string
}

// ShareChangeInput is a share change to save, dated as an InvestmentRoundInput is.
type ShareChangeInput struct {
	ID            string
	AccountID     string
	Date          date.Date
	DeltaShares   float64
	TotalPrice    float64
	EventID       string
	DateOffset    *date.Duration
	OffsetEventID string
}

type StartupShareOptionInput struct {
	ID                  string
	AccountID           string
	SourceAccountID     string
	Shares              float64
	StrikePricePerShare float64
	GrantDate           date.Date
	EndDate             date.Date
}

type DerivedStartupShareSummary struct {
	SharesOwned              float64
	TotalShares              float64
	AvgPurchasePricePerShare float64
}

func startupShareAccountFromDB(s pdb.StartupShareAccount) StartupShareAccount {
	return StartupShareAccount{
		AccountID:               s.AccountID,
		TaxRate:                 s.TaxRate,
		ValuationDiscountFactor: s.ValuationDiscountFactor,
	}
}

// investmentRoundFromDB is the stored investment round i with its Event at at.
func investmentRoundFromDB(i pdb.InvestmentRound, at date.Date) InvestmentRound {
	return InvestmentRound{
		ID:             i.ID,
		AccountID:      i.AccountID,
		Date:           placeDate(i.Date, at),
		Valuation:      i.Valuation,
		PreMoneyShares: i.PreMoneyShares,
		Investment:     i.Investment,
		EventID:        ui.OrDefault(i.EventID),
		CreatedAt:      time.UnixMilli(i.CreatedAt),
		UpdatedAt:      time.UnixMilli(i.UpdatedAt),
	}
}

// shareChangeFromDB is the stored share change s with its Event at at.
func shareChangeFromDB(s pdb.ShareChange, at date.Date) ShareChange {
	return ShareChange{
		ID:          s.ID,
		AccountID:   s.AccountID,
		Date:        placeDate(s.Date, at),
		DeltaShares: s.DeltaShares,
		TotalPrice:  s.TotalPrice,
		EventID:     ui.OrDefault(s.EventID),
		CreatedAt:   time.UnixMilli(s.CreatedAt),
		UpdatedAt:   time.UnixMilli(s.UpdatedAt),
	}
}

func startupShareOptionFromDB(o pdb.StartupShareOption) StartupShareOption {
	return StartupShareOption{
		ID:                  o.ID,
		AccountID:           o.AccountID,
		SourceAccountID:     o.SourceAccountID,
		Shares:              o.Shares,
		StrikePricePerShare: o.StrikePricePerShare,
		GrantDate:           date.Date(o.GrantDate),
		EndDate:             date.Date(o.EndDate),
		CreatedAt:           time.UnixMilli(o.CreatedAt),
		UpdatedAt:           time.UnixMilli(o.UpdatedAt),
	}
}

func (s *Service) UpsertStartupShareAccount(ctx context.Context, inp StartupShareAccountInput) (StartupShareAccount, error) {
	// The account is checked here, not in startupShareAccount: UpsertAccountWithStartupShares checks
	// the rest before the account it is for is saved.
	if err := checkRequired("account", inp.AccountID); err != nil {
		return StartupShareAccount{}, err
	}
	ssa, err := inp.startupShareAccount()
	if err != nil {
		return StartupShareAccount{}, err
	}
	saved, err := s.q.UpsertStartupShareAccount(ctx, pdb.UpsertStartupShareAccountParams{
		AccountID:               ssa.AccountID,
		TaxRate:                 ssa.TaxRate,
		ValuationDiscountFactor: ssa.ValuationDiscountFactor,
	})
	if err != nil {
		return StartupShareAccount{}, fmt.Errorf("failed to upsert startup share account: %w", err)
	}
	return startupShareAccountFromDB(saved), nil
}

func (s *Service) GetStartupShareAccount(ctx context.Context, accountID string) (StartupShareAccount, error) {
	ssa, err := s.q.GetStartupShareAccount(ctx, accountID)
	if err != nil {
		return StartupShareAccount{}, err
	}
	return startupShareAccountFromDB(ssa), nil
}

func (s *Service) DeleteStartupShareAccount(ctx context.Context, accountID string) error {
	if err := s.q.DeleteStartupShareAccount(ctx, accountID); err != nil {
		return fmt.Errorf("failed to delete startup share account: %w", err)
	}
	return nil
}

func (s *Service) UpsertInvestmentRound(ctx context.Context, inp InvestmentRoundInput) (InvestmentRound, error) {
	dating, err := s.entryDating(ctx, inp.EventID, inp.OffsetEventID)
	if err != nil {
		return InvestmentRound{}, err
	}
	if inp.Date, err = dating.firstDay("date", inp.Date, inp.DateOffset); err != nil {
		return InvestmentRound{}, err
	}
	if err := errors.Join(checkRequired("account", inp.AccountID), checkRequired("date", inp.Date)); err != nil {
		return InvestmentRound{}, err
	}
	if inp.ID == "" {
		inp.ID = sid.MustNewString(32)
	}
	// Saving a round never touches another: several may be on one day, and compareInvestmentRounds says
	// which of them applies.
	ir, err := s.q.UpsertInvestmentRound(ctx, pdb.UpsertInvestmentRoundParams{
		ID:             inp.ID,
		AccountID:      inp.AccountID,
		Date:           dating.store(inp.Date),
		Valuation:      inp.Valuation,
		PreMoneyShares: inp.PreMoneyShares,
		Investment:     inp.Investment,
		EventID:        ui.WithDefaultNull(inp.EventID),
		CreatedAt:      time.Now().UnixMilli(),
		UpdatedAt:      time.Now().UnixMilli(),
	})
	if err != nil {
		return InvestmentRound{}, fmt.Errorf("failed to upsert investment round: %w", err)
	}
	return investmentRoundFromDB(ir, dating.at), nil
}

// GetInvestmentRound returns the investment round id, in an Event at the Event's date.
func (s *Service) GetInvestmentRound(ctx context.Context, id string) (InvestmentRound, error) {
	ir, err := s.q.GetInvestmentRound(ctx, id)
	if err != nil {
		return InvestmentRound{}, fmt.Errorf("failed to get investment round: %w", err)
	}
	at, err := s.eventDate(ctx, ui.OrDefault(ir.EventID))
	if err != nil {
		return InvestmentRound{}, err
	}
	return investmentRoundFromDB(ir, at), nil
}

// input is the investment round as an input that saves it unchanged.
func (ir InvestmentRound) input() InvestmentRoundInput {
	return InvestmentRoundInput{
		ID: ir.ID, AccountID: ir.AccountID, Date: ir.Date, Valuation: ir.Valuation, PreMoneyShares: ir.PreMoneyShares, Investment: ir.Investment, EventID: ir.EventID,
	}
}

// ListInvestmentRounds returns the account's investment rounds, latest first, each in an Event at the
// Event's date.
func (s *Service) ListInvestmentRounds(ctx context.Context, accountID string) ([]InvestmentRound, error) {
	rounds, err := s.q.ListInvestmentRounds(ctx, accountID)
	if err != nil {
		return nil, fmt.Errorf("failed to list investment rounds: %w", err)
	}
	dates, err := s.eventDates(ctx)
	if err != nil {
		return nil, err
	}
	return investmentRoundsOf(rounds, dates), nil
}

// compareInvestmentRounds orders investment rounds by date, then by when they were last saved, then by
// ID. An account may have several rounds on one day, in Events or not: of those with shares and a
// price per share, the last in this order applies, the one saved last. See latestRoundOn.
func compareInvestmentRounds(a, b InvestmentRound) int {
	return cmp.Or(cmp.Compare(a.Date, b.Date), a.UpdatedAt.Compare(b.UpdatedAt), cmp.Compare(a.ID, b.ID))
}

func (s *Service) DeleteInvestmentRound(ctx context.Context, id string) error {
	if err := s.q.DeleteInvestmentRound(ctx, id); err != nil {
		return fmt.Errorf("failed to delete investment round: %w", err)
	}
	return nil
}

func (s *Service) UpsertShareChange(ctx context.Context, inp ShareChangeInput) (ShareChange, error) {
	dating, err := s.entryDating(ctx, inp.EventID, inp.OffsetEventID)
	if err != nil {
		return ShareChange{}, err
	}
	if inp.Date, err = dating.firstDay("date", inp.Date, inp.DateOffset); err != nil {
		return ShareChange{}, err
	}
	if err := errors.Join(checkRequired("account", inp.AccountID), checkRequired("date", inp.Date)); err != nil {
		return ShareChange{}, err
	}
	if inp.ID == "" {
		inp.ID = sid.MustNewString(32)
	}
	sc, err := s.q.UpsertShareChange(ctx, pdb.UpsertShareChangeParams{
		ID:          inp.ID,
		AccountID:   inp.AccountID,
		Date:        dating.store(inp.Date),
		DeltaShares: inp.DeltaShares,
		TotalPrice:  inp.TotalPrice,
		EventID:     ui.WithDefaultNull(inp.EventID),
		CreatedAt:   time.Now().UnixMilli(),
		UpdatedAt:   time.Now().UnixMilli(),
	})
	if err != nil {
		return ShareChange{}, fmt.Errorf("failed to upsert share change: %w", err)
	}
	return shareChangeFromDB(sc, dating.at), nil
}

// GetShareChange returns the share change id, in an Event at the Event's date.
func (s *Service) GetShareChange(ctx context.Context, id string) (ShareChange, error) {
	sc, err := s.q.GetShareChange(ctx, id)
	if err != nil {
		return ShareChange{}, fmt.Errorf("failed to get share change: %w", err)
	}
	at, err := s.eventDate(ctx, ui.OrDefault(sc.EventID))
	if err != nil {
		return ShareChange{}, err
	}
	return shareChangeFromDB(sc, at), nil
}

// input is the share change as an input that saves it unchanged.
func (sc ShareChange) input() ShareChangeInput {
	return ShareChangeInput{ID: sc.ID, AccountID: sc.AccountID, Date: sc.Date, DeltaShares: sc.DeltaShares, TotalPrice: sc.TotalPrice, EventID: sc.EventID}
}

// ListShareChanges returns the account's share changes by date and ID, each in an Event at the Event's
// date.
func (s *Service) ListShareChanges(ctx context.Context, accountID string) ([]ShareChange, error) {
	changes, err := s.q.ListShareChanges(ctx, accountID)
	if err != nil {
		return nil, fmt.Errorf("failed to list share changes: %w", err)
	}
	dates, err := s.eventDates(ctx)
	if err != nil {
		return nil, err
	}
	return shareChangesOf(changes, dates), nil
}

// dayAndID are the share change's date and ID, which order it among the others.
func (sc ShareChange) dayAndID() (date.Date, string) { return sc.Date, sc.ID }

func (s *Service) DeleteShareChange(ctx context.Context, id string) error {
	if err := s.q.DeleteShareChange(ctx, id); err != nil {
		return fmt.Errorf("failed to delete share change: %w", err)
	}
	return nil
}

func DeriveShareState(changes []ShareChange, asOf date.Date) (sharesOwned float64, avgPurchasePricePerShare float64) {
	var totalCostBasis float64
	var costBasisShares float64
	for _, c := range changes {
		if c.Date > asOf {
			continue
		}
		sharesOwned += c.DeltaShares
		if c.DeltaShares > 0 {
			totalCostBasis += c.TotalPrice
			costBasisShares += c.DeltaShares
		}
	}
	if costBasisShares <= 0 {
		return sharesOwned, 0
	}
	avgPurchasePricePerShare = totalCostBasis / costBasisShares
	return sharesOwned, avgPurchasePricePerShare
}

func PostMoneyValuationAndShares(preMoneyValuation, preMoneyShares, investment float64) (postMoneyValuation, postMoneyShares float64) {
	postMoneyValuation = preMoneyValuation + investment
	if preMoneyShares <= 0 {
		return postMoneyValuation, preMoneyShares
	}
	pricePerShare := preMoneyValuation / preMoneyShares
	newShares := investment / pricePerShare
	postMoneyShares = preMoneyShares + newShares
	return postMoneyValuation, postMoneyShares
}

func (s *Service) UpsertStartupShareOption(ctx context.Context, inp StartupShareOptionInput) (StartupShareOption, error) {
	if err := errors.Join(
		checkRequired("account", inp.AccountID),
		checkRequired("source account", inp.SourceAccountID),
		checkRequired("grant date", inp.GrantDate),
		checkRequired("expiry date", inp.EndDate),
	); err != nil {
		return StartupShareOption{}, err
	}
	if inp.ID == "" {
		inp.ID = sid.MustNewString(32)
	}
	opt, err := s.q.UpsertStartupShareOption(ctx, pdb.UpsertStartupShareOptionParams{
		ID:                  inp.ID,
		AccountID:           inp.AccountID,
		SourceAccountID:     inp.SourceAccountID,
		Shares:              inp.Shares,
		StrikePricePerShare: inp.StrikePricePerShare,
		GrantDate:           int64(inp.GrantDate),
		EndDate:             int64(inp.EndDate),
		CreatedAt:           time.Now().UnixMilli(),
		UpdatedAt:           time.Now().UnixMilli(),
	})
	if err != nil {
		return StartupShareOption{}, fmt.Errorf("failed to upsert startup share option: %w", err)
	}
	return startupShareOptionFromDB(opt), nil
}

func (s *Service) GetStartupShareOption(ctx context.Context, id string) (StartupShareOption, error) {
	opt, err := s.q.GetStartupShareOption(ctx, id)
	if err != nil {
		return StartupShareOption{}, fmt.Errorf("failed to get startup share option: %w", err)
	}
	return startupShareOptionFromDB(opt), nil
}

func (s *Service) ListStartupShareOptions(ctx context.Context, accountID string) ([]StartupShareOption, error) {
	opts, err := s.q.ListStartupShareOptions(ctx, accountID)
	if err != nil {
		return nil, fmt.Errorf("failed to list startup share options: %w", err)
	}
	return startupShareOptionsOf(opts), nil
}

// startupShareOptionsOf are the stored Options, in their order.
func startupShareOptionsOf(rows []pdb.StartupShareOption) []StartupShareOption {
	opts := make([]StartupShareOption, len(rows))
	for i, o := range rows {
		opts[i] = startupShareOptionFromDB(o)
	}
	return opts
}

func (s *Service) DeleteStartupShareOption(ctx context.Context, id string) error {
	if err := s.q.DeleteStartupShareOption(ctx, id); err != nil {
		return fmt.Errorf("failed to delete startup share option: %w", err)
	}
	return nil
}

func CalculateStartupShareBalance(cfg *uncertain.Config, valuation uncertain.Value, sharesOwned float64, purchasePricePerShare float64, taxRate float64, totalShares float64, discountFactor float64) uncertain.Value {
	grossValue := valuation.Mul(cfg, uncertain.NewFixed(sharesOwned/totalShares*discountFactor))
	purchasePrice := purchasePricePerShare * sharesOwned

	if purchasePrice > grossValue.Mean() {
		return grossValue
	}
	capitalGains := grossValue.Sub(cfg, uncertain.NewFixed(purchasePrice))
	tax := capitalGains.Mul(cfg, uncertain.NewFixed(taxRate))
	return grossValue.Sub(cfg, tax)
}

// startupShares is a Startup Share account with its investment rounds, share changes and options.
type startupShares struct {
	StartupShareAccount
	rounds       []InvestmentRound    // by date
	shareChanges []ShareChange        // by date and ID
	options      []StartupShareOption // by grant date and ID
}

// latestRoundOn is the latest investment round on or before day with shares and a price per share,
// or nil without one: the round that values the account on day, in a Forecast, its Balance History and
// its page alike. A round without shares or a price per share values nothing, so it never hides another.
func (ss *startupShares) latestRoundOn(day date.Date) *InvestmentRound {
	var latest *InvestmentRound
	for i, r := range ss.rounds {
		if r.Date.After(day) {
			break
		}
		if r.hasSharesAndPrice() {
			latest = &ss.rounds[i]
		}
	}
	return latest
}

// growthOn is the account's StartupGrowth in a Forecast that starts on day. It holds the shares on
// day, valued by the latest investment round on or before day with shares and a price per share, so
// its Balance is the account's derived balance on day: 0 without such a round. It changes with the
// later investment rounds and share changes, and with the options on their End Dates.
func (ss *startupShares) growthOn(day date.Date) *finance.StartupGrowth {
	sharesOwned, avgPurchasePrice := DeriveShareState(ss.shareChanges, day)
	growth := &finance.StartupGrowth{
		TotalShares:           uncertain.NewFixed(0),
		OwnedShares:           uncertain.NewFixed(sharesOwned),
		Valuation:             uncertain.NewFixed(0),
		TaxRate:               uncertain.NewFixed(ss.TaxRate),
		DiscountFactor:        uncertain.NewFixed(ss.ValuationDiscountFactor),
		PurchasePricePerShare: uncertain.NewFixed(avgPurchasePrice),
		InvestmentRounds:      make(map[date.Date]finance.StartupGrowthInvestmentRound),
		ShareChanges:          make(map[date.Date][]finance.StartupGrowthShareChange),
		Options:               make(map[date.Date][]finance.StartupGrowthOption, len(ss.options)),
	}
	if round := ss.latestRoundOn(day); round != nil {
		postMoneyValuation, postMoneyShares := PostMoneyValuationAndShares(round.Valuation, round.PreMoneyShares, round.Investment)
		growth.Valuation = uncertain.NewFixed(postMoneyValuation)
		growth.TotalShares = uncertain.NewFixed(postMoneyShares)
	}
	// What is dated on or before day is in the shares on day already: applying it again on day would
	// count it twice. A round without shares or a price per share values nothing, as in latestRoundOn,
	// so it never hides another round on its day.
	for _, r := range ss.rounds {
		if r.Date.After(day) && r.hasSharesAndPrice() {
			growth.InvestmentRounds[r.Date] = finance.StartupGrowthInvestmentRound{
				PreMoneyValuation: uncertain.NewFixed(r.Valuation),
				PreMoneyShares:    uncertain.NewFixed(r.PreMoneyShares),
				Investment:        uncertain.NewFixed(r.Investment),
			}
		}
	}
	for _, c := range ss.shareChanges {
		if c.Date.After(day) {
			growth.ShareChanges[c.Date] = append(growth.ShareChanges[c.Date], finance.StartupGrowthShareChange{
				DeltaShares: uncertain.NewFixed(c.DeltaShares),
				TotalPrice:  uncertain.NewFixed(c.TotalPrice),
			})
		}
	}
	for _, o := range ss.options {
		growth.Options[o.EndDate] = append(growth.Options[o.EndDate], finance.StartupGrowthOption{
			StrikePricePerShare: uncertain.NewFixed(o.StrikePricePerShare),
			NumShares:           uncertain.NewFixed(o.Shares),
			SourceAccountID:     o.SourceAccountID,
		})
	}
	return growth
}

// history is the account's derived balance on each day one of its investment rounds or share changes
// is dated, from the first day it has shares to value, so selling them all shows as 0. In a Forecast,
// which starts on start, it leaves out the days from start on and ends with its balance on start.
func (ss *startupShares) history(ucfg *uncertain.Config, start date.Date, isForecast bool) []finance.BalanceSnapshot {
	days := make([]date.Date, 0, len(ss.rounds)+len(ss.shareChanges))
	for _, r := range ss.rounds {
		days = append(days, r.Date)
	}
	for _, c := range ss.shareChanges {
		days = append(days, c.Date)
	}
	slices.Sort(days)
	var history []finance.BalanceSnapshot
	for _, day := range slices.Compact(days) {
		if isForecast && !day.Before(start) {
			break
		}
		if ss.latestRoundOn(day) == nil {
			continue
		}
		if growth := ss.growthOn(day); len(history) > 0 || growth.OwnedShares.Mean() != 0 {
			history = append(history, finance.BalanceSnapshot{Date: day, Balance: growth.Balance(ucfg)})
		}
	}
	if isForecast {
		history = append(history, finance.BalanceSnapshot{Date: start, Balance: ss.growthOn(start).Balance(ucfg)})
	}
	return history
}

// balanceOn is the account's balance on day, the mean of the value derived from the round that values
// it on day and its share changes on or before day. ok is false without such a round.
func (ss *startupShares) balanceOn(ucfg *uncertain.Config, day date.Date) (balance float64, ok bool) {
	round := ss.latestRoundOn(day)
	if round == nil {
		return 0, false
	}
	postMoneyValuation, postMoneyShares := PostMoneyValuationAndShares(round.Valuation, round.PreMoneyShares, round.Investment)
	sharesOwned, avgPurchasePrice := DeriveShareState(ss.shareChanges, day)
	return CalculateStartupShareBalance(
		ucfg,
		uncertain.NewFixed(postMoneyValuation),
		sharesOwned,
		avgPurchasePrice,
		ss.TaxRate,
		postMoneyShares,
		ss.ValuationDiscountFactor,
	).Mean(), true
}

// summaryOn is what the account holds on day: the shares of its share changes on or before day, and the
// total shares after the round that values it on day, or none without one.
func (ss *startupShares) summaryOn(day date.Date) DerivedStartupShareSummary {
	var totalShares float64
	if round := ss.latestRoundOn(day); round != nil {
		_, totalShares = PostMoneyValuationAndShares(round.Valuation, round.PreMoneyShares, round.Investment)
	}
	sharesOwned, avgPrice := DeriveShareState(ss.shareChanges, day)
	return DerivedStartupShareSummary{SharesOwned: sharesOwned, TotalShares: totalShares, AvgPurchasePricePerShare: avgPrice}
}

// startupShareSummary is what the Startup Share account acc holds on day, as the plans of its Event
// show it (see placings.shownBy): in the Baseline, or for an account of an unplanned Event, in the
// Baseline with that Event planned. It is nil for an account that isn't a Startup Share account.
func (pl *placings) startupShareSummary(acc Account, day date.Date) (*DerivedStartupShareSummary, error) {
	p, _, err := pl.shownBy(acc.EventID)
	if err != nil {
		return nil, err
	}
	ss, ok := p.startupShares[acc.ID]
	if !ok {
		return nil, nil
	}
	summary := ss.summaryOn(day)
	return &summary, nil
}

// investmentRoundsOf are the stored rounds, each at its own Event's date of dates, latest first: of
// two on one day, the one that applies first.
func investmentRoundsOf(rows []pdb.InvestmentRound, dates map[string]date.Date) []InvestmentRound {
	rounds := make([]InvestmentRound, len(rows))
	for i, r := range rows {
		rounds[i] = investmentRoundFromDB(r, ownDate(dates, r.EventID))
	}
	slices.SortStableFunc(rounds, func(a, b InvestmentRound) int { return compareInvestmentRounds(b, a) })
	return rounds
}

// shareChangesOf are the stored share changes, each at its own Event's date of dates, by date and ID.
func shareChangesOf(rows []pdb.ShareChange, dates map[string]date.Date) []ShareChange {
	changes := make([]ShareChange, len(rows))
	for i, c := range rows {
		changes[i] = shareChangeFromDB(c, ownDate(dates, c.EventID))
	}
	sortByDayAndID(changes, ShareChange.dayAndID)
	return changes
}
