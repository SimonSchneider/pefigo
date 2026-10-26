package model

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"time"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/goslu/sid"
	"github.com/SimonSchneider/pefigo/internal/pdb"
	"github.com/SimonSchneider/pefigo/pkg/finance"
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
	CreatedAt      time.Time
	UpdatedAt      time.Time
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
	CreatedAt   time.Time
	UpdatedAt   time.Time
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

type StartupShareAccountInput struct {
	AccountID               string
	TaxRate                 float64
	ValuationDiscountFactor float64
}

type InvestmentRoundInput struct {
	ID             string
	AccountID      string
	Date           date.Date
	Valuation      float64
	PreMoneyShares float64
	Investment     float64
}

type ShareChangeInput struct {
	ID          string
	AccountID   string
	Date        date.Date
	DeltaShares float64
	TotalPrice  float64
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

func investmentRoundFromDB(i pdb.InvestmentRound) InvestmentRound {
	return InvestmentRound{
		ID:             i.ID,
		AccountID:      i.AccountID,
		Date:           date.Date(i.Date),
		Valuation:      i.Valuation,
		PreMoneyShares: i.PreMoneyShares,
		Investment:     i.Investment,
		CreatedAt:      time.UnixMilli(i.CreatedAt),
		UpdatedAt:      time.UnixMilli(i.UpdatedAt),
	}
}

func shareChangeFromDB(s pdb.ShareChange) ShareChange {
	return ShareChange{
		ID:          s.ID,
		AccountID:   s.AccountID,
		Date:        date.Date(s.Date),
		DeltaShares: s.DeltaShares,
		TotalPrice:  s.TotalPrice,
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
	ssa, err := s.q.UpsertStartupShareAccount(ctx, pdb.UpsertStartupShareAccountParams{
		AccountID:               inp.AccountID,
		TaxRate:                 inp.TaxRate,
		ValuationDiscountFactor: inp.ValuationDiscountFactor,
	})
	if err != nil {
		return StartupShareAccount{}, fmt.Errorf("failed to upsert startup share account: %w", err)
	}
	return startupShareAccountFromDB(ssa), nil
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
	if inp.ID == "" {
		inp.ID = sid.MustNewString(32)
	}
	ir, err := s.q.UpsertInvestmentRound(ctx, pdb.UpsertInvestmentRoundParams{
		ID:             inp.ID,
		AccountID:      inp.AccountID,
		Date:           int64(inp.Date),
		Valuation:      inp.Valuation,
		PreMoneyShares: inp.PreMoneyShares,
		Investment:     inp.Investment,
		CreatedAt:      time.Now().UnixMilli(),
		UpdatedAt:      time.Now().UnixMilli(),
	})
	if err != nil {
		return InvestmentRound{}, fmt.Errorf("failed to upsert investment round: %w", err)
	}
	return investmentRoundFromDB(ir), nil
}

func (s *Service) GetInvestmentRound(ctx context.Context, id string) (InvestmentRound, error) {
	ir, err := s.q.GetInvestmentRound(ctx, id)
	if err != nil {
		return InvestmentRound{}, fmt.Errorf("failed to get investment round: %w", err)
	}
	return investmentRoundFromDB(ir), nil
}

func (s *Service) ListInvestmentRounds(ctx context.Context, accountID string) ([]InvestmentRound, error) {
	rounds, err := s.q.ListInvestmentRounds(ctx, accountID)
	if err != nil {
		return nil, fmt.Errorf("failed to list investment rounds: %w", err)
	}
	result := make([]InvestmentRound, len(rounds))
	for i, r := range rounds {
		result[i] = investmentRoundFromDB(r)
	}
	return result, nil
}

func (s *Service) GetLatestInvestmentRound(ctx context.Context, accountID string, d date.Date) (InvestmentRound, error) {
	ir, err := s.q.GetLatestInvestmentRound(ctx, pdb.GetLatestInvestmentRoundParams{
		AccountID: accountID,
		Date:      int64(d),
	})
	if err != nil {
		if err == sql.ErrNoRows {
			return InvestmentRound{}, sql.ErrNoRows
		}
		return InvestmentRound{}, fmt.Errorf("failed to get latest investment round: %w", err)
	}
	return investmentRoundFromDB(ir), nil
}

func (s *Service) DeleteInvestmentRound(ctx context.Context, id string) error {
	if err := s.q.DeleteInvestmentRound(ctx, id); err != nil {
		return fmt.Errorf("failed to delete investment round: %w", err)
	}
	return nil
}

func (s *Service) UpsertShareChange(ctx context.Context, inp ShareChangeInput) (ShareChange, error) {
	if inp.ID == "" {
		inp.ID = sid.MustNewString(32)
	}
	sc, err := s.q.UpsertShareChange(ctx, pdb.UpsertShareChangeParams{
		ID:          inp.ID,
		AccountID:   inp.AccountID,
		Date:        int64(inp.Date),
		DeltaShares: inp.DeltaShares,
		TotalPrice:  inp.TotalPrice,
		CreatedAt:   time.Now().UnixMilli(),
		UpdatedAt:   time.Now().UnixMilli(),
	})
	if err != nil {
		return ShareChange{}, fmt.Errorf("failed to upsert share change: %w", err)
	}
	return shareChangeFromDB(sc), nil
}

func (s *Service) GetShareChange(ctx context.Context, id string) (ShareChange, error) {
	sc, err := s.q.GetShareChange(ctx, id)
	if err != nil {
		return ShareChange{}, fmt.Errorf("failed to get share change: %w", err)
	}
	return shareChangeFromDB(sc), nil
}

func (s *Service) ListShareChanges(ctx context.Context, accountID string) ([]ShareChange, error) {
	changes, err := s.q.ListShareChanges(ctx, accountID)
	if err != nil {
		return nil, fmt.Errorf("failed to list share changes: %w", err)
	}
	result := make([]ShareChange, len(changes))
	for i, c := range changes {
		result[i] = shareChangeFromDB(c)
	}
	return result, nil
}

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
	result := make([]StartupShareOption, len(opts))
	for i, o := range opts {
		result[i] = startupShareOptionFromDB(o)
	}
	return result, nil
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

// listStartupShares returns every Startup Share account, keyed by account ID, with a fixed number of
// queries.
func (s *Service) listStartupShares(ctx context.Context) (map[string]*startupShares, error) {
	accounts, err := s.q.ListStartupShareAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing startup share accounts: %w", err)
	}
	rounds, err := s.q.ListAllInvestmentRounds(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing investment rounds: %w", err)
	}
	changes, err := s.q.ListAllShareChanges(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing share changes: %w", err)
	}
	options, err := s.q.ListAllStartupShareOptions(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing startup share options: %w", err)
	}
	byAccount := make(map[string]*startupShares, len(accounts))
	for _, a := range accounts {
		byAccount[a.AccountID] = &startupShares{StartupShareAccount: startupShareAccountFromDB(a)}
	}
	for _, r := range rounds {
		if ss, ok := byAccount[r.AccountID]; ok {
			ss.rounds = append(ss.rounds, investmentRoundFromDB(r))
		}
	}
	for _, c := range changes {
		if ss, ok := byAccount[c.AccountID]; ok {
			ss.shareChanges = append(ss.shareChanges, shareChangeFromDB(c))
		}
	}
	for _, o := range options {
		if ss, ok := byAccount[o.AccountID]; ok {
			ss.options = append(ss.options, startupShareOptionFromDB(o))
		}
	}
	return byAccount, nil
}

// latestRoundOn is the latest investment round on or before day with shares and a price per share,
// or nil without one.
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
	// count it twice.
	for _, r := range ss.rounds {
		if r.Date.After(day) {
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

// startupShareHistory is a Startup Share account without regular snapshots: its balance is derived
// from its investment rounds and share changes.
type startupShareHistory struct {
	typeID       string // "" for Uncategorized
	rounds       []pdb.ListStartupShareSnapshotHistoryRow
	shareChanges []ShareChange
}

// listStartupShareHistories returns the Startup Share accounts with investment rounds that aren't
// in withSnapshots, keyed by account ID. A nil withSnapshots leaves none out.
func (s *Service) listStartupShareHistories(ctx context.Context, withSnapshots map[string]struct{}) (map[string]*startupShareHistory, error) {
	rows, err := s.q.ListStartupShareSnapshotHistory(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing startup share snapshot history: %w", err)
	}
	histories := make(map[string]*startupShareHistory)
	for _, r := range rows {
		if _, has := withSnapshots[r.AccountID]; has {
			continue
		}
		h, ok := histories[r.AccountID]
		if !ok {
			h = &startupShareHistory{typeID: r.TypeID}
			histories[r.AccountID] = h
		}
		h.rounds = append(h.rounds, r)
	}
	for accountID, h := range histories {
		changes, err := s.ListShareChanges(ctx, accountID)
		if err != nil {
			return nil, fmt.Errorf("listing share changes for %s: %w", accountID, err)
		}
		h.shareChanges = changes
	}
	return histories, nil
}

// balanceOn is the account's balance on day, the mean of the value derived from its latest
// investment round and its share changes on or before day. ok is false when there is no such
// investment round, or it has no shares or no price per share.
func (h *startupShareHistory) balanceOn(ucfg *uncertain.Config, day date.Date) (balance float64, ok bool) {
	var best *pdb.ListStartupShareSnapshotHistoryRow
	for i := range h.rounds {
		if date.Date(h.rounds[i].Date) <= day {
			best = &h.rounds[i]
		}
	}
	if best == nil {
		return 0, false
	}
	postMoneyValuation, postMoneyShares := PostMoneyValuationAndShares(best.Valuation, best.PreMoneyShares, best.Investment)
	if !(postMoneyShares > 0) { // NaN when the round has neither a valuation nor an investment
		return 0, false
	}
	sharesOwned, avgPurchasePrice := DeriveShareState(h.shareChanges, day)
	return CalculateStartupShareBalance(
		ucfg,
		uncertain.NewFixed(postMoneyValuation),
		sharesOwned,
		avgPurchasePrice,
		best.TaxRate,
		postMoneyShares,
		best.ValuationDiscountFactor,
	).Mean(), true
}
