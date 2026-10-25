package model

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/goslu/sid"
	"github.com/SimonSchneider/pefigo/internal/pdb"
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
