package finance

import (
	"fmt"
	"math"
	"sort"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

type GrowthModel interface {
	StartsOn() date.Date // Returns the start date of the growth model
	IsActiveOn(date date.Date) bool
	Apply(ucfg *uncertain.Config, day date.Date, entities map[string]*ModeledEntity, totalBalance uncertain.Value) (delta uncertain.Value)
}

type TimeFrameGrowth struct {
	StartDate date.Date
	EndDate   *date.Date // optional
}

func (i *TimeFrameGrowth) StartsOn() date.Date {
	return i.StartDate
}

func (i *TimeFrameGrowth) IsActiveOn(day date.Date) bool {
	return appliesOn(i.StartDate, i.EndDate, day)
}

type GrowthCombined struct {
	Growths []GrowthModel
}

func NewGrowthCombined(growths ...GrowthModel) *GrowthCombined {
	// Sort growths by start date, keeping the given order for equal start dates
	sort.SliceStable(growths, func(i, j int) bool {
		return growths[i].StartsOn().Before(growths[j].StartsOn())
	})
	return &GrowthCombined{Growths: growths}
}

func (g *GrowthCombined) StartsOn() date.Date {
	if len(g.Growths) == 0 {
		return date.Date(0) // Return zero date if no growths are defined
	}
	return g.Growths[0].StartsOn()
}

func (g *GrowthCombined) IsActiveOn(day date.Date) bool {
	for _, gr := range g.Growths {
		if gr.IsActiveOn(day) {
			return true
		}
	}
	return false
}

// Apply applies the latest-starting growth model that applies on day. With equal start dates, the one
// given last wins.
func (g *GrowthCombined) Apply(ucfg *uncertain.Config, day date.Date, entities map[string]*ModeledEntity, totalBalance uncertain.Value) uncertain.Value {
	for i := len(g.Growths) - 1; i >= 0; i-- {
		if g.Growths[i].IsActiveOn(day) {
			return g.Growths[i].Apply(ucfg, day, entities, totalBalance)
		}
	}
	return uncertain.NewFixed(0.0)
}

var _ GrowthModel = &GrowthCombined{}
var _ GrowthModel = &FixedGrowth{}
var _ GrowthModel = &LogNormalGrowth{}
var _ GrowthModel = &StartupGrowth{}

type FixedGrowth struct {
	TimeFrameGrowth
	AnnualRate uncertain.Value // Annual growth rate, e.g. 0.05 for 5%
}

func (i *FixedGrowth) Apply(ucfg *uncertain.Config, day date.Date, entities map[string]*ModeledEntity, totalBalance uncertain.Value) uncertain.Value {
	dailyGrowthFactor := i.AnnualRate.Add(ucfg, uncertain.NewFixed(1)).Pow(ucfg, uncertain.NewFixed(1.0/365.0))
	return totalBalance.Mul(ucfg, dailyGrowthFactor.Sub(ucfg, uncertain.NewFixed(1)))
}

type LogNormalGrowth struct {
	TimeFrameGrowth
	AnnualRate       uncertain.Value
	AnnualVolatility uncertain.Value // Optional, can be used for more complex models
}

func (i *LogNormalGrowth) Apply(ucfg *uncertain.Config, day date.Date, entities map[string]*ModeledEntity, totalBalance uncertain.Value) uncertain.Value {
	dailyMu := i.AnnualRate.Mul(ucfg, uncertain.NewFixed(1.0/365.0))
	dailySigma := i.AnnualVolatility.Mul(ucfg, uncertain.NewFixed(1.0/math.Sqrt(365)))
	if !i.AnnualVolatility.Valid() {
		dailySigma = uncertain.NewFixed(0.0) // If no volatility is set, use 0
	}

	// Daily log return is normally distributed: N(dailyMu, dailySigma)
	dailyLogReturn := uncertain.NewMapped(
		func(cfg *uncertain.Config) float64 {
			mu := dailyMu.Sample(cfg)
			sigma := dailySigma.Sample(cfg)
			return cfg.RNG.NormFloat64()*sigma + mu
		},
	)

	// Convert to growth factor: exp(log_return) - 1
	dailyGrowth := dailyLogReturn.Exp().Sub(ucfg, uncertain.NewFixed(1))

	return totalBalance.Mul(ucfg, dailyGrowth)
}

type StartupGrowthInvestmentRound struct {
	PreMoneyValuation uncertain.Value
	PreMoneyShares    uncertain.Value
	Investment        uncertain.Value
}

type StartupGrowthShareChange struct {
	DeltaShares uncertain.Value
	TotalPrice  uncertain.Value
}

type StartupGrowthOption struct {
	StrikePricePerShare uncertain.Value
	NumShares           uncertain.Value
	SourceAccountID     string
}

type StartupGrowth struct {
	TimeFrameGrowth
	TotalShares uncertain.Value
	OwnedShares uncertain.Value
	Valuation   uncertain.Value

	TaxRate               uncertain.Value
	DiscountFactor        uncertain.Value
	PurchasePricePerShare uncertain.Value

	InvestmentRounds map[date.Date]StartupGrowthInvestmentRound
	ShareChanges     map[date.Date]StartupGrowthShareChange
	Options          map[date.Date]StartupGrowthOption
}

func (s *StartupGrowth) Apply(ucfg *uncertain.Config, day date.Date, entities map[string]*ModeledEntity, totalBalance uncertain.Value) uncertain.Value {
	var changed bool
	// A round without pre-money shares or a price per share is ignored.
	if round, ok := s.InvestmentRounds[day]; ok && round.PreMoneyShares.Mean() > 0 && round.PreMoneyValuation.Mean() > 0 {
		pricePerShare := round.PreMoneyValuation.Div(ucfg, round.PreMoneyShares)
		issuedShares := round.Investment.Div(ucfg, pricePerShare)
		s.TotalShares = round.PreMoneyShares.Add(ucfg, issuedShares)
		s.Valuation = round.PreMoneyValuation.Add(ucfg, round.Investment)
		changed = true
	}
	if sc, ok := s.ShareChanges[day]; ok {
		prevOwned := s.OwnedShares
		s.OwnedShares = s.OwnedShares.Add(ucfg, sc.DeltaShares)
		if sc.DeltaShares.Mean() > 0 {
			currentCostBasis := s.PurchasePricePerShare.Mul(ucfg, prevOwned)
			newCostBasis := currentCostBasis.Add(ucfg, sc.TotalPrice)
			s.PurchasePricePerShare = newCostBasis.Div(ucfg, s.OwnedShares)
		}
		changed = true
	}
	if opt, ok := s.Options[day]; ok {
		sourceAccount, ok := entities[opt.SourceAccountID]
		if !ok {
			panic(fmt.Sprintf("no account found for %s", opt.SourceAccountID))
		}
		currentPricePerShare := s.Valuation.Div(ucfg, s.TotalShares)
		if currentPricePerShare.Mean() > opt.StrikePricePerShare.Mean() {
			strikeCost := opt.StrikePricePerShare.Mul(ucfg, opt.NumShares)
			sourceAccount.balance = sourceAccount.balance.Sub(ucfg, strikeCost)
			s.TotalShares = s.TotalShares.Add(ucfg, opt.NumShares)
			s.Valuation = s.Valuation.Add(ucfg, strikeCost)
			s.OwnedShares = s.OwnedShares.Add(ucfg, opt.NumShares)
			changed = true
		}
	}
	if changed {
		return s.Balance(ucfg).Sub(ucfg, totalBalance)
	}
	return uncertain.NewFixed(0.0)
}

// hasPricePerShare reports whether the shares are valued by an investment round with shares and a price per share.
func (s *StartupGrowth) hasPricePerShare() bool {
	return s.TotalShares.Mean() > 0 && s.Valuation.Mean() > 0
}

// Balance is the value of the owned shares after the discount and the tax on capital gains. It is 0
// without an investment round with shares and a price per share.
func (s *StartupGrowth) Balance(ucfg *uncertain.Config) uncertain.Value {
	if !s.hasPricePerShare() {
		return uncertain.NewFixed(0.0)
	}
	fractionOwned := s.OwnedShares.Div(ucfg, s.TotalShares)
	grossValue := s.Valuation.Mul(ucfg, fractionOwned)
	discountedGrossValue := grossValue.Mul(ucfg, s.DiscountFactor)
	purchasePrice := s.PurchasePricePerShare.Mul(ucfg, s.OwnedShares)
	if purchasePrice.Mean() > discountedGrossValue.Mean() {
		return discountedGrossValue
	}
	capitalGains := discountedGrossValue.Sub(ucfg, purchasePrice)
	tax := capitalGains.Mul(ucfg, s.TaxRate)
	return discountedGrossValue.Sub(ucfg, tax)
}
