package model

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/pdb"
	finance2 "github.com/SimonSchneider/pefigo/pkg/finance"
	"github.com/SimonSchneider/pefigo/pkg/swe"
	"github.com/SimonSchneider/pefigo/pkg/ui"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

type PredictionParams struct {
	Duration date.Duration
	Samples  int64
	// Quantile is the share of outcomes between each point's lower and upper bound. Without it, no
	// bounds are calculated.
	Quantile         float64
	SnapshotInterval date.Cron
	GroupBy          GroupBy
}

type GroupBy string

const (
	GroupByNone  GroupBy = "none"
	GroupByType  GroupBy = "type"
	GroupByTotal GroupBy = "total"
)

func ParseGroupBy(val string) (GroupBy, error) {
	switch val {
	case "none":
		return GroupByNone, nil
	case "type":
		return GroupByType, nil
	case "total":
		return GroupByTotal, nil
	default:
		return GroupByNone, fmt.Errorf("invalid group by: %s", val)
	}
}

type PredictionBalanceSnapshot struct {
	ID         string    `json:"id"`
	Day        date.Date `json:"day"`
	Balance    float64   `json:"balance"`
	LowerBound float64   `json:"lowerBound"`
	UpperBound float64   `json:"upperBound"`
}

type PredictionFinancialEntity struct {
	ID        string                      `json:"id"`
	Name      string                      `json:"name"`
	Color     string                      `json:"color"`
	Snapshots []PredictionBalanceSnapshot `json:"snapshots"`
}

// Markline is a date line on a forecast chart.
type Markline struct {
	Date  date.Date `json:"date"`
	Color string    `json:"color"`
	Name  string    `json:"name"`
}

// dateLines are the date lines of a forecast chart: today and every Special Date, by date.
func dateLines(specialDates []SpecialDate, today date.Date) []Markline {
	lines := make([]Markline, 0, len(specialDates)+1)
	lines = append(lines, Markline{Date: today, Name: "Today"})
	for _, sd := range specialDates {
		lines = append(lines, Markline{Date: sd.Date, Color: sd.Color, Name: sd.Name})
	}
	slices.SortStableFunc(lines, func(a, b Markline) int { return cmp.Compare(a.Date, b.Date) })
	return lines
}

type PredictionSetupEvent struct {
	Max       date.Date                   `json:"max"`
	Entities  []PredictionFinancialEntity `json:"entities"`
	Marklines []Markline                  `json:"marklines"`
}

type PredictionEventHandler interface {
	Setup(PredictionSetupEvent) error
	Snapshot(PredictionBalanceSnapshot) error
	Close() error
}

func (s *Service) RunPrediction(ctx context.Context, eventHandler PredictionEventHandler, params PredictionParams) error {
	q := s.q
	q1, q2 := (1-params.Quantile)/2, (1+params.Quantile)/2

	entities := make([]finance2.Entity, 0)
	accs, err := q.ListAccounts(ctx)
	if err != nil {
		return fmt.Errorf("listing accounts for Prediction: %w", err)
	}
	schedule, err := s.TransferSchedule(ctx)
	if err != nil {
		return fmt.Errorf("loading transfer schedule for Prediction: %w", err)
	}
	accountTypes, err := q.ListAccountTypes(ctx)
	if err != nil {
		return fmt.Errorf("listing account types for Prediction: %w", err)
	}
	specialDates, err := s.ListSpecialDates(ctx)
	if err != nil {
		return fmt.Errorf("listing special dates for Prediction: %w", err)
	}
	sweParams, err := s.ListSweYearlyParams(ctx)
	if err != nil {
		return fmt.Errorf("listing swe yearly params for Prediction: %w", err)
	}
	accountTypesById := KeyBy(accountTypes, func(at pdb.AccountType) string { return at.ID })
	accsById := make(map[string]pdb.Account, len(accs))
	startDate := date.Today()
	ucfg := uncertain.NewConfig(time.Now().UnixMilli(), params.Samples)
	for _, acc := range accs {
		accsById[acc.ID] = acc
		snaps, err := s.ListAccountSnapshots(ctx, acc.ID)
		if err != nil {
			return fmt.Errorf("getting snapshots for account %s: %w", acc.ID, err)
		}
		gms, err := s.ListAccountGrowthModels(ctx, acc.ID)
		if err != nil {
			return fmt.Errorf("getting growth models for account %s: %w", acc.ID, err)
		}
		var balanceLimit finance2.BalanceLimit
		if acc.BalanceUpperLimit != nil {
			balanceLimit = finance2.BalanceLimit{
				Upper: uncertain.NewFixed(*acc.BalanceUpperLimit),
			}
		}
		entity := finance2.Entity{
			ID:           acc.ID,
			Name:         acc.Name,
			BalanceLimit: balanceLimit,
			Snapshots:    make([]finance2.BalanceSnapshot, 0, len(snaps)),
		}
		if acc.CashFlowFrequency != nil || acc.CashFlowDestinationID != nil {
			entity.CashFlow = &finance2.CashFlowModel{
				Frequency:     date.Cron(ui.OrDefault(acc.CashFlowFrequency)),
				DestinationID: ui.OrDefault(acc.CashFlowDestinationID),
			}
		}
		ssa, err := s.GetStartupShareAccount(ctx, acc.ID)
		if err == nil && ssa.AccountID == acc.ID {
			rounds, err := s.ListInvestmentRounds(ctx, acc.ID)
			if err != nil {
				return fmt.Errorf("listing investment rounds for account %s: %w", acc.ID, err)
			}
			shareChanges, err := s.ListShareChanges(ctx, acc.ID)
			if err != nil {
				return fmt.Errorf("listing share changes for account %s: %w", acc.ID, err)
			}
			opts, err := s.ListStartupShareOptions(ctx, acc.ID)
			if err != nil {
				return fmt.Errorf("listing startup share options for account %s: %w", acc.ID, err)
			}
			slices.Reverse(rounds)

			ssResult := buildStartupShareForecastState(ucfg, rounds, shareChanges, opts, ssa, startDate)

			entity.GrowthModel = ssResult.GrowthModel
			entity.Snapshots = ssResult.Snapshots
		} else {
			for _, snap := range snaps {
				entity.Snapshots = append(entity.Snapshots, snap.ToFinance())
				if snap.Date.After(startDate) {
					startDate = snap.Date
				}
			}
			entity.GrowthModel = GrowthModels(gms).ToFinance()
		}
		if acc.IsIsk != 0 {
			entity.TaxModel = &swe.ISKTax{
				ParamsFunc: func(d date.Date) swe.ISKParams {
					return swe.ISKParams{
						SchablonRanta: activeSchablonRantaAt(sweParams, d),
						Fribelopp:     activeISKFribeloppAt(sweParams, d),
					}
				},
			}
		}
		if len(entity.Snapshots) > 0 {
			entities = append(entities, entity)
		}
	}
	startDay := startDate
	startDate += 1
	endDate := startDate.Add(params.Duration)

	h := &groupingEventHandler{
		eventHandler:     eventHandler,
		ucfg:             ucfg,
		accsById:         accsById,
		accountTypesById: accountTypesById,
		groupBy:          params.GroupBy,
		startDay:         startDay,
		bounds:           params.Quantile != 0,
		q1:               q1,
		q2:               q2,
	}

	if err := h.setup(entities, endDate, dateLines(specialDates, date.Today())); err != nil {
		return fmt.Errorf("setting up grouping event handler: %w", err)
	}

	snapshotRecorder := finance2.SnapshotRecorderFunc(func(accountID string, day date.Date, balance uncertain.Value) error {
		return h.snapshot(accountID, day, balance)
	})
	if err := finance2.RunPrediction(ctx, ucfg, startDate, endDate, params.SnapshotInterval, entities, schedule.ForecastTransfers(), finance2.CompositeRecorder{SnapshotRecorder: snapshotRecorder}); err != nil {
		return fmt.Errorf("running prediction for SSE: %w", err)
	}
	return h.close()
}

type startupShareForecastState struct {
	GrowthModel *finance2.StartupGrowth
	Snapshots   []finance2.BalanceSnapshot
}

func buildStartupShareForecastState(
	ucfg *uncertain.Config,
	rounds []InvestmentRound,
	shareChanges []ShareChange,
	opts []StartupShareOption,
	ssa StartupShareAccount,
	startDate date.Date,
) startupShareForecastState {
	currentValuation := uncertain.NewFixed(0.0)
	currentTotalShares := uncertain.NewFixed(0.0)
	for _, round := range rounds {
		if round.Date <= startDate {
			postVal, postShares := PostMoneyValuationAndShares(round.Valuation, round.PreMoneyShares, round.Investment)
			currentValuation = uncertain.NewFixed(postVal)
			currentTotalShares = uncertain.NewFixed(postShares)
		}
	}
	sharesOwnedAtStart, avgPurchaseAtStart := DeriveShareState(shareChanges, startDate)

	investmentRounds := make(map[date.Date]finance2.StartupGrowthInvestmentRound)
	for _, round := range rounds {
		investmentRounds[round.Date] = finance2.StartupGrowthInvestmentRound{
			PreMoneyValuation: uncertain.NewFixed(round.Valuation),
			PreMoneyShares:    uncertain.NewFixed(round.PreMoneyShares),
			Investment:        uncertain.NewFixed(round.Investment),
		}
	}

	shareChangesMap := make(map[date.Date]finance2.StartupGrowthShareChange)
	for _, sc := range shareChanges {
		if sc.Date >= startDate {
			shareChangesMap[sc.Date] = finance2.StartupGrowthShareChange{
				DeltaShares: uncertain.NewFixed(sc.DeltaShares),
				TotalPrice:  uncertain.NewFixed(sc.TotalPrice),
			}
		}
	}

	options := make(map[date.Date]finance2.StartupGrowthOption)
	for _, opt := range opts {
		options[opt.EndDate] = finance2.StartupGrowthOption{
			StrikePricePerShare: uncertain.NewFixed(opt.StrikePricePerShare),
			NumShares:           uncertain.NewFixed(opt.Shares),
			SourceAccountID:     opt.SourceAccountID,
		}
	}

	growthModel := &finance2.StartupGrowth{
		TimeFrameGrowth: finance2.TimeFrameGrowth{
			StartDate: 0,
			EndDate:   nil,
		},
		TotalShares: currentTotalShares,
		OwnedShares: uncertain.NewFixed(sharesOwnedAtStart),
		Valuation:   currentValuation,

		TaxRate:               uncertain.NewFixed(ssa.TaxRate),
		DiscountFactor:        uncertain.NewFixed(ssa.ValuationDiscountFactor),
		PurchasePricePerShare: uncertain.NewFixed(avgPurchaseAtStart),

		InvestmentRounds: investmentRounds,
		ShareChanges:     shareChangesMap,
		Options:          options,
	}

	eventDates := make(map[date.Date]struct{})
	for _, round := range rounds {
		if round.Date < startDate {
			eventDates[round.Date] = struct{}{}
		}
	}
	for _, sc := range shareChanges {
		if sc.Date < startDate {
			eventDates[sc.Date] = struct{}{}
		}
	}
	sortedEventDates := make([]date.Date, 0, len(eventDates))
	for d := range eventDates {
		sortedEventDates = append(sortedEventDates, d)
	}
	sort.Slice(sortedEventDates, func(i, j int) bool { return sortedEventDates[i] < sortedEventDates[j] })

	snapshots := make([]finance2.BalanceSnapshot, 0, len(sortedEventDates)+1)
	for _, eventDate := range sortedEventDates {
		var best *InvestmentRound
		for i := range rounds {
			if rounds[i].Date <= eventDate {
				best = &rounds[i]
			}
		}
		if best == nil {
			continue
		}
		postMoneyValuation, postMoneyShares := PostMoneyValuationAndShares(best.Valuation, best.PreMoneyShares, best.Investment)
		if postMoneyShares <= 0 {
			continue
		}
		sharesOwned, avgPrice := DeriveShareState(shareChanges, eventDate)
		if sharesOwned == 0 {
			continue
		}
		balance := CalculateStartupShareBalance(
			ucfg,
			uncertain.NewFixed(postMoneyValuation),
			sharesOwned,
			avgPrice,
			ssa.TaxRate,
			postMoneyShares,
			ssa.ValuationDiscountFactor,
		)
		snapshots = append(snapshots, finance2.BalanceSnapshot{
			Date:    eventDate,
			Balance: balance,
		})
	}
	snapshots = append(snapshots, finance2.BalanceSnapshot{
		Date:    startDate,
		Balance: growthModel.Balance(ucfg),
	})
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].Date < snapshots[j].Date
	})

	return startupShareForecastState{
		GrowthModel: growthModel,
		Snapshots:   snapshots,
	}
}

type groupingEventHandler struct {
	eventHandler     PredictionEventHandler
	ucfg             *uncertain.Config
	accsById         map[string]pdb.Account
	accountTypesById map[string]pdb.AccountType
	groupBy          GroupBy
	// startDay is the day the prediction starts: today, or the latest snapshot's day if that is
	// later. Until then a stale account's estimated growth is predicted while the other accounts
	// are not, so a grouped point would leave them out.
	startDay date.Date
	bounds   bool // whether to calculate the lower and upper bound between q1 and q2
	q1       float64
	q2       float64

	currentDate date.Date
	currentAccs map[string]uncertain.Value
}

func (h *groupingEventHandler) setup(entities []finance2.Entity, endDate date.Date, marklines []Markline) error {
	for _, e := range h.accsById {
		if e.TypeID == nil {
			color := uncategorized.Color
			h.accountTypesById[""] = pdb.AccountType{ID: uncategorized.ID, Name: uncategorized.Name, Color: &color}
		}
	}

	type GroupedEntities struct {
		name  string
		color string
		dates map[date.Date]uncertain.Value
	}

	groupedEntities := make(map[string]GroupedEntities)
	switch h.groupBy {
	case GroupByTotal:
		groupedEntities["total"] = GroupedEntities{
			name:  "total",
			color: "",
			dates: make(map[date.Date]uncertain.Value),
		}
	case GroupByType:
		for _, accountType := range h.accountTypesById {
			groupedEntities[accountType.ID] = GroupedEntities{
				name:  accountType.Name,
				color: ui.OrDefault(accountType.Color),
				dates: make(map[date.Date]uncertain.Value),
			}
		}
	case GroupByNone:
		for _, account := range h.accsById {
			groupedEntities[account.ID] = GroupedEntities{
				name:  account.Name,
				color: "",
				dates: make(map[date.Date]uncertain.Value),
			}
		}
	}

	for _, e := range entities {
		key := h.getKey(e.ID)

		ent := groupedEntities[key]

		for _, s := range e.Snapshots {
			amount := ent.dates[s.Date]
			if amount.Zero() {
				amount = s.Balance
			} else {
				amount = amount.Add(h.ucfg, s.Balance)
			}
			ent.dates[s.Date] = amount
		}
	}

	sssEntities := make([]PredictionFinancialEntity, 0, len(groupedEntities))
	for id, e := range groupedEntities {
		ent := PredictionFinancialEntity{
			ID:        id,
			Name:      e.name,
			Color:     e.color,
			Snapshots: make([]PredictionBalanceSnapshot, 0, len(e.dates)),
		}
		for day, amount := range e.dates {
			ent.Snapshots = append(ent.Snapshots, h.balanceSnapshot(id, day, amount))
		}
		sort.Slice(ent.Snapshots, func(i, j int) bool {
			return ent.Snapshots[i].Day < ent.Snapshots[j].Day
		})
		sssEntities = append(sssEntities, ent)
	}
	h.currentDate = endDate
	h.currentAccs = make(map[string]uncertain.Value, len(h.accsById))
	return h.eventHandler.Setup(PredictionSetupEvent{
		Max:       endDate,
		Entities:  sssEntities,
		Marklines: marklines,
	})
}

func (h *groupingEventHandler) getKey(id string) string {
	switch h.groupBy {
	case GroupByType:
		return h.accountTypesById[ui.OrDefault(h.accsById[id].TypeID)].ID
	case GroupByNone:
		return id
	case GroupByTotal:
		return "total"
	default:
		panic("invalid group by")
	}
}

func (h *groupingEventHandler) snapshot(id string, day date.Date, balance uncertain.Value) error {
	if h.groupBy != GroupByNone && !day.After(h.startDay) {
		return nil
	}
	if h.currentDate != day {
		if err := h.flush(); err != nil {
			return err
		}
		h.currentDate = day
	}

	key := h.getKey(id)
	acc, ok := h.currentAccs[key]
	if !ok {
		h.currentAccs[key] = balance
	} else {
		h.currentAccs[key] = acc.Add(h.ucfg, balance)
	}
	return nil
}

func (h *groupingEventHandler) flush() error {
	for id, balance := range h.currentAccs {
		if err := h.eventHandler.Snapshot(h.balanceSnapshot(id, h.currentDate, balance)); err != nil {
			return err
		}
	}
	clear(h.currentAccs)
	return nil
}

func (h *groupingEventHandler) balanceSnapshot(id string, day date.Date, balance uncertain.Value) PredictionBalanceSnapshot {
	snap := PredictionBalanceSnapshot{ID: id, Day: day, Balance: balance.Mean()}
	if h.bounds {
		q := balance.Quantiles()
		snap.LowerBound, snap.UpperBound = q(h.q1), q(h.q2)
	}
	return snap
}

func (h *groupingEventHandler) close() error {
	if err := h.flush(); err != nil {
		return err
	}
	return h.eventHandler.Close()
}
