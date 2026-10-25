package model

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/SimonSchneider/goslu/date"
	finance2 "github.com/SimonSchneider/pefigo/pkg/finance"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

type TransfersView struct {
	Day date.Date

	TransferTemplatesThatNeedAmount []ScheduledTransfer // Amount is the one entered, or 0
	TransferTemplates               []ScheduledTransfer

	TransfersReady      bool
	IncompleteTransfers bool
	Accounts            map[string]Account
	Transfers           []Transfer
}

type Transfer struct {
	FromAccountID string
	ToAccountID   string
	Amount        float64
}

func (s *Service) ComputeTransfersView(ctx context.Context, day date.Date, amounts map[string]float64) (*TransfersView, error) {
	view := &TransfersView{Day: day}
	schedule, err := s.TransferSchedule(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading transfer schedule: %w", err)
	}
	view.TransfersReady = false
	for _, t := range schedule.On(day).Charged() {
		if t.AmountType == "fixed" && t.AmountFixed.Distribution != uncertain.DistFixed {
			amount, ok := amounts[t.ID]
			if ok {
				view.TransfersReady = true
			}
			t.Amount = amount
			view.TransferTemplatesThatNeedAmount = append(view.TransferTemplatesThatNeedAmount, t)
		} else {
			view.TransferTemplates = append(view.TransferTemplates, t)
		}
	}
	if view.TransfersReady {
		ucfg := uncertain.NewConfig(time.Now().UnixMilli(), 1)
		transfers := make([]finance2.TransferTemplate, 0)
		entities := make([]finance2.Entity, 0)
		accounts, err := s.ListAccountsDetailed(ctx, day)
		if err != nil {
			return nil, fmt.Errorf("listing accounts: %w", err)
		}
		view.Accounts = make(map[string]Account)
		for _, a := range accounts {
			if a.StartupShareAccount != nil {
				continue
			}
			if a.LastSnapshot == nil {
				continue
			}

			view.Accounts[a.ID] = a.Account
			ul := uncertain.Value{}
			if a.BalanceUpperLimit != nil {
				ul = uncertain.NewFixed(*a.BalanceUpperLimit)
			}
			entity := finance2.Entity{
				ID:   a.ID,
				Name: a.Name,
				BalanceLimit: finance2.BalanceLimit{
					Upper: ul,
				},
				Snapshots: []finance2.BalanceSnapshot{a.LastSnapshot.ToFinance()},
			}
			if a.GrowthModel != nil {
				entity.GrowthModel = GrowthModels([]GrowthModel{*a.GrowthModel}).ToFinance()
			}
			entities = append(entities, entity)
		}

		for _, t := range view.TransferTemplatesThatNeedAmount {
			ft := t.TransferTemplate.ToFinanceTransferTemplate()
			ft.AmountFixed.Amount = uncertain.NewFixed(t.Amount)
			transfers = append(transfers, ft)
		}
		for _, t := range view.TransferTemplates {
			transfers = append(transfers, t.ToFinanceTransferTemplate())
		}

		transferRecorder := finance2.TransferRecorderFunc(func(sourceAccountID, destinationAccountID string, transferDay date.Date, amount uncertain.Value) error {
			if amount.Distribution != uncertain.DistFixed {
				view.IncompleteTransfers = true
				return nil
			}
			view.Transfers = append(view.Transfers, Transfer{
				FromAccountID: sourceAccountID,
				ToAccountID:   destinationAccountID,
				Amount:        amount.Fixed.Value,
			})
			return nil
		})
		if err := finance2.RunPrediction(ctx, ucfg, day, day.Add(1*date.Day), date.Cron(day.String()), entities, transfers, finance2.CompositeRecorder{TransferRecorder: transferRecorder}); err != nil {
			return nil, fmt.Errorf("running prediction for SSE: %w", err)
		}
	}
	view.Transfers = SimplifyTransfers(view.Transfers)
	return view, nil
}

func SimplifyTransfers(transfers []Transfer) []Transfer {
	simplified := make([]Transfer, 0)
	for _, t := range transfers {
		if t.FromAccountID != "" && t.ToAccountID != "" && t.FromAccountID != t.ToAccountID {
			simplified = append(simplified, t)
		}
	}

	type Key struct {
		FromAccountID string
		ToAccountID   string
	}
	netAmounts := make(map[Key]float64)
	for _, t := range simplified {
		key := Key{FromAccountID: t.FromAccountID, ToAccountID: t.ToAccountID}
		reverseKey := Key{FromAccountID: t.ToAccountID, ToAccountID: t.FromAccountID}

		if reverseAmount, exists := netAmounts[reverseKey]; exists {
			netAmount := reverseAmount - t.Amount
			if netAmount > 0 {
				netAmounts[reverseKey] = netAmount
			} else if netAmount < 0 {
				delete(netAmounts, reverseKey)
				netAmounts[key] = -netAmount
			} else {
				delete(netAmounts, reverseKey)
			}
		} else {
			netAmounts[key] += t.Amount
		}
	}

	result := make([]Transfer, 0)
	for key, amount := range netAmounts {
		if amount != 0 {
			result = append(result, Transfer{
				FromAccountID: key.FromAccountID,
				ToAccountID:   key.ToAccountID,
				Amount:        amount,
			})
		}
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].FromAccountID == result[j].FromAccountID {
			return result[i].ToAccountID < result[j].ToAccountID
		}
		return result[i].FromAccountID < result[j].FromAccountID
	})

	return result
}

type TransferChartLink struct {
	Source string  `json:"source"`
	Target string  `json:"target"`
	Value  float64 `json:"value"`
	Label  string  `json:"label"`
}

func SimplifyChartLinks(links []TransferChartLink) []TransferChartLink {
	type Key struct {
		Source string
		Target string
	}
	type LinkInfo struct {
		Value float64
		Label string
	}

	netLinks := make(map[Key]LinkInfo)
	for _, link := range links {
		if link.Source == link.Target {
			continue
		}

		key := Key{Source: link.Source, Target: link.Target}
		reverseKey := Key{Source: link.Target, Target: link.Source}

		if reverseInfo, exists := netLinks[reverseKey]; exists {
			netAmount := reverseInfo.Value - link.Value
			if netAmount > 0 {
				netLinks[reverseKey] = LinkInfo{
					Value: netAmount,
					Label: reverseInfo.Label,
				}
			} else if netAmount < 0 {
				delete(netLinks, reverseKey)
				netLinks[key] = LinkInfo{
					Value: -netAmount,
					Label: link.Label,
				}
			} else {
				delete(netLinks, reverseKey)
			}
		} else {
			if existing, exists := netLinks[key]; exists {
				netLinks[key] = LinkInfo{
					Value: existing.Value + link.Value,
					Label: existing.Label + ", " + link.Label,
				}
			} else {
				netLinks[key] = LinkInfo{
					Value: link.Value,
					Label: link.Label,
				}
			}
		}
	}

	result := make([]TransferChartLink, 0, len(netLinks))
	seenPairs := make(map[string]bool)
	for key, info := range netLinks {
		if info.Value > 0 && key.Source != key.Target {
			reversePair := key.Target + ":" + key.Source
			if seenPairs[reversePair] {
				continue
			}
			pair := key.Source + ":" + key.Target
			seenPairs[pair] = true
			result = append(result, TransferChartLink{
				Source: key.Source,
				Target: key.Target,
				Value:  info.Value,
				Label:  info.Label,
			})
		}
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].Source == result[j].Source {
			return result[i].Target < result[j].Target
		}
		return result[i].Source < result[j].Source
	})

	return result
}

type TransferChartGroupBy string

const (
	GroupByAccount     TransferChartGroupBy = "account"
	GroupByAccountType TransferChartGroupBy = "account_type"
)

func ParseTransferChartGroupBy(val string) (TransferChartGroupBy, error) {
	switch val {
	case "account":
		return GroupByAccount, nil
	case "account_type":
		return GroupByAccountType, nil
	default:
		return GroupByAccount, fmt.Errorf("invalid group by: %s", val)
	}
}

type TransferChartDataNodeStyle struct {
	Color string `json:"color"`
}

func newNodeStyle(color string) *TransferChartDataNodeStyle {
	if color == "" {
		return nil
	}
	return &TransferChartDataNodeStyle{Color: color}
}

type TransferChartDataNode struct {
	Name      string                      `json:"name"`
	Label     string                      `json:"label"`
	ItemStyle *TransferChartDataNodeStyle `json:"itemStyle,omitempty"`
}

type TransferChartDataEnvelope struct {
	Data  []TransferChartDataNode `json:"data"`
	Links []TransferChartLink     `json:"links"`
}

func (s *Service) GetTransferChartData(ctx context.Context, groupBy TransferChartGroupBy) (*TransferChartDataEnvelope, error) {
	schedule, err := s.TransferSchedule(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading transfer schedule: %w", err)
	}
	accounts, err := s.ListAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing accounts: %w", err)
	}
	accountTypes, err := s.ListAccountTypes(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing account types: %w", err)
	}
	accountTypesById := KeyBy(accountTypes, func(a AccountType) string { return a.ID })

	accountsById := KeyBy(accounts, func(a Account) string { return a.ID })

	if groupBy == GroupByAccountType {
		for _, a := range accounts {
			if a.TypeID == "" {
				accountTypesById[""] = AccountType{
					ID:    "",
					Name:  "unknown",
					Color: "",
				}
				break
			}
		}
	}

	var chartData []TransferChartDataNode
	var accountToNodeName map[string]string

	if groupBy == GroupByAccountType {
		accountTypeSet := make(map[string]AccountType)
		for _, a := range accounts {
			typeID := a.TypeID
			if at, exists := accountTypesById[typeID]; exists {
				accountTypeSet[typeID] = at
			} else {
				accountTypeSet[""] = AccountType{ID: "", Name: "unknown", Color: ""}
			}
		}

		chartData = make([]TransferChartDataNode, 0, len(accountTypeSet))
		for _, at := range accountTypeSet {
			chartData = append(chartData, TransferChartDataNode{
				Name:      at.Name,
				Label:     at.Name,
				ItemStyle: newNodeStyle(at.Color),
			})
		}

		accountToNodeName = make(map[string]string, len(accounts))
		for _, a := range accounts {
			typeID := a.TypeID
			if at, exists := accountTypesById[typeID]; exists {
				accountToNodeName[a.ID] = at.Name
			} else {
				accountToNodeName[a.ID] = "unknown"
			}
		}
	} else {
		chartData = make([]TransferChartDataNode, 0, len(accounts))
		for _, a := range accounts {
			typeID := a.TypeID
			at, exists := accountTypesById[typeID]
			if !exists {
				at = AccountType{ID: "", Name: "unknown", Color: ""}
			}
			chartData = append(chartData, TransferChartDataNode{
				Name:      a.Name,
				Label:     a.Name,
				ItemStyle: newNodeStyle(at.Color),
			})
		}

		accountToNodeName = make(map[string]string, len(accounts))
		for _, a := range accounts {
			accountToNodeName[a.ID] = a.Name
		}
	}

	chartData = append(chartData, TransferChartDataNode{Name: "Income", Label: "Income", ItemStyle: newNodeStyle("#388E3C")})
	chartData = append(chartData, TransferChartDataNode{Name: "Expenses", Label: "Expenses", ItemStyle: newNodeStyle("#D32F2F")})

	type LinkKey struct {
		Source string
		Target string
	}
	aggregatedLinks := make(map[LinkKey]struct {
		Value float64
		Label string
	})

	for _, t := range schedule.On(date.Today()).Monthly() {
		if strings.Contains(string(t.Name), "Matkort") {
			continue
		}
		if t.Amount > 0 {
			var source, target string
			if t.FromAccountID == "" {
				source = "Income"
			} else {
				if nodeName, exists := accountToNodeName[t.FromAccountID]; exists {
					source = nodeName
				} else {
					if acc, exists := accountsById[t.FromAccountID]; exists {
						source = acc.Name
					}
				}
			}
			if t.ToAccountID == "" {
				target = "Expenses"
			} else {
				if nodeName, exists := accountToNodeName[t.ToAccountID]; exists {
					target = nodeName
				} else {
					if acc, exists := accountsById[t.ToAccountID]; exists {
						target = acc.Name
					}
				}
			}

			if source == "" || target == "" || source == target {
				continue
			}

			key := LinkKey{Source: source, Target: target}
			if existing, exists := aggregatedLinks[key]; exists {
				aggregatedLinks[key] = struct {
					Value float64
					Label string
				}{
					Value: existing.Value + t.Amount,
					Label: existing.Label + ", " + t.Name,
				}
			} else {
				aggregatedLinks[key] = struct {
					Value float64
					Label string
				}{
					Value: t.Amount,
					Label: t.Name,
				}
			}
		}
	}

	chartLinks := make([]TransferChartLink, 0, len(aggregatedLinks))
	for key, info := range aggregatedLinks {
		if key.Source != key.Target && info.Value > 0 {
			chartLinks = append(chartLinks, TransferChartLink{
				Source: key.Source,
				Target: key.Target,
				Value:  info.Value,
				Label:  info.Label,
			})
		}
	}

	chartLinks = SimplifyChartLinks(chartLinks)

	finalChartData := make([]TransferChartDataNode, 0, len(chartData))
	for _, node := range chartData {
		for _, edge := range chartLinks {
			if edge.Source == node.Name || edge.Target == node.Name {
				finalChartData = append(finalChartData, node)
				break
			}
		}
	}

	return &TransferChartDataEnvelope{
		Data:  finalChartData,
		Links: chartLinks,
	}, nil
}
