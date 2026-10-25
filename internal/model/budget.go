package model

import (
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/SimonSchneider/goslu/date"
)

type BudgetItem struct {
	Name   string
	Amount float64
	Source string
}

type BudgetCategoryGroup struct {
	Category TransferTemplateCategory
	Items    []BudgetItem
	Total    float64
}

type BudgetChartItem struct {
	Name  string  `json:"name"`
	Value float64 `json:"value"`
}

type BudgetChartEntry struct {
	Name  string            `json:"name"`
	Value float64           `json:"value"`
	Color string            `json:"color"`
	Items []BudgetChartItem `json:"items"`
}

type BudgetView struct {
	Categories []BudgetCategoryGroup
	GrandTotal float64
	ChartData  []BudgetChartEntry
}

func (s *Service) GetBudgetData(ctx context.Context) (*BudgetView, error) {
	today := date.Today()

	schedule, err := s.TransferSchedule(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading transfer schedule: %w", err)
	}

	budgetAccounts, err := s.ListBudgetAccounts(ctx, today)
	if err != nil {
		return nil, fmt.Errorf("listing budget accounts: %w", err)
	}

	allCategories, err := s.ListCategories(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing categories: %w", err)
	}
	categoriesByID := make(map[string]TransferTemplateCategory)
	for _, c := range allCategories {
		categoriesByID[c.ID] = c
	}

	groupMap := make(map[string]*BudgetCategoryGroup)

	for _, t := range schedule.On(today).Monthly() {
		if t.BudgetCategoryID == nil {
			continue
		}
		amount := math.Abs(t.Amount)
		if amount == 0 {
			continue
		}
		catID := *t.BudgetCategoryID
		group := getOrCreateGroup(groupMap, catID, categoriesByID)
		group.Items = append(group.Items, BudgetItem{
			Name:   t.Name,
			Amount: amount,
			Source: "transfer",
		})
		group.Total += amount
	}

	for _, acc := range budgetAccounts {
		if acc.BudgetCategoryID == nil || acc.GrowthModel == nil {
			continue
		}
		balance := 0.0
		if acc.LastSnapshot != nil {
			balance = acc.LastSnapshot.Balance.Mean()
		}
		annualRate := acc.GrowthModel.AnnualRate.Mean()
		monthlyInterest := math.Abs(balance * annualRate / 12.0)
		if monthlyInterest == 0 {
			continue
		}
		catID := *acc.BudgetCategoryID
		group := getOrCreateGroup(groupMap, catID, categoriesByID)
		group.Items = append(group.Items, BudgetItem{
			Name:   acc.Name + " (interest)",
			Amount: monthlyInterest,
			Source: "interest",
		})
		group.Total += monthlyInterest
	}

	categories := make([]BudgetCategoryGroup, 0, len(groupMap))
	grandTotal := 0.0
	for _, group := range groupMap {
		categories = append(categories, *group)
		grandTotal += group.Total
	}
	for i := range categories {
		sort.Slice(categories[i].Items, func(a, b int) bool {
			return categories[i].Items[a].Amount > categories[i].Items[b].Amount
		})
	}
	sort.Slice(categories, func(i, j int) bool {
		return categories[i].Total > categories[j].Total
	})

	chartEntries := make([]BudgetChartEntry, 0, len(categories))
	for _, cat := range categories {
		color := "#999999"
		if cat.Category.Color != nil {
			color = *cat.Category.Color
		}
		items := make([]BudgetChartItem, 0, len(cat.Items))
		for _, item := range cat.Items {
			items = append(items, BudgetChartItem{
				Name:  item.Name,
				Value: math.Round(item.Amount*100) / 100,
			})
		}
		chartEntries = append(chartEntries, BudgetChartEntry{
			Name:  cat.Category.Name,
			Value: math.Round(cat.Total*100) / 100,
			Color: color,
			Items: items,
		})
	}
	return &BudgetView{
		Categories: categories,
		GrandTotal: grandTotal,
		ChartData:  chartEntries,
	}, nil
}

func getOrCreateGroup(groupMap map[string]*BudgetCategoryGroup, catID string, categoriesByID map[string]TransferTemplateCategory) *BudgetCategoryGroup {
	group, exists := groupMap[catID]
	if !exists {
		cat := categoriesByID[catID]
		group = &BudgetCategoryGroup{
			Category: cat,
		}
		groupMap[catID] = group
	}
	return group
}
