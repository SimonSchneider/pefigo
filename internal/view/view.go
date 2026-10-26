package view

import (
	"context"
	"net/http"
	"net/url"

	"github.com/SimonSchneider/pefigo/internal/model"
	"github.com/SimonSchneider/pefigo/pkg/swe"
	"github.com/a-h/templ"
)

type View struct {
	ctx context.Context
	w   http.ResponseWriter
	r   *http.Request
}

func NewView(ctx context.Context, w http.ResponseWriter, r *http.Request) *View {
	return &View{ctx: ctx, w: w, r: r}
}

func (v *View) Render(c templ.Component) error {
	v.setupHeaders(false)
	ctx := context.WithValue(v.ctx, sidebarExpandedKey{}, isSidebarExpanded(v.r))
	return c.Render(ctx, v.w)
}

type sidebarExpandedKey struct{}

func isSidebarExpanded(r *http.Request) bool {
	c, err := r.Cookie("sidebar-expanded")
	if err != nil {
		return true // default: expanded
	}
	return c.Value != "false"
}

func getSidebarExpanded(ctx context.Context) bool {
	v, ok := ctx.Value(sidebarExpandedKey{}).(bool)
	if !ok {
		return true
	}
	return v
}

func (v *View) setupHeaders(cache bool) {
	v.w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if cache {
		v.w.Header().Set("Cache-Control", "public, max-age=3600")
	} else {
		v.w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		v.w.Header().Set("Pragma", "no-cache")
		v.w.Header().Set("Expires", "0")
	}
}

// Type aliases for model types used by templ templates.
// Templates stay in the view package and reference these aliases,
// while the real type definitions live in the model package.

type (
	Account                          = model.Account
	AccountDetailed                  = model.AccountDetailed
	AccountInput                     = model.AccountInput
	AccountType                      = model.AccountType
	AccountTypeInput                 = model.AccountTypeInput
	AccountTypeWithFilter            = model.AccountTypeWithFilter
	AccountTypesWithFilter           = model.AccountTypesWithFilter
	TransferTemplate                 = model.TransferTemplate
	ScheduledTransfer                = model.ScheduledTransfer
	TransferScheduleRow              = model.TransferScheduleRow
	TransferTemplateCategory         = model.TransferTemplateCategory
	TransferTemplateCategoryInput    = model.TransferTemplateCategoryInput
	AccountSnapshot                  = model.AccountSnapshot
	AccountSnapshotInput             = model.AccountSnapshotInput
	AccountSnapshotCell              = model.AccountSnapshotCell
	BalanceChange                    = model.BalanceChange
	GrowthModel                      = model.GrowthModel
	GrowthModels                     = model.GrowthModels
	AccountGrowthModelInput          = model.AccountGrowthModelInput
	InvestmentRound                  = model.InvestmentRound
	InvestmentRoundInput             = model.InvestmentRoundInput
	ShareChange                      = model.ShareChange
	ShareChangeInput                 = model.ShareChangeInput
	StartupShareOption               = model.StartupShareOption
	StartupShareOptionInput          = model.StartupShareOptionInput
	StartupShareAccount              = model.StartupShareAccount
	StartupShareAccountInput         = model.StartupShareAccountInput
	DerivedStartupShareSummary       = model.DerivedStartupShareSummary
	SpecialDate                      = model.SpecialDate
	SpecialDateInput                 = model.SpecialDateInput
	DashboardView                    = model.DashboardView
	BudgetView                       = model.BudgetView
	BudgetCategoryGroup              = model.BudgetCategoryGroup
	BudgetChartEntry                 = model.BudgetChartEntry
	BudgetItem                       = model.BudgetItem
	AccountsView                     = model.AccountsView
	AccountEditView2                 = model.AccountEditView2
	TransferTemplatesView2           = model.TransferTemplatesView2
	TransferTemplateEditView         = model.TransferTemplateEditView
	TransfersView                    = model.TransfersView
	Transfer                         = model.Transfer
	SnapshotsTableView               = model.SnapshotsTableView
	SnapshotsRow                     = model.SnapshotsRow
	TransferTemplateCategoriesView   = model.TransferTemplateCategoriesView
	TransferTemplateCategoryEditView = model.TransferTemplateCategoryEditView
	CategoriesPageView               = model.CategoriesPageView
	SettingsPageView                 = model.SettingsPageView
	PredictionParams                 = model.PredictionParams
	AccountTypeGroup                 = model.AccountTypeGroup
	AccountTypeChartEntry            = model.AccountTypeChartEntry
	SnapshotHistoryChartData         = model.SnapshotHistoryChartData
	TransferChartGroupBy             = model.TransferChartGroupBy
	GroupBy                          = model.GroupBy
	TransferChartLink                = model.TransferChartLink
	TransferChartDataNode            = model.TransferChartDataNode
	TransferChartDataEnvelope        = model.TransferChartDataEnvelope
	PredictionSetupEvent             = model.PredictionSetupEvent
	PredictionBalanceSnapshot        = model.PredictionBalanceSnapshot
	PredictionFinancialEntity        = model.PredictionFinancialEntity
	Markline                         = model.Markline
	PredictionEventHandler           = model.PredictionEventHandler
	BudgetChartItem                  = model.BudgetChartItem
	SnapshotHistorySeries            = model.SnapshotHistorySeries
	AccountChartEntry                = model.AccountChartEntry
	Salary                           = model.Salary
	SalaryAmount                     = model.SalaryAmount
	SalaryAdjustment                 = model.SalaryAdjustment
	SalaryEditView                   = model.SalaryEditView
	PartialParentalLeave             = model.PartialParentalLeave
	FullParentalLeave                = model.FullParentalLeave
	SweYearlyParams                  = model.SweYearlyParams
	TransferTemplateSource           = model.TransferTemplateSource
	NetSalarySegmentBreakdown        = model.NetSalarySegmentBreakdown
	SalaryBreakdown                  = swe.SalaryBreakdown
	BillAccount                      = model.BillAccount
	Bill                             = model.Bill
	BillAmount                       = model.BillAmount
	BillsPageData                    = model.BillsPageData
	BillAccountEditView              = model.BillAccountEditView
	BillEditView                     = model.BillEditView
)

// The recurrences a new Salary and Bill account are pre-filled with, the ones the Service saves when
// none is given.
const (
	DefaultSalaryRecurrence      = model.DefaultSalaryRecurrence
	DefaultBillAccountRecurrence = model.DefaultBillAccountRecurrence
)

const (
	BalanceUnchanged BalanceChange = model.BalanceUnchanged
	BalanceIncreased BalanceChange = model.BalanceIncreased
	BalanceDecreased BalanceChange = model.BalanceDecreased
)

const (
	GroupByNone  GroupBy = model.GroupByNone
	GroupByType  GroupBy = model.GroupByType
	GroupByTotal GroupBy = model.GroupByTotal
)

const (
	GroupByAccount     TransferChartGroupBy = model.GroupByAccount
	GroupByAccountType TransferChartGroupBy = model.GroupByAccountType
)

func nextEncoded(path string) string {
	return url.QueryEscape(path)
}

func billFaviconURL(bill Bill) string {
	domain := model.ExtractDomain(bill.URL)
	if domain == "" {
		return ""
	}
	return "/favicons/" + domain
}

func billCompanyName(bill Bill) string {
	return model.ExtractCompanyName(bill.URL)
}
