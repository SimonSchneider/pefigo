package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"time"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/goslu/srvu"
	"github.com/SimonSchneider/goslu/static/shttp"
	"github.com/SimonSchneider/pefigo/internal/model"
	"github.com/SimonSchneider/pefigo/internal/view"
	"github.com/SimonSchneider/pefigo/pkg/ui"
)

type Handler struct {
	svc *model.Service
}

// errHandler serves h, answering an error that rejects the input (model.ErrInvalidInput) with
// 400 Bad Request and every other error with 500.
func errHandler(h srvu.HandlerFunc) http.Handler {
	return srvu.ErrHandlerFunc(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		err := h(ctx, w, r)
		if errors.Is(err, model.ErrInvalidInput) {
			return srvu.Err(http.StatusBadRequest, err)
		}
		return err
	})
}

func deleteHandler(deleteFn func(ctx context.Context, id string) error, redirectURL string) http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		if err := deleteFn(ctx, r.PathValue("id")); err != nil {
			return err
		}
		shttp.RedirectToNext(w, r, redirectURL)
		return nil
	})
}

func deleteWithLookupHandler[T any](getFn func(ctx context.Context, id string) (T, error), deleteFn func(ctx context.Context, id string) error, redirectFn func(T) string) http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		id := r.PathValue("id")
		entity, err := getFn(ctx, id)
		if err != nil {
			return err
		}
		if err := deleteFn(ctx, id); err != nil {
			return err
		}
		shttp.RedirectToNext(w, r, redirectFn(entity))
		return nil
	})
}

// upsertHandler decodes the form F, saves it with upsertFn and redirects to next, falling back to
// the URL redirectFn gives for the saved entity. what names the entity in an error.
func upsertHandler[F, T any](what string, upsertFn func(ctx context.Context, form F) (T, error), redirectFn func(T) string) http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		var form F
		if err := srvu.Decode(r, &form, false); err != nil {
			return fmt.Errorf("decoding input: %w", err)
		}
		saved, err := upsertFn(ctx, form)
		if err != nil {
			return fmt.Errorf("upserting %s: %w", what, err)
		}
		shttp.RedirectToNext(w, r, redirectFn(saved))
		return nil
	})
}

// accountEditURL is the edit page of the account accountID, where everything that belongs to an
// account is edited.
func accountEditURL(accountID string) string {
	return fmt.Sprintf("/accounts/%s/edit", accountID)
}

// salaryEditURL is the edit page of the salary salaryID, where everything that belongs to a salary
// is edited.
func salaryEditURL(salaryID string) string {
	return fmt.Sprintf("/salaries/%s/edit", salaryID)
}

// billAccountEditURL is the edit page of the bill account billAccountID, which lists its bills.
func billAccountEditURL(billAccountID string) string {
	return fmt.Sprintf("/bills/%s/edit", billAccountID)
}

// billEditURL is the edit page of the bill billID, where its amounts are edited.
func billEditURL(billID string) string {
	return fmt.Sprintf("/bill-items/%s/edit", billID)
}

// eventEditURL is the edit page of the Event eventID, where its entries and ends are edited.
func eventEditURL(eventID string) string {
	return fmt.Sprintf("/events/%s/edit", eventID)
}

// transferTemplateEditURL is the edit page of the transfer template transferTemplateID.
func transferTemplateEditURL(transferTemplateID string) string {
	return fmt.Sprintf("/transfer-templates/%s/edit", transferTemplateID)
}

func NewHandler(svc *model.Service, public fs.FS) http.Handler {
	h := &Handler{svc: svc}
	mux := http.NewServeMux()
	mux.Handle("GET /static/public/", srvu.With(http.StripPrefix("/static/public/", http.FileServerFS(public)), srvu.WithCacheCtrlHeader(365*24*time.Hour)))

	mux.Handle("GET /{$}", h.dashboardPage())
	mux.Handle("GET /accounts", h.accountsPage())
	mux.Handle("GET /accounts/new", h.accountNewPage())
	mux.Handle("GET /accounts/{id}/edit", h.accountEditPage())
	mux.Handle("GET /transfer-templates", h.transferTemplatesPage())
	mux.Handle("GET /transfer-templates/new", h.transferTemplatesNewPage())
	mux.Handle("GET /transfer-templates/{id}/edit", h.transferTemplatesEditPage())

	mux.Handle("GET /settings", h.settingsPage())

	mux.Handle("GET /account-types/new", h.accountTypeNewPage())
	mux.Handle("GET /account-types/{id}/edit", h.accountTypeEditPage())
	mux.Handle("POST /account-types/{$}", h.accountTypeUpsert())
	mux.Handle("POST /account-types/{id}/delete", h.accountTypeDelete())

	mux.Handle("GET /special-dates/new", h.specialDateNewPage())
	mux.Handle("GET /special-dates/{id}/edit", h.specialDateEditPage())
	mux.Handle("POST /special-dates/{$}", h.specialDateUpsert())
	mux.Handle("POST /special-dates/{id}/delete", h.specialDateDelete())

	mux.Handle("GET /events", h.eventsPage())
	mux.Handle("GET /events/new", h.eventNewPage())
	mux.Handle("GET /events/{id}/edit", h.eventEditPage())
	mux.Handle("POST /events/{$}", h.eventUpsert())
	mux.Handle("GET /events/{id}/delete", h.eventDeletePage())
	mux.Handle("POST /events/{id}/delete", h.eventDelete())
	mux.Handle("POST /events/{id}/entries/{$}", h.eventAction("adding to event", func(ctx context.Context, eventID string, f eventEntryForm) error {
		return h.svc.AddToEvent(ctx, eventID, f.Entry)
	}))
	mux.Handle("POST /events/{id}/entries/remove", h.eventAction("removing from event", func(ctx context.Context, _ string, f eventEntryForm) error {
		return h.svc.RemoveFromEvent(ctx, f.Entry)
	}))
	mux.Handle("POST /events/{id}/ends/{$}", h.eventAction("ending in event", func(ctx context.Context, eventID string, f eventEntryForm) error {
		return h.svc.UpsertEventEnd(ctx, model.EventEndInput{EventID: eventID, Entry: f.Entry, EndDate: f.EndDate, EndOffset: f.EndOffset})
	}))
	mux.Handle("POST /events/{id}/ends/remove", h.eventAction("lifting the end", func(ctx context.Context, eventID string, f eventEntryForm) error {
		return h.svc.DeleteEventEnd(ctx, eventID, f.Entry)
	}))

	mux.Handle("GET /snapshots-table", h.snapshotsTablePage())
	mux.Handle("POST /snapshots-table/modify-date", h.snapshotsTableModifyDate())
	mux.Handle("GET /snapshots-table/empty-row", h.snapshotsTableEmptyRow())
	mux.Handle("POST /accounts/{id}/snapshots/{date}/", h.accountSnapshotUpsert())

	mux.Handle("GET /transfers", h.transfersPage())
	mux.Handle("GET /transfers/chart/{$}", h.transferChartPage())
	mux.Handle("GET /transfers/chart/data", h.transferChartData())

	mux.Handle("GET /budget", h.budgetPage())

	mux.Handle("GET /chart", h.chartPage())
	mux.Handle("GET /chart/stream", h.chartsDataStream())

	mux.Handle("GET /dashboard/forecast/stream", h.dashboardForecastStream())

	mux.Handle("POST /accounts/{$}", h.accountUpsert())
	mux.Handle("POST /accounts/{id}/delete", h.accountDelete())

	mux.Handle("POST /growth-models/", h.accountGrowthModelUpsert())
	mux.Handle("POST /growth-models/{id}/delete", h.accountGrowthModelDelete())

	mux.Handle("POST /startup-share-accounts/", h.startupShareAccountUpsert())
	mux.Handle("POST /investment-rounds/", h.investmentRoundUpsert())
	mux.Handle("POST /investment-rounds/{id}/delete", h.investmentRoundDelete())
	mux.Handle("POST /share-changes/", h.shareChangeUpsert())
	mux.Handle("POST /share-changes/{id}/delete", h.shareChangeDelete())
	mux.Handle("POST /startup-share-options/", h.startupShareOptionUpsert())
	mux.Handle("POST /startup-share-options/{id}/delete", h.startupShareOptionDelete())

	mux.Handle("GET /salaries", h.salariesPage())
	mux.Handle("GET /salaries/new", h.salaryNewPage())
	mux.Handle("GET /salaries/{id}/edit", h.salaryEditPage())
	mux.Handle("GET /salaries/kommuner", h.salaryKommuner())
	mux.Handle("GET /salaries/forsamlingar", h.salaryForsamlingar())
	mux.Handle("POST /salaries/{$}", h.salaryUpsert())
	mux.Handle("POST /salaries/{id}/delete", h.salaryDelete())
	mux.Handle("POST /salary-amounts/{$}", h.salaryAmountUpsert())
	mux.Handle("POST /salary-amounts/{id}/delete", h.salaryAmountDelete())
	mux.Handle("POST /salary-adjustments/{$}", h.salaryAdjustmentUpsert())
	mux.Handle("POST /salary-adjustments/{id}/delete", h.salaryAdjustmentDelete())
	mux.Handle("POST /partial-parental-leaves/{$}", h.partialParentalLeaveUpsert())
	mux.Handle("POST /partial-parental-leaves/{id}/delete", h.partialParentalLeaveDelete())
	mux.Handle("POST /full-parental-leaves/{$}", h.fullParentalLeaveUpsert())
	mux.Handle("POST /full-parental-leaves/{id}/delete", h.fullParentalLeaveDelete())

	mux.Handle("GET /bills", h.billsPage())
	mux.Handle("GET /bills/new", h.billAccountNewPage())
	mux.Handle("GET /bills/{id}/edit", h.billAccountEditPage())
	mux.Handle("POST /bills/{$}", h.billAccountUpsert())
	mux.Handle("POST /bills/{id}/delete", h.billAccountDelete())
	mux.Handle("GET /bill-items/{id}/edit", h.billEditPage())
	mux.Handle("POST /bill-items/{$}", h.billUpsert())
	mux.Handle("POST /bill-items/{id}/delete", h.billDelete())
	mux.Handle("POST /bill-amounts/{$}", h.billAmountUpsert())
	mux.Handle("POST /bill-amounts/{id}/delete", h.billAmountDelete())
	mux.Handle("GET /favicons/{domain}", h.faviconHandler())

	mux.Handle("POST /settings/currency", h.currencySettingsSave())
	mux.Handle("POST /settings/forecast", h.forecastSettingsSave())

	mux.Handle("GET /settings/swe-yearly-params/new", h.sweYearlyParamsNewPage())
	mux.Handle("GET /settings/swe-yearly-params/{id}/edit", h.sweYearlyParamsEditPage())
	mux.Handle("POST /settings/swe-yearly-params/{$}", h.sweYearlyParamsUpsert())
	mux.Handle("POST /settings/swe-yearly-params/{id}/delete", h.sweYearlyParamsDelete())

	mux.Handle("POST /transfers/{$}", h.transferTemplateUpsert())
	mux.Handle("POST /transfers/{id}/duplicate", h.transferTemplateDuplicate())
	mux.Handle("POST /transfers/{id}/delete", h.transferTemplateDelete())

	mux.Handle("GET /transfer-template-categories/new", h.transferTemplateCategoryNewPage())
	mux.Handle("GET /transfer-template-categories/{id}/edit", h.transferTemplateCategoryEditPage())
	mux.Handle("POST /transfer-template-categories/{$}", h.transferTemplateCategoryUpsert())
	mux.Handle("POST /transfer-template-categories/{id}/delete", h.transferTemplateCategoryDelete())

	mux.Handle("POST /sleep/{$}", errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		time.Sleep(1 * time.Second)
		w.WriteHeader(200)
		return nil
	}))

	return mux
}

// ---- Dashboard ----

func (h *Handler) dashboardPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		data, err := h.svc.GetDashboardData(ctx)
		if err != nil {
			return fmt.Errorf("computing dashboard view: %w", err)
		}
		return view.NewView(ctx, w, r).Render(view.Page("Dashboard", view.PageDashboard(data)))
	})
}

// ---- Budget ----

func (h *Handler) budgetPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		data, err := h.svc.GetBudgetData(ctx)
		if err != nil {
			return fmt.Errorf("computing budget view: %w", err)
		}
		return view.NewView(ctx, w, r).Render(view.Page("Budget", view.PageBudget(data)))
	})
}

// ---- Accounts ----

func (h *Handler) accountsPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		data, err := h.svc.GetAccountsPageData(ctx, extractExcludedTypeIDs(r))
		if err != nil {
			return fmt.Errorf("getting accounts page data: %w", err)
		}
		return view.NewView(ctx, w, r).Render(view.Page("Accounts", view.PageAccounts(data)))
	})
}

func (h *Handler) accountNewPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		data, err := h.svc.GetAccountNewPageData(ctx, extractExcludedTypeIDs(r), r.URL.Query().Get("event_id"))
		if err != nil {
			return fmt.Errorf("getting account new page data: %w", err)
		}
		return view.NewView(ctx, w, r).Render(view.Page("Accounts", view.PageEditAccount(data)))
	})
}

func (h *Handler) accountEditPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		data, err := h.svc.GetAccountEditPageData(ctx, r.PathValue("id"), extractExcludedTypeIDs(r))
		if err != nil {
			return fmt.Errorf("getting account edit page data: %w", err)
		}
		return view.NewView(ctx, w, r).Render(view.Page("Accounts", view.PageEditAccount(data)))
	})
}

func (h *Handler) accountUpsert() http.Handler {
	return upsertHandler("account", func(ctx context.Context, f accountInputForm) (model.Account, error) {
		return h.svc.UpsertAccountWithStartupShares(ctx, f.AccountInput, f.StartupShares)
	}, func(acc model.Account) string {
		return accountEditURL(acc.ID)
	})
}

func (h *Handler) accountDelete() http.Handler {
	return deleteHandler(h.svc.DeleteAccount, "/accounts")
}

// ---- Account Growth Models ----

func (h *Handler) accountGrowthModelUpsert() http.Handler {
	return upsertHandler("account growth model", func(ctx context.Context, f accountGrowthModelInputForm) (model.GrowthModel, error) {
		return h.svc.UpsertAccountGrowthModel(ctx, f.AccountGrowthModelInput)
	}, func(gm model.GrowthModel) string {
		return accountEditURL(gm.AccountID)
	})
}

func (h *Handler) accountGrowthModelDelete() http.Handler {
	return deleteWithLookupHandler(h.svc.GetGrowthModel, h.svc.DeleteAccountGrowthModel, func(gm model.GrowthModel) string {
		return accountEditURL(gm.AccountID)
	})
}

// ---- Startup Shares ----

func (h *Handler) startupShareAccountUpsert() http.Handler {
	return upsertHandler("startup share account", func(ctx context.Context, f startupShareAccountInputForm) (model.StartupShareAccount, error) {
		return h.svc.UpsertStartupShareAccount(ctx, f.StartupShareAccountInput)
	}, func(ssa model.StartupShareAccount) string {
		return accountEditURL(ssa.AccountID)
	})
}

func (h *Handler) investmentRoundUpsert() http.Handler {
	return upsertHandler("investment round", func(ctx context.Context, f investmentRoundInputForm) (model.InvestmentRound, error) {
		return h.svc.UpsertInvestmentRound(ctx, f.InvestmentRoundInput)
	}, func(r model.InvestmentRound) string {
		return accountEditURL(r.AccountID)
	})
}

func (h *Handler) investmentRoundDelete() http.Handler {
	return deleteWithLookupHandler(h.svc.GetInvestmentRound, h.svc.DeleteInvestmentRound, func(r model.InvestmentRound) string {
		return accountEditURL(r.AccountID)
	})
}

func (h *Handler) shareChangeUpsert() http.Handler {
	return upsertHandler("share change", func(ctx context.Context, f shareChangeInputForm) (model.ShareChange, error) {
		return h.svc.UpsertShareChange(ctx, f.ShareChangeInput)
	}, func(sc model.ShareChange) string {
		return accountEditURL(sc.AccountID)
	})
}

func (h *Handler) shareChangeDelete() http.Handler {
	return deleteWithLookupHandler(h.svc.GetShareChange, h.svc.DeleteShareChange, func(sc model.ShareChange) string {
		return accountEditURL(sc.AccountID)
	})
}

func (h *Handler) startupShareOptionUpsert() http.Handler {
	return upsertHandler("startup share option", func(ctx context.Context, f startupShareOptionInputForm) (model.StartupShareOption, error) {
		return h.svc.UpsertStartupShareOption(ctx, f.StartupShareOptionInput)
	}, func(o model.StartupShareOption) string {
		return accountEditURL(o.AccountID)
	})
}

func (h *Handler) startupShareOptionDelete() http.Handler {
	return deleteWithLookupHandler(h.svc.GetStartupShareOption, h.svc.DeleteStartupShareOption, func(o model.StartupShareOption) string {
		return accountEditURL(o.AccountID)
	})
}

// ---- Salaries ----

func (h *Handler) salariesPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		salaries, err := h.svc.GetSalariesPageData(ctx)
		if err != nil {
			return fmt.Errorf("getting salaries page data: %w", err)
		}
		return view.NewView(ctx, w, r).Render(view.Page("Salaries", view.PageSalaries(view.SalariesListView(salaries))))
	})
}

func (h *Handler) salaryNewPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		data, err := h.svc.GetSalaryNewPageData(ctx)
		if err != nil {
			return fmt.Errorf("getting salary new page data: %w", err)
		}
		return view.NewView(ctx, w, r).Render(view.Page("Salaries", view.PageEditSalary(view.SalaryEditContent(data))))
	})
}

func (h *Handler) salaryEditPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		id := r.PathValue("id")
		data, err := h.svc.GetSalaryEditPageData(ctx, id)
		if err != nil {
			return fmt.Errorf("getting salary edit page data: %w", err)
		}
		return view.NewView(ctx, w, r).Render(view.Page("Salaries", view.PageEditSalary(view.SalaryEditContent(data))))
	})
}

func (h *Handler) salaryUpsert() http.Handler {
	return upsertHandler("salary", func(ctx context.Context, f salaryInputForm) (model.Salary, error) {
		return h.svc.UpsertSalary(ctx, f.Salary)
	}, func(s model.Salary) string {
		return salaryEditURL(s.ID)
	})
}

func (h *Handler) salaryDelete() http.Handler {
	return deleteHandler(h.svc.DeleteSalary, "/salaries")
}

func (h *Handler) salaryAmountUpsert() http.Handler {
	return upsertHandler("salary amount", func(ctx context.Context, f salaryAmountInputForm) (model.SalaryAmount, error) {
		return h.svc.UpsertSalaryAmount(ctx, f.SalaryAmount)
	}, func(a model.SalaryAmount) string {
		return salaryEditURL(a.SalaryID)
	})
}

func (h *Handler) salaryAmountDelete() http.Handler {
	return deleteHandler(h.svc.DeleteSalaryAmount, "/salaries")
}

func (h *Handler) salaryAdjustmentUpsert() http.Handler {
	return upsertHandler("salary adjustment", func(ctx context.Context, f salaryAdjustmentInputForm) (model.SalaryAdjustment, error) {
		return h.svc.UpsertSalaryAdjustment(ctx, f.SalaryAdjustment)
	}, func(a model.SalaryAdjustment) string {
		return salaryEditURL(a.SalaryID)
	})
}

func (h *Handler) salaryAdjustmentDelete() http.Handler {
	return deleteHandler(h.svc.DeleteSalaryAdjustment, "/salaries")
}

func (h *Handler) partialParentalLeaveUpsert() http.Handler {
	return upsertHandler("partial parental leave", func(ctx context.Context, f partialParentalLeaveInputForm) (model.PartialParentalLeave, error) {
		return h.svc.UpsertPartialParentalLeave(ctx, f.PartialParentalLeave)
	}, func(l model.PartialParentalLeave) string {
		return salaryEditURL(l.SalaryID)
	})
}

func (h *Handler) partialParentalLeaveDelete() http.Handler {
	return deleteHandler(h.svc.DeletePartialParentalLeave, "/salaries")
}

func (h *Handler) fullParentalLeaveUpsert() http.Handler {
	return upsertHandler("full parental leave", func(ctx context.Context, f fullParentalLeaveInputForm) (model.FullParentalLeave, error) {
		return h.svc.UpsertFullParentalLeave(ctx, f.FullParentalLeave)
	}, func(l model.FullParentalLeave) string {
		return salaryEditURL(l.SalaryID)
	})
}

func (h *Handler) fullParentalLeaveDelete() http.Handler {
	return deleteHandler(h.svc.DeleteFullParentalLeave, "/salaries")
}

// ---- Bills ----

func (h *Handler) billsPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		pageData, err := h.svc.GetBillAccountsPageData(ctx)
		if err != nil {
			return fmt.Errorf("getting bills page data: %w", err)
		}
		return view.NewView(ctx, w, r).Render(view.Page("Bills", view.PageBills(view.BillsListView(pageData))))
	})
}

func (h *Handler) billAccountNewPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		data, err := h.svc.GetBillAccountNewPageData(ctx)
		if err != nil {
			return fmt.Errorf("getting bill account new page data: %w", err)
		}
		return view.NewView(ctx, w, r).Render(view.Page("Bills", view.PageEditBillAccount(view.BillAccountEditContent(data))))
	})
}

func (h *Handler) billAccountEditPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		data, err := h.svc.GetBillAccountEditPageData(ctx, r.PathValue("id"))
		if err != nil {
			return fmt.Errorf("getting bill account edit page data: %w", err)
		}
		return view.NewView(ctx, w, r).Render(view.Page("Bills", view.PageEditBillAccount(view.BillAccountEditContent(data))))
	})
}

func (h *Handler) billAccountUpsert() http.Handler {
	return upsertHandler("bill account", func(ctx context.Context, f billAccountInputForm) (model.BillAccount, error) {
		return h.svc.UpsertBillAccount(ctx, f.BillAccount)
	}, func(ba model.BillAccount) string {
		return billAccountEditURL(ba.ID)
	})
}

func (h *Handler) billAccountDelete() http.Handler {
	return deleteHandler(h.svc.DeleteBillAccount, "/bills")
}

func (h *Handler) billEditPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		data, err := h.svc.GetBillEditPageData(ctx, r.PathValue("id"))
		if err != nil {
			return fmt.Errorf("getting bill edit page data: %w", err)
		}
		return view.NewView(ctx, w, r).Render(view.Page("Bills", view.PageEditBill(view.BillEditContent(data))))
	})
}

func (h *Handler) billUpsert() http.Handler {
	return upsertHandler("bill", func(ctx context.Context, f billInputForm) (model.Bill, error) {
		return h.svc.UpsertBill(ctx, f.Bill)
	}, func(b model.Bill) string {
		return billEditURL(b.ID)
	})
}

func (h *Handler) billDelete() http.Handler {
	return deleteWithLookupHandler(h.svc.GetBill, h.svc.DeleteBill, func(b model.Bill) string {
		return billAccountEditURL(b.BillAccountID)
	})
}

func (h *Handler) billAmountUpsert() http.Handler {
	return upsertHandler("bill amount", func(ctx context.Context, f billAmountInputForm) (model.BillAmount, error) {
		return h.svc.UpsertBillAmount(ctx, f.BillAmount)
	}, func(a model.BillAmount) string {
		return billEditURL(a.BillID)
	})
}

func (h *Handler) billAmountDelete() http.Handler {
	return deleteHandler(h.svc.DeleteBillAmount, "/bills")
}

func (h *Handler) faviconHandler() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		domain := r.PathValue("domain")
		data, contentType, err := h.svc.GetOrFetchFavicon(ctx, domain)
		if err != nil {
			return fmt.Errorf("getting favicon for %s: %w", domain, err)
		}
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Cache-Control", "public, max-age=604800")
		w.Write(data)
		return nil
	})
}

func (h *Handler) salaryKommuner() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		year := r.URL.Query().Get("year")
		if year == "" {
			year = fmt.Sprintf("%d", time.Now().Year())
		}
		selected := r.URL.Query().Get("selected")
		kommuner, err := h.svc.SweClient().ListKommuner(ctx, year)
		if err != nil {
			return fmt.Errorf("listing kommuner: %w", err)
		}
		return view.SalaryKommunOptions(kommuner, selected).Render(ctx, w)
	})
}

func (h *Handler) salaryForsamlingar() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		kommun := r.URL.Query().Get("kommun")
		year := r.URL.Query().Get("year")
		if year == "" {
			year = fmt.Sprintf("%d", time.Now().Year())
		}
		selected := r.URL.Query().Get("selected")
		if kommun == "" {
			return view.SalaryForsamlingOptions(nil, selected).Render(ctx, w)
		}
		forsamlingar, err := h.svc.SweClient().ListForsamlingar(ctx, kommun, year)
		if err != nil {
			return fmt.Errorf("listing forsamlingar: %w", err)
		}
		return view.SalaryForsamlingOptions(forsamlingar, selected).Render(ctx, w)
	})
}

// ---- Transfer Templates ----

func (h *Handler) transferTemplatesPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		data, err := h.svc.GetTransferTemplatesPageData(ctx)
		if err != nil {
			return fmt.Errorf("getting transfer templates page data: %w", err)
		}
		return view.NewView(ctx, w, r).Render(view.Page("Transfer Templates", view.PageTransferTemplates(data)))
	})
}

func (h *Handler) transferTemplatesNewPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		data, err := h.svc.GetTransferTemplateNewPageData(ctx, r.URL.Query().Get("event_id"))
		if err != nil {
			return fmt.Errorf("getting transfer template new page data: %w", err)
		}
		return view.NewView(ctx, w, r).Render(view.Page("Transfer Templates", view.PageEditTransferTemplate(data)))
	})
}

func (h *Handler) transferTemplatesEditPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		data, err := h.svc.GetTransferTemplateEditPageData(ctx, r.PathValue("id"))
		if err != nil {
			return fmt.Errorf("getting transfer template edit page data: %w", err)
		}
		return view.NewView(ctx, w, r).Render(view.Page("Transfer Templates", view.PageEditTransferTemplate(data)))
	})
}

func (h *Handler) transferTemplateUpsert() http.Handler {
	return upsertHandler("transfer template", func(ctx context.Context, f transferTemplateForm) (model.TransferTemplate, error) {
		return h.svc.UpsertTransferTemplate(ctx, f.TransferTemplate)
	}, func(t model.TransferTemplate) string {
		return transferTemplateEditURL(t.ID)
	})
}

func (h *Handler) transferTemplateDuplicate() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		tr, err := h.svc.DuplicateTransferTemplate(ctx, r.PathValue("id"))
		if err != nil {
			return fmt.Errorf("duplicating transfer template: %w", err)
		}
		shttp.RedirectToNext(w, r, transferTemplateEditURL(tr.ID))
		return nil
	})
}

func (h *Handler) transferTemplateDelete() http.Handler {
	return deleteHandler(h.svc.DeleteTransferTemplate, "/")
}

// ---- Settings (unified) ----

var validTabs = map[string]bool{
	"categories":        true,
	"currency":          true,
	"swe-yearly-params": true,
	"special-dates":     true,
	"forecast":          true,
}

func (h *Handler) settingsPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		tab := r.URL.Query().Get("tab")
		if !validTabs[tab] {
			tab = "categories"
		}
		data, err := h.svc.GetSettingsPageData(ctx)
		if err != nil {
			return fmt.Errorf("getting settings page data: %w", err)
		}
		return view.NewView(ctx, w, r).Render(view.Page("Settings", view.PageSettings(data, tab)))
	})
}

// ---- Transfer Template Categories ----

func (h *Handler) transferTemplateCategoryNewPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		return view.NewView(ctx, w, r).Render(view.Page("Transfer Template Categories", view.PageEditTransferTemplateCategory(&model.TransferTemplateCategoryEditView{})))
	})
}

func (h *Handler) transferTemplateCategoryEditPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		c, err := h.svc.GetCategory(ctx, r.PathValue("id"))
		if err != nil {
			return fmt.Errorf("getting category: %w", err)
		}
		return view.NewView(ctx, w, r).Render(view.Page("Transfer Template Categories", view.PageEditTransferTemplateCategory(&model.TransferTemplateCategoryEditView{
			Category: c,
		})))
	})
}

func (h *Handler) transferTemplateCategoryUpsert() http.Handler {
	return upsertHandler("category", func(ctx context.Context, f transferTemplateCategoryInputForm) (model.TransferTemplateCategory, error) {
		return h.svc.UpsertCategory(ctx, f.TransferTemplateCategoryInput)
	}, func(model.TransferTemplateCategory) string {
		return "/settings?tab=categories"
	})
}

func (h *Handler) transferTemplateCategoryDelete() http.Handler {
	return deleteHandler(h.svc.DeleteCategory, "/settings?tab=categories")
}

// ---- Account Types ----

func (h *Handler) accountTypeNewPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		return view.NewView(ctx, w, r).Render(view.Page("Account Types", view.PageEditAccountType(view.AccountTypeEditView(model.AccountType{}))))
	})
}

func (h *Handler) accountTypeEditPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		at, err := h.svc.GetAccountType(ctx, r.PathValue("id"))
		if err != nil {
			return fmt.Errorf("getting account type: %w", err)
		}
		return view.NewView(ctx, w, r).Render(view.Page("Account Types", view.PageEditAccountType(view.AccountTypeEditView(at))))
	})
}

func (h *Handler) accountTypeUpsert() http.Handler {
	return upsertHandler("account type", func(ctx context.Context, f accountTypeInputForm) (model.AccountType, error) {
		return h.svc.UpsertAccountType(ctx, f.AccountTypeInput)
	}, func(model.AccountType) string {
		return "/settings?tab=categories"
	})
}

func (h *Handler) accountTypeDelete() http.Handler {
	return deleteHandler(h.svc.DeleteAccountType, "/settings?tab=categories")
}

// ---- Events ----

func (h *Handler) eventsPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		data, err := h.svc.GetEventsPageData(ctx)
		if err != nil {
			return fmt.Errorf("getting events page data: %w", err)
		}
		return view.NewView(ctx, w, r).Render(view.Page("Events", view.PageEvents(data)))
	})
}

func (h *Handler) eventNewPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		data, err := h.svc.GetEventNewPageData(ctx)
		if err != nil {
			return fmt.Errorf("getting event new page data: %w", err)
		}
		return view.NewView(ctx, w, r).Render(view.Page("Events", view.PageEditEvent(data)))
	})
}

func (h *Handler) eventEditPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		data, err := h.svc.GetEventEditPageData(ctx, r.PathValue("id"))
		if err != nil {
			return fmt.Errorf("getting event edit page data: %w", err)
		}
		return view.NewView(ctx, w, r).Render(view.Page("Events", view.PageEditEvent(data)))
	})
}

func (h *Handler) eventUpsert() http.Handler {
	return upsertHandler("event", func(ctx context.Context, f eventInputForm) (model.Event, error) {
		return h.svc.UpsertEvent(ctx, f.EventInput)
	}, func(ev model.Event) string {
		return eventEditURL(ev.ID)
	})
}

// eventDeletePage confirms deleting an Event, listing what it deletes.
func (h *Handler) eventDeletePage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		data, err := h.svc.GetEventDeletePageData(ctx, r.PathValue("id"))
		if err != nil {
			return fmt.Errorf("getting event delete page data: %w", err)
		}
		return view.NewView(ctx, w, r).Render(view.Page("Events", view.PageDeleteEvent(data)))
	})
}

func (h *Handler) eventDelete() http.Handler {
	return deleteHandler(h.svc.DeleteEvent, "/events")
}

// eventAction decodes an entry of the Event in the path, acts on it with act and redirects to next,
// falling back to the Event's edit page. what names the action in an error.
func (h *Handler) eventAction(what string, act func(ctx context.Context, eventID string, f eventEntryForm) error) http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		var form eventEntryForm
		if err := srvu.Decode(r, &form, false); err != nil {
			return fmt.Errorf("decoding input: %w", err)
		}
		eventID := r.PathValue("id")
		if err := act(ctx, eventID, form); err != nil {
			return fmt.Errorf("%s: %w", what, err)
		}
		shttp.RedirectToNext(w, r, eventEditURL(eventID))
		return nil
	})
}

// ---- Special Dates ----

func (h *Handler) specialDateNewPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		return view.NewView(ctx, w, r).Render(view.Page("Special Dates", view.PageEditSpecialDate(view.SpecialDateEditView(model.SpecialDate{}))))
	})
}

func (h *Handler) specialDateEditPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		sd, err := h.svc.GetSpecialDate(ctx, r.PathValue("id"))
		if err != nil {
			return fmt.Errorf("getting special date: %w", err)
		}
		return view.NewView(ctx, w, r).Render(view.Page("Special Dates", view.PageEditSpecialDate(view.SpecialDateEditView(sd))))
	})
}

func (h *Handler) specialDateUpsert() http.Handler {
	return upsertHandler("special date", func(ctx context.Context, f specialDateInputForm) (model.SpecialDate, error) {
		return h.svc.UpsertSpecialDate(ctx, f.SpecialDateInput)
	}, func(model.SpecialDate) string {
		return "/settings?tab=special-dates"
	})
}

func (h *Handler) specialDateDelete() http.Handler {
	return deleteHandler(h.svc.DeleteSpecialDate, "/settings?tab=special-dates")
}

// ---- Snapshots Table ----

func (h *Handler) snapshotsTablePage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		data, err := h.svc.GetSnapshotsTablePageData(ctx, extractExcludedTypeIDs(r))
		if err != nil {
			return fmt.Errorf("getting snapshots table page data: %w", err)
		}
		return view.NewView(ctx, w, r).Render(view.Page("Snapshots Table", view.PageSnapshotsTable(data)))
	})
}

func (h *Handler) snapshotsTableModifyDate() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		var inp dateInputForm
		if err := srvu.Decode(r, &inp, false); err != nil {
			return fmt.Errorf("decoding input: %w", err)
		}
		row, err := h.svc.ModifySnapshotDateRow(ctx, inp.OldDate, inp.NewDate)
		if err != nil {
			return fmt.Errorf("modifying snapshot date: %w", err)
		}
		return view.NewView(ctx, w, r).Render(view.SnapshotsTableRow(row))
	})
}

func (h *Handler) snapshotsTableEmptyRow() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		row, err := h.svc.GetEmptySnapshotRow(ctx)
		if err != nil {
			return fmt.Errorf("getting empty snapshot row: %w", err)
		}
		return view.NewView(ctx, w, r).Render(view.SnapshotsTableRow(row))
	})
}

func (h *Handler) accountSnapshotUpsert() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		var inp accountSnapshotInputForm
		if err := srvu.Decode(r, &inp, false); err != nil {
			return fmt.Errorf("decoding input: %w", err)
		}
		accID := r.PathValue("id")
		snap, err := h.svc.UpsertOrDeleteSnapshot(ctx, accID, inp.AccountSnapshotInput)
		if err != nil {
			return fmt.Errorf("upserting snapshot: %w", err)
		}
		if r.Header.Get("HX-Request") == "true" {
			return view.NewView(ctx, w, r).Render(view.SnapshotCell(accID, inp.Date, snap))
		}
		shttp.RedirectToNext(w, r, accountEditURL(accID))
		return nil
	})
}

// ---- Transfers ----

func (h *Handler) transfersPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		var day date.Date
		if err := shttp.Parse(&day, date.ParseDate, r.FormValue("day"), date.Today()); err != nil {
			return fmt.Errorf("parsing day: %w", err)
		}
		amounts := make(map[string]float64)
		if err := r.ParseForm(); err == nil {
			for key, values := range r.Form {
				if len(key) > 7 && key[:7] == "amount_" {
					templateID := key[7:]
					if amount, err := ui.ParseAmount(values[0]); err == nil {
						amounts[templateID] = amount
					}
				}
			}
		}
		data, err := h.svc.ComputeTransfersView(ctx, day, amounts)
		if err != nil {
			return fmt.Errorf("computing transfers view: %w", err)
		}
		return view.NewView(ctx, w, r).Render(view.Page("Transfers", view.PageTransfers(data)))
	})
}

func (h *Handler) transferChartPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		var groupBy model.TransferChartGroupBy
		if err := shttp.Parse(&groupBy, model.ParseTransferChartGroupBy, r.FormValue("group_by"), model.GroupByAccount); err != nil {
			return fmt.Errorf("parsing group_by: %w", err)
		}
		return view.NewView(ctx, w, r).Render(view.Page("Transfers Chart", view.PageTransfersChart(groupBy)))
	})
}

func (h *Handler) transferChartData() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		var groupBy model.TransferChartGroupBy
		if err := shttp.Parse(&groupBy, model.ParseTransferChartGroupBy, r.FormValue("group_by"), model.GroupByAccount); err != nil {
			return fmt.Errorf("parsing group_by: %w", err)
		}
		data, err := h.svc.GetTransferChartData(ctx, groupBy)
		if err != nil {
			return fmt.Errorf("getting transfer chart data: %w", err)
		}
		if err := json.NewEncoder(w).Encode(data); err != nil {
			return fmt.Errorf("encoding JSON: %w", err)
		}
		return nil
	})
}

// ---- Chart ----

func (h *Handler) chartPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		var inp predictionParamsForm
		if err := srvu.Decode(r, &inp, false); err != nil {
			return fmt.Errorf("decoding input: %w", err)
		}
		params, err := inp.WithDefaults()
		if err != nil {
			return err
		}
		return view.NewView(ctx, w, r).Render(view.Page("Chart", view.PageChart(params)))
	})
}

// ssePredictionEventHandler streams a Forecast as server-sent events. The stream starts with the first
// event, so an error before it, such as invalid parameters, is still answered with its own status.
type ssePredictionEventHandler struct {
	w      http.ResponseWriter
	stream *srvu.SSESender
}

func (s *ssePredictionEventHandler) sender() *srvu.SSESender {
	if s.stream == nil {
		s.stream = srvu.SSEResponse(s.w)
	}
	return s.stream
}

func (s *ssePredictionEventHandler) Setup(e model.PredictionSetupEvent) error {
	return s.sender().SendNamedJson("setup", e)
}
func (s *ssePredictionEventHandler) Snapshot(e model.PredictionBalanceSnapshot) error {
	return s.sender().SendNamedJson("balanceSnapshot", e)
}
func (s *ssePredictionEventHandler) Close() error {
	return s.sender().SendEventWithoutData("close")
}

func (h *Handler) chartsDataStream() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		var params predictionParamsForm
		if err := srvu.Decode(r, &params, false); err != nil {
			return fmt.Errorf("decoding input: %w", err)
		}
		if err := h.svc.RunPrediction(ctx, &ssePredictionEventHandler{w: w}, params.PredictionParams); err != nil {
			return fmt.Errorf("running prediction: %w", err)
		}
		return nil
	})
}

func (h *Handler) sweYearlyParamsNewPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		return view.NewView(ctx, w, r).Render(view.Page("New SWE Yearly Params", view.SweYearlyParamsEditPage(model.SweYearlyParams{}, false)))
	})
}

func (h *Handler) sweYearlyParamsEditPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		p, err := h.svc.GetSweYearlyParams(ctx, r.PathValue("id"))
		if err != nil {
			return fmt.Errorf("getting swe yearly params: %w", err)
		}
		return view.NewView(ctx, w, r).Render(view.Page("Edit SWE Yearly Params", view.SweYearlyParamsEditPage(p, true)))
	})
}

func (h *Handler) sweYearlyParamsUpsert() http.Handler {
	return upsertHandler("swe yearly params", func(ctx context.Context, f sweYearlyParamsInputForm) (model.SweYearlyParams, error) {
		return h.svc.UpsertSweYearlyParams(ctx, f.SweYearlyParams)
	}, func(p model.SweYearlyParams) string {
		return fmt.Sprintf("/settings/swe-yearly-params/%s/edit", p.ID)
	})
}

func (h *Handler) sweYearlyParamsDelete() http.Handler {
	return deleteHandler(h.svc.DeleteSweYearlyParams, "/settings?tab=swe-yearly-params")
}

func (h *Handler) currencySettingsSave() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		if err := r.ParseForm(); err != nil {
			return fmt.Errorf("parsing form: %w", err)
		}
		if err := h.svc.SetDefaultCurrency(ctx, r.FormValue("currency")); err != nil {
			return fmt.Errorf("setting default currency: %w", err)
		}
		shttp.RedirectToNext(w, r, "/settings?tab=currency")
		return nil
	})
}

func (h *Handler) forecastSettingsSave() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		var inp forecastSettingsInputForm
		if err := srvu.Decode(r, &inp, false); err != nil {
			return fmt.Errorf("decoding input: %w", err)
		}
		if err := h.svc.SetForecastSettings(ctx, inp.ForecastSettingsInput); err != nil {
			return fmt.Errorf("setting forecast settings: %w", err)
		}
		shttp.RedirectToNext(w, r, "/settings?tab=forecast")
		return nil
	})
}

// dashboardForecastStream streams the Dashboard Forecast: the setup (the series with their Balance
// History, and the marklines), then the stored forecast as the events that would have built it, then
// every change as it happens. Every Forecast Run that starts after connecting follows a Forecast
// Input write, so the current setup is sent again before its started.
func (h *Handler) dashboardForecastStream() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		sub, err := h.svc.DashboardForecast().Subscribe(ctx)
		if err != nil {
			return fmt.Errorf("subscribing to the dashboard forecast: %w", err)
		}
		defer sub.Unsubscribe()
		setup, err := h.svc.GetDashboardForecastHistory(ctx, sub.Rows)
		if err != nil {
			return fmt.Errorf("getting the dashboard forecast history: %w", err)
		}

		sse := srvu.SSEResponse(w)
		if err := sse.SendNamedJson("setup", setup); err != nil {
			return err
		}
		for _, evt := range forecastSubscriptionEvents(sub) {
			if err := sendForecastEvent(sse, evt); err != nil {
				return err
			}
		}
		for {
			select {
			case <-ctx.Done():
				return nil
			case evt, ok := <-sub.Events:
				if !ok {
					return nil
				}
				if evt.Type == model.ForecastEventStarted {
					// The Forecast Run has just cleared the stored rows, so none adds a series.
					setup, err := h.svc.GetDashboardForecastHistory(ctx, nil)
					if err != nil {
						return fmt.Errorf("refreshing the dashboard forecast history: %w", err)
					}
					if err := sse.SendNamedJson("setup", setup); err != nil {
						return err
					}
				}
				if err := sendForecastEvent(sse, evt); err != nil {
					return err
				}
			}
		}
	})
}

// forecastSubscriptionEvents turns the Dashboard Forecast as it was when subscribing into the
// events that would have built it, so the client handles it exactly like live events.
func forecastSubscriptionEvents(sub model.ForecastSubscription) []model.ForecastEvent {
	events := make([]model.ForecastEvent, 0, len(sub.Rows)+1)
	if sub.Status.State == model.ForecastStateRunning {
		events = append(events, model.ForecastEvent{Type: model.ForecastEventStarted})
	}
	for i := range sub.Rows {
		events = append(events, model.ForecastEvent{Type: model.ForecastEventSnapshot, Snapshot: &sub.Rows[i]})
	}
	if sub.Status.State == model.ForecastStateFailed {
		events = append(events, model.ForecastEvent{Type: model.ForecastEventFailed, Message: sub.Status.Message})
	}
	return events
}

func sendForecastEvent(sse *srvu.SSESender, evt model.ForecastEvent) error {
	switch evt.Type {
	case model.ForecastEventStarted:
		return sse.SendEventWithoutData("started")
	case model.ForecastEventSnapshot:
		return sse.SendNamedJson("snapshot", evt.Snapshot)
	case model.ForecastEventDone:
		return sse.SendEventWithoutData("done")
	case model.ForecastEventFailed:
		return sse.SendNamedJson("failed", map[string]string{"message": evt.Message})
	}
	return nil
}
