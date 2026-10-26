package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"strconv"
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
		data, err := h.svc.GetAccountNewPageData(ctx, extractExcludedTypeIDs(r))
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
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		var inp accountInputForm
		if err := srvu.Decode(r, &inp, false); err != nil {
			return fmt.Errorf("decoding input: %w", err)
		}
		var startupShares *model.StartupShareAccountInput
		if r.FormValue("account_form_mode") == "startup" {
			var ssaInp startupShareAccountInputForm
			if err := srvu.Decode(r, &ssaInp, false); err != nil {
				return fmt.Errorf("decoding startup share account input: %w", err)
			}
			startupShares = &ssaInp.StartupShareAccountInput
		}
		acc, err := h.svc.UpsertAccountWithStartupShares(ctx, inp.AccountInput, startupShares)
		if err != nil {
			return fmt.Errorf("upserting account: %w", err)
		}
		shttp.RedirectToNext(w, r, fmt.Sprintf("/accounts/%s", acc.ID))
		return nil
	})
}

func (h *Handler) accountDelete() http.Handler {
	return deleteHandler(h.svc.DeleteAccount, "/accounts/")
}

// ---- Account Growth Models ----

func (h *Handler) accountGrowthModelUpsert() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		var inp accountGrowthModelInputForm
		if err := srvu.Decode(r, &inp, false); err != nil {
			return fmt.Errorf("decoding input: %w", err)
		}
		_, err := h.svc.UpsertAccountGrowthModel(ctx, inp.AccountGrowthModelInput)
		if err != nil {
			return fmt.Errorf("upserting account growth model: %w", err)
		}
		shttp.RedirectToNext(w, r, fmt.Sprintf("/accounts/%s", inp.AccountID))
		return nil
	})
}

func (h *Handler) accountGrowthModelDelete() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		id := r.PathValue("id")
		if err := h.svc.DeleteAccountGrowthModel(ctx, id); err != nil {
			return err
		}
		shttp.RedirectToNext(w, r, fmt.Sprintf("/accounts/%s", id))
		return nil
	})
}

// ---- Startup Shares ----

func (h *Handler) startupShareAccountUpsert() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		var inp startupShareAccountInputForm
		if err := srvu.Decode(r, &inp, false); err != nil {
			return fmt.Errorf("decoding input: %w", err)
		}
		_, err := h.svc.UpsertStartupShareAccount(ctx, inp.StartupShareAccountInput)
		if err != nil {
			return fmt.Errorf("upserting startup share account: %w", err)
		}
		shttp.RedirectToNext(w, r, fmt.Sprintf("/accounts/%s/edit", inp.AccountID))
		return nil
	})
}

func (h *Handler) investmentRoundUpsert() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		var inp investmentRoundInputForm
		if err := srvu.Decode(r, &inp, false); err != nil {
			return fmt.Errorf("decoding input: %w", err)
		}
		_, err := h.svc.UpsertInvestmentRound(ctx, inp.InvestmentRoundInput)
		if err != nil {
			return fmt.Errorf("upserting investment round: %w", err)
		}
		shttp.RedirectToNext(w, r, fmt.Sprintf("/accounts/%s/edit", inp.AccountID))
		return nil
	})
}

func (h *Handler) investmentRoundDelete() http.Handler {
	return deleteWithLookupHandler(h.svc.GetInvestmentRound, h.svc.DeleteInvestmentRound, func(r model.InvestmentRound) string {
		return fmt.Sprintf("/accounts/%s/edit", r.AccountID)
	})
}

func (h *Handler) shareChangeUpsert() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		var inp shareChangeInputForm
		if err := srvu.Decode(r, &inp, false); err != nil {
			return fmt.Errorf("decoding input: %w", err)
		}
		_, err := h.svc.UpsertShareChange(ctx, inp.ShareChangeInput)
		if err != nil {
			return fmt.Errorf("upserting share change: %w", err)
		}
		shttp.RedirectToNext(w, r, fmt.Sprintf("/accounts/%s/edit", inp.AccountID))
		return nil
	})
}

func (h *Handler) shareChangeDelete() http.Handler {
	return deleteWithLookupHandler(h.svc.GetShareChange, h.svc.DeleteShareChange, func(sc model.ShareChange) string {
		return fmt.Sprintf("/accounts/%s/edit", sc.AccountID)
	})
}

func (h *Handler) startupShareOptionUpsert() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		var inp startupShareOptionInputForm
		if err := srvu.Decode(r, &inp, false); err != nil {
			return fmt.Errorf("decoding input: %w", err)
		}
		_, err := h.svc.UpsertStartupShareOption(ctx, inp.StartupShareOptionInput)
		if err != nil {
			return fmt.Errorf("upserting startup share option: %w", err)
		}
		shttp.RedirectToNext(w, r, fmt.Sprintf("/accounts/%s/edit", inp.AccountID))
		return nil
	})
}

func (h *Handler) startupShareOptionDelete() http.Handler {
	return deleteWithLookupHandler(h.svc.GetStartupShareOption, h.svc.DeleteStartupShareOption, func(o model.StartupShareOption) string {
		return fmt.Sprintf("/accounts/%s/edit", o.AccountID)
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
		breakdowns, err := h.svc.ComputeSalaryBreakdowns(ctx, id)
		if err != nil {
			return fmt.Errorf("computing salary breakdowns: %w", err)
		}
		data.Breakdowns = breakdowns
		return view.NewView(ctx, w, r).Render(view.Page("Salaries", view.PageEditSalary(view.SalaryEditContent(data))))
	})
}

func (h *Handler) salaryUpsert() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		var inp salaryInputForm
		if err := srvu.Decode(r, &inp, false); err != nil {
			return fmt.Errorf("decoding input: %w", err)
		}
		sal, err := h.svc.UpsertSalary(ctx, inp.Salary)
		if err != nil {
			return fmt.Errorf("upserting salary: %w", err)
		}
		shttp.RedirectToNext(w, r, fmt.Sprintf("/salaries/%s/edit", sal.ID))
		return nil
	})
}

func (h *Handler) salaryDelete() http.Handler {
	return deleteHandler(h.svc.DeleteSalary, "/salaries")
}

func (h *Handler) salaryAmountUpsert() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		var inp salaryAmountInputForm
		if err := srvu.Decode(r, &inp, false); err != nil {
			return fmt.Errorf("decoding input: %w", err)
		}
		_, err := h.svc.UpsertSalaryAmount(ctx, inp.SalaryAmount)
		if err != nil {
			return fmt.Errorf("upserting salary amount: %w", err)
		}
		shttp.RedirectToNext(w, r, fmt.Sprintf("/salaries/%s/edit", inp.SalaryID))
		return nil
	})
}

func (h *Handler) salaryAmountDelete() http.Handler {
	return deleteHandler(h.svc.DeleteSalaryAmount, "/salaries")
}

func (h *Handler) salaryAdjustmentUpsert() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		var inp salaryAdjustmentInputForm
		if err := srvu.Decode(r, &inp, false); err != nil {
			return fmt.Errorf("decoding input: %w", err)
		}
		_, err := h.svc.UpsertSalaryAdjustment(ctx, inp.SalaryAdjustment)
		if err != nil {
			return fmt.Errorf("upserting salary adjustment: %w", err)
		}
		shttp.RedirectToNext(w, r, fmt.Sprintf("/salaries/%s/edit", inp.SalaryID))
		return nil
	})
}

func (h *Handler) salaryAdjustmentDelete() http.Handler {
	return deleteHandler(h.svc.DeleteSalaryAdjustment, "/salaries")
}

func (h *Handler) partialParentalLeaveUpsert() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		var inp partialParentalLeaveInputForm
		if err := srvu.Decode(r, &inp, false); err != nil {
			return fmt.Errorf("decoding input: %w", err)
		}
		_, err := h.svc.UpsertPartialParentalLeave(ctx, inp.PartialParentalLeave)
		if err != nil {
			return fmt.Errorf("upserting partial parental leave: %w", err)
		}
		shttp.RedirectToNext(w, r, fmt.Sprintf("/salaries/%s/edit", inp.SalaryID))
		return nil
	})
}

func (h *Handler) partialParentalLeaveDelete() http.Handler {
	return deleteHandler(h.svc.DeletePartialParentalLeave, "/salaries")
}

func (h *Handler) fullParentalLeaveUpsert() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		var inp fullParentalLeaveInputForm
		if err := srvu.Decode(r, &inp, false); err != nil {
			return fmt.Errorf("decoding input: %w", err)
		}
		_, err := h.svc.UpsertFullParentalLeave(ctx, inp.FullParentalLeave)
		if err != nil {
			return fmt.Errorf("upserting full parental leave: %w", err)
		}
		shttp.RedirectToNext(w, r, fmt.Sprintf("/salaries/%s/edit", inp.SalaryID))
		return nil
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
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		var inp billAccountInputForm
		if err := srvu.Decode(r, &inp, false); err != nil {
			return fmt.Errorf("decoding input: %w", err)
		}
		ba, err := h.svc.UpsertBillAccount(ctx, inp.BillAccount)
		if err != nil {
			return fmt.Errorf("upserting bill account: %w", err)
		}
		shttp.RedirectToNext(w, r, fmt.Sprintf("/bills/%s/edit", ba.ID))
		return nil
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
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		var inp billInputForm
		if err := srvu.Decode(r, &inp, false); err != nil {
			return fmt.Errorf("decoding input: %w", err)
		}
		bill, err := h.svc.UpsertBill(ctx, inp.Bill)
		if err != nil {
			return fmt.Errorf("upserting bill: %w", err)
		}
		shttp.RedirectToNext(w, r, fmt.Sprintf("/bill-items/%s/edit", bill.ID))
		return nil
	})
}

func (h *Handler) billDelete() http.Handler {
	return deleteWithLookupHandler(h.svc.GetBill, h.svc.DeleteBill, func(b model.Bill) string {
		return fmt.Sprintf("/bills/%s/edit", b.BillAccountID)
	})
}

func (h *Handler) billAmountUpsert() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		var inp billAmountInputForm
		if err := srvu.Decode(r, &inp, false); err != nil {
			return fmt.Errorf("decoding input: %w", err)
		}
		_, err := h.svc.UpsertBillAmount(ctx, inp.BillAmount)
		if err != nil {
			return fmt.Errorf("upserting bill amount: %w", err)
		}
		shttp.RedirectToNext(w, r, fmt.Sprintf("/bill-items/%s/edit", inp.BillID))
		return nil
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
		data, err := h.svc.GetTransferTemplateNewPageData(ctx)
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
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		if err := r.ParseForm(); err != nil {
			return fmt.Errorf("parsing form: %w", err)
		}
		var inp transferTemplateForm
		if err := srvu.Decode(r, &inp, false); err != nil {
			return fmt.Errorf("decoding input: %w", err)
		}
		t, err := h.svc.UpsertTransferTemplate(ctx, inp.TransferTemplate)
		if err != nil {
			return fmt.Errorf("upserting transfer template: %w", err)
		}
		shttp.RedirectToNext(w, r, fmt.Sprintf("/transfer-templates/%s/edit", t.ID))
		return nil
	})
}

func (h *Handler) transferTemplateDuplicate() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		tr, err := h.svc.DuplicateTransferTemplate(ctx, r.PathValue("id"))
		if err != nil {
			return fmt.Errorf("duplicating transfer template: %w", err)
		}
		shttp.RedirectToNext(w, r, fmt.Sprintf("/transfer-templates/%s/edit", tr.ID))
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
		return view.NewView(ctx, w, r).Render(view.Page("Transfer Template Categories", view.PageEditTransferTemplateCategory(&view.TransferTemplateCategoryEditView{})))
	})
}

func (h *Handler) transferTemplateCategoryEditPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		c, err := h.svc.GetCategory(ctx, r.PathValue("id"))
		if err != nil {
			return fmt.Errorf("getting category: %w", err)
		}
		return view.NewView(ctx, w, r).Render(view.Page("Transfer Template Categories", view.PageEditTransferTemplateCategory(&view.TransferTemplateCategoryEditView{
			Category: c,
		})))
	})
}

func (h *Handler) transferTemplateCategoryUpsert() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		var inp transferTemplateCategoryInputForm
		if err := srvu.Decode(r, &inp, false); err != nil {
			return fmt.Errorf("decoding input: %w", err)
		}
		if _, err := h.svc.UpsertCategory(ctx, inp.TransferTemplateCategoryInput); err != nil {
			return fmt.Errorf("upserting category: %w", err)
		}
		shttp.RedirectToNext(w, r, "/settings?tab=categories")
		return nil
	})
}

func (h *Handler) transferTemplateCategoryDelete() http.Handler {
	return deleteHandler(h.svc.DeleteCategory, "/settings?tab=categories")
}

// ---- Account Types ----

func (h *Handler) accountTypeNewPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		return view.NewView(ctx, w, r).Render(view.Page("Account Types", view.PageEditAccountType(view.AccountTypeEditView(view.AccountType{}))))
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
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		var inp accountTypeInputForm
		if err := srvu.Decode(r, &inp, false); err != nil {
			return fmt.Errorf("decoding input: %w", err)
		}
		_, err := h.svc.UpsertAccountType(ctx, inp.AccountTypeInput)
		if err != nil {
			return fmt.Errorf("upserting account type: %w", err)
		}
		shttp.RedirectToNext(w, r, "/settings?tab=categories")
		return nil
	})
}

func (h *Handler) accountTypeDelete() http.Handler {
	return deleteHandler(h.svc.DeleteAccountType, "/settings?tab=categories")
}

// ---- Special Dates ----

func (h *Handler) specialDateNewPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		return view.NewView(ctx, w, r).Render(view.Page("Special Dates", view.PageEditSpecialDate(view.SpecialDateEditView(view.SpecialDate{}))))
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
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		var inp specialDateInputForm
		if err := srvu.Decode(r, &inp, false); err != nil {
			return fmt.Errorf("decoding input: %w", err)
		}
		_, err := h.svc.UpsertSpecialDate(ctx, inp.SpecialDateInput)
		if err != nil {
			return fmt.Errorf("upserting special date: %w", err)
		}
		shttp.RedirectToNext(w, r, "/settings?tab=special-dates")
		return nil
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
		shttp.RedirectToNext(w, r, fmt.Sprintf("/accounts/%s", accID))
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
		var groupBy view.TransferChartGroupBy
		if err := shttp.Parse(&groupBy, model.ParseTransferChartGroupBy, r.FormValue("group_by"), model.GroupByAccount); err != nil {
			return fmt.Errorf("parsing group_by: %w", err)
		}
		return view.NewView(ctx, w, r).Render(view.Page("Transfers Chart", view.PageTransfersChart(groupBy)))
	})
}

func (h *Handler) transferChartData() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		var groupBy view.TransferChartGroupBy
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
		var p predictionParamsForm
		if err := srvu.Decode(r, &p, false); err != nil {
			return fmt.Errorf("decoding input: %w", err)
		}
		return view.NewView(ctx, w, r).Render(view.Page("Chart", view.PageChart(p.PredictionParams)))
	})
}

type ssePredictionEventHandler struct {
	w *srvu.SSESender
}

func (s *ssePredictionEventHandler) Setup(e model.PredictionSetupEvent) error {
	return s.w.SendNamedJson("setup", e)
}
func (s *ssePredictionEventHandler) Snapshot(e model.PredictionBalanceSnapshot) error {
	return s.w.SendNamedJson("balanceSnapshot", e)
}
func (s *ssePredictionEventHandler) Close() error {
	return s.w.SendEventWithoutData("close")
}

func (h *Handler) chartsDataStream() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		var params predictionParamsForm
		if err := srvu.Decode(r, &params, false); err != nil {
			return fmt.Errorf("decoding input: %w", err)
		}
		if err := h.svc.RunPrediction(ctx, &ssePredictionEventHandler{w: srvu.SSEResponse(w)}, params.PredictionParams); err != nil {
			return fmt.Errorf("running prediction: %w", err)
		}
		return nil
	})
}

func (h *Handler) sweYearlyParamsNewPage() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		return view.NewView(ctx, w, r).Render(view.Page("New SWE Yearly Params", view.SweYearlyParamsEditPage(view.SweYearlyParams{}, false)))
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
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		var inp sweYearlyParamsInputForm
		if err := srvu.Decode(r, &inp, false); err != nil {
			return fmt.Errorf("decoding input: %w", err)
		}
		p, err := h.svc.UpsertSweYearlyParams(ctx, inp.SweYearlyParams)
		if err != nil {
			return fmt.Errorf("upserting swe yearly params: %w", err)
		}
		shttp.RedirectToNext(w, r, fmt.Sprintf("/settings/swe-yearly-params/%s/edit", p.ID))
		return nil
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
		if err := r.ParseForm(); err != nil {
			return fmt.Errorf("parsing form: %w", err)
		}
		samplesStr := r.FormValue("samples")
		if samplesStr != "" {
			samples, err := strconv.ParseInt(samplesStr, 10, 64)
			if err != nil {
				return fmt.Errorf("parsing samples: %w", err)
			}
			if err := h.svc.SetForecastSamples(ctx, samples); err != nil {
				return fmt.Errorf("setting forecast samples: %w", err)
			}
		}
		snapshotInterval := r.FormValue("snapshot_interval")
		if snapshotInterval != "" {
			if err := h.svc.SetForecastSnapshotInterval(ctx, snapshotInterval); err != nil {
				return fmt.Errorf("setting forecast snapshot interval: %w", err)
			}
		}
		startDays := r.FormValue("start_days")
		if startDays != "" {
			if err := h.svc.SetForecastStartDays(ctx, startDays); err != nil {
				return fmt.Errorf("setting forecast start days: %w", err)
			}
		}
		shttp.RedirectToNext(w, r, "/settings?tab=forecast")
		return nil
	})
}

// dashboardForecastStream streams the Dashboard Forecast: the history and marklines, then the
// stored forecast as the events that would have built it, then every change as it happens.
func (h *Handler) dashboardForecastStream() http.Handler {
	return errHandler(func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		sub, err := h.svc.DashboardForecast().Subscribe(ctx)
		if err != nil {
			return fmt.Errorf("subscribing to the dashboard forecast: %w", err)
		}
		defer sub.Unsubscribe()
		history, err := h.svc.GetDashboardForecastHistory(ctx, sub.Rows)
		if err != nil {
			return fmt.Errorf("getting the dashboard forecast history: %w", err)
		}

		sse := srvu.SSEResponse(w)
		for _, series := range history.Series {
			if err := sse.SendNamedJson("entity", series); err != nil {
				return err
			}
		}
		if err := sse.SendNamedJson("marklines", history.Marklines); err != nil {
			return err
		}
		if err := sse.SendEventWithoutData("setup-done"); err != nil {
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
