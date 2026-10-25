package model_test

import (
	"maps"
	"slices"
	"testing"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

// mustGroceries adds a Transfer Template out of fromAccountID whose amount is uncertain, so the
// Transfers page asks for it before it computes the transfers.
func mustGroceries(t *testing.T, svc *model.Service, fromAccountID string, recurrence date.Cron) model.TransferTemplate {
	t.Helper()
	return mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Groceries", FromAccountID: fromAccountID, AmountType: "fixed", AmountFixed: uncertain.NewUniform(100, 200),
		Priority: 9, Recurrence: recurrence, StartDate: mustParseDate("2020-01-01"), Enabled: true,
	})
}

// mustEmptyInto adds a Transfer Template that moves all of from's balance into to.
func mustEmptyInto(t *testing.T, svc *model.Service, from, to string, recurrence date.Cron) {
	t.Helper()
	mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Empty", FromAccountID: from, ToAccountID: to, AmountType: "percent", AmountFixed: newFixedValue(0), AmountPercent: 1,
		Priority: 1, Recurrence: recurrence, StartDate: mustParseDate("2020-01-01"), Enabled: true,
	})
}

// transfersOn computes the Transfers page on day, with 0 entered for groceries.
func transfersOn(t *testing.T, svc *model.Service, day string, groceries model.TransferTemplate) *model.TransfersView {
	t.Helper()
	view, err := svc.ComputeTransfersView(t.Context(), mustParseDate(day), map[string]float64{groceries.ID: 0})
	if err != nil {
		t.Fatalf("ComputeTransfersView: %v", err)
	}
	if !view.TransfersReady {
		t.Fatalf("expected the transfers on %s to be computed", day)
	}
	return view
}

// movedFrom is what each account moves into another account on the Transfers page.
func movedFrom(view *model.TransfersView) map[string]float64 {
	moved := make(map[string]float64, len(view.Transfers))
	for _, tr := range view.Transfers {
		moved[tr.FromAccountID] += tr.Amount
	}
	return moved
}

func expectMoved(t *testing.T, view *model.TransfersView, want map[string]float64) {
	t.Helper()
	if got := movedFrom(view); !maps.Equal(got, want) {
		t.Fatalf("on %s: expected the transfers %v, got %+v", view.Day, want, view.Transfers)
	}
}

func TestTransfersPageMovesTheStartingBalanceWithoutInterestThatIsNotPaidOut(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	savings, err := svc.UpsertAccount(ctx, model.AccountInput{Name: "Savings", CashFlowFrequency: "*-12-31"})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	checking := mustAccount(t, svc, "Checking")
	mustSnapshot(t, svc, savings.ID, mustParseDate("2025-01-01"), 100_000)
	if _, err := svc.UpsertAccountGrowthModel(ctx, model.AccountGrowthModelInput{
		AccountID: savings.ID, Type: "fixed", AnnualRate: newFixedValue(0.05), AnnualVolatility: newFixedValue(0), StartDate: mustParseDate("2025-01-01"),
	}); err != nil {
		t.Fatalf("create growth model: %v", err)
	}
	mustEmptyInto(t, svc, savings.ID, checking.ID, "*-10-25")
	groceries := mustGroceries(t, svc, checking.ID, "*-10-25")

	view := transfersOn(t, svc, "2025-10-25", groceries)

	// The interest is paid out on December 31, so none of it is in the balance on October 25.
	if want := []model.Transfer{{FromAccountID: savings.ID, ToAccountID: checking.ID, Amount: 100_000}}; !slices.Equal(view.Transfers, want) {
		t.Fatalf("expected the transfers %+v, got %+v", want, view.Transfers)
	}
	for _, acc := range []model.Account{savings, checking} {
		if view.Accounts[acc.ID].Name != acc.Name {
			t.Errorf("expected the accounts to hold %s, got %+v", acc.Name, view.Accounts)
		}
	}
}

func TestTransfersPageUsesTheSnapshotOnOrBeforeItsDay(t *testing.T) {
	svc := newTestService(t)
	savings := mustAccount(t, svc, "Savings")
	checking := mustAccount(t, svc, "Checking")
	mustSnapshot(t, svc, savings.ID, mustParseDate("2025-01-01"), 100_000)
	mustSnapshot(t, svc, savings.ID, mustParseDate("2025-12-01"), 50_000)
	mustEmptyInto(t, svc, savings.ID, checking.ID, "*-*-25")
	groceries := mustGroceries(t, svc, checking.ID, "*-*-25")

	expectMoved(t, transfersOn(t, svc, "2024-12-25", groceries), map[string]float64{})
	expectMoved(t, transfersOn(t, svc, "2025-10-25", groceries), map[string]float64{savings.ID: 100_000})
	expectMoved(t, transfersOn(t, svc, "2025-12-25", groceries), map[string]float64{savings.ID: 50_000})
}

func TestTransfersPageModelsEveryAccountAsAForecastDoes(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	checking := mustAccount(t, svc, "Checking")
	isk, err := svc.UpsertAccount(ctx, model.AccountInput{Name: "ISK", IsISK: true})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	mustSnapshot(t, svc, isk.ID, mustParseDate("2025-01-01"), 50_000)
	startup := mustAccount(t, svc, "Startup")
	mustStartupShareAccount(t, svc, startup.ID,
		[]model.InvestmentRoundInput{{Date: mustParseDate("2025-01-01"), Valuation: 1e6, PreMoneyShares: 1000}},
		[]model.ShareChangeInput{
			{Date: mustParseDate("2025-01-01"), DeltaShares: 10, TotalPrice: 100},
			{Date: mustParseDate("2025-10-25"), DeltaShares: 10, TotalPrice: 100},
		},
	)
	for _, from := range []string{isk.ID, startup.ID} {
		mustEmptyInto(t, svc, from, checking.ID, "*-10-25")
	}
	groceries := mustGroceries(t, svc, checking.ID, "*-10-25")

	view := transfersOn(t, svc, "2025-10-25", groceries)

	// The Startup Share account is worth its 20 shares, the day's share change included.
	expectMoved(t, view, map[string]float64{isk.ID: 50_000, startup.ID: 20_000})
	for _, acc := range []model.Account{checking, isk, startup} {
		if view.Accounts[acc.ID].Name != acc.Name {
			t.Errorf("expected the accounts to hold %s, got %+v", acc.Name, view.Accounts)
		}
	}
}

func TestTransfersPageIgnoresSnapshotsOnAStartupShareAccount(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	startup := mustAccount(t, svc, "Startup")
	mustStartupShareAccount(t, svc, startup.ID,
		[]model.InvestmentRoundInput{{Date: mustParseDate("2025-01-01"), Valuation: 1e6, PreMoneyShares: 1000}},
		[]model.ShareChangeInput{{Date: mustParseDate("2025-01-01"), DeltaShares: 10, TotalPrice: 100}},
	)
	mustEmptyInto(t, svc, startup.ID, checking.ID, "*-10-25")
	groceries := mustGroceries(t, svc, checking.ID, "*-10-25")
	expectMoved(t, transfersOn(t, svc, "2025-10-25", groceries), map[string]float64{startup.ID: 10_000})

	mustSnapshot(t, svc, startup.ID, mustParseDate("2025-06-01"), 777)
	mustSnapshot(t, svc, startup.ID, mustParseDate("2025-12-01"), 999)

	expectMoved(t, transfersOn(t, svc, "2025-10-25", groceries), map[string]float64{startup.ID: 10_000})
}

func TestAccountsShowAStartupShareAccountsDerivedBalance(t *testing.T) {
	svc := newTestService(t)
	startup := mustAccount(t, svc, "Startup")
	mustStartupShareAccount(t, svc, startup.ID,
		[]model.InvestmentRoundInput{{Date: mustParseDate("2025-01-01"), Valuation: 1e6, PreMoneyShares: 1000}},
		[]model.ShareChangeInput{{Date: mustParseDate("2025-01-01"), DeltaShares: 10, TotalPrice: 100}},
	)

	accounts, err := svc.ListAccountsDetailed(t.Context(), mustParseDate("2025-03-15"))
	if err != nil {
		t.Fatalf("ListAccountsDetailed: %v", err)
	}

	if len(accounts) != 1 || accounts[0].StartupShareAccount == nil || accounts[0].LastSnapshot == nil {
		t.Fatalf("expected the Startup Share account with a balance, got %+v", accounts)
	}
	if got := *accounts[0].LastSnapshot; got.Date != mustParseDate("2025-01-01") || got.Balance.Mean() != 10_000 {
		t.Fatalf("expected the balance 10000 from the investment round on 2025-01-01, got %+v", got)
	}
}
