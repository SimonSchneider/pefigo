package model_test

import (
	"slices"
	"testing"
	"time"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
)

func seriesByID(data *model.ForecastDashboardData) map[string]model.DashboardForecastSeries {
	byID := make(map[string]model.DashboardForecastSeries, len(data.Series))
	for _, s := range data.Series {
		byID[s.ID] = s
	}
	return byID
}

func dashboardForecastHistory(t *testing.T, svc *model.Service, rows []model.ForecastCacheRow) *model.ForecastDashboardData {
	t.Helper()
	data, err := svc.GetDashboardForecastHistory(t.Context(), rows)
	if err != nil {
		t.Fatalf("get dashboard forecast history: %v", err)
	}
	return data
}

func mustAccountType(t *testing.T, svc *model.Service, name, color string) model.AccountType {
	t.Helper()
	at, err := svc.UpsertAccountType(t.Context(), model.AccountTypeInput{Name: name, Color: color})
	if err != nil {
		t.Fatalf("create account type %q: %v", name, err)
	}
	return at
}

func mustTypedAccount(t *testing.T, svc *model.Service, name, typeID string) model.Account {
	t.Helper()
	acc, err := svc.UpsertAccount(t.Context(), model.AccountInput{Name: name, TypeID: typeID})
	if err != nil {
		t.Fatalf("create account %q: %v", name, err)
	}
	return acc
}

func mustSnapshot(t *testing.T, svc *model.Service, accountID string, day date.Date, balance float64) {
	t.Helper()
	if _, err := svc.UpsertAccountSnapshot(t.Context(), accountID, model.AccountSnapshotInput{Date: day, Balance: newFixedValue(balance)}); err != nil {
		t.Fatalf("create snapshot on %s: %v", day, err)
	}
}

func points(pairs ...any) []model.BalancePoint {
	pts := make([]model.BalancePoint, 0, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		pts = append(pts, model.BalancePoint{Date: pairs[i].(date.Date), Balance: pairs[i+1].(float64)})
	}
	return pts
}

func expectHistory(t *testing.T, got model.DashboardForecastSeries, want []model.BalancePoint) {
	t.Helper()
	if got.History == nil || !slices.Equal(got.History, want) {
		t.Fatalf("expected %q to have the history %v, got %v", got.Name, want, got.History)
	}
}

func TestDashboardForecastHistoryIdentifiesSeriesByAccountTypeWithoutForecastRows(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	at := mustAccountType(t, svc, "Savings", "#00ff00")
	mustSnapshot(t, svc, mustTypedAccount(t, svc, "Test", at.ID).ID, mustParseDate("2025-01-01"), 10000)
	mustAccountType(t, svc, "Unused", "#0000ff")
	sd, err := svc.UpsertSpecialDate(ctx, model.SpecialDateInput{Name: "Retirement", Date: mustParseDate("2040-01-01"), Color: "#ff0000"})
	if err != nil {
		t.Fatalf("create special date: %v", err)
	}

	data := dashboardForecastHistory(t, svc, nil)

	want := []model.DashboardForecastSeries{{
		ID:      at.ID,
		Name:    "Savings",
		Color:   "#00ff00",
		History: points(mustParseDate("2025-01-01"), 10000.0),
	}}
	if !slices.EqualFunc(data.Series, want, func(a, b model.DashboardForecastSeries) bool {
		return a.ID == b.ID && a.Name == b.Name && a.Color == b.Color && slices.Equal(a.History, b.History)
	}) {
		t.Fatalf("expected series %+v, got %+v", want, data.Series)
	}
	wantMarklines := []model.Markline{
		{Date: date.Today(), Name: "Today"},
		{Date: sd.Date, Color: "#ff0000", Name: "Retirement"},
	}
	if !slices.Equal(data.Marklines, wantMarklines) {
		t.Fatalf("expected marklines %+v, got %+v", wantMarklines, data.Marklines)
	}
}

func TestDashboardForecastHistoryHoldsHistoryPointsOnly(t *testing.T) {
	svc := newTestService(t)
	savings := mustAccountType(t, svc, "Savings", "#00ff00")
	pension := mustAccountType(t, svc, "Pension", "#0000ff")
	acc := mustTypedAccount(t, svc, "Test", savings.ID)
	for _, d := range []string{"2025-01-01", "2025-02-01"} {
		mustSnapshot(t, svc, acc.ID, mustParseDate(d), 10000)
	}
	future := date.Today().Add(date.Year)
	rows := []model.ForecastCacheRow{
		{AccountTypeID: savings.ID, BalancePoint: model.BalancePoint{Date: future, Balance: 12000}},
		{AccountTypeID: pension.ID, BalancePoint: model.BalancePoint{Date: future, Balance: 5000}},
		{AccountTypeID: "deleted-type", BalancePoint: model.BalancePoint{Date: future, Balance: 1}},
	}

	data := dashboardForecastHistory(t, svc, rows)

	if len(data.Series) != 2 {
		t.Fatalf("expected a series for Savings and Pension only, got %+v", data.Series)
	}
	byID := seriesByID(data)
	expectHistory(t, byID[savings.ID], points(mustParseDate("2025-01-01"), 10000.0, mustParseDate("2025-02-01"), 10000.0))
	if got := byID[pension.ID]; got.Name != "Pension" || got.Color != "#0000ff" {
		t.Fatalf("expected Pension, got %+v", got)
	}
	expectHistory(t, byID[pension.ID], []model.BalancePoint{})
}

func TestDashboardForecastHistoryCountsEveryAccountAtItsLatestBalance(t *testing.T) {
	svc := newTestService(t)
	savings := mustAccountType(t, svc, "Savings", "#00ff00")
	a := mustTypedAccount(t, svc, "A", savings.ID)
	b := mustTypedAccount(t, svc, "B", savings.ID)
	mustSnapshot(t, svc, a.ID, mustParseDate("2025-01-01"), 1000)
	mustSnapshot(t, svc, b.ID, mustParseDate("2025-01-15"), 5000)
	mustSnapshot(t, svc, a.ID, mustParseDate("2025-02-01"), 1500)

	data := dashboardForecastHistory(t, svc, nil)

	expectHistory(t, seriesByID(data)[savings.ID], points(
		mustParseDate("2025-01-01"), 1000.0,
		mustParseDate("2025-01-15"), 6000.0,
		mustParseDate("2025-02-01"), 6500.0,
	))
}

// monthlySnapshots records balance on the 28th of n months from January 2023 and returns those days.
func monthlySnapshots(t *testing.T, svc *model.Service, accountID string, n int, balance float64) []date.Date {
	t.Helper()
	days := make([]date.Date, n)
	for i := range days {
		days[i] = date.FromTime(time.Date(2023, time.January+time.Month(i), 28, 0, 0, 0, 0, time.UTC))
		mustSnapshot(t, svc, accountID, days[i], balance)
	}
	return days
}

func TestDashboardForecastHistoryHasAPointOnEveryRecordedDay(t *testing.T) {
	svc := newTestService(t)
	savings := mustAccountType(t, svc, "Savings", "#00ff00")
	days := monthlySnapshots(t, svc, mustTypedAccount(t, svc, "A", savings.ID).ID, 30, 1000)

	data := dashboardForecastHistory(t, svc, nil)

	want := make([]model.BalancePoint, len(days))
	for i, d := range days {
		want[i] = model.BalancePoint{Date: d, Balance: 1000}
	}
	expectHistory(t, seriesByID(data)[savings.ID], want)
}

func TestDashboardForecastHistoryEndsToday(t *testing.T) {
	svc := newTestService(t)
	savings := mustAccountType(t, svc, "Savings", "#00ff00")
	acc := mustTypedAccount(t, svc, "A", savings.ID)
	today := date.Today()
	mustSnapshot(t, svc, acc.ID, mustParseDate("2025-01-01"), 1000)
	mustSnapshot(t, svc, acc.ID, today, 2000)
	mustSnapshot(t, svc, acc.ID, today.Add(10*date.Day), 9999)

	data := dashboardForecastHistory(t, svc, nil)

	expectHistory(t, seriesByID(data)[savings.ID], points(mustParseDate("2025-01-01"), 1000.0, today, 2000.0))
}

func TestDashboardForecastHistoryValuesAStartupShareAccountOnEverySnapshotDay(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	equity := mustAccountType(t, svc, "Equity", "#ff00ff")
	acc := mustTypedAccount(t, svc, "Startup", equity.ID)
	savings := mustTypedAccount(t, svc, "Savings", mustAccountType(t, svc, "Savings", "#00ff00").ID)
	for _, d := range []date.Date{mustParseDate("2025-01-15"), mustParseDate("2025-02-15"), mustParseDate("2025-03-01"), mustParseDate("2025-04-15"), date.Today().Add(10 * date.Day)} {
		mustSnapshot(t, svc, savings.ID, d, 1000)
	}
	if _, err := svc.UpsertStartupShareAccount(ctx, model.StartupShareAccountInput{AccountID: acc.ID, TaxRate: ptr(0.0), ValuationDiscountFactor: ptr(1.0)}); err != nil {
		t.Fatalf("create startup share account: %v", err)
	}
	for _, sc := range []model.ShareChangeInput{
		{Date: mustParseDate("2025-01-01"), DeltaShares: 10, TotalPrice: 100},
		{Date: mustParseDate("2025-04-01"), DeltaShares: 10, TotalPrice: 300},
	} {
		sc.AccountID = acc.ID
		if _, err := svc.UpsertShareChange(ctx, sc); err != nil {
			t.Fatalf("create share change: %v", err)
		}
	}
	for _, ir := range []model.InvestmentRoundInput{
		{Date: mustParseDate("2025-02-01"), Valuation: 1e6, PreMoneyShares: 1000},
		{Date: mustParseDate("2025-03-01"), Valuation: 2e6, PreMoneyShares: 1000},
		{Date: date.Today().Add(date.Year), Valuation: 9e6, PreMoneyShares: 1000},
	} {
		ir.AccountID = acc.ID
		if _, err := svc.UpsertInvestmentRound(ctx, ir); err != nil {
			t.Fatalf("create investment round: %v", err)
		}
	}

	data := dashboardForecastHistory(t, svc, nil)

	// On each snapshot day, so that day's total includes it: 10 of 1000 shares at a valuation of 1M,
	// then 2M, then 20 shares at 2M. It has no value before its first investment round, and the
	// snapshot and round after today aren't history.
	expectHistory(t, seriesByID(data)[equity.ID], points(
		mustParseDate("2025-02-15"), 10000.0,
		mustParseDate("2025-03-01"), 20000.0,
		mustParseDate("2025-04-15"), 40000.0,
	))
}

func TestDashboardForecastHistoryKeepsAnUncategorizedSnapshotTheBarChartThinsOut(t *testing.T) {
	svc := newTestService(t)
	savings := mustAccountType(t, svc, "Savings", "#00ff00")
	monthlySnapshots(t, svc, mustTypedAccount(t, svc, "A", savings.ID).ID, 30, 1000)
	// The bar chart keeps the last day of every two months, so the 1st of a month is thinned out.
	mustSnapshot(t, svc, mustAccount(t, svc, "No type").ID, mustParseDate("2024-03-01"), 500)

	data := dashboardForecastHistory(t, svc, nil)

	got := seriesByID(data)[""]
	if got.Name != "Uncategorized" || got.Color != "#999999" {
		t.Fatalf("expected the Uncategorized series, got %+v", data.Series)
	}
	expectHistory(t, got, points(mustParseDate("2024-03-01"), 500.0))
	if last := data.Series[len(data.Series)-1]; last.ID != "" {
		t.Fatalf("expected Uncategorized to be the last series, got %+v", data.Series)
	}
}

func TestDashboardForecastHistoryHasUncategorizedWithOnlyForecastRows(t *testing.T) {
	svc := newTestService(t)
	mustAccount(t, svc, "No type")
	rows := []model.ForecastCacheRow{{AccountTypeID: "", BalancePoint: model.BalancePoint{Date: date.Today().Add(date.Day), Balance: 300}}}

	data := dashboardForecastHistory(t, svc, rows)

	if len(data.Series) != 1 || data.Series[0].ID != "" || data.Series[0].Name != "Uncategorized" || data.Series[0].Color != "#999999" {
		t.Fatalf("expected only the Uncategorized series, got %+v", data.Series)
	}
	expectHistory(t, data.Series[0], []model.BalancePoint{})
}

func TestDashboardForecastHistorySkipsAnInvestmentRoundWithoutASharePrice(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	equity := mustAccountType(t, svc, "Equity", "#ff00ff")
	acc := mustTypedAccount(t, svc, "Startup", equity.ID)
	if _, err := svc.UpsertStartupShareAccount(ctx, model.StartupShareAccountInput{AccountID: acc.ID, TaxRate: ptr(0.0), ValuationDiscountFactor: ptr(1.0)}); err != nil {
		t.Fatalf("create startup share account: %v", err)
	}
	if _, err := svc.UpsertShareChange(ctx, model.ShareChangeInput{AccountID: acc.ID, Date: mustParseDate("2025-01-01"), DeltaShares: 10, TotalPrice: 100}); err != nil {
		t.Fatalf("create share change: %v", err)
	}
	for _, ir := range []model.InvestmentRoundInput{
		{Date: mustParseDate("2025-02-01"), Valuation: 0, PreMoneyShares: 1000},
		{Date: mustParseDate("2025-03-01"), Valuation: 1e6, PreMoneyShares: 1000},
	} {
		ir.AccountID = acc.ID
		if _, err := svc.UpsertInvestmentRound(ctx, ir); err != nil {
			t.Fatalf("create investment round: %v", err)
		}
	}
	savings := mustTypedAccount(t, svc, "Savings", mustAccountType(t, svc, "Savings", "#00ff00").ID)
	mustSnapshot(t, svc, savings.ID, mustParseDate("2025-02-15"), 1000)
	mustSnapshot(t, svc, savings.ID, mustParseDate("2025-03-15"), 1000)

	data := dashboardForecastHistory(t, svc, nil)

	// A round without a valuation or an investment has no price per share, so no value.
	expectHistory(t, seriesByID(data)[equity.ID], points(mustParseDate("2025-03-15"), 10000.0))
}

func TestDashboardForecastHistoryHasASeriesForEveryAccountTypeTheForecastProjects(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	future := date.Today().Add(30 * date.Day)
	pension := mustAccountType(t, svc, "Pension", "#0000ff")
	mustSnapshot(t, svc, mustTypedAccount(t, svc, "Future pension", pension.ID).ID, future, 1000)
	equity := mustAccountType(t, svc, "Equity", "#ff00ff")
	startup := mustTypedAccount(t, svc, "Startup", equity.ID)
	if _, err := svc.UpsertStartupShareAccount(ctx, model.StartupShareAccountInput{AccountID: startup.ID, TaxRate: ptr(0.0), ValuationDiscountFactor: ptr(1.0)}); err != nil {
		t.Fatalf("create startup share account: %v", err)
	}
	empty := mustAccountType(t, svc, "Empty", "#00ffff")
	mustTypedAccount(t, svc, "Not recorded yet", empty.ID)
	mustAccountType(t, svc, "Unused", "#ffff00")
	mustSnapshot(t, svc, mustAccount(t, svc, "No type").ID, future, 500)

	// No Forecast Run has stored rows yet, but a Forecast projects every account: the series of each
	// Account Type with an account must exist before the first live forecast point arrives.
	data := dashboardForecastHistory(t, svc, nil)

	var ids []string
	for _, s := range data.Series {
		expectHistory(t, s, []model.BalancePoint{})
		ids = append(ids, s.ID)
	}
	if want := []string{empty.ID, equity.ID, pension.ID, ""}; !slices.Equal(ids, want) {
		t.Fatalf("expected series for Empty, Equity, Pension and Uncategorized only (%v), got %+v", want, data.Series)
	}
}

func TestDashboardForecastHistoryHasNoSeriesToProjectWithoutAForecast(t *testing.T) {
	svc := newTestService(t)
	equity := mustAccountType(t, svc, "Equity", "#ff00ff")
	startup := mustTypedAccount(t, svc, "Startup", equity.ID)
	if _, err := svc.UpsertStartupShareAccount(t.Context(), model.StartupShareAccountInput{AccountID: startup.ID, TaxRate: ptr(0.0), ValuationDiscountFactor: ptr(1.0)}); err != nil {
		t.Fatalf("create startup share account: %v", err)
	}
	// A Snapshot of a Startup Share account doesn't count, so there is no Forecast.
	mustSnapshot(t, svc, startup.ID, date.Today().Add(30*date.Day), 1000)
	mustTypedAccount(t, svc, "Not recorded yet", mustAccountType(t, svc, "Empty", "#00ffff").ID)

	if data := dashboardForecastHistory(t, svc, nil); len(data.Series) != 0 {
		t.Fatalf("expected no series without a Forecast, got %+v", data.Series)
	}
}

func TestDashboardForecastHistoryIgnoresSnapshotsOnAStartupShareAccount(t *testing.T) {
	svc := newTestService(t)
	equity := mustAccountType(t, svc, "Equity", "#ff00ff")
	startup := mustTypedAccount(t, svc, "Startup", equity.ID)
	mustStartupShareAccount(t, svc, startup.ID,
		[]model.InvestmentRoundInput{{Date: mustParseDate("2025-01-01"), Valuation: 1e6, PreMoneyShares: 1000}},
		[]model.ShareChangeInput{{Date: mustParseDate("2025-01-01"), DeltaShares: 10, TotalPrice: 100}},
	)
	mustSnapshot(t, svc, startup.ID, mustParseDate("2025-03-01"), 777)
	mustSnapshot(t, svc, mustTypedAccount(t, svc, "Savings", mustAccountType(t, svc, "Savings", "#00ff00").ID).ID, mustParseDate("2025-01-15"), 1000)

	data := dashboardForecastHistory(t, svc, nil)

	// Valued as derived on the regular Snapshot's day, the Forecast Start, and not after it.
	expectHistory(t, seriesByID(data)[equity.ID], points(mustParseDate("2025-01-15"), 10000.0))
}
