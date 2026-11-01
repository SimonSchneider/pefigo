package model_test

import (
	"slices"
	"testing"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
)

// apartmentPlans are the plans of the issue's example: a Checking and a Savings account with a monthly
// Savings Transfer Template between them, and an Apartment Event that adds a Property and a Mortgage
// account, the Purchase from the Mortgage to the Property on its date, and a monthly Amortization from
// a month after it.
type apartmentPlans struct {
	apartment              model.Event
	checking, savings      model.Account
	property, mortgage     model.Account
	savingsTT              model.TransferTemplate
	purchase, amortization model.TransferTemplate
}

// mustApartmentPlans stores the apartment plans with the Apartment Event on day and Snapshots of
// 1 000 000 in Checking and 0 in Savings on 2028-01-01, the Forecast Start.
func mustApartmentPlans(t *testing.T, svc *model.Service, day string) apartmentPlans {
	t.Helper()
	var p apartmentPlans
	start := mustParseDate("2028-01-01")
	p.checking = mustAccount(t, svc, "Checking")
	p.savings = mustAccount(t, svc, "Savings")
	mustSnapshot(t, svc, p.checking.ID, start, 1e6)
	mustSnapshot(t, svc, p.savings.ID, start, 0)
	p.savingsTT = mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Savings", FromAccountID: p.checking.ID, ToAccountID: p.savings.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(10000),
		Recurrence: "*-*-25", StartDate: mustParseDate("2027-01-01"), Enabled: true,
	})

	p.apartment = mustEvent(t, svc, model.EventInput{Name: "Apartment", Date: mustParseDate(day)})
	p.property = mustEventAccount(t, svc, "Property", p.apartment.ID)
	p.mortgage = mustEventAccount(t, svc, "Mortgage", p.apartment.ID)
	p.purchase = mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Purchase", FromAccountID: p.mortgage.ID, ToAccountID: p.property.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(3e6),
		Recurrence: date.Cron(day), Enabled: true, EventID: p.apartment.ID,
	})
	p.amortization = mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Amortization", FromAccountID: p.checking.ID, ToAccountID: p.mortgage.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(5000),
		Recurrence: "*-*-25", StartDate: p.apartment.Date.Add(31 * date.Day), Enabled: true, EventID: p.apartment.ID,
	})
	return p
}

// forecastThrough is the Forecast of every account through day, with a point on the 28th of every
// month, placing the Events as in the Baseline except for placements.
func forecastThrough(t *testing.T, svc *model.Service, day string, placements model.EventPlacements) *recordedPrediction {
	t.Helper()
	return forecastWith(t, svc, model.PredictionParams{
		Through: mustParseDate(day), Samples: ptr[int64](1), Quantile: ptr(0.0), SnapshotInterval: "*-*-28", GroupBy: model.GroupByNone, Placements: placements,
	})
}

// monthlyPoints are the points of id on the 28th of every month from January 2028, one per balance.
func monthlyPoints(id string, balances ...float64) []model.PredictionBalanceSnapshot {
	points := make([]model.PredictionBalanceSnapshot, len(balances))
	day := mustParseDate("2028-01-28")
	for i, b := range balances {
		points[i] = model.PredictionBalanceSnapshot{ID: id, Day: day, Balance: b}
		day = date.FromTime(day.ToStdTime().UTC().AddDate(0, 1, 0))
	}
	return points
}

// balanceOn is the balance of id on day in the Forecast, failing without a point there.
func balanceOn(t *testing.T, rec *recordedPrediction, id string, day string) float64 {
	t.Helper()
	for _, s := range rec.pointsOf(id) {
		if s.Day == mustParseDate(day) {
			return s.Balance
		}
	}
	t.Fatalf("no point of %s on %s", id, day)
	return 0
}

func entityIDs(rec *recordedPrediction) []string {
	ids := make([]string, len(rec.setup.Entities))
	for i, e := range rec.setup.Entities {
		ids[i] = e.ID
	}
	slices.Sort(ids)
	return ids
}

func hasMarkline(rec *recordedPrediction, name, day string) bool {
	return slices.ContainsFunc(rec.setup.Marklines, func(m model.Markline) bool {
		return m.Name == name && m.Date == mustParseDate(day)
	})
}

func TestAPlannedEventAppliesItsChangesAtItsDate(t *testing.T) {
	svc := newTestService(t)
	p := mustApartmentPlans(t, svc, "2028-03-01")

	rec := forecastThrough(t, svc, "2028-04-30", nil)

	expectPoints(t, rec.pointsOf(p.property.ID), monthlyPoints(p.property.ID, 0, 0, 3e6, 3e6)...)
	expectPoints(t, rec.pointsOf(p.mortgage.ID), monthlyPoints(p.mortgage.ID, 0, 0, -3e6, -3e6+5000)...)
	expectPoints(t, rec.pointsOf(p.savings.ID), monthlyPoints(p.savings.ID, 10000, 20000, 30000, 40000)...)
	expectPoints(t, rec.pointsOf(p.checking.ID), monthlyPoints(p.checking.ID, 990000, 980000, 970000, 955000)...)
	if !hasMarkline(rec, "Apartment", "2028-03-01") {
		t.Errorf("expected a marker for the Apartment on 2028-03-01, got %+v", rec.setup.Marklines)
	}
}

func TestMovingAPlannedEventMovesItsChangesInTheForecast(t *testing.T) {
	svc := newTestService(t)
	p := mustApartmentPlans(t, svc, "2028-03-01")
	moveEvent(t, svc, p.apartment, "2030-03-01")

	rec := forecastThrough(t, svc, "2030-04-30", nil)

	for _, tc := range []struct {
		day                         string
		property, mortgage, savings float64
	}{
		{"2028-04-28", 0, 0, 40000},
		{"2030-02-28", 0, 0, 260000},
		{"2030-03-28", 3e6, -3e6, 270000},
		{"2030-04-28", 3e6, -3e6 + 5000, 280000},
	} {
		got := [3]float64{balanceOn(t, rec, p.property.ID, tc.day), balanceOn(t, rec, p.mortgage.ID, tc.day), balanceOn(t, rec, p.savings.ID, tc.day)}
		if want := [3]float64{tc.property, tc.mortgage, tc.savings}; got != want {
			t.Errorf("on %s: expected property, mortgage and savings %v, got %v", tc.day, want, got)
		}
	}
	if !hasMarkline(rec, "Apartment", "2030-03-01") {
		t.Errorf("expected the Apartment marker to move to 2030-03-01, got %+v", rec.setup.Marklines)
	}
}

func TestAnUnplannedEventHasNoEffectOnTheBaseline(t *testing.T) {
	svc := newTestService(t)
	p := mustApartmentPlans(t, svc, "2028-03-01")
	moveEvent(t, svc, model.Event{ID: p.apartment.ID, Name: "Apartment", Date: p.apartment.Date, Planned: false}, "2028-03-01")

	t.Run("dashboard forecast", func(t *testing.T) {
		rec := forecastThrough(t, svc, "2028-04-30", nil)
		if want := sortedIDs(p.checking.ID, p.savings.ID); !slices.Equal(entityIDs(rec), want) {
			t.Fatalf("expected only Checking and Savings in the Forecast, got %v", entityIDs(rec))
		}
		expectPoints(t, rec.pointsOf(p.savings.ID), monthlyPoints(p.savings.ID, 10000, 20000, 30000, 40000)...)
		expectPoints(t, rec.pointsOf(p.checking.ID), monthlyPoints(p.checking.ID, 990000, 980000, 970000, 960000)...)
		if hasMarkline(rec, "Apartment", "2028-03-01") {
			t.Errorf("expected no marker for an unplanned Event, got %+v", rec.setup.Marklines)
		}
	})

	t.Run("transfer schedule", func(t *testing.T) {
		for _, tt := range []model.TransferTemplate{p.purchase, p.amortization} {
			if state := transferStateOn(t, svc, tt.ID, "2028-04-25"); state != "absent" {
				t.Errorf("expected %s left out of the Baseline, got %s", tt.Name, state)
			}
		}
		if state := transferStateOn(t, svc, p.savingsTT.ID, "2028-04-25"); state != model.TransferActive {
			t.Errorf("expected Savings to run on unchanged, got %s", state)
		}
	})

	t.Run("transfer templates page", func(t *testing.T) {
		page, err := svc.GetTransferTemplatesPageData(t.Context())
		if err != nil {
			t.Fatalf("transfer templates page: %v", err)
		}
		var leftOut []string
		for _, tt := range page.LeftOut {
			leftOut = append(leftOut, tt.Name)
		}
		slices.Sort(leftOut)
		if want := []string{"Amortization", "Purchase"}; !slices.Equal(leftOut, want) {
			t.Fatalf("expected the unplanned Event's Transfer Templates listed apart, got %v", leftOut)
		}
	})

	t.Run("accounts page", func(t *testing.T) {
		page, err := svc.GetAccountsPageData(t.Context(), nil)
		if err != nil {
			t.Fatalf("accounts page: %v", err)
		}
		i := slices.IndexFunc(page.Accounts, func(a model.AccountDetailed) bool { return a.ID == p.property.ID })
		if i < 0 || page.Accounts[i].EventID != p.apartment.ID || page.GetEvent(p.apartment.ID).Name != "Apartment" {
			t.Fatalf("expected the Property account listed with its Event, got %+v", page.Accounts)
		}
	})
}

func sortedIDs(ids ...string) []string {
	slices.Sort(ids)
	return ids
}

func TestAnUnplannedEventIsLeftOutOfTheTransfersPageAndTheBudget(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	checking := mustAccount(t, svc, "Checking")
	savings := mustAccount(t, svc, "Savings")
	color := "#ff0000"
	housing, err := svc.UpsertCategory(ctx, model.TransferTemplateCategoryInput{Name: "Housing", Color: &color})
	if err != nil {
		t.Fatalf("create category: %v", err)
	}
	groceries := mustGroceries(t, svc, checking.ID, "*-*-25")
	today := date.Today()
	ev := mustEvent(t, svc, model.EventInput{Name: "Apartment", Date: today.Add(-40 * date.Day)})
	mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Amortization", FromAccountID: checking.ID, ToAccountID: savings.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(5000),
		Recurrence: "*-*-25", Enabled: true, EventID: ev.ID, BudgetCategoryID: &housing.ID,
	})
	payday := date.Today()
	for payday.Day() != 25 {
		payday = payday.Add(date.Day)
	}

	budgetItems := func(t *testing.T) []string {
		t.Helper()
		budget, err := svc.GetBudgetData(ctx)
		if err != nil {
			t.Fatalf("budget: %v", err)
		}
		var names []string
		for _, c := range budget.Categories {
			for _, item := range c.Items {
				names = append(names, item.Name)
			}
		}
		return names
	}

	expectMoved(t, transfersOn(t, svc, payday.String(), groceries), map[string]float64{checking.ID: 5000})
	if got := budgetItems(t); !slices.Equal(got, []string{"Amortization"}) {
		t.Fatalf("expected the planned Event's Amortization in the budget, got %v", got)
	}

	moveEvent(t, svc, model.Event{ID: ev.ID, Name: ev.Name, Date: ev.Date, Planned: false}, ev.Date.String())

	expectMoved(t, transfersOn(t, svc, payday.String(), groceries), map[string]float64{})
	if got := budgetItems(t); len(got) != 0 {
		t.Fatalf("expected an unplanned Event left out of the budget, got %v", got)
	}
}

func TestAPlannedEventBeforeTheForecastStartOnlyCountsItsLaterChanges(t *testing.T) {
	svc := newTestService(t)
	p := mustApartmentPlans(t, svc, "2027-12-01")

	rec := forecastThrough(t, svc, "2028-01-31", nil)

	// The Purchase is taken to be in the Snapshots, so only January's Amortization reaches the
	// Mortgage.
	expectPoints(t, rec.pointsOf(p.property.ID), monthlyPoints(p.property.ID, 0)...)
	expectPoints(t, rec.pointsOf(p.mortgage.ID), monthlyPoints(p.mortgage.ID, 5000)...)
	expectPoints(t, rec.pointsOf(p.savings.ID), monthlyPoints(p.savings.ID, 10000)...)
	expectPoints(t, rec.pointsOf(p.checking.ID), monthlyPoints(p.checking.ID, 1e6-15000)...)
}

func TestAForecastPlacesEventsDifferentlyFromTheBaseline(t *testing.T) {
	svc := newTestService(t)
	p := mustApartmentPlans(t, svc, "2028-03-01")
	bonus := mustEvent(t, svc, model.EventInput{Name: "Bonus", Date: mustParseDate("2028-02-01"), Planned: ptr(false)})
	mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Bonus", ToAccountID: p.checking.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(100000),
		Recurrence: "2028-02-01", Enabled: true, EventID: bonus.ID,
	})

	t.Run("placed", func(t *testing.T) {
		rec := forecastThrough(t, svc, "2028-04-30", model.EventPlacements{
			p.apartment.ID: {LeftOut: true},
			bonus.ID:       {Date: mustParseDate("2028-02-15")},
		})
		if want := sortedIDs(p.checking.ID, p.savings.ID); !slices.Equal(entityIDs(rec), want) {
			t.Fatalf("expected the Apartment's accounts left out, got %v", entityIDs(rec))
		}
		expectPoints(t, rec.pointsOf(p.checking.ID), monthlyPoints(p.checking.ID, 990000, 1080000, 1070000, 1060000)...)
		expectPoints(t, rec.pointsOf(p.savings.ID), monthlyPoints(p.savings.ID, 10000, 20000, 30000, 40000)...)
		if !hasMarkline(rec, "Bonus", "2028-02-15") || hasMarkline(rec, "Apartment", "2028-03-01") {
			t.Errorf("expected a marker for the Bonus where it is placed and none for the Apartment, got %+v", rec.setup.Marklines)
		}
	})

	t.Run("baseline", func(t *testing.T) {
		rec := forecastThrough(t, svc, "2028-04-30", nil)
		expectPoints(t, rec.pointsOf(p.checking.ID), monthlyPoints(p.checking.ID, 990000, 980000, 970000, 955000)...)
		if hasMarkline(rec, "Bonus", "2028-02-01") || !hasMarkline(rec, "Apartment", "2028-03-01") {
			t.Errorf("expected a marker for the Apartment only, got %+v", rec.setup.Marklines)
		}
	})
}

func TestEntriesThatUseALeftOutAccountAreLeftOutToo(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	mustSnapshot(t, svc, checking.ID, mustParseDate("2028-01-01"), 1000)
	ev := mustEvent(t, svc, model.EventInput{Name: "Car", Date: mustParseDate("2028-01-01"), Planned: ptr(false)})
	car := mustEventAccount(t, svc, "Car", ev.ID)
	carSavings := mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Car savings", FromAccountID: checking.ID, ToAccountID: car.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(100),
		Recurrence: "*-*-10", StartDate: mustParseDate("2028-01-01"), Enabled: true,
	})

	if state := transferStateOn(t, svc, carSavings.ID, "2028-01-10"); state != "absent" {
		t.Fatalf("expected the Transfer Template into the left-out account left out, got %s", state)
	}
	rec := forecastThrough(t, svc, "2028-01-31", nil)
	expectPoints(t, rec.pointsOf(checking.ID), monthlyPoints(checking.ID, 1000)...)

	// Checking's interest is paid out to the Car account, so it stays in Checking while the Car is
	// left out.
	if _, err := svc.UpsertAccount(t.Context(), model.AccountInput{ID: checking.ID, Name: "Checking", CashFlowFrequency: "*-*-27", CashFlowDestinationID: car.ID}); err != nil {
		t.Fatalf("update checking: %v", err)
	}
	mustGrowthModel(t, svc, model.AccountGrowthModelInput{AccountID: checking.ID, AnnualRate: newFixedValue(0.1), StartDate: mustParseDate("2027-01-01")})
	rec = forecastThrough(t, svc, "2028-01-31", nil)
	if got := balanceOn(t, rec, checking.ID, "2028-01-28"); got <= 1000 {
		t.Fatalf("expected Checking to keep its interest, got %v", got)
	}
}

func TestPlannedEventsAreMarkedOnTheDashboardForecastChart(t *testing.T) {
	svc := newTestService(t)
	mustEvent(t, svc, model.EventInput{Name: "Apartment", Date: mustParseDate("2028-03-01")})
	mustEvent(t, svc, model.EventInput{Name: "Sabbatical", Date: mustParseDate("2029-01-01"), Planned: ptr(false)})
	if _, err := svc.UpsertSpecialDate(t.Context(), model.SpecialDateInput{Name: "Retirement", Date: mustParseDate("2040-01-01")}); err != nil {
		t.Fatalf("create special date: %v", err)
	}

	data := dashboardForecastHistory(t, svc, nil)

	var names []string
	for _, m := range data.Marklines {
		names = append(names, m.Name)
	}
	if want := []string{"Today", "Apartment", "Retirement"}; !slices.Equal(names, want) {
		t.Fatalf("expected the date lines %v, got %+v", want, data.Marklines)
	}
}

func TestAnUnplannedEventsAccountsAreLeftOutOfTheDashboardForecastSeriesAndTheCashflows(t *testing.T) {
	svc := newTestService(t)
	savings := mustAccountType(t, svc, "Savings", "#00ff00")
	property := mustAccountType(t, svc, "Property", "#0000ff")
	checking := mustTypedAccount(t, svc, "Checking", savings.ID)
	mustSnapshot(t, svc, checking.ID, mustParseDate("2025-01-01"), 1000)
	ev := mustEvent(t, svc, model.EventInput{Name: "Cabin", Date: mustParseDate("2030-01-01"), Planned: ptr(false)})
	cabin, err := svc.UpsertAccount(t.Context(), model.AccountInput{Name: "Cabin", TypeID: property.ID, EventID: ev.ID})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Cabin savings", FromAccountID: checking.ID, ToAccountID: cabin.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(100),
		Recurrence: "*-*-25", StartDate: mustParseDate("2020-01-01"), Enabled: true,
	})

	if _, ok := seriesByID(dashboardForecastHistory(t, svc, nil))[property.ID]; ok {
		t.Errorf("expected no Dashboard Forecast series for an Account Type of an unplanned Event's accounts only")
	}
	data, err := svc.GetTransferChartData(t.Context(), model.GroupByAccount)
	if err != nil {
		t.Fatalf("cashflows: %v", err)
	}
	for _, node := range data.Data {
		if node.Name == cabin.ID || node.Label == "Cabin" {
			t.Errorf("expected the Cabin left out of the cashflows, got %+v", node)
		}
	}

	moveEvent(t, svc, model.Event{ID: ev.ID, Name: ev.Name, Date: ev.Date, Planned: true}, "2030-01-01")
	if _, ok := seriesByID(dashboardForecastHistory(t, svc, nil))[property.ID]; !ok {
		t.Errorf("expected a Dashboard Forecast series for the planned Event's Account Type")
	}
}
