package model_test

import (
	"slices"
	"testing"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
)

func entitiesByID(data *model.ForecastDashboardData) map[string]model.PredictionFinancialEntity {
	byID := make(map[string]model.PredictionFinancialEntity, len(data.Entities))
	for _, e := range data.Entities {
		byID[e.ID] = e
	}
	return byID
}

func TestDashboardForecastHistoryIdentifiesEntitiesByAccountTypeWithoutForecastRows(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	at, err := svc.UpsertAccountType(ctx, model.AccountTypeInput{Name: "Savings", Color: "#00ff00"})
	if err != nil {
		t.Fatalf("create account type: %v", err)
	}
	acc, err := svc.UpsertAccount(ctx, model.AccountInput{Name: "Test", TypeID: at.ID})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	if _, err := svc.UpsertAccountSnapshot(ctx, acc.ID, model.AccountSnapshotInput{Date: mustParseDate("2026-01-01"), Balance: newFixedValue(10000)}); err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	if _, err := svc.UpsertAccountType(ctx, model.AccountTypeInput{Name: "Unused", Color: "#0000ff"}); err != nil {
		t.Fatalf("create account type: %v", err)
	}
	sd, err := svc.UpsertSpecialDate(ctx, model.SpecialDateInput{Name: "Retirement", Date: mustParseDate("2040-01-01"), Color: "#ff0000"})
	if err != nil {
		t.Fatalf("create special date: %v", err)
	}

	data, err := svc.GetDashboardForecastHistory(ctx, nil)
	if err != nil {
		t.Fatalf("get dashboard forecast history: %v", err)
	}

	want := []model.PredictionFinancialEntity{{
		ID:        at.ID,
		Name:      "Savings",
		Color:     "#00ff00",
		Snapshots: []model.PredictionBalanceSnapshot{{Day: mustParseDate("2026-01-01").ToStdTime().UnixMilli(), Balance: 10000}},
	}}
	if !slices.EqualFunc(data.Entities, want, func(a, b model.PredictionFinancialEntity) bool {
		return a.ID == b.ID && a.Name == b.Name && a.Color == b.Color && slices.Equal(a.Snapshots, b.Snapshots)
	}) {
		t.Fatalf("expected entities %+v, got %+v", want, data.Entities)
	}
	wantMarklines := []model.Markline{
		{Date: date.Today().ToStdTime().UnixMilli(), Name: "Today"},
		{Date: sd.Date.ToStdTime().UnixMilli(), Color: "#ff0000", Name: "Retirement"},
	}
	if !slices.Equal(data.Marklines, wantMarklines) {
		t.Fatalf("expected marklines %+v, got %+v", wantMarklines, data.Marklines)
	}
}

func TestDashboardForecastHistoryHoldsHistoryPointsOnly(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	savings, err := svc.UpsertAccountType(ctx, model.AccountTypeInput{Name: "Savings", Color: "#00ff00"})
	if err != nil {
		t.Fatalf("create account type: %v", err)
	}
	pension, err := svc.UpsertAccountType(ctx, model.AccountTypeInput{Name: "Pension", Color: "#0000ff"})
	if err != nil {
		t.Fatalf("create account type: %v", err)
	}
	acc, err := svc.UpsertAccount(ctx, model.AccountInput{Name: "Test", TypeID: savings.ID})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	for _, d := range []string{"2026-01-01", "2026-02-01"} {
		if _, err := svc.UpsertAccountSnapshot(ctx, acc.ID, model.AccountSnapshotInput{Date: mustParseDate(d), Balance: newFixedValue(10000)}); err != nil {
			t.Fatalf("create snapshot: %v", err)
		}
	}
	future := date.Today().Add(date.Year).ToStdTime().UnixMilli()
	rows := []model.ForecastCacheRow{
		{Date: future, AccountTypeID: savings.ID, Median: 12000},
		{Date: future, AccountTypeID: pension.ID, Median: 5000},
		{Date: future, AccountTypeID: "deleted-type", Median: 1},
	}

	data, err := svc.GetDashboardForecastHistory(ctx, rows)
	if err != nil {
		t.Fatalf("get dashboard forecast history: %v", err)
	}

	if len(data.Entities) != 2 {
		t.Fatalf("expected an entity for Savings and Pension only, got %+v", data.Entities)
	}
	byID := entitiesByID(data)
	if got := byID[savings.ID]; got.Name != "Savings" || len(got.Snapshots) != 2 || slices.ContainsFunc(got.Snapshots, func(s model.PredictionBalanceSnapshot) bool { return s.Day == future }) {
		t.Fatalf("expected Savings to hold its 2 history points only, got %+v", got)
	}
	if got := byID[pension.ID]; got.Name != "Pension" || got.Color != "#0000ff" || got.Snapshots == nil || len(got.Snapshots) != 0 {
		t.Fatalf("expected Pension without history points, got %+v", got)
	}
}

func TestDashboardForecastHistoryKeepsTheUncategorizedSeries(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	acc, err := svc.UpsertAccount(ctx, model.AccountInput{Name: "No type"})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	if _, err := svc.UpsertAccountSnapshot(ctx, acc.ID, model.AccountSnapshotInput{Date: mustParseDate("2026-01-01"), Balance: newFixedValue(500)}); err != nil {
		t.Fatalf("create snapshot: %v", err)
	}

	data, err := svc.GetDashboardForecastHistory(ctx, nil)
	if err != nil {
		t.Fatalf("get dashboard forecast history: %v", err)
	}

	if len(data.Entities) != 1 || data.Entities[0].ID != "" || data.Entities[0].Name != "Uncategorized" || len(data.Entities[0].Snapshots) != 1 {
		t.Fatalf("expected the Uncategorized history series, got %+v", data.Entities)
	}
}
