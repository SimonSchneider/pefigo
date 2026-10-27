package model_test

import (
	"context"
	"testing"

	"github.com/SimonSchneider/goslu/date"
	pefigo "github.com/SimonSchneider/pefigo"
	"github.com/SimonSchneider/pefigo/internal/model"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"

	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
	"github.com/ncruces/go-sqlite3/vfs/memdb"
)

func newBenchService(b *testing.B) *model.Service {
	b.Helper()
	svc, err := model.Open(context.Background(), pefigo.StaticEmbeddedFS, "static/migrations", memdb.TestDB(b))
	if err != nil {
		b.Fatalf("failed to create bench db: %v", err)
	}
	b.Cleanup(func() { svc.DB().Close() })
	seedBenchData(b, svc)
	return svc
}

func seedBenchData(b *testing.B, svc *model.Service) {
	b.Helper()
	ctx := context.Background()
	acc1, err := svc.UpsertAccount(ctx, model.AccountInput{Name: "Checking"})
	if err != nil {
		b.Fatalf("seed account: %v", err)
	}
	acc2, err := svc.UpsertAccount(ctx, model.AccountInput{Name: "Savings"})
	if err != nil {
		b.Fatalf("seed account: %v", err)
	}
	names := []string{"Rent", "Groceries", "Gym", "Netflix", "Insurance", "Phone", "Internet", "Electricity", "Water", "Gas"}
	recurrences := []string{"*-*-01", "*-*-15", "*-*/3-01"}
	start, _ := date.ParseDate("2024-01-01")
	for i := range 30 {
		_, err := svc.UpsertTransferTemplate(ctx, model.TransferTemplate{
			Name:          names[i%len(names)],
			FromAccountID: acc1.ID,
			ToAccountID:   acc2.ID,
			AmountType:    "fixed",
			AmountFixed:   uncertain.NewFixed(float64(500 + i*100)),
			Recurrence:    date.Cron(recurrences[i%len(recurrences)]),
			StartDate:     start,
			Enabled:       true,
			Priority:      int64(i % 5),
		})
		if err != nil {
			b.Fatalf("seed template %d: %v", i, err)
		}
	}
}

func BenchmarkTransferSchedule(b *testing.B) {
	svc := newBenchService(b)
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		_, err := svc.TransferSchedule(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetTransferTemplatesPageData(b *testing.B) {
	svc := newBenchService(b)
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		_, err := svc.GetTransferTemplatesPageData(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetDashboardData(b *testing.B) {
	svc := newBenchService(b)
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		_, err := svc.GetDashboardData(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// discardedForecast drops every event of a Forecast.
type discardedForecast struct{}

func (discardedForecast) Setup(model.PredictionSetupEvent) error         { return nil }
func (discardedForecast) Snapshot(model.PredictionBalanceSnapshot) error { return nil }
func (discardedForecast) Close() error                                   { return nil }

func BenchmarkForecast(b *testing.B) {
	svc := newBenchService(b)
	ctx := context.Background()
	accounts, err := svc.ListAccounts(ctx)
	if err != nil {
		b.Fatal(err)
	}
	start, _ := date.ParseDate("2024-01-01")
	for _, acc := range accounts {
		if _, err := svc.UpsertAccountSnapshot(ctx, acc.ID, model.AccountSnapshotInput{Date: start, Balance: uncertain.NewFixed(100_000)}); err != nil {
			b.Fatalf("seed snapshot: %v", err)
		}
	}
	params := model.PredictionParams{Duration: ptr(date.Year), Samples: ptr[int64](100), Quantile: ptr(0.8), SnapshotInterval: "*-*-28", GroupBy: model.GroupByType}
	b.ResetTimer()
	for b.Loop() {
		if err := svc.RunPrediction(ctx, discardedForecast{}, params); err != nil {
			b.Fatal(err)
		}
	}
}
