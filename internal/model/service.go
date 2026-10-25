package model

import (
	"context"
	"database/sql"
	"flag"
	"io/fs"

	"github.com/SimonSchneider/goslu/config"
	"github.com/SimonSchneider/goslu/migrate"
	"github.com/SimonSchneider/pefigo/internal/pdb"
	"github.com/SimonSchneider/pefigo/pkg/currency"
	"github.com/SimonSchneider/pefigo/pkg/swe"
	"github.com/ncruces/go-sqlite3/driver"
)

type Service struct {
	db                *sql.DB
	q                 *pdb.Queries
	sweClient         *swe.Client
	currencyClient    *currency.Client
	forecastInputs    *forecastInputWatcher
	dashboardForecast *DashboardForecast
}

// ForecastInputGeneration advances every time a Forecast Input is written.
func (s *Service) ForecastInputGeneration() uint64 {
	return s.forecastInputs.generation.Load()
}

type ServiceOption func(*Service)

func WithCurrencyOptions(opts ...currency.ClientOption) ServiceOption {
	return func(s *Service) {
		ttlCache := NewTTLSQLiteCache(s.db)
		s.currencyClient = currency.NewClient(ttlCache, opts...)
	}
}

// Open opens and migrates the SQLite database at conn, with the migrations in dir under path, and
// returns a Service on it. Writes to Forecast Inputs are detected on every pooled connection and
// mark the Dashboard Forecast Stale, which only schedules Forecast Runs once it is started.
func Open(ctx context.Context, dir fs.FS, path string, conn string, opts ...ServiceOption) (*Service, error) {
	dashboardForecast := newDashboardForecast()
	forecastInputs := &forecastInputWatcher{dashboardForecast: dashboardForecast}
	db, err := driver.Open(conn, forecastInputs.register)
	if err != nil {
		return nil, err
	}
	migrations, err := fs.Sub(dir, path)
	if err != nil {
		db.Close()
		return nil, err
	}
	if err := migrate.Migrate(ctx, migrations, db); err != nil {
		db.Close()
		return nil, err
	}
	cache := NewSQLiteCache(db)
	ttlCache := NewTTLSQLiteCache(db)
	s := &Service{
		db:                db,
		q:                 pdb.New(db),
		sweClient:         swe.NewClient(cache),
		currencyClient:    currency.NewClient(ttlCache),
		forecastInputs:    forecastInputs,
		dashboardForecast: dashboardForecast,
	}
	dashboardForecast.svc = s
	dashboardForecast.compute = s
	for _, o := range opts {
		o(s)
	}
	return s, nil
}

func (s *Service) SweClient() *swe.Client {
	return s.sweClient
}

func (s *Service) DB() *sql.DB {
	return s.db
}

type Config struct {
	Addr  string
	Watch bool
	DbURL string
}

func ParseConfig(args []string, getEnv func(string) string) (cfg Config, err error) {
	err = config.ParseInto(&cfg, flag.NewFlagSet("", flag.ExitOnError), args, getEnv)
	return cfg, err
}

func KeyBy[T any](items []T, key func(T) string) map[string]T {
	m := make(map[string]T)
	for _, item := range items {
		m[key(item)] = item
	}
	return m
}

func ptr[T any](v T) *T {
	return &v
}

func initMap[K comparable, V any](m map[K]V, ks ...K) map[K]V {
	var v V
	for _, k := range ks {
		if _, ok := m[k]; !ok {
			m[k] = v
		}
	}
	return m
}
