package model

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io/fs"

	"github.com/SimonSchneider/goslu/config"
	"github.com/SimonSchneider/goslu/migrate"
	"github.com/SimonSchneider/pefigo/internal/pdb"
	"github.com/SimonSchneider/pefigo/pkg/currency"
	"github.com/SimonSchneider/pefigo/pkg/swe"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
	"github.com/ncruces/go-sqlite3/driver"
)

type Service struct {
	db *sql.DB
	q  *pdb.Queries
	// tx is the transaction q is in, or nil outside one. See Service.inTx.
	tx                *sql.Tx
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

// WithSweOptions builds the Service's swe client with opts, such as swe.WithTaxData for in-memory Tax
// Tables instead of Skatteverket's.
func WithSweOptions(opts ...swe.ClientOption) ServiceOption {
	return func(s *Service) {
		s.sweClient = swe.NewClient(NewSQLiteCache(s.db), opts...)
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

// inTx runs f with the Service in one transaction, and keeps what f writes only when it succeeds: all
// of it, or none. Within a transaction already, f runs in it.
func (s *Service) inTx(ctx context.Context, f func(tx *Service) error) error {
	if s.tx != nil {
		return f(s)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning a transaction: %w", err)
	}
	defer tx.Rollback()
	in := *s
	in.q, in.tx = s.q.WithTx(tx), tx
	if err := f(&in); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing a transaction: %w", err)
	}
	return nil
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

// orZero is v, or a fixed 0 when v isn't given: the zero uncertain.Value an empty amount field maps to.
// It is the default of every amount, as 0 is of every other number.
func orZero(v uncertain.Value) uncertain.Value {
	if v.Distribution == "" {
		return uncertain.NewFixed(0)
	}
	return v
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
