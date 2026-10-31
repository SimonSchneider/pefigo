package model

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/pdb"
	"github.com/SimonSchneider/pefigo/pkg/currency"
)

const (
	settingDefaultCurrency          = "default_currency"
	settingForecastSamples          = "forecast_samples"
	settingForecastSnapshotInterval = "forecast_snapshot_interval"
	settingForecastStartDays        = "forecast_start_days"
)

func (s *Service) GetDefaultCurrency(ctx context.Context) (string, error) {
	val, err := s.q.GetSetting(ctx, settingDefaultCurrency)
	if errors.Is(err, sql.ErrNoRows) {
		return "SEK", nil
	}
	if err != nil {
		return "", fmt.Errorf("getting default currency: %w", err)
	}
	return val, nil
}

// SetDefaultCurrency stores the default currency, which must be a supported currency.
func (s *Service) SetDefaultCurrency(ctx context.Context, code string) error {
	if err := checkSupportedCurrency("default currency", code); err != nil {
		return err
	}
	if err := s.q.UpsertSetting(ctx, pdb.UpsertSettingParams{
		Key:   settingDefaultCurrency,
		Value: code,
	}); err != nil {
		return fmt.Errorf("setting default currency: %w", err)
	}
	return nil
}

// ForecastSettings are the settings of the Dashboard Forecast: how many samples each Forecast Run
// takes, and the Forecast Interval. Every Forecast starts on the Forecast Start Days.
type ForecastSettings struct {
	Samples   int64
	Interval  date.Cron
	StartDays date.Cron
}

// The Forecast Settings' defaults: the value of a setting that isn't stored, or isn't given when the
// Forecast Settings are saved.
const (
	DefaultForecastSamples   int64     = 10000
	DefaultForecastInterval  date.Cron = "*-01-01"
	DefaultForecastStartDays date.Cron = "*-*-*"
)

// The range the Forecast Samples must be within.
const (
	MinForecastSamples int64 = 100
	MaxForecastSamples int64 = 100000
)

// check names every setting the Dashboard Forecast can't be run with. It doesn't wrap ErrInvalidInput:
// saving rejects the input with it, and a Forecast Run fails on stored settings with it.
func (fs ForecastSettings) check() error {
	var samples error
	if fs.Samples < MinForecastSamples || fs.Samples > MaxForecastSamples {
		samples = fmt.Errorf("samples %d is not within %d–%d", fs.Samples, MinForecastSamples, MaxForecastSamples)
	}
	return errors.Join(samples, datePatternError("Forecast Interval", fs.Interval), fs.checkStartDays())
}

// checkStartDays checks only the Forecast Start Days, the one setting every Forecast uses: the chart
// page's and the Transfers page's too, not only the Dashboard Forecast's.
func (fs ForecastSettings) checkStartDays() error {
	return datePatternError("Forecast Start Days", fs.StartDays)
}

// ForecastSettingsInput is a save of the Forecast Settings. A setting that isn't given, a nil Samples
// or an empty pattern, saves its default.
type ForecastSettingsInput struct {
	Samples   *int64
	Interval  date.Cron
	StartDays date.Cron
}

// forecastSettings applies the defaults to inp and checks the result.
func (inp ForecastSettingsInput) forecastSettings() (ForecastSettings, error) {
	fs := ForecastSettings{Samples: DefaultForecastSamples, Interval: inp.Interval, StartDays: inp.StartDays}
	if inp.Samples != nil {
		fs.Samples = *inp.Samples
	}
	if fs.Interval == "" {
		fs.Interval = DefaultForecastInterval
	}
	if fs.StartDays == "" {
		fs.StartDays = DefaultForecastStartDays
	}
	if err := fs.check(); err != nil {
		return ForecastSettings{}, fmt.Errorf("%w: %w", ErrInvalidInput, err)
	}
	return fs, nil
}

// GetForecastSettings returns the stored Forecast Settings, with the default of each one that isn't
// stored. It doesn't check them, so the settings page shows a stored invalid setting to be fixed; a
// Forecast fails on one it uses instead.
func (s *Service) GetForecastSettings(ctx context.Context) (ForecastSettings, error) {
	rows, err := s.q.ListSettings(ctx)
	if err != nil {
		return ForecastSettings{}, fmt.Errorf("listing settings: %w", err)
	}
	fs := ForecastSettings{Samples: DefaultForecastSamples, Interval: DefaultForecastInterval, StartDays: DefaultForecastStartDays}
	for _, row := range rows {
		switch row.Key {
		case settingForecastSamples:
			fs.Samples, err = strconv.ParseInt(row.Value, 10, 64)
			if err != nil {
				return ForecastSettings{}, fmt.Errorf("the stored forecast samples %q are not a number", row.Value)
			}
		case settingForecastSnapshotInterval:
			fs.Interval = date.Cron(row.Value)
		case settingForecastStartDays:
			fs.StartDays = date.Cron(row.Value)
		}
	}
	return fs, nil
}

// SetForecastSettings saves the Forecast Settings, each one that isn't given at its default. It checks
// them all before saving any, and saves them in one transaction, so a rejected or failed save changes
// none.
func (s *Service) SetForecastSettings(ctx context.Context, inp ForecastSettingsInput) error {
	fs, err := inp.forecastSettings()
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning to save the forecast settings: %w", err)
	}
	defer tx.Rollback()
	q := s.q.WithTx(tx)
	for _, setting := range []pdb.UpsertSettingParams{
		{Key: settingForecastSamples, Value: strconv.FormatInt(fs.Samples, 10)},
		{Key: settingForecastSnapshotInterval, Value: string(fs.Interval)},
		{Key: settingForecastStartDays, Value: string(fs.StartDays)},
	} {
		if err := q.UpsertSetting(ctx, setting); err != nil {
			return fmt.Errorf("saving %s: %w", setting.Key, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("saving the forecast settings: %w", err)
	}
	return nil
}

type SettingsView struct {
	AccountTypes     []AccountType
	Categories       []TransferTemplateCategory
	SpecialDates     []SpecialDate
	SweYearlyParams  []SweYearlyParams
	CurrentCurrency  string
	Currencies       []currency.Currency
	ForecastSettings ForecastSettings
}

func (s *Service) GetSettingsPageData(ctx context.Context) (*SettingsView, error) {
	accountTypes, err := s.ListAccountTypes(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing account types: %w", err)
	}
	categories, err := s.ListCategories(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing categories: %w", err)
	}
	specialDates, err := s.ListSpecialDates(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing special dates: %w", err)
	}
	ibbs, err := s.ListSweYearlyParams(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing swe yearly params: %w", err)
	}
	cur, err := s.GetDefaultCurrency(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting default currency: %w", err)
	}
	forecastSettings, err := s.GetForecastSettings(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting forecast settings: %w", err)
	}
	return &SettingsView{
		AccountTypes:     accountTypes,
		Categories:       categories,
		SpecialDates:     specialDates,
		SweYearlyParams:  ibbs,
		CurrentCurrency:  cur,
		Currencies:       currency.SupportedCurrencies(),
		ForecastSettings: forecastSettings,
	}, nil
}
