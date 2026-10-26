package model

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/pdb"
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

func (s *Service) GetForecastSamples(ctx context.Context) (int64, error) {
	val, err := s.q.GetSetting(ctx, settingForecastSamples)
	if errors.Is(err, sql.ErrNoRows) {
		return 10000, nil
	}
	if err != nil {
		return 0, fmt.Errorf("getting forecast samples: %w", err)
	}
	n, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing forecast samples: %w", err)
	}
	return n, nil
}

func (s *Service) SetForecastSamples(ctx context.Context, samples int64) error {
	if samples < 100 || samples > 100000 {
		return fmt.Errorf("samples must be between 100 and 100000, got %d", samples)
	}
	if err := s.q.UpsertSetting(ctx, pdb.UpsertSettingParams{
		Key:   settingForecastSamples,
		Value: strconv.FormatInt(samples, 10),
	}); err != nil {
		return err
	}
	return nil
}

func (s *Service) GetForecastSnapshotInterval(ctx context.Context) (string, error) {
	val, err := s.q.GetSetting(ctx, settingForecastSnapshotInterval)
	if errors.Is(err, sql.ErrNoRows) {
		return "*-01-01", nil
	}
	if err != nil {
		return "", fmt.Errorf("getting forecast snapshot interval: %w", err)
	}
	return val, nil
}

func (s *Service) SetForecastSnapshotInterval(ctx context.Context, interval string) error {
	if err := checkDatePattern("forecast interval", date.Cron(interval)); err != nil {
		return err
	}
	if err := s.q.UpsertSetting(ctx, pdb.UpsertSettingParams{
		Key:   settingForecastSnapshotInterval,
		Value: interval,
	}); err != nil {
		return err
	}
	return nil
}

// GetForecastStartDays returns the Forecast Start Days, a date pattern that matches every day by
// default.
func (s *Service) GetForecastStartDays(ctx context.Context) (string, error) {
	val, err := s.q.GetSetting(ctx, settingForecastStartDays)
	if errors.Is(err, sql.ErrNoRows) {
		return "*-*-*", nil
	}
	if err != nil {
		return "", fmt.Errorf("getting forecast start days: %w", err)
	}
	return val, nil
}

func (s *Service) SetForecastStartDays(ctx context.Context, days string) error {
	if err := checkDatePattern("forecast start days", date.Cron(days)); err != nil {
		return err
	}
	if err := s.q.UpsertSetting(ctx, pdb.UpsertSettingParams{
		Key:   settingForecastStartDays,
		Value: days,
	}); err != nil {
		return err
	}
	return nil
}
