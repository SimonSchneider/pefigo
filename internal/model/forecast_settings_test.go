package model_test

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
)

func mustForecastSettings(t *testing.T, svc *model.Service, inp model.ForecastSettingsInput) {
	t.Helper()
	if err := svc.SetForecastSettings(t.Context(), inp); err != nil {
		t.Fatalf("set forecast settings: %v", err)
	}
}

func mustGetForecastSettings(t *testing.T, svc *model.Service) model.ForecastSettings {
	t.Helper()
	settings, err := svc.GetForecastSettings(t.Context())
	if err != nil {
		t.Fatalf("get forecast settings: %v", err)
	}
	return settings
}

var defaultForecastSettings = model.ForecastSettings{Samples: 10000, Interval: "*-01-01", StartDays: "*-*-*"}

// someForecastSettings are valid Forecast Settings, none of them at its default.
var someForecastSettings = model.ForecastSettingsInput{Samples: ptr[int64](500), Interval: "*-*-01", StartDays: "*-*-28"}

func TestTheForecastSettingsAreAtTheirDefaultsWhenNoneAreStored(t *testing.T) {
	svc := newTestService(t)

	if got := mustGetForecastSettings(t, svc); got != defaultForecastSettings {
		t.Fatalf("expected the default Forecast Settings %+v, got %+v", defaultForecastSettings, got)
	}
}

func TestSavedForecastSettingsAreReadBack(t *testing.T) {
	svc := newTestService(t)

	mustForecastSettings(t, svc, someForecastSettings)

	want := model.ForecastSettings{Samples: 500, Interval: "*-*-01", StartDays: "*-*-28"}
	if got := mustGetForecastSettings(t, svc); got != want {
		t.Fatalf("expected the saved Forecast Settings %+v, got %+v", want, got)
	}
}

func TestABlankForecastSettingSavesItsDefault(t *testing.T) {
	for _, tc := range []struct {
		name  string
		blank func(*model.ForecastSettingsInput)
		want  model.ForecastSettings
	}{
		{"samples", func(inp *model.ForecastSettingsInput) { inp.Samples = nil }, model.ForecastSettings{Samples: 10000, Interval: "*-*-01", StartDays: "*-*-28"}},
		{"Forecast Interval", func(inp *model.ForecastSettingsInput) { inp.Interval = "" }, model.ForecastSettings{Samples: 500, Interval: "*-01-01", StartDays: "*-*-28"}},
		{"Forecast Start Days", func(inp *model.ForecastSettingsInput) { inp.StartDays = "" }, model.ForecastSettings{Samples: 500, Interval: "*-*-01", StartDays: "*-*-*"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := newTestService(t)
			mustForecastSettings(t, svc, someForecastSettings)

			inp := someForecastSettings
			tc.blank(&inp)
			mustForecastSettings(t, svc, inp)

			if got := mustGetForecastSettings(t, svc); got != tc.want {
				t.Fatalf("expected the blank %s to save its default, got %+v", tc.name, got)
			}
		})
	}
}

func TestSavingForecastSettingsWithOneInvalidSettingSavesNone(t *testing.T) {
	for _, tc := range []struct {
		name, value string
		invalid     func(*model.ForecastSettingsInput)
	}{
		{"samples", "99", func(inp *model.ForecastSettingsInput) { inp.Samples = ptr[int64](99) }},
		{"samples", "100001", func(inp *model.ForecastSettingsInput) { inp.Samples = ptr[int64](100001) }},
		{"Forecast Interval", strconv.Quote("28"), func(inp *model.ForecastSettingsInput) { inp.Interval = "28" }},
		{"Forecast Start Days", strconv.Quote("*-*"), func(inp *model.ForecastSettingsInput) { inp.StartDays = "*-*" }},
	} {
		t.Run(tc.name+" "+tc.value, func(t *testing.T) {
			svc := newTestService(t)
			mustForecastSettings(t, svc, model.ForecastSettingsInput{Samples: ptr[int64](200), Interval: "*-*-15", StartDays: "*-*-25"})
			before := mustGetForecastSettings(t, svc)

			inp := someForecastSettings
			tc.invalid(&inp)
			err := svc.SetForecastSettings(t.Context(), inp)

			if !errors.Is(err, model.ErrInvalidInput) {
				t.Fatalf("expected ErrInvalidInput, got %v", err)
			}
			if !strings.Contains(err.Error(), tc.name) || !strings.Contains(err.Error(), tc.value) {
				t.Errorf("expected the error to name the %s %s, got %q", tc.name, tc.value, err)
			}
			if got := mustGetForecastSettings(t, svc); got != before {
				t.Fatalf("expected no Forecast Setting to change from %+v, got %+v", before, got)
			}
		})
	}
}

func TestTheForecastSettingsAreSavedInOneTransaction(t *testing.T) {
	svc := newTestService(t)
	// Fail the write of the Forecast Start Days, after the others are written.
	mustExec(t, svc, `CREATE TRIGGER fail_forecast_start_days BEFORE INSERT ON app_setting
		WHEN NEW.key = 'forecast_start_days' BEGIN SELECT RAISE(ABORT, 'no start days'); END`)

	if err := svc.SetForecastSettings(t.Context(), someForecastSettings); err == nil {
		t.Fatal("expected the failed write to fail the save")
	}

	if got := mustGetForecastSettings(t, svc); got != defaultForecastSettings {
		t.Fatalf("expected no Forecast Setting saved, got %+v", got)
	}
}

func TestStoredInvalidForecastSettingsAreReadAsStored(t *testing.T) {
	svc := newTestService(t)
	mustForecastSettings(t, svc, someForecastSettings)
	mustExec(t, svc, `UPDATE app_setting SET value = '50' WHERE key = 'forecast_samples'`)
	mustExec(t, svc, `UPDATE app_setting SET value = '*-13x-01' WHERE key = 'forecast_snapshot_interval'`)
	mustExec(t, svc, `UPDATE app_setting SET value = '28' WHERE key = 'forecast_start_days'`)

	// So the settings page shows them to be fixed.
	want := model.ForecastSettings{Samples: 50, Interval: "*-13x-01", StartDays: "28"}
	if got := mustGetForecastSettings(t, svc); got != want {
		t.Fatalf("expected the stored Forecast Settings %+v, got %+v", want, got)
	}
}

func TestStoredSamplesThatAreNotANumberFailGettingTheForecastSettings(t *testing.T) {
	svc := newTestService(t)
	mustForecastSettings(t, svc, someForecastSettings)
	mustExec(t, svc, `UPDATE app_setting SET value = 'many' WHERE key = 'forecast_samples'`)

	_, err := svc.GetForecastSettings(t.Context())
	if err == nil || !strings.Contains(err.Error(), "samples") || !strings.Contains(err.Error(), `"many"`) {
		t.Fatalf("expected an error naming the samples and their stored value, got %v", err)
	}
}

func TestAForecastRunWithStoredSamplesOutOfRangeFailsNamingThem(t *testing.T) {
	svc, timers := newForecastTestService(t, nil)
	checking := mustAccount(t, svc, "Checking")
	mustSnapshot(t, svc, checking.ID, mustParseDate("2025-01-01"), 10_000)
	mustForecastSettings(t, svc, someForecastSettings)
	mustExec(t, svc, `UPDATE app_setting SET value = '50' WHERE key = 'forecast_samples'`)
	addFutureSpecialDate(t, svc)
	startForecast(svc)

	sub := subscribe(t, svc)
	timers.fire()

	got := received(sub.Events)
	expectEventTypes(t, got, model.ForecastEventStarted, model.ForecastEventFailed)
	if msg := got[len(got)-1].Message; !strings.Contains(msg, "samples") || !strings.Contains(msg, "50") {
		t.Fatalf("expected the Failed message to name the samples and their stored value, got %q", msg)
	}
}

// The chart page's and the Transfers page's Forecasts don't use the Dashboard Forecast's samples or
// Forecast Interval, so stored invalid ones don't stop them.
func TestAForecastOnlyChecksTheStoredForecastStartDays(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	mustSnapshot(t, svc, checking.ID, mustParseDate("2025-03-28"), 1000)
	mustForecastSettings(t, svc, someForecastSettings)
	mustExec(t, svc, `UPDATE app_setting SET value = '50' WHERE key = 'forecast_samples'`)
	mustExec(t, svc, `UPDATE app_setting SET value = '*-13x-01' WHERE key = 'forecast_snapshot_interval'`)

	rec := forecastWith(t, svc, model.PredictionParams{Duration: ptr(date.Day), Samples: ptr[int64](1), Quantile: ptr(0.0), SnapshotInterval: "*-*-*", GroupBy: model.GroupByNone})

	expectPoints(t, rec.snapshots, point(checking.ID, "2025-03-28", 1000))
}
