package controller_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/SimonSchneider/pefigo"
	"github.com/SimonSchneider/pefigo/internal/controller"
	"github.com/SimonSchneider/pefigo/internal/model"
	"github.com/ncruces/go-sqlite3/vfs/memdb"
)

func newSettingsServer(t *testing.T) (*model.Service, *httptest.Server) {
	t.Helper()
	svc, err := model.Open(t.Context(), pefigo.StaticEmbeddedFS, "static/migrations", memdb.TestDB(t))
	if err != nil {
		t.Fatalf("failed to create test db: %v", err)
	}
	t.Cleanup(func() { svc.DB().Close() })
	srv := httptest.NewServer(controller.NewHandler(svc, fstest.MapFS{}))
	t.Cleanup(srv.Close)
	return svc, srv
}

func TestForecastSettingsHaveNoConfidenceInterval(t *testing.T) {
	_, srv := newSettingsServer(t)

	resp, err := srv.Client().Get(srv.URL + "/settings?tab=forecast")
	if err != nil {
		t.Fatalf("get settings: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read settings: %v", err)
	}
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `name="samples"`) {
		t.Fatalf("expected the Forecast settings, got %d: %s", resp.StatusCode, body)
	}
	if strings.Contains(string(body), "Confidence Interval") || strings.Contains(string(body), `name="confidence"`) {
		t.Fatal("expected no Confidence Interval control on the Forecast settings")
	}
}

func TestSavingTheForecastSettings(t *testing.T) {
	svc, srv := newSettingsServer(t)
	client := srv.Client()
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

	resp, err := client.PostForm(srv.URL+"/settings/forecast", url.Values{
		"samples":           {"500"},
		"snapshot_interval": {"*-*-01"},
		"start_days":        {"*-*-28"},
	})
	if err != nil {
		t.Fatalf("save forecast settings: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther && resp.StatusCode != http.StatusFound {
		t.Fatalf("expected a redirect, got %d", resp.StatusCode)
	}

	want := model.ForecastSettings{Samples: 500, Interval: "*-*-01", StartDays: "*-*-28"}
	if settings, err := svc.GetForecastSettings(t.Context()); err != nil || settings != want {
		t.Fatalf("expected the Forecast Settings %+v, got %+v (%v)", want, settings, err)
	}
}

func TestSavingBlankForecastSettingsSavesTheirDefaults(t *testing.T) {
	svc, srv := newSettingsServer(t)
	samples := int64(500)
	if err := svc.SetForecastSettings(t.Context(), model.ForecastSettingsInput{Samples: &samples, Interval: "*-*-01", StartDays: "*-*-28"}); err != nil {
		t.Fatalf("set forecast settings: %v", err)
	}

	postForm(t, srv, "/settings/forecast", url.Values{"samples": {""}, "snapshot_interval": {""}, "start_days": {""}})

	want := model.ForecastSettings{Samples: 10000, Interval: "*-01-01", StartDays: "*-*-*"}
	if settings, err := svc.GetForecastSettings(t.Context()); err != nil || settings != want {
		t.Fatalf("expected the default Forecast Settings %+v, got %+v (%v)", want, settings, err)
	}
}

func TestSavingForecastSettingsWithOneInvalidSettingIsABadRequest(t *testing.T) {
	for _, tc := range []struct{ field, value, named string }{
		{"samples", "50", "samples 50"},
		{"snapshot_interval", "28", `Forecast Interval "28"`},
		{"start_days", "*-*", `Forecast Start Days "*-*"`},
	} {
		t.Run(tc.field, func(t *testing.T) {
			svc, srv := newSettingsServer(t)
			form := url.Values{"samples": {"500"}, "snapshot_interval": {"*-*-01"}, "start_days": {"*-*-28"}}
			form.Set(tc.field, tc.value)

			resp, err := srv.Client().PostForm(srv.URL+"/settings/forecast", form)
			if err != nil {
				t.Fatalf("save forecast settings: %v", err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read response: %v", err)
			}
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d: %s", resp.StatusCode, body)
			}
			if !strings.Contains(string(body), tc.named) {
				t.Errorf("expected the response to name the %s, got %q", tc.named, body)
			}
			want := model.ForecastSettings{Samples: 10000, Interval: "*-01-01", StartDays: "*-*-*"}
			if settings, err := svc.GetForecastSettings(t.Context()); err != nil || settings != want {
				t.Fatalf("expected no Forecast Setting saved, got %+v (%v)", settings, err)
			}
		})
	}
}

func TestTheForecastSettingsShowAStoredInvalidForecastIntervalToBeFixed(t *testing.T) {
	svc, srv := newSettingsServer(t)
	if _, err := svc.DB().ExecContext(t.Context(), `INSERT INTO app_setting (key, value) VALUES ('forecast_snapshot_interval', '*-13x-01')`); err != nil {
		t.Fatalf("store an invalid Forecast Interval: %v", err)
	}

	page := getPage(t, srv, "/settings?tab=forecast")

	if input := inputNamed(page, "snapshot_interval"); !strings.Contains(input, `value="*-13x-01"`) {
		t.Fatalf("expected the Forecast Interval field to show the stored *-13x-01, got %s", input)
	}
}

func TestTheForecastIntervalIsLabelledForecastInterval(t *testing.T) {
	_, srv := newSettingsServer(t)

	for _, path := range []string{"/chart", "/settings?tab=forecast"} {
		resp, err := srv.Client().Get(srv.URL + path)
		if err != nil {
			t.Fatalf("get %s: %v", path, err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `name="snapshot_interval"`) {
			t.Fatalf("expected %s to have the Forecast Interval setting, got %d: %s", path, resp.StatusCode, body)
		}
		if !strings.Contains(string(body), "Forecast interval") {
			t.Errorf("expected %s to label the setting \"Forecast interval\"", path)
		}
		for _, old := range []string{"Snapshot Interval", "Snapshot Frequency"} {
			if strings.Contains(string(body), old) {
				t.Errorf("expected %s not to label the setting %q", path, old)
			}
		}
	}
}

func TestTheForecastSettingsShowTheForecastStartDaysAsForecastStartsOn(t *testing.T) {
	svc, srv := newSettingsServer(t)
	if err := svc.SetForecastSettings(t.Context(), model.ForecastSettingsInput{StartDays: "*-*-28"}); err != nil {
		t.Fatalf("set forecast settings: %v", err)
	}

	resp, err := srv.Client().Get(srv.URL + "/settings?tab=forecast")
	if err != nil {
		t.Fatalf("get settings: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read settings: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected the Forecast settings, got %d: %s", resp.StatusCode, body)
	}
	for _, want := range []string{"Forecast starts on", `name="start_days"`, `value="*-*-28"`} {
		if !strings.Contains(string(body), want) {
			t.Errorf("expected the Forecast settings to contain %s", want)
		}
	}
}

func TestSavingAnUnsupportedDefaultCurrencyIsABadRequest(t *testing.T) {
	svc, srv := newSettingsServer(t)

	resp, err := srv.Client().PostForm(srv.URL+"/settings/currency", url.Values{"currency": {"XYZ"}})
	if err != nil {
		t.Fatalf("save currency settings: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), `"XYZ"`) {
		t.Errorf("expected the response to name the currency, got %q", body)
	}
	if code, err := svc.GetDefaultCurrency(t.Context()); err != nil || code != "SEK" {
		t.Fatalf("expected the default currency to stay SEK, got %q (%v)", code, err)
	}
}
