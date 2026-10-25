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
	})
	if err != nil {
		t.Fatalf("save forecast settings: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther && resp.StatusCode != http.StatusFound {
		t.Fatalf("expected a redirect, got %d", resp.StatusCode)
	}

	if samples, err := svc.GetForecastSamples(t.Context()); err != nil || samples != 500 {
		t.Fatalf("expected 500 samples, got %d (%v)", samples, err)
	}
	if interval, err := svc.GetForecastSnapshotInterval(t.Context()); err != nil || interval != "*-*-01" {
		t.Fatalf("expected the snapshot interval *-*-01, got %q (%v)", interval, err)
	}
}
