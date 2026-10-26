package controller_test

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestTheChartPageShowsTheDefaultPredictionParameters(t *testing.T) {
	_, srv := newSettingsServer(t)

	page := getPage(t, srv, "/chart")

	for name, value := range map[string]string{
		"duration":          "1y",
		"samples":           "2000",
		"quantile":          "0.8",
		"snapshot_interval": "*-*-28",
	} {
		if input := inputNamed(page, name); !strings.Contains(input, `value="`+value+`"`) {
			t.Errorf("expected the %s field to show %s, got %s", name, value, input)
		}
	}
	if !strings.Contains(page, `value="type" selected`) {
		t.Errorf("expected the chart to be grouped by type")
	}
}

func TestAnInvalidForecastIntervalOnTheChartIsABadRequest(t *testing.T) {
	_, srv := newSettingsServer(t)

	for _, path := range []string{"/chart", "/chart/stream"} {
		t.Run(path, func(t *testing.T) {
			resp, err := srv.Client().Get(srv.URL + path + "?snapshot_interval=abc")
			if err != nil {
				t.Fatalf("get %s: %v", path, err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d: %s", resp.StatusCode, body)
			}
			if !strings.Contains(string(body), `forecast interval "abc" is not a date pattern`) {
				t.Errorf("expected the response to say why, got %q", body)
			}
		})
	}
}
