package controller_test

import (
	"strings"
	"testing"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

// statValue is the value of the stat card titled title on page, or "" without one.
func statValue(page, title string) string {
	i := strings.Index(page, `<div class="stat-title">`+title+`</div>`)
	if i < 0 {
		return ""
	}
	const valueTag = `<div class="stat-value">`
	start := strings.Index(page[i:], valueTag)
	if start < 0 {
		return ""
	}
	value := page[i+start+len(valueTag):]
	return value[:strings.Index(value, "</div>")]
}

func TestTheDashboardAndTheAccountsPageShowTheSameNetWorth(t *testing.T) {
	svc, srv := newSettingsServer(t)
	ctx := t.Context()
	recorded, _ := date.ParseDate("2025-01-01")
	for name, balance := range map[string]float64{"Savings": 100000, "Loan": -40000} {
		acc, err := svc.UpsertAccount(ctx, model.AccountInput{Name: name})
		if err != nil {
			t.Fatalf("create account %s: %v", name, err)
		}
		if _, err := svc.UpsertAccountSnapshot(ctx, acc.ID, model.AccountSnapshotInput{Date: recorded, Balance: uncertain.NewFixed(balance)}); err != nil {
			t.Fatalf("record the balance of %s: %v", name, err)
		}
	}

	for _, path := range []string{"/", "/accounts"} {
		page := getPage(t, srv, path)
		for title, want := range map[string]string{"Net Worth": "60,000", "Assets": "100,000", "Liabilities": "40,000"} {
			if got := statValue(page, title); got != want {
				t.Errorf("%s: expected %s %s, got %q", path, title, want, got)
			}
		}
	}
}
