package swe_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/SimonSchneider/pefigo/pkg/swe"
)

type fakeCache struct {
	data map[string]string
}

func newFakeCache() *fakeCache {
	return &fakeCache{data: make(map[string]string)}
}

func (c *fakeCache) Get(_ context.Context, key string) (string, bool, error) {
	v, ok := c.data[key]
	return v, ok, nil
}

func (c *fakeCache) Set(_ context.Context, key string, value string) error {
	c.data[key] = value
	return nil
}

// rowStoreServer serves rows as Skatteverket's row store does: only the rows whose fields equal every
// query parameter but paging.
func rowStoreServer(t *testing.T, rows []map[string]string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var results []map[string]string
	rows:
		for _, row := range rows {
			for field, values := range r.URL.Query() {
				if !strings.HasPrefix(field, "_") && row[field] != values[0] {
					continue rows
				}
			}
			results = append(results, row)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(swe.RowStoreResponse{ResultCount: len(results), Results: results})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func taxRateServer(t *testing.T) *httptest.Server {
	return rowStoreServer(t, []map[string]string{
		{
			"kommun":                   "STOCKHOLM",
			"församling":               "ADOLF FREDRIKS FÖRSAMLING",
			"summa, exkl. kyrkoavgift": "30.67",
			"summa, inkl. kyrkoavgift": "31.85",
			"år":                       "2025",
		},
		{
			"kommun":                   "STOCKHOLM",
			"församling":               "KATARINA FÖRSAMLING",
			"summa, exkl. kyrkoavgift": "30.67",
			"summa, inkl. kyrkoavgift": "31.72",
			"år":                       "2025",
		},
	})
}

// taxTable31For2025 are the real rows of Skatteverket's Tax Table 31 for 2025.
func taxTable31For2025(t *testing.T) []map[string]string {
	t.Helper()
	raw, err := os.ReadFile("testdata/tax_table_31_2025.json")
	if err != nil {
		t.Fatalf("read tax table fixture: %v", err)
	}
	var rows []map[string]string
	if err := json.Unmarshal(raw, &rows); err != nil {
		t.Fatalf("decode tax table fixture: %v", err)
	}
	return rows
}

func TestGetTaxTableNumber(t *testing.T) {
	client := swe.NewClient(newFakeCache(), swe.WithTaxRateURL(taxRateServer(t).URL))

	ctx := context.Background()

	t.Run("excl church", func(t *testing.T) {
		got, err := client.GetTaxTableNumber(ctx, "STOCKHOLM", "ADOLF FREDRIKS FÖRSAMLING", "2025", false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != 31 {
			t.Errorf("got table %d, want 31", got)
		}
	})

	t.Run("incl church", func(t *testing.T) {
		got, err := client.GetTaxTableNumber(ctx, "STOCKHOLM", "ADOLF FREDRIKS FÖRSAMLING", "2025", true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != 32 {
			t.Errorf("got table %d, want 32", got)
		}
	})
}

func TestTaxTable31For2025TaxesEveryIncome(t *testing.T) {
	client := swe.NewClient(newFakeCache(), swe.WithTaxTableURL(rowStoreServer(t, taxTable31For2025(t)).URL))
	lookup, err := client.NewTaxLookup(context.Background(), 31, "2025")
	if err != nil {
		t.Fatalf("load tax table: %v", err)
	}

	for _, tc := range []struct {
		name   string
		income float64
		want   float64
	}{
		{"the table's limit is taxed its amount", 80000, 25985},
		{"an income is taxed by the row of its whole kronor", 80000.6, 25985},
		{"above the limit, the row's percentage of the whole income", 100000, 36000},
		{"far above the limit", 150000, 61500},
		{"above the last row's start, which has no upper limit", 2000000, 1000000},
		{"a low income", 2050, 150},
		{"no income", 0, 0},
		{"below the first row", 0.5, 0},
		{"a negative income", -1000, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := lookup(tc.income); got != tc.want {
				t.Errorf("tax on %v = %v, want %v", tc.income, got, tc.want)
			}
		})
	}
}

func TestLookupTax(t *testing.T) {
	client := swe.NewClient(newFakeCache(), swe.WithTaxTableURL(rowStoreServer(t, taxTable31For2025(t)).URL))

	got, err := client.LookupTax(context.Background(), 31, "2025", 100000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 36000 {
		t.Errorf("got tax %v, want 36000", got)
	}
}

func TestATaxTableWithoutRowsAboveItsAmountsDoesNotLoad(t *testing.T) {
	client := swe.NewClient(newFakeCache(), swe.WithTaxTableURL(rowStoreServer(t, []map[string]string{
		{"inkomst fr.o.m.": "1", "inkomst t.o.m.": "24999", "kolumn 1": "0", "tabellnr": "31", "år": "2025", "antal dgr": "30B"},
		{"inkomst fr.o.m.": "25000", "inkomst t.o.m.": "99999", "kolumn 1": "7500", "tabellnr": "31", "år": "2025", "antal dgr": "30B"},
	}).URL))

	if _, err := client.NewTaxLookup(context.Background(), 31, "2025"); err == nil {
		t.Fatal("expected a Tax Table that taxes no income above 99 999 not to load")
	}
}

func TestAMissingTaxTableDoesNotLoad(t *testing.T) {
	client := swe.NewClient(newFakeCache(), swe.WithTaxTableURL(rowStoreServer(t, taxTable31For2025(t)).URL))

	if _, err := client.NewTaxLookup(context.Background(), 31, "2020"); err == nil {
		t.Fatal("expected no Tax Table 31 for 2020")
	}
}

func TestListKommuner(t *testing.T) {
	client := swe.NewClient(newFakeCache(), swe.WithTaxRateURL(taxRateServer(t).URL))

	ctx := context.Background()

	kommuner, err := client.ListKommuner(ctx, "2025")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(kommuner) != 1 {
		t.Fatalf("expected 1 kommun, got %d", len(kommuner))
	}
	if kommuner[0] != "STOCKHOLM" {
		t.Errorf("expected STOCKHOLM, got %s", kommuner[0])
	}
}

func TestListForsamlingar(t *testing.T) {
	client := swe.NewClient(newFakeCache(), swe.WithTaxRateURL(taxRateServer(t).URL))

	ctx := context.Background()

	forsamlingar, err := client.ListForsamlingar(ctx, "STOCKHOLM", "2025")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(forsamlingar) != 2 {
		t.Fatalf("expected 2 forsamlingar, got %d", len(forsamlingar))
	}
	if forsamlingar[0] != "ADOLF FREDRIKS FÖRSAMLING" {
		t.Errorf("expected ADOLF FREDRIKS FÖRSAMLING first, got %s", forsamlingar[0])
	}
	if forsamlingar[1] != "KATARINA FÖRSAMLING" {
		t.Errorf("expected KATARINA FÖRSAMLING second, got %s", forsamlingar[1])
	}
}

func TestCachingBehavior(t *testing.T) {
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		resp := swe.RowStoreResponse{
			ResultCount: 1,
			Results: []map[string]string{
				{
					"kommun":                   "STOCKHOLM",
					"församling":               "TEST",
					"summa, exkl. kyrkoavgift": "30.67",
					"summa, inkl. kyrkoavgift": "31.85",
					"år":                       "2025",
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	cache := newFakeCache()
	client := swe.NewClient(cache, swe.WithTaxRateURL(srv.URL))
	ctx := context.Background()

	client.GetTaxTableNumber(ctx, "STOCKHOLM", "TEST", "2025", false)
	client.GetTaxTableNumber(ctx, "STOCKHOLM", "TEST", "2025", false)

	if callCount != 1 {
		t.Errorf("expected 1 API call (second should hit cache), got %d", callCount)
	}
}
