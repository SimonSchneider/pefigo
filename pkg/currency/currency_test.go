package currency_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SimonSchneider/pefigo/pkg/currency"
)

func TestSupportedCurrencies(t *testing.T) {
	currencies := currency.SupportedCurrencies()

	codes := make(map[string]bool)
	for _, c := range currencies {
		if c.Code == "" {
			t.Error("currency code must not be empty")
		}
		if c.Name == "" {
			t.Errorf("currency %s name must not be empty", c.Code)
		}
		if c.Format == "" {
			t.Errorf("currency %s format must not be empty", c.Code)
		}
		if codes[c.Code] {
			t.Errorf("duplicate currency code: %s", c.Code)
		}
		codes[c.Code] = true
	}

	for _, code := range []string{"SEK", "EUR", "USD"} {
		if !codes[code] {
			t.Errorf("expected %s to be in supported currencies", code)
		}
	}
}

func TestCurrencyFormatAmount(t *testing.T) {
	tests := []struct {
		code string
		val  float64
		want string
	}{
		{"SEK", 1234.50, "1\u00a0235 kr"},
		{"SEK", -99.99, "−100 kr"},
		{"EUR", 1234.50, "€1,235"},
		{"USD", 1234.50, "$1,235"},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s/%v", tt.code, tt.val), func(t *testing.T) {
			c, ok := currency.Get(tt.code)
			if !ok {
				t.Fatalf("currency %s not found", tt.code)
			}
			got := c.FormatAmount(tt.val)
			if got != tt.want {
				t.Errorf("FormatAmount(%v) = %q, want %q", tt.val, got, tt.want)
			}
		})
	}
}

func TestGetUnknownCurrency(t *testing.T) {
	_, ok := currency.Get("XYZ")
	if ok {
		t.Error("expected Get(XYZ) to return false")
	}
}

func TestParseRatesResponse(t *testing.T) {
	body := []byte(`{"amount":1.0,"base":"SEK","date":"2026-03-25","rates":{"EUR":0.09284,"USD":0.10762}}`)

	rates, err := currency.ParseRatesResponse(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rates) != 2 {
		t.Fatalf("expected 2 rates, got %d", len(rates))
	}
	if rates["EUR"] != 0.09284 {
		t.Errorf("EUR rate = %v, want 0.09284", rates["EUR"])
	}
	if rates["USD"] != 0.10762 {
		t.Errorf("USD rate = %v, want 0.10762", rates["USD"])
	}
}

func TestParseRatesResponseInvalid(t *testing.T) {
	_, err := currency.ParseRatesResponse([]byte(`{invalid}`))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

// --- in-memory cache for testing ---

type cacheEntry struct {
	value     string
	createdAt time.Time
}

type memCache struct {
	entries map[string]cacheEntry
}

func newMemCache() *memCache {
	return &memCache{entries: make(map[string]cacheEntry)}
}

func (c *memCache) Get(_ context.Context, key string, maxAge time.Duration) (string, bool, error) {
	e, ok := c.entries[key]
	if !ok {
		return "", false, nil
	}
	if time.Since(e.createdAt) > maxAge {
		return "", false, nil
	}
	return e.value, true, nil
}

func (c *memCache) Set(_ context.Context, key string, value string) error {
	c.entries[key] = cacheEntry{value: value, createdAt: time.Now()}
	return nil
}

// --- Client tests ---

func TestClientGetRate(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Query().Get("base") != "EUR" {
			t.Errorf("expected base=EUR, got %s", r.URL.Query().Get("base"))
		}
		if r.URL.Query().Get("symbols") != "SEK" {
			t.Errorf("expected symbols=SEK, got %s", r.URL.Query().Get("symbols"))
		}
		fmt.Fprint(w, `{"amount":1.0,"base":"EUR","date":"2026-03-25","rates":{"SEK":11.47}}`)
	}))
	defer srv.Close()

	client := currency.NewClient(newMemCache(), currency.WithBaseURL(srv.URL))
	rate, err := client.GetRate(context.Background(), "EUR", "SEK")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rate != 11.47 {
		t.Errorf("rate = %v, want 11.47", rate)
	}
	if calls.Load() != 1 {
		t.Errorf("expected 1 HTTP call, got %d", calls.Load())
	}
}

func TestClientGetRateCached(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		fmt.Fprint(w, `{"amount":1.0,"base":"EUR","date":"2026-03-25","rates":{"SEK":11.47}}`)
	}))
	defer srv.Close()

	client := currency.NewClient(newMemCache(), currency.WithBaseURL(srv.URL))

	_, err := client.GetRate(context.Background(), "EUR", "SEK")
	if err != nil {
		t.Fatalf("first call: %v", err)
	}

	rate, err := client.GetRate(context.Background(), "EUR", "SEK")
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if rate != 11.47 {
		t.Errorf("rate = %v, want 11.47", rate)
	}
	if calls.Load() != 1 {
		t.Errorf("expected 1 HTTP call (cached), got %d", calls.Load())
	}
}

func TestClientGetRateExpired(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		fmt.Fprint(w, `{"amount":1.0,"base":"EUR","date":"2026-03-25","rates":{"SEK":11.47}}`)
	}))
	defer srv.Close()

	cache := newMemCache()
	ttl := 50 * time.Millisecond
	client := currency.NewClient(cache, currency.WithBaseURL(srv.URL), currency.WithTTL(ttl))

	_, err := client.GetRate(context.Background(), "EUR", "SEK")
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("expected 1 call after first fetch, got %d", calls.Load())
	}

	time.Sleep(ttl + 10*time.Millisecond)

	_, err = client.GetRate(context.Background(), "EUR", "SEK")
	if err != nil {
		t.Fatalf("second call after expiry: %v", err)
	}
	if calls.Load() != 2 {
		t.Errorf("expected 2 HTTP calls (cache expired), got %d", calls.Load())
	}
}

func TestClientGetRateWithoutAnyRateFetchedFailsNamingThePair(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	client := currency.NewClient(newMemCache(), currency.WithBaseURL(srv.URL))
	_, err := client.GetRate(context.Background(), "EUR", "SEK")
	if err == nil {
		t.Fatal("expected error for server error response")
	}
	if !strings.Contains(err.Error(), "EUR to SEK") {
		t.Errorf("expected the error to name the currency pair, got %q", err)
	}
}

func TestClientGetRateFallsBackToTheLastRateFetchedWhenAFetchFails(t *testing.T) {
	var fail atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		fmt.Fprint(w, `{"amount":1.0,"base":"EUR","date":"2026-03-25","rates":{"SEK":11.47}}`)
	}))
	defer srv.Close()

	cache := newMemCache()
	client := currency.NewClient(cache, currency.WithBaseURL(srv.URL))
	if _, err := client.GetRate(context.Background(), "EUR", "SEK"); err != nil {
		t.Fatalf("first call: %v", err)
	}
	// The rate was fetched a year ago: long expired.
	entry := cache.entries["exchange_rate:EUR:SEK"]
	entry.createdAt = time.Now().AddDate(-1, 0, 0)
	cache.entries["exchange_rate:EUR:SEK"] = entry
	fail.Store(true)

	rate, err := client.GetRate(context.Background(), "EUR", "SEK")
	if err != nil {
		t.Fatalf("expected the year-old rate when the fetch fails, got %v", err)
	}
	if rate != 11.47 {
		t.Errorf("rate = %v, want 11.47", rate)
	}
}

func TestClientGetRateDoesNotKeepAResponseWithoutTheRate(t *testing.T) {
	var missing atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if missing.Load() {
			fmt.Fprint(w, `{"amount":1.0,"base":"EUR","date":"2026-03-26","rates":{}}`)
			return
		}
		fmt.Fprint(w, `{"amount":1.0,"base":"EUR","date":"2026-03-25","rates":{"SEK":11.47}}`)
	}))
	defer srv.Close()

	client := currency.NewClient(newMemCache(), currency.WithBaseURL(srv.URL), currency.WithTTL(0))
	if _, err := client.GetRate(context.Background(), "EUR", "SEK"); err != nil {
		t.Fatalf("first call: %v", err)
	}
	missing.Store(true)
	for range 2 {
		rate, err := client.GetRate(context.Background(), "EUR", "SEK")
		if err != nil || rate != 11.47 {
			t.Fatalf("expected the last rate fetched, 11.47, got %v (%v)", rate, err)
		}
	}
}

func TestConvertAmount(t *testing.T) {
	tests := []struct {
		name   string
		amount float64
		rate   float64
		want   float64
	}{
		{"100 EUR to SEK at 10.76", 100, 10.76, 1076},
		{"zero amount", 0, 10.76, 0},
		{"rate of 1 (same currency)", 500, 1.0, 500},
		{"negative amount", -100, 10.76, -1076},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := currency.ConvertAmount(tt.amount, tt.rate)
			if got != tt.want {
				t.Errorf("ConvertAmount(%v, %v) = %v, want %v", tt.amount, tt.rate, got, tt.want)
			}
		})
	}
}
