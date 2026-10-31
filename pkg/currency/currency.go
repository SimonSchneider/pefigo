package currency

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

type Currency struct {
	Code    string
	Name    string
	Format  string // fmt.Sprintf format string, e.g. "%.2f kr" or "$%.2f"
	printer *message.Printer
}

func (c Currency) FormatAmount(val float64) string {
	rounded := math.Round(val)
	return c.printer.Sprintf(c.Format, rounded)
}

var supported = []Currency{
	{"EUR", "Euro", "€%.0f", message.NewPrinter(language.English)},
	{"SEK", "Swedish Krona", "%.0f kr", message.NewPrinter(language.Swedish)},
	{"USD", "United States Dollar", "$%.0f", message.NewPrinter(language.English)},
	{"GBP", "British Pound", "£%.0f", message.NewPrinter(language.English)},
}

var byCode map[string]Currency

func init() {
	byCode = make(map[string]Currency, len(supported))
	for _, c := range supported {
		byCode[c.Code] = c
	}
}

func Get(code string) (Currency, bool) {
	c, ok := byCode[code]
	return c, ok
}

func SupportedCurrencies() []Currency {
	out := make([]Currency, len(supported))
	copy(out, supported)
	return out
}

type ratesResponse struct {
	Rates map[string]float64 `json:"rates"`
}

func ParseRatesResponse(data []byte) (map[string]float64, error) {
	var resp ratesResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("parsing rates response: %w", err)
	}
	return resp.Rates, nil
}

func ConvertAmount(amount, rate float64) float64 {
	return amount * rate
}

// Cache provides TTL-aware key-value storage for exchange rate caching. Get returns the value set for
// key no longer than maxAge ago, and an expired value is kept to be read with a longer one.
type Cache interface {
	Get(ctx context.Context, key string, maxAge time.Duration) (string, bool, error)
	Set(ctx context.Context, key string, value string) error
}

// anyAge is the maxAge that reads a cached value however old.
const anyAge = time.Duration(math.MaxInt64)

const defaultBaseURL = "https://api.frankfurter.dev/v1"
const defaultTTL = 24 * time.Hour

type Client struct {
	cache      Cache
	httpClient *http.Client
	baseURL    string
	ttl        time.Duration
}

type ClientOption func(*Client)

func WithBaseURL(u string) ClientOption {
	return func(c *Client) { c.baseURL = u }
}

func WithTTL(d time.Duration) ClientOption {
	return func(c *Client) { c.ttl = d }
}

func WithHTTPClient(hc *http.Client) ClientOption {
	return func(c *Client) { c.httpClient = hc }
}

func NewClient(cache Cache, opts ...ClientOption) *Client {
	c := &Client{
		cache:      cache,
		httpClient: http.DefaultClient,
		baseURL:    defaultBaseURL,
		ttl:        defaultTTL,
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// GetRate returns the latest Exchange Rate from one currency to another: a rate fetched within the TTL,
// a newly fetched one, or when that fetch fails the last one fetched, however old. It fails only when
// no rate from one to the other was ever fetched.
func (c *Client) GetRate(ctx context.Context, from, to string) (float64, error) {
	cacheKey := fmt.Sprintf("exchange_rate:%s:%s", from, to)

	if rate, ok, err := c.cachedRate(ctx, cacheKey, to, c.ttl); err != nil || ok {
		return rate, err
	}
	body, rate, fetchErr := c.fetchRate(ctx, from, to)
	if fetchErr == nil {
		if err := c.cache.Set(ctx, cacheKey, string(body)); err != nil {
			return 0, fmt.Errorf("cache set: %w", err)
		}
		return rate, nil
	}
	rate, ok, err := c.cachedRate(ctx, cacheKey, to, anyAge)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("no exchange rate from %s to %s was ever fetched: %w", from, to, fetchErr)
	}
	return rate, nil
}

func (c *Client) cachedRate(ctx context.Context, cacheKey, to string, maxAge time.Duration) (float64, bool, error) {
	raw, ok, err := c.cache.Get(ctx, cacheKey, maxAge)
	if err != nil {
		return 0, false, fmt.Errorf("cache get: %w", err)
	}
	if !ok {
		return 0, false, nil
	}
	rates, err := ParseRatesResponse([]byte(raw))
	if err != nil {
		return 0, false, err
	}
	rate, ok := rates[to]
	if !ok {
		return 0, false, fmt.Errorf("cached response missing rate for %s", to)
	}
	return rate, true, nil
}

// fetchRate fetches the latest rate from the rate server, returning the response it is in.
func (c *Client) fetchRate(ctx context.Context, from, to string) ([]byte, float64, error) {
	u, err := url.Parse(c.baseURL + "/latest")
	if err != nil {
		return nil, 0, fmt.Errorf("parsing URL: %w", err)
	}
	q := u.Query()
	q.Set("base", from)
	q.Set("symbols", to)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, 0, fmt.Errorf("creating request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("fetching exchange rate: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, 0, fmt.Errorf("exchange rate API returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, fmt.Errorf("reading response: %w", err)
	}

	rates, err := ParseRatesResponse(body)
	if err != nil {
		return nil, 0, err
	}
	rate, ok := rates[to]
	if !ok {
		return nil, 0, fmt.Errorf("response missing rate for %s", to)
	}
	return body, rate, nil
}
