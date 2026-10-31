package swe

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
)

const (
	defaultTaxRateURL  = "https://skatteverket.entryscape.net/rowstore/dataset/c67b320b-ffee-4876-b073-dd9236cd2a99"
	defaultTaxTableURL = "https://skatteverket.entryscape.net/rowstore/dataset/88320397-5c32-4c16-ae79-d36d95b17b95"
)

type RowStoreResponse struct {
	ResultCount int                 `json:"resultCount"`
	Results     []map[string]string `json:"results"`
	Next        string              `json:"next,omitempty"`
}

type Client struct {
	cache       Cache
	httpClient  *http.Client
	taxRateURL  string
	taxTableURL string
	taxData     TaxData
}

type ClientOption func(*Client)

func WithTaxRateURL(u string) ClientOption {
	return func(c *Client) { c.taxRateURL = u }
}

func WithTaxTableURL(u string) ClientOption {
	return func(c *Client) { c.taxTableURL = u }
}

func WithHTTPClient(hc *http.Client) ClientOption {
	return func(c *Client) { c.httpClient = hc }
}

// WithTaxData has the Client get its tax data from d instead of from Skatteverket.
func WithTaxData(d TaxData) ClientOption {
	return func(c *Client) { c.taxData = d }
}

func NewClient(cache Cache, opts ...ClientOption) *Client {
	c := &Client{
		cache:       cache,
		httpClient:  http.DefaultClient,
		taxRateURL:  defaultTaxRateURL,
		taxTableURL: defaultTaxTableURL,
	}
	for _, o := range opts {
		o(c)
	}
	if c.taxData == nil {
		c.taxData = skatteverketTaxData{c}
	}
	return c
}

func (c *Client) fetchAllPages(ctx context.Context, baseURL string, params url.Values) ([]map[string]string, error) {
	var allResults []map[string]string
	params.Set("_limit", "500")
	offset := 0

	for {
		params.Set("_offset", strconv.Itoa(offset))
		reqURL := baseURL + "?" + params.Encode()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			return nil, fmt.Errorf("creating request: %w", err)
		}
		req.Header.Set("Accept", "application/json")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("fetching %s: %w", reqURL, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("unexpected status %d from %s", resp.StatusCode, reqURL)
		}

		var result RowStoreResponse
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return nil, fmt.Errorf("decoding response: %w", err)
		}

		allResults = append(allResults, result.Results...)

		if len(allResults) >= result.ResultCount || result.Next == "" {
			break
		}
		offset += 500
	}

	return allResults, nil
}

func (c *Client) fetchCached(ctx context.Context, cacheKey, baseURL string, params url.Values) ([]map[string]string, error) {
	if raw, ok, err := c.cache.Get(ctx, cacheKey); err != nil {
		return nil, fmt.Errorf("cache get: %w", err)
	} else if ok {
		var results []map[string]string
		if err := json.Unmarshal([]byte(raw), &results); err != nil {
			return nil, fmt.Errorf("decoding cached data: %w", err)
		}
		return results, nil
	}

	results, err := c.fetchAllPages(ctx, baseURL, params)
	if err != nil {
		return nil, err
	}

	raw, err := json.Marshal(results)
	if err != nil {
		return nil, fmt.Errorf("encoding for cache: %w", err)
	}
	if err := c.cache.Set(ctx, cacheKey, string(raw)); err != nil {
		return nil, fmt.Errorf("cache set: %w", err)
	}

	return results, nil
}

// GetTaxTableNumber resolves the tax table number for a kommun/forsamling/year.
func (c *Client) GetTaxTableNumber(ctx context.Context, kommun, forsamling, year string, churchMember bool) (int, error) {
	return c.taxData.TaxTableNumber(ctx, kommun, forsamling, year, churchMember)
}

// skatteverketTaxData is the Client's TaxData unless WithTaxData replaces it: fetched from
// Skatteverket's open data and cached.
type skatteverketTaxData struct {
	c *Client
}

// TaxTableNumber fetches the total tax rate of the kommun and församling and rounds it to the nearest
// integer.
func (d skatteverketTaxData) TaxTableNumber(ctx context.Context, kommun, forsamling, year string, churchMember bool) (int, error) {
	cacheKey := fmt.Sprintf("tax_rate:%s:%s:%s", kommun, forsamling, year)
	params := url.Values{}
	params.Set("kommun", kommun)
	params.Set("församling", forsamling)
	params.Set("år", year)

	results, err := d.c.fetchCached(ctx, cacheKey, d.c.taxRateURL, params)
	if err != nil {
		return 0, fmt.Errorf("fetching tax rates: %w", err)
	}
	if len(results) == 0 {
		return 0, fmt.Errorf("no tax rate found for %s/%s/%s", kommun, forsamling, year)
	}

	row := results[0]
	field := "summa, exkl. kyrkoavgift"
	if churchMember {
		field = "summa, inkl. kyrkoavgift"
	}

	rateStr, ok := row[field]
	if !ok {
		return 0, fmt.Errorf("field %q not found in response", field)
	}

	rate, err := strconv.ParseFloat(strings.ReplaceAll(rateStr, ",", "."), 64)
	if err != nil {
		return 0, fmt.Errorf("parsing rate %q: %w", rateStr, err)
	}

	return int(math.Round(rate)), nil
}

// TaxTable fetches the table's monthly rows: its amounts ("30B") and its percentages ("30%").
func (d skatteverketTaxData) TaxTable(ctx context.Context, tableNumber int, year string) ([]TaxTableRow, error) {
	tabellnr := strconv.Itoa(tableNumber)
	var rows []TaxTableRow
	for _, days := range []string{"30B", "30%"} {
		params := url.Values{}
		params.Set("tabellnr", tabellnr)
		params.Set("år", year)
		params.Set("antal dgr", days)
		results, err := d.c.fetchCached(ctx, fmt.Sprintf("tax_table:%s:%s:%s", tabellnr, year, days), d.c.taxTableURL, params)
		if err != nil {
			return nil, fmt.Errorf("fetching the %s rows: %w", days, err)
		}
		for _, result := range results {
			row, err := taxTableRowFromSkatteverket(result)
			if err != nil {
				return nil, err
			}
			rows = append(rows, row)
		}
	}
	return rows, nil
}

func taxTableRowFromSkatteverket(r map[string]string) (TaxTableRow, error) {
	row := TaxTableRow{Percent: r["antal dgr"] == "30%"}
	var err error
	if row.From, err = strconv.ParseFloat(r["inkomst fr.o.m."], 64); err != nil {
		return TaxTableRow{}, fmt.Errorf("parsing the income from %q: %w", r["inkomst fr.o.m."], err)
	}
	if to := r["inkomst t.o.m."]; to != "" {
		if row.To, err = strconv.ParseFloat(to, 64); err != nil {
			return TaxTableRow{}, fmt.Errorf("parsing the income to %q: %w", to, err)
		}
	}
	if row.Tax, err = strconv.ParseFloat(r["kolumn 1"], 64); err != nil {
		return TaxTableRow{}, fmt.Errorf("parsing the tax %q of income %s: %w", r["kolumn 1"], r["inkomst fr.o.m."], err)
	}
	return row, nil
}

// NewTaxLookup loads the Tax Table tableNumber of year and returns the monthly tax on every income: up
// to the table's limit the amount of the income's row, above it the row's percentage of the whole
// income, and 0 for an income of 0 or less. It fails when there is no such table, or when its rows end
// at an income.
func (c *Client) NewTaxLookup(ctx context.Context, tableNumber int, year string) (func(monthlyIncome float64) float64, error) {
	rows, err := c.taxData.TaxTable(ctx, tableNumber, year)
	if err != nil {
		return nil, fmt.Errorf("fetching tax table %d for %s: %w", tableNumber, year, err)
	}
	lookup, err := taxLookup(rows)
	if err != nil {
		return nil, fmt.Errorf("tax table %d for %s: %w", tableNumber, year, err)
	}
	return lookup, nil
}

func taxLookup(rows []TaxTableRow) (func(monthlyIncome float64) float64, error) {
	if len(rows) == 0 {
		return nil, errors.New("no rows found")
	}
	rows = slices.Clone(rows)
	slices.SortFunc(rows, func(a, b TaxTableRow) int { return cmp.Compare(a.From, b.From) })
	if last := rows[len(rows)-1]; last.To != 0 {
		return nil, fmt.Errorf("no row for incomes above %.0f", last.To)
	}
	return func(monthlyIncome float64) float64 {
		// The income's row is the last one starting at or below it.
		i := sort.Search(len(rows), func(i int) bool { return rows[i].From > monthlyIncome }) - 1
		if monthlyIncome <= 0 || i < 0 {
			return 0
		}
		if rows[i].Percent {
			return monthlyIncome * rows[i].Tax / 100
		}
		return rows[i].Tax
	}, nil
}

// LookupTax finds the tax for a given table number, year and monthly income.
func (c *Client) LookupTax(ctx context.Context, tableNumber int, year string, monthlyIncome float64) (float64, error) {
	lookup, err := c.NewTaxLookup(ctx, tableNumber, year)
	if err != nil {
		return 0, err
	}
	return lookup(monthlyIncome), nil
}

// ListKommuner returns all distinct kommun names for a given year, sorted alphabetically.
func (c *Client) ListKommuner(ctx context.Context, year string) ([]string, error) {
	cacheKey := fmt.Sprintf("kommuner:%s", year)
	params := url.Values{}
	params.Set("år", year)

	results, err := c.fetchCached(ctx, cacheKey, c.taxRateURL, params)
	if err != nil {
		return nil, fmt.Errorf("fetching kommuner: %w", err)
	}

	seen := make(map[string]struct{})
	var kommuner []string
	for _, row := range results {
		k := row["kommun"]
		if k == "" {
			continue
		}
		if _, ok := seen[k]; !ok {
			seen[k] = struct{}{}
			kommuner = append(kommuner, k)
		}
	}
	sort.Strings(kommuner)
	return kommuner, nil
}

// ListForsamlingar returns all distinct forsamling names for a given kommun and year, sorted alphabetically.
func (c *Client) ListForsamlingar(ctx context.Context, kommun, year string) ([]string, error) {
	cacheKey := fmt.Sprintf("forsamlingar:%s:%s", kommun, year)
	params := url.Values{}
	params.Set("kommun", kommun)
	params.Set("år", year)

	results, err := c.fetchCached(ctx, cacheKey, c.taxRateURL, params)
	if err != nil {
		return nil, fmt.Errorf("fetching forsamlingar: %w", err)
	}

	seen := make(map[string]struct{})
	var forsamlingar []string
	for _, row := range results {
		f := row["församling"]
		if f == "" {
			continue
		}
		if _, ok := seen[f]; !ok {
			seen[f] = struct{}{}
			forsamlingar = append(forsamlingar, f)
		}
	}
	sort.Strings(forsamlingar)
	return forsamlingar, nil
}
