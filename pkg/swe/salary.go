package swe

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"
)

type GrossSalaryInput struct {
	Kommun       string
	Forsamling   string
	Year         string
	ChurchMember bool
}

type GrossSalaryInputWithAmount struct {
	GrossSalaryInput
	GrossMonthly float64
}

type NetSalaryResult struct {
	GrossMonthly float64
	Tax          float64
	NetMonthly   float64
}

// NetSalaryCalculator loads the Tax Table of the input's kommun, församling and year, no later than
// this year, or of the year before when that year has none, and returns the net of every monthly gross.
func (c *Client) NetSalaryCalculator(ctx context.Context, input GrossSalaryInput) (func(grossMonthly float64) NetSalaryResult, error) {
	year, err := strconv.Atoi(input.Year)
	if err != nil {
		return nil, fmt.Errorf("parsing year %q: %w", input.Year, err)
	}

	effectiveYear := min(year, time.Now().Year())
	var errs []error
	for _, tryYear := range []string{strconv.Itoa(effectiveYear), strconv.Itoa(effectiveYear - 1)} {
		taxLookup, err := c.taxLookupFor(ctx, input, tryYear)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		return func(grossMonthly float64) NetSalaryResult {
			tax := taxLookup(grossMonthly)
			return NetSalaryResult{
				GrossMonthly: grossMonthly,
				Tax:          tax,
				NetMonthly:   grossMonthly - tax,
			}
		}, nil
	}
	return nil, fmt.Errorf("no tax data for %s, %s in %d or %d: %w", input.Kommun, input.Forsamling, effectiveYear, effectiveYear-1, errors.Join(errs...))
}

func (c *Client) taxLookupFor(ctx context.Context, input GrossSalaryInput, year string) (func(float64) float64, error) {
	tableNumber, err := c.GetTaxTableNumber(ctx, input.Kommun, input.Forsamling, year, input.ChurchMember)
	if err != nil {
		return nil, err
	}
	return c.NewTaxLookup(ctx, tableNumber, year)
}

// CalculateNetSalary computes net salary from gross input using cached tax table lookups.
func (c *Client) CalculateNetSalary(ctx context.Context, input GrossSalaryInputWithAmount) (NetSalaryResult, error) {
	calculator, err := c.NetSalaryCalculator(ctx, input.GrossSalaryInput)
	if err != nil {
		return NetSalaryResult{}, fmt.Errorf("creating net salary calculator: %w", err)
	}
	return calculator(input.GrossMonthly), nil
}
