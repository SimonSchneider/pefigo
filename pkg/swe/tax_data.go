package swe

import (
	"context"
	"fmt"
)

// TaxData supplies Skatteverket's tax data: the number of the Tax Table a kommun, församling, year and
// church membership are taxed by, and each Tax Table's rows. The Client fetches it from Skatteverket
// unless WithTaxData gives it another.
type TaxData interface {
	TaxTableNumber(ctx context.Context, kommun, forsamling, year string, churchMember bool) (int, error)
	// TaxTable returns no rows when there is no Tax Table tableNumber for year.
	TaxTable(ctx context.Context, tableNumber int, year string) ([]TaxTableRow, error)
}

// TaxTableRow is one row of a Tax Table: the monthly tax, in column 1, on the incomes from From up to
// and including To. The tax is in kronor, or for a Percent row a percentage of the whole income. The
// last row has no To (0).
type TaxTableRow struct {
	From    float64
	To      float64
	Percent bool
	Tax     float64
}

// TaxTableNumberKey is what a Tax Table number is looked up by.
type TaxTableNumberKey struct {
	Kommun       string
	Forsamling   string
	Year         string
	ChurchMember bool
}

// TaxTableKey identifies a Tax Table: its number and year.
type TaxTableKey struct {
	Number int
	Year   string
}

// MemoryTaxData is TaxData held in memory, such as a test's.
type MemoryTaxData struct {
	TableNumbers map[TaxTableNumberKey]int
	Tables       map[TaxTableKey][]TaxTableRow
}

func (d MemoryTaxData) TaxTableNumber(_ context.Context, kommun, forsamling, year string, churchMember bool) (int, error) {
	number, ok := d.TableNumbers[TaxTableNumberKey{Kommun: kommun, Forsamling: forsamling, Year: year, ChurchMember: churchMember}]
	if !ok {
		return 0, fmt.Errorf("no tax rate found for %s/%s/%s", kommun, forsamling, year)
	}
	return number, nil
}

func (d MemoryTaxData) TaxTable(_ context.Context, tableNumber int, year string) ([]TaxTableRow, error) {
	return d.Tables[TaxTableKey{Number: tableNumber, Year: year}], nil
}
