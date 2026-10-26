package model

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/SimonSchneider/pefigo/pkg/currency"
)

// ErrInvalidInput is wrapped by every error that rejects what a caller asked to save, as opposed to a
// failure to save it.
var ErrInvalidInput = errors.New("invalid input")

// checkOneOf rejects value, the field named field, unless it is one of set.
func checkOneOf(field, value string, set ...string) error {
	if slices.Contains(set, value) {
		return nil
	}
	return fmt.Errorf("%w: %s %q is not %s", ErrInvalidInput, field, value, strings.Join(set, " or "))
}

// checkSupportedCurrency rejects code, the field named field, unless it is a supported currency.
func checkSupportedCurrency(field, code string) error {
	if _, ok := currency.Get(code); !ok {
		return fmt.Errorf("%w: %s %q is not a supported currency", ErrInvalidInput, field, code)
	}
	return nil
}
