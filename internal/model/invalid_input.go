package model

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/pkg/currency"
)

// ErrInvalidInput is wrapped by every error that rejects what a caller asked to save, as opposed to a
// failure to save it.
var ErrInvalidInput = errors.New("invalid input")

// checkRequired rejects value, the field named field, when it isn't given: when it is the zero value an
// empty form field maps to, such as "" for a name or a parent link and the zero date.Date for a date.
func checkRequired[T comparable](field string, value T) error {
	var notGiven T
	if value == notGiven {
		return fmt.Errorf("%w: %s is required", ErrInvalidInput, field)
	}
	return nil
}

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

// checkDatePattern rejects pattern, the field named field, unless it is a valid date pattern: it is
// datePatternError wrapped in ErrInvalidInput.
func checkDatePattern(field string, pattern date.Cron) error {
	if err := datePatternError(field, pattern); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidInput, err)
	}
	return nil
}

// datePatternError says why pattern, the field named field, isn't a valid date pattern, or is nil when
// it is one. Unlike checkDatePattern it doesn't wrap ErrInvalidInput, so it suits stored data too.
func datePatternError(field string, pattern date.Cron) error {
	if validDatePattern(pattern) {
		return nil
	}
	return fmt.Errorf("%s %q is not a date pattern: year-month-day, each *, */n or a number", field, pattern)
}

// validDatePattern reports whether pattern is a date pattern date.Cron can match days with: a year, a
// month and a day separated by dashes, each "*", "*/n" with n a positive integer, or an unsigned
// integer. Every recurrence is one: of a Transfer Template, a Salary and a Bill account, an account's
// cash-flow frequency, the Forecast Interval and the Forecast Start Days.
func validDatePattern(pattern date.Cron) bool {
	parts := strings.Split(string(pattern), "-")
	if len(parts) != 3 {
		return false
	}
	for _, part := range parts {
		if !validDatePatternPart(part) {
			return false
		}
	}
	return true
}

func validDatePatternPart(part string) bool {
	if part == "*" {
		return true
	}
	if n, ok := strings.CutPrefix(part, "*/"); ok {
		every, ok := unsignedInt(n)
		return ok && every > 0
	}
	_, ok := unsignedInt(part)
	return ok
}

// unsignedInt parses s when it is only digits.
func unsignedInt(s string) (int, bool) {
	if s == "" || strings.Trim(s, "0123456789") != "" {
		return 0, false
	}
	n, err := strconv.Atoi(s)
	return n, err == nil
}
