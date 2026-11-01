package ui

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/goslu/static/shttp"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

type UncertainValue struct {
	Distribution uncertain.DistributionType `json:"distribution"`
	Parameters   map[string]float64         `json:"parameters"`
	Samples      []float64                  `json:"samples"`
}

func ParseUncertainValue(val string) (uncertain.Value, error) {
	if len(val) > 0 && (unicode.IsDigit(rune(val[0])) || (len(val) > 1 && val[0] == '-' && unicode.IsDigit(rune(val[1]))) || val[0] == '(') {
		// Simple float or expression (e.g. 500+23+43-294*1.23 or (100+50)*1.23); parse and return fixed.
		f, err := ParseAmount(val)
		if err != nil {
			return uncertain.Value{}, fmt.Errorf("parsing amount: %w", err)
		}
		return uncertain.NewFixed(f), nil
	}
	// Otherwise, parse it as an uncertain value
	var value uncertain.Value
	if err := value.Decode(val); err != nil {
		return uncertain.Value{}, fmt.Errorf("decoding uncertain value: %w", err)
	}
	return value, nil
}

func ParseHumanNumber[T int | int64 | int32 | float64 | float32](delegate func(string) (T, error)) func(string) (T, error) {
	return func(val string) (T, error) {
		if val == "" {
			return 0, nil
		}
		suffix := val[len(val)-1]
		mult := T(1)
		if suffix == 'k' || suffix == 'K' {
			mult = 1_000
			val = val[:len(val)-1]
		} else if suffix == 'm' || suffix == 'M' {
			mult = 1_000_000
			val = val[:len(val)-1]
		}
		i, err := delegate(val)
		return i * mult, err
	}
}

func ParseInt64(val string) (int64, error) {
	if val == "" {
		return 0, nil
	}
	i, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing int64: %w", err)
	}
	return i, nil
}

func ParseNullableFloat(val string) (*float64, error) {
	if val == "" {
		return nil, nil
	}
	f, err := shttp.ParseFloat(val)
	if err != nil {
		return nil, fmt.Errorf("parsing float: %w", err)
	}
	return &f, nil
}

// ParseNullable makes parse return nil for an empty value, or one of only spaces: "not given",
// separately from a given 0.
func ParseNullable[T any](parse func(string) (T, error)) func(string) (*T, error) {
	return func(val string) (*T, error) {
		if strings.TrimSpace(val) == "" {
			return nil, nil
		}
		v, err := parse(val)
		if err != nil {
			return nil, err
		}
		return &v, nil
	}
}

// ParsePercent parses a percentage into the fraction it stands for: "15" is 0.15, and "", or only
// spaces, is 0. FormatPercent is its inverse.
func ParsePercent(val string) (float64, error) {
	val = strings.TrimSpace(val)
	if val == "" {
		return 0, nil
	}
	percent, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing percent: %w", err)
	}
	if math.IsNaN(percent) || math.IsInf(percent, 0) {
		return 0, fmt.Errorf("parsing percent: %q is not a number", val)
	}
	// Moving the decimal point, rather than dividing by 100, gives the fraction closest to what was
	// typed: "99.9" is 0.999, not 0.9990000000000001.
	return strconv.ParseFloat(strconv.FormatFloat(percent, 'f', -1, 64)+"e-2", 64)
}

func WithDefaultNull[T comparable](val T) *T {
	var zero T
	if val == zero {
		return nil
	}
	return &val
}

func OrDefault[T any](val *T) T {
	if val == nil {
		var zero T
		return zero
	}
	return *val
}

func ParseDateCron(val string) (date.Cron, error) {
	return date.Cron(val), nil
}

// ParseDays parses a whole number of days with an optional sign, such as "+20", "20" or "-5": the
// days an entry in an Event is after the Event's date.
func ParseDays(val string) (date.Duration, error) {
	n, err := strconv.ParseInt(strings.TrimSpace(val), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%q is not a whole number of days, such as +20 or -5", val)
	}
	return date.Duration(n), nil
}
