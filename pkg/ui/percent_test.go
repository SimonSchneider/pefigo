package ui_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/SimonSchneider/pefigo/pkg/ui"
)

func TestParsePercent(t *testing.T) {
	tests := []struct {
		in   string
		want float64
	}{
		{"15", 0.15},
		{"7", 0.07},
		{"12.5", 0.125},
		{"99.9", 0.999},
		{"-5", -0.05},
		{"150", 1.5},
		{"0", 0},
		{" 40 ", 0.4},
		{"1e1", 0.1},
		{"", 0},
		{"  ", 0},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%q is %g", tt.in, tt.want), func(t *testing.T) {
			got, err := ui.ParsePercent(tt.in)
			if err != nil {
				t.Fatalf("ParsePercent(%q) err = %v", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("ParsePercent(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestParsePercentRejectsWhatIsNotANumber(t *testing.T) {
	for _, in := range []string{"abc", "15%", "NaN", "Inf", "-Inf", "1e400"} {
		t.Run(in, func(t *testing.T) {
			if got, err := ui.ParsePercent(in); err == nil {
				t.Errorf("ParsePercent(%q) = %v, want an error", in, got)
			}
		})
	}
}

func TestFormatPercent(t *testing.T) {
	tests := []struct {
		in   float64
		want string
	}{
		{0.15, "15"},
		{0.07, "7"},
		{0.125, "12.5"},
		{0.999, "99.9"},
		{-0.05, "-5"},
		{1.5, "150"},
		{0, "0"},
		{0.4, "40"},
		{0.1, "10"},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%g is %q", tt.in, tt.want), func(t *testing.T) {
			if got := ui.FormatPercent(tt.in); got != tt.want {
				t.Errorf("FormatPercent(%v) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestParsePercentReadsBackWhatFormatPercentFormats(t *testing.T) {
	// Every fraction from -100% to 200% in steps of 0.01%.
	for i := -10_000; i <= 20_000; i++ {
		fraction, err := strconv.ParseFloat(fmt.Sprintf("%de-4", i), 64)
		if err != nil {
			t.Fatalf("parsing %de-4: %v", i, err)
		}
		got, err := ui.ParsePercent(ui.FormatPercent(fraction))
		if err != nil {
			t.Fatalf("ParsePercent(FormatPercent(%v)) err = %v", fraction, err)
		}
		if got != fraction {
			t.Fatalf("ParsePercent(FormatPercent(%v)) = %v via %q", fraction, got, ui.FormatPercent(fraction))
		}
	}
}
