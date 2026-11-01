package ui_test

import (
	"testing"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/pkg/ui"
)

func TestParseNullableIsNilForAnEmptyValue(t *testing.T) {
	got, err := ui.ParseNullable(ui.ParsePercent)("")
	if err != nil {
		t.Fatalf("ParseNullable(ParsePercent)(\"\") err = %v", err)
	}
	if got != nil {
		t.Errorf("ParseNullable(ParsePercent)(\"\") = %v, want nil", *got)
	}
}

func TestParseNullableIsNilForAValueOfOnlySpaces(t *testing.T) {
	got, err := ui.ParseNullable(ui.ParsePercent)("  ")
	if err != nil {
		t.Fatalf("ParseNullable(ParsePercent)(\"  \") err = %v", err)
	}
	if got != nil {
		t.Errorf("ParseNullable(ParsePercent)(\"  \") = %v, want nil", *got)
	}
}

func TestParseNullableParsesAGivenZero(t *testing.T) {
	got, err := ui.ParseNullable(ui.ParsePercent)("0")
	if err != nil {
		t.Fatalf("ParseNullable(ParsePercent)(\"0\") err = %v", err)
	}
	if got == nil || *got != 0 {
		t.Errorf("ParseNullable(ParsePercent)(\"0\") = %v, want 0", got)
	}
}

func TestParseNullableParsesAGivenValue(t *testing.T) {
	got, err := ui.ParseNullable(ui.ParsePercent)("15")
	if err != nil {
		t.Fatalf("ParseNullable(ParsePercent)(\"15\") err = %v", err)
	}
	if got == nil || *got != 0.15 {
		t.Errorf("ParseNullable(ParsePercent)(\"15\") = %v, want 0.15", got)
	}
}

func TestParseNullableRejectsWhatItsParserRejects(t *testing.T) {
	if got, err := ui.ParseNullable(ui.ParsePercent)("abc"); err == nil {
		t.Errorf("ParseNullable(ParsePercent)(\"abc\") = %v, want an error", got)
	}
}

func TestParseDaysTakesASignedWholeNumberOfDays(t *testing.T) {
	for in, want := range map[string]date.Duration{"+20": 20, "20": 20, "-5": -5, "0": 0, "+0": 0, " +7 ": 7} {
		got, err := ui.ParseDays(in)
		if err != nil || got != want {
			t.Errorf("ParseDays(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "+", "2027-09-30", "1.5", "20d", "+-3"} {
		if _, err := ui.ParseDays(in); err == nil {
			t.Errorf("ParseDays(%q): expected an error", in)
		}
	}
}

func TestFormatDaysGivesTheSignAsParseDaysTakesIt(t *testing.T) {
	for in, want := range map[date.Duration]string{20: "+20", -5: "-5", 0: "+0"} {
		if got := ui.FormatDays(in); got != want {
			t.Errorf("FormatDays(%d) = %q; want %q", in, got, want)
		}
	}
}
