package ui_test

import (
	"testing"

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
