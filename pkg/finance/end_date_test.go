package finance_test

import (
	"slices"
	"testing"
	"time"

	"github.com/SimonSchneider/goslu/date"
	finance2 "github.com/SimonSchneider/pefigo/pkg/finance"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

func TestRunPredictionChargesATransferFromItsStartDateUpToButNotOnItsEndDate(t *testing.T) {
	acc := newAccount("Checking", withBalance(firstDate, uncertain.NewFixed(0)))
	endDate := Must(date.ParseDate("2000-04-01"))
	transfer := newTransfer("", acc.ID, 1, "*-*-01", withFixed(uncertain.NewFixed(100)), func(tt *finance2.TransferTemplate) {
		tt.EffectiveFrom = Must(date.ParseDate("2000-02-01"))
		tt.EffectiveTo = &endDate
	})

	var charged []string
	recorder := finance2.TransferRecorderFunc(func(_, _ string, day date.Date, _ uncertain.Value) error {
		charged = append(charged, day.String())
		return nil
	})
	err := finance2.RunPrediction(t.Context(), uncertain.NewConfig(time.Now().UnixMilli(), 1), startDate, Must(date.ParseDate("2000-06-01")), "*-*-02",
		mks(*acc), mks(transfer), finance2.CompositeRecorder{TransferRecorder: recorder})
	if err != nil {
		t.Fatalf("run prediction: %v", err)
	}

	if want := []string{"2000-02-01", "2000-03-01"}; !slices.Equal(charged, want) {
		t.Errorf("expected charges on %v, got %v", want, charged)
	}
}

func TestGrowthModelAppliesFromItsStartDateUpToButNotOnItsEndDate(t *testing.T) {
	start := Must(date.ParseDate("2000-02-01"))
	end := Must(date.ParseDate("2000-04-01"))
	growth := finance2.TimeFrameGrowth{StartDate: start, EndDate: &end}

	for day, want := range map[string]bool{
		"2000-01-31": false,
		"2000-02-01": true,
		"2000-03-31": true,
		"2000-04-01": false,
	} {
		if got := growth.IsActiveOn(Must(date.ParseDate(day))); got != want {
			t.Errorf("IsActiveOn(%s) = %t, want %t", day, got, want)
		}
	}
}
