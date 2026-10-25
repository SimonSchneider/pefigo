package finance_test

import (
	"slices"
	"testing"
	"time"

	"github.com/SimonSchneider/goslu/date"
	finance2 "github.com/SimonSchneider/pefigo/pkg/finance"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

func TestRunForecastChargesATransferFromItsStartDateUpToButNotOnItsEndDate(t *testing.T) {
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
	err := finance2.RunForecast(t.Context(), uncertain.NewConfig(time.Now().UnixMilli(), 1), startDate, Must(date.ParseDate("2000-06-01")), "*-*-02",
		mks(*acc), mks(transfer), finance2.CompositeRecorder{TransferRecorder: recorder})
	if err != nil {
		t.Fatalf("run forecast: %v", err)
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

func TestGrowthModelsApplyTheLatestStartingModelThatAppliesOnTheDay(t *testing.T) {
	ucfg := uncertain.NewConfig(1, 1)
	feb := Must(date.ParseDate("2000-02-01"))
	day := Must(date.ParseDate("2000-06-01"))
	fixed := func(start string, end *date.Date, rate float64) *finance2.FixedGrowth {
		return &finance2.FixedGrowth{
			TimeFrameGrowth: finance2.TimeFrameGrowth{StartDate: Must(date.ParseDate(start)), EndDate: end},
			AnnualRate:      uncertain.NewFixed(rate),
		}
	}
	balance := uncertain.NewFixed(1_000_000)
	want := finance2.NewGrowthCombined(fixed("2000-02-01", nil, 0.10)).Apply(ucfg, day, nil, balance).Mean()

	for name, growth := range map[string]*finance2.GrowthCombined{
		"after an ended model":   finance2.NewGrowthCombined(fixed("2000-01-01", &feb, 0.03), fixed("2000-02-01", nil, 0.10)),
		"over an earlier model":  finance2.NewGrowthCombined(fixed("2000-01-01", nil, 0.03), fixed("2000-02-01", nil, 0.10)),
		"given in reverse order": finance2.NewGrowthCombined(fixed("2000-02-01", nil, 0.10), fixed("2000-01-01", nil, 0.03)),
	} {
		if got := growth.Apply(ucfg, day, nil, balance).Mean(); got != want {
			t.Errorf("%s: expected the 10%% model's growth %f, got %f", name, want, got)
		}
	}
}

func TestRunForecastAppliesADaysTransfersInPriorityOrder(t *testing.T) {
	salary := newAccount("Salary", withBalance(firstDate, uncertain.NewFixed(0)))
	savings := newAccount("Savings", withBalance(firstDate, uncertain.NewFixed(0)))
	// Given out of priority order: the income (priority 2) comes before the saving (priority 1).
	income := newTransfer("", salary.ID, 2, "*-*-25", withFixed(uncertain.NewFixed(4000)))
	saving := newTransfer(salary.ID, savings.ID, 1, "*-*-25", withPercent(0.5))

	saved := map[string]float64{}
	recorder := finance2.TransferRecorderFunc(func(from, to string, day date.Date, amount uncertain.Value) error {
		if to == savings.ID {
			saved[day.String()] = amount.Mean()
		}
		return nil
	})
	err := finance2.RunForecast(t.Context(), uncertain.NewConfig(1, 1), startDate, Must(date.ParseDate("2000-02-01")), "*-*-28",
		mks(*salary, *savings), mks(income, saving), finance2.CompositeRecorder{TransferRecorder: recorder})
	if err != nil {
		t.Fatalf("run forecast: %v", err)
	}

	// The saving runs before the income, while Salary is still empty.
	if got := saved["2000-01-25"]; got != 0 {
		t.Errorf("expected the priority 1 saving to move 0 before the priority 2 income, got %f", got)
	}
}
