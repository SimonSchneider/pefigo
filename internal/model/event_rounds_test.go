package model_test

import (
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/SimonSchneider/pefigo/internal/model"
)

// savedLater waits long enough that what is saved next is saved later than what was saved before.
func savedLater() {
	time.Sleep(2 * time.Millisecond)
}

func TestARoundWithoutSharesOrAValuationNeverHidesAnotherRoundOnItsDay(t *testing.T) {
	svc := newTestService(t)
	p := mustSeriesAPlans(t, svc, "2027-06-01", true)
	// A Note Event on the Series A's day adds a round with neither shares nor a valuation, saved last.
	note := mustEvent(t, svc, model.EventInput{Name: "Note", Date: mustParseDate("2027-06-01")})
	savedLater()
	mustInvestmentRound(t, svc, model.InvestmentRoundInput{ID: "zzz-note", AccountID: p.startup.ID, EventID: note.ID, DateOffset: days(0), Investment: 5e5})

	rec := forecastThrough(t, svc, "2027-07-31", nil)
	if got := balanceOn(t, rec, p.startup.ID, "2027-06-28"); !approxEqual(got, 400000, 0.01) {
		t.Errorf("from a Forecast Start before the rounds: expected the Series A round to value Startup Co at 400 000, got %v", got)
	}
	// A Snapshot after the rounds starts the Forecast after them: the same round values Startup Co.
	mustSnapshot(t, svc, p.checking.ID, mustParseDate("2027-07-01"), 0)
	rec = forecastThrough(t, svc, "2027-07-31", nil)
	if got := balanceOn(t, rec, p.startup.ID, "2027-07-28"); !approxEqual(got, 400000, 0.01) {
		t.Errorf("from a Forecast Start after the rounds: expected the Series A round to value Startup Co at 400 000, got %v", got)
	}
}

func TestSavingARoundOnADayThatHasOneInTheSameEventReplacesIt(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	p := mustSeriesAPlans(t, svc, "2027-06-01", true)
	mustInvestmentRound(t, svc, model.InvestmentRoundInput{AccountID: p.startup.ID, EventID: p.seriesA.ID, DateOffset: days(0), Valuation: 8e6, PreMoneyShares: 10000})
	// Outside Events too: a new round on the day of Startup Co's first round replaces it.
	mustInvestmentRound(t, svc, model.InvestmentRoundInput{AccountID: p.startup.ID, Date: mustParseDate("2027-01-01"), Valuation: 2e6, PreMoneyShares: 10000})

	page, err := svc.GetAccountEditPageData(ctx, p.startup.ID, nil)
	if err != nil {
		t.Fatalf("account edit page: %v", err)
	}
	var valuations []float64
	for _, r := range page.InvestmentRounds {
		valuations = append(valuations, r.Valuation)
	}
	slices.Sort(valuations)
	if want := []float64{2e6, 8e6}; !slices.Equal(valuations, want) {
		t.Errorf("expected each new round to replace the one on its day, leaving rounds valued %v, got %v", want, valuations)
	}
}

func TestAnEventListsItsRoundsAndShareChangesOnAnAccountThatNoLongerHoldsStartupShares(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	p := mustSeriesAPlans(t, svc, "2027-06-01", true)
	change, err := svc.UpsertShareChange(ctx, model.ShareChangeInput{AccountID: p.startup.ID, EventID: p.seriesA.ID, DateOffset: days(0), DeltaShares: 100, TotalPrice: 100})
	if err != nil {
		t.Fatalf("save share change: %v", err)
	}
	if _, err := svc.UpsertAccountWithStartupShares(ctx, model.AccountInput{ID: p.startup.ID, Name: p.startup.Name}, nil); err != nil {
		t.Fatalf("make Startup Co a regular account: %v", err)
	}

	want := map[model.EventEntryKind][]string{model.EventEntryInvestmentRound: {p.round.ID}, model.EventEntryShareChange: {change.ID}}
	page, err := svc.GetEventEditPageData(ctx, p.seriesA.ID)
	if err != nil {
		t.Fatalf("event edit page: %v", err)
	}
	if got := eventPartIDs(page.Event); !maps.EqualFunc(got, want, slices.Equal) {
		t.Errorf("expected the Series A to list %v, got %v", want, got)
	}
	confirm, err := svc.GetEventDeletePageData(ctx, p.seriesA.ID)
	if err != nil {
		t.Fatalf("delete confirmation: %v", err)
	}
	if got := eventPartIDs(confirm.Event); !maps.EqualFunc(got, want, slices.Equal) {
		t.Errorf("expected deleting the Series A to list %v, got %v", want, got)
	}
}
