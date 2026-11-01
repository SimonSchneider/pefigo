package model_test

import (
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/SimonSchneider/goslu/date"
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

// roundValuations are the valuations of the account's rounds on its page, in order.
func roundValuations(t *testing.T, svc *model.Service, accountID string) []float64 {
	t.Helper()
	page, err := svc.GetAccountEditPageData(t.Context(), accountID, nil)
	if err != nil {
		t.Fatalf("account edit page: %v", err)
	}
	var valuations []float64
	for _, r := range page.InvestmentRounds {
		valuations = append(valuations, r.Valuation)
	}
	slices.Sort(valuations)
	return valuations
}

func TestSavingARoundOnADayThatHasOneAddsAnother(t *testing.T) {
	svc := newTestService(t)
	p := mustSeriesAPlans(t, svc, "2027-06-01", true)
	savedLater()
	mustInvestmentRound(t, svc, model.InvestmentRoundInput{AccountID: p.startup.ID, EventID: p.seriesA.ID, DateOffset: days(0), Valuation: 8e6, PreMoneyShares: 10000})
	// Outside Events too: a new round on the day of Startup Co's first round sits beside it.
	mustInvestmentRound(t, svc, model.InvestmentRoundInput{AccountID: p.startup.ID, Date: mustParseDate("2027-01-01"), Valuation: 2e6, PreMoneyShares: 10000})

	if got, want := roundValuations(t, svc, p.startup.ID), []float64{1e6, 2e6, 4e6, 8e6}; !slices.Equal(got, want) {
		t.Errorf("expected every round kept, valued %v, got %v", want, got)
	}
	// Of the Series A's two rounds on its day, the one saved last values Startup Co: 1 000 of 10 000
	// shares at 8 000 000.
	rec := forecastThrough(t, svc, "2027-06-30", nil)
	if got := balanceOn(t, rec, p.startup.ID, "2027-06-28"); !approxEqual(got, 800000, 0.01) {
		t.Errorf("expected the round saved last on the Series A's day to apply, worth 800 000, got %v", got)
	}
}

func TestARoundLeavingItsEventKeepsTheBaselinesRoundOnItsDay(t *testing.T) {
	svc := newTestService(t)
	p := mustSeriesAPlans(t, svc, "2027-06-01", true)
	mustInvestmentRound(t, svc, model.InvestmentRoundInput{AccountID: p.startup.ID, Date: mustParseDate("2027-06-01"), Valuation: 2e6, PreMoneyShares: 10000})

	mustRemoveFromEvent(t, svc, model.EventEntry{Kind: model.EventEntryInvestmentRound, ID: p.round.ID})

	if got, want := roundValuations(t, svc, p.startup.ID), []float64{1e6, 2e6, 4e6}; !slices.Equal(got, want) {
		t.Errorf("expected the Series A round to leave its Event beside the Baseline's round on its day, valued %v, got %v", want, got)
	}
}

func TestARoundJoiningAnEventKeepsTheEventsRoundOnItsDay(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	p := mustSeriesAPlans(t, svc, "2027-06-01", true)
	baseline := mustInvestmentRound(t, svc, model.InvestmentRoundInput{AccountID: p.startup.ID, Date: mustParseDate("2027-06-01"), Valuation: 2e6, PreMoneyShares: 10000})

	if _, err := svc.UpsertInvestmentRound(ctx, model.InvestmentRoundInput{
		ID: baseline.ID, AccountID: p.startup.ID, EventID: p.seriesA.ID, Date: baseline.Date, Valuation: 2e6, PreMoneyShares: 10000,
	}); err != nil {
		t.Fatalf("join the Series A: %v", err)
	}

	if got, want := roundValuations(t, svc, p.startup.ID), []float64{1e6, 2e6, 4e6}; !slices.Equal(got, want) {
		t.Errorf("expected the round to join the Series A beside its round on the day, valued %v, got %v", want, got)
	}
}

// mustNoteRound adds a Note Event on day that adds a round to the Startup Co with an investment but
// neither shares nor a valuation, saved after every other round.
func mustNoteRound(t *testing.T, svc *model.Service, p seriesAPlans, day date.Date) {
	t.Helper()
	note := mustEvent(t, svc, model.EventInput{Name: "Note", Date: day})
	savedLater()
	mustInvestmentRound(t, svc, model.InvestmentRoundInput{ID: "zzz-note", AccountID: p.startup.ID, EventID: note.ID, DateOffset: days(0), Investment: 5e5})
}

func TestTheAccountPageSummaryIsValuedByTheRoundTheForecastUses(t *testing.T) {
	svc := newTestService(t)
	today := date.Today()
	p := mustSeriesAPlansFrom(t, svc, today.Add(-100*date.Day), today.Add(-10*date.Day).String(), true)
	mustNoteRound(t, svc, p, p.seriesA.Date)

	page, err := svc.GetAccountEditPageData(t.Context(), p.startup.ID, nil)
	if err != nil {
		t.Fatalf("account edit page: %v", err)
	}
	if got := page.DerivedStartupShareSummary.TotalShares; got != 12500 {
		t.Errorf("expected the Series A round, not the Note's without shares, to give the 12 500 shares, got %v", got)
	}
}

func TestBalanceHistoryValuesAStartupShareAccountByTheRoundTheForecastUses(t *testing.T) {
	svc := newTestService(t)
	equity := mustAccountType(t, svc, "Equity", "#ff00ff")
	startup := mustTypedAccount(t, svc, "Startup", equity.ID)
	mustStartupShareAccount(t, svc, startup.ID,
		[]model.InvestmentRoundInput{
			{Date: mustParseDate("2021-01-01"), Valuation: 1e6, PreMoneyShares: 1000},
			{Date: mustParseDate("2021-03-01"), Valuation: 2e6, PreMoneyShares: 1000},
		},
		[]model.ShareChangeInput{{Date: mustParseDate("2021-02-01"), DeltaShares: 10, TotalPrice: 100}},
	)
	mustNoteRound(t, svc, seriesAPlans{startup: startup}, mustParseDate("2021-03-01"))
	savings := mustTypedAccount(t, svc, "Savings", mustAccountType(t, svc, "Savings", "#00ff00").ID)
	mustSnapshot(t, svc, savings.ID, mustParseDate("2021-02-15"), 1000)
	mustSnapshot(t, svc, savings.ID, mustParseDate("2021-03-15"), 1000)

	rec := forecastWith(t, svc, model.PredictionParams{Duration: ptr(date.Year), Samples: ptr[int64](1), Quantile: ptr(0.0), SnapshotInterval: "*-*-*", GroupBy: model.GroupByType})

	// On 2021-03-15 the round of 2021-03-01 with shares values it, not the Note's saved after it.
	expectPoints(t, entityByID(t, rec, equity.ID).Snapshots, point(equity.ID, "2021-02-15", 10000), point(equity.ID, "2021-03-15", 20000))
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
