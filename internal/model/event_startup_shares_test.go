package model_test

import (
	"testing"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
)

// seriesAPlans are a Startup Co account holding 1 000 of 10 000 shares valued at 1 000 000 from
// 2027-01-01, the Forecast Start, so it is worth 100 000, and a Series A Event that adds an investment
// round at +0: 4 000 000 pre-money for 10 000 shares and 1 000 000 invested, which puts its 1 000 of the
// 12 500 shares at 400 000.
type seriesAPlans struct {
	startup  model.Account
	seriesA  model.Event
	round    model.InvestmentRound
	checking model.Account
}

func mustSeriesAPlans(t *testing.T, svc *model.Service, day string, planned bool) seriesAPlans {
	t.Helper()
	return mustSeriesAPlansFrom(t, svc, mustParseDate("2027-01-01"), day, planned)
}

// mustSeriesAPlansFrom are the Series A plans with the first round, share change and Snapshot on start.
func mustSeriesAPlansFrom(t *testing.T, svc *model.Service, start date.Date, day string, planned bool) seriesAPlans {
	t.Helper()
	var p seriesAPlans
	p.checking = mustAccount(t, svc, "Checking")
	mustSnapshot(t, svc, p.checking.ID, start, 0)
	p.startup = mustAccount(t, svc, "Startup Co")
	mustStartupShareAccount(t, svc, p.startup.ID,
		[]model.InvestmentRoundInput{{Date: start, Valuation: 1e6, PreMoneyShares: 10000}},
		[]model.ShareChangeInput{{Date: start, DeltaShares: 1000, TotalPrice: 1000}},
	)
	p.seriesA = mustEvent(t, svc, model.EventInput{Name: "Series A", Date: mustParseDate(day), Planned: ptr(planned)})
	p.round = mustInvestmentRound(t, svc, model.InvestmentRoundInput{
		AccountID: p.startup.ID, EventID: p.seriesA.ID, DateOffset: days(0), Valuation: 4e6, PreMoneyShares: 10000, Investment: 1e6,
	})
	return p
}

func mustInvestmentRound(t *testing.T, svc *model.Service, inp model.InvestmentRoundInput) model.InvestmentRound {
	t.Helper()
	ir, err := svc.UpsertInvestmentRound(t.Context(), inp)
	if err != nil {
		t.Fatalf("save investment round: %v", err)
	}
	return ir
}

func TestAnEventAddsAnInvestmentRoundAtItsDate(t *testing.T) {
	svc := newTestService(t)
	p := mustSeriesAPlans(t, svc, "2027-06-01", true)

	if p.round.Date != mustParseDate("2027-06-01") || p.round.EventID != p.seriesA.ID {
		t.Fatalf("expected the round in the Series A on its date, got %+v", p.round)
	}
	rec := forecastThrough(t, svc, "2027-06-30", nil)
	for day, want := range map[string]float64{"2027-05-28": 100000, "2027-06-28": 400000} {
		if got := balanceOn(t, rec, p.startup.ID, day); !approxEqual(got, want, 0.01) {
			t.Errorf("on %s: expected Startup Co worth %v, got %v", day, want, got)
		}
	}
}

func TestMovingAnEventMovesItsInvestmentRound(t *testing.T) {
	svc := newTestService(t)
	p := mustSeriesAPlans(t, svc, "2027-06-01", true)
	moveEvent(t, svc, p.seriesA, "2027-08-01")

	rec := forecastThrough(t, svc, "2027-08-31", nil)
	for day, want := range map[string]float64{"2027-06-28": 100000, "2027-07-28": 100000, "2027-08-28": 400000} {
		if got := balanceOn(t, rec, p.startup.ID, day); !approxEqual(got, want, 0.01) {
			t.Errorf("on %s: expected Startup Co worth %v, got %v", day, want, got)
		}
	}
}

func TestAnUnplannedEventsInvestmentRoundIsLeftOutOfTheBaseline(t *testing.T) {
	svc := newTestService(t)
	p := mustSeriesAPlans(t, svc, "2027-06-01", false)

	rec := forecastThrough(t, svc, "2027-08-31", nil)
	if got := balanceOn(t, rec, p.startup.ID, "2027-08-28"); !approxEqual(got, 100000, 0.01) {
		t.Errorf("expected Startup Co worth 100 000 without the unplanned Series A, got %v", got)
	}
}

func TestTwoEventsMayEachAddAnInvestmentRoundOnTheSameDay(t *testing.T) {
	svc := newTestService(t)
	p := mustSeriesAPlans(t, svc, "2027-06-01", true)
	// A second Event on the same day adds another round at +0: the one with the later ID applies.
	bridge := mustEvent(t, svc, model.EventInput{Name: "Bridge", Date: mustParseDate("2027-06-01")})
	mustInvestmentRound(t, svc, model.InvestmentRoundInput{
		ID: "zzz-bridge", AccountID: p.startup.ID, EventID: bridge.ID, DateOffset: days(0), Valuation: 9e6, PreMoneyShares: 10000, Investment: 1e6,
	})

	rec := forecastThrough(t, svc, "2027-06-30", nil)
	// 9 000 000 pre-money for 10 000 shares and 1 000 000 invested: 1 000 of 11 111.1 shares of 10 000 000.
	if got, want := balanceOn(t, rec, p.startup.ID, "2027-06-28"), 1e7*1000/(10000+1e6/900); !approxEqual(got, want, 0.01) {
		t.Errorf("expected the Bridge round, with the later ID, to apply: Startup Co worth %v, got %v", want, got)
	}
	moveEvent(t, svc, bridge, "2027-07-01")
	rec = forecastThrough(t, svc, "2027-07-31", nil)
	if got := balanceOn(t, rec, p.startup.ID, "2027-06-28"); !approxEqual(got, 400000, 0.01) {
		t.Errorf("expected only the Series A round in June once the Bridge moves, got %v", got)
	}
}

func TestAStartupShareAccountsPageListsItsRoundsAtTheirEventsDatesAndDerivesItsSharesFromTheBaseline(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	today := date.Today()
	p := mustSeriesAPlansFrom(t, svc, today.Add(-100*date.Day), today.Add(-10*date.Day).String(), true)

	page, err := svc.GetAccountEditPageData(ctx, p.startup.ID, nil)
	if err != nil {
		t.Fatalf("account edit page: %v", err)
	}
	if got := page.DerivedStartupShareSummary.TotalShares; got != 12500 {
		t.Errorf("expected the Series A round in the Baseline's 12 500 shares, got %v", got)
	}
	var inEvent *model.InvestmentRound
	for i, r := range page.InvestmentRounds {
		if r.EventID == p.seriesA.ID {
			inEvent = &page.InvestmentRounds[i]
		}
	}
	if inEvent == nil || inEvent.Date != p.seriesA.Date {
		t.Fatalf("expected the Series A round listed on the Event's date, got %+v", page.InvestmentRounds)
	}

	moveEvent(t, svc, model.Event{ID: p.seriesA.ID, Name: p.seriesA.Name, Date: p.seriesA.Date, Planned: false}, p.seriesA.Date.String())
	page, err = svc.GetAccountEditPageData(ctx, p.startup.ID, nil)
	if err != nil {
		t.Fatalf("account edit page: %v", err)
	}
	if got := page.DerivedStartupShareSummary.TotalShares; got != 10000 {
		t.Errorf("expected an unplanned Series A left out of the Baseline's 10 000 shares, got %v", got)
	}
	accounts, err := svc.ListAccountsDetailed(ctx, today)
	if err != nil {
		t.Fatalf("list accounts: %v", err)
	}
	for _, acc := range accounts {
		if acc.ID == p.startup.ID && (acc.LastSnapshot == nil || !approxEqual(acc.LastSnapshot.Balance.Mean(), 100000, 0.01)) {
			t.Errorf("expected Startup Co's derived balance without the unplanned round, got %+v", acc.LastSnapshot)
		}
	}
}
