package model_test

import (
	"testing"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
)

func TestDeletingAnEventListsTheOptionsItsAccountsFund(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	startup := mustAccount(t, svc, "Startup")
	mustStartupShareAccount(t, svc, startup.ID, []model.InvestmentRoundInput{{Date: mustParseDate("2021-01-01"), Valuation: 1e6, PreMoneyShares: 1000}}, nil)
	apartment := mustEvent(t, svc, model.EventInput{Name: "Apartment", Date: mustParseDate("2028-03-01")})
	bridge := mustEventAccount(t, svc, "Bridge loan", apartment.ID)
	if _, err := svc.UpsertStartupShareOption(ctx, model.StartupShareOptionInput{
		AccountID: startup.ID, SourceAccountID: bridge.ID, Shares: 9000, StrikePricePerShare: 10,
		GrantDate: mustParseDate("2026-01-01"), EndDate: mustParseDate("2029-01-01"),
	}); err != nil {
		t.Fatalf("create option: %v", err)
	}

	view, err := svc.GetEventDeletePageData(ctx, apartment.ID)
	if err != nil {
		t.Fatalf("get delete page: %v", err)
	}
	if got := view.AlsoDeletedOptions; len(got) != 1 || got[0].AccountName != "Startup" || got[0].SourceAccountName != "Bridge loan" || got[0].Shares != 9000 {
		t.Fatalf("expected the confirmation to list the Option on Startup funded by Bridge loan, got %+v", got)
	}

	if err := svc.DeleteEvent(ctx, apartment.ID); err != nil {
		t.Fatalf("delete event: %v", err)
	}
	if opts, err := svc.ListStartupShareOptions(ctx, startup.ID); err != nil || len(opts) != 0 {
		t.Fatalf("expected the Option deleted with Bridge loan, as listed, got %+v (%v)", opts, err)
	}
}

func TestAGrowthModelJoiningAnEventOnItsStartDateKeepsWhichGrowthModelApplies(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	savings := mustAccount(t, svc, "Savings")
	mustSnapshot(t, svc, savings.ID, mustParseDate("2021-01-10"), 1000)
	still := mustGrowthModel(t, svc, model.AccountGrowthModelInput{AccountID: savings.ID, AnnualRate: newFixedValue(0), StartDate: mustParseDate("2020-01-01")})
	doubling := mustGrowthModel(t, svc, model.AccountGrowthModelInput{AccountID: savings.ID, AnnualRate: newFixedValue(1), StartDate: mustParseDate("2020-01-01")})
	// Of two growth models starting on the same day, the one with the later ID applies.
	winner := still
	if doubling.ID > still.ID {
		winner = doubling
	}
	params := model.PredictionParams{Duration: ptr(60 * date.Day), Samples: ptr[int64](1), Quantile: ptr(0.0), SnapshotInterval: "*-*-10", GroupBy: model.GroupByNone}
	applied := func() string {
		t.Helper()
		accounts, err := svc.ListAccountsDetailed(ctx, mustParseDate("2021-02-01"))
		if err != nil {
			t.Fatalf("list accounts: %v", err)
		}
		for _, acc := range accounts {
			if acc.ID == savings.ID && acc.GrowthModel != nil {
				return acc.GrowthModel.ID
			}
		}
		return "none"
	}
	if got := applied(); got != winner.ID {
		t.Fatalf("expected the growth model with the later ID to apply, got %s", got)
	}
	before := forecastWith(t, svc, params).pointsOf(savings.ID)

	// The winner joins an Event on its Start Date, so the dates it applies on don't change.
	raise := mustEvent(t, svc, model.EventInput{Name: "Raise", Date: mustParseDate("2020-01-01")})
	mustGrowthModel(t, svc, model.AccountGrowthModelInput{
		ID: winner.ID, AccountID: savings.ID, AnnualRate: winner.AnnualRate, StartDate: winner.StartDate, EventID: raise.ID,
	})

	if got := applied(); got != winner.ID {
		t.Errorf("expected the same growth model to apply after joining an Event on its Start Date, got %s", got)
	}
	expectPoints(t, forecastWith(t, svc, params).pointsOf(savings.ID), before...)
}

func TestAnOptionFundedByALeftOutAccountIsLeftOutOfTheForecast(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	equity := mustAccountType(t, svc, "Equity", "#ff00ff")
	startup := mustTypedAccount(t, svc, "Startup", equity.ID)
	checking := mustAccount(t, svc, "Checking")
	mustStartupShareAccount(t, svc, startup.ID,
		[]model.InvestmentRoundInput{{Date: mustParseDate("2021-01-01"), Valuation: 1e6, PreMoneyShares: 1000}},
		[]model.ShareChangeInput{{Date: mustParseDate("2021-01-01"), DeltaShares: 10, TotalPrice: 100}},
	)
	sabbatical := mustEvent(t, svc, model.EventInput{Name: "Sabbatical", Date: mustParseDate("2021-01-01"), Planned: ptr(false)})
	bridge := mustEventAccount(t, svc, "Bridge loan", sabbatical.ID)
	if _, err := svc.UpsertStartupShareOption(ctx, model.StartupShareOptionInput{
		AccountID: startup.ID, SourceAccountID: bridge.ID, Shares: 20, StrikePricePerShare: 10,
		GrantDate: mustParseDate("2020-01-01"), EndDate: mustParseDate("2021-03-01"),
	}); err != nil {
		t.Fatalf("create option: %v", err)
	}
	mustSnapshot(t, svc, checking.ID, mustParseDate("2021-02-10"), 100_000)

	rec := forecastWith(t, svc, model.PredictionParams{Duration: ptr(30 * date.Day), Samples: ptr[int64](1), Quantile: ptr(0.0), SnapshotInterval: "*-*-10", GroupBy: model.GroupByNone})

	// The unplanned Sabbatical leaves out Bridge loan and the Option it funds: Startup keeps its 10 shares.
	expectPoints(t, rec.pointsOf(startup.ID), point(startup.ID, "2021-02-10", 10000), point(startup.ID, "2021-03-10", 10000))
}
