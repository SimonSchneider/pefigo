package model_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo"
	"github.com/SimonSchneider/pefigo/internal/model"
	"github.com/ncruces/go-sqlite3/vfs/memdb"
)

// fakeTimers replaces the debounce timer, so a test fires the scheduled Forecast Run by hand. A
// fired run executes inside fire, so when fire returns the test has the full event sequence.
type fakeTimers struct {
	t      *testing.T
	mu     sync.Mutex
	timers []*fakeTimer
}

type fakeTimer struct {
	owner   *fakeTimers
	f       func()
	stopped bool
	fired   bool
}

func (ts *fakeTimers) afterFunc(_ time.Duration, f func()) model.ForecastTimer {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	timer := &fakeTimer{owner: ts, f: f}
	ts.timers = append(ts.timers, timer)
	return timer
}

func (t *fakeTimer) Stop() bool {
	t.owner.mu.Lock()
	defer t.owner.mu.Unlock()
	if t.stopped || t.fired {
		return false
	}
	t.stopped = true
	return true
}

// pending counts the timers that are scheduled and neither stopped nor fired.
func (ts *fakeTimers) pending() int {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	n := 0
	for _, timer := range ts.timers {
		if !timer.stopped && !timer.fired {
			n++
		}
	}
	return n
}

// fire runs the one pending timer's func in the caller.
func (ts *fakeTimers) fire() {
	ts.t.Helper()
	ts.mu.Lock()
	var pending []*fakeTimer
	for _, timer := range ts.timers {
		if !timer.stopped && !timer.fired {
			pending = append(pending, timer)
		}
	}
	if len(pending) != 1 {
		ts.mu.Unlock()
		ts.t.Fatalf("expected exactly one pending forecast timer, got %d", len(pending))
		return
	}
	pending[0].fired = true
	ts.mu.Unlock()
	pending[0].f()
}

// fireInBackground fires the pending timer on another goroutine and returns a channel that is
// closed once the fired func has returned.
func (ts *fakeTimers) fireInBackground() <-chan struct{} {
	ts.t.Helper()
	ts.mu.Lock()
	var pending []*fakeTimer
	for _, timer := range ts.timers {
		if !timer.stopped && !timer.fired {
			pending = append(pending, timer)
		}
	}
	if len(pending) != 1 {
		ts.mu.Unlock()
		ts.t.Fatalf("expected exactly one pending forecast timer, got %d", len(pending))
	}
	pending[0].fired = true
	ts.mu.Unlock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		pending[0].f()
	}()
	return done
}

// computeFunc is a fake ForecastCompute.
type computeFunc func(ctx context.Context, h model.PredictionEventHandler, params model.PredictionParams) error

func (f computeFunc) RunPrediction(ctx context.Context, h model.PredictionEventHandler, params model.PredictionParams) error {
	return f(ctx, h, params)
}

// recordingLogger keeps every logged line.
type recordingLogger struct {
	mu    sync.Mutex
	lines []string
}

func (l *recordingLogger) Printf(format string, v ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, v...))
}

func (l *recordingLogger) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.lines, "\n")
}

// newForecastTestService opens a Service whose Dashboard Forecast uses fake timers and, unless
// compute is nil, a fake compute. The Dashboard Forecast is not started.
func newForecastTestService(t *testing.T, compute model.ForecastCompute) (*model.Service, *fakeTimers) {
	t.Helper()
	timers := &fakeTimers{t: t}
	opts := []model.ServiceOption{model.WithForecastTimer(timers.afterFunc)}
	if compute != nil {
		opts = append(opts, model.WithForecastCompute(compute))
	}
	svc, err := model.Open(t.Context(), pefigo.StaticEmbeddedFS, "static/migrations", memdb.TestDB(t), opts...)
	if err != nil {
		t.Fatalf("failed to create test db: %v", err)
	}
	t.Cleanup(func() {
		svc.DashboardForecast().Stop()
		svc.DB().Close()
	})
	return svc, timers
}

// startForecast starts the Dashboard Forecast and returns its logger.
func startForecast(svc *model.Service) *recordingLogger {
	logger := &recordingLogger{}
	svc.DashboardForecast().Start(logger)
	return logger
}

func subscribe(t *testing.T, svc *model.Service) model.ForecastSubscription {
	t.Helper()
	sub, err := svc.DashboardForecast().Subscribe(t.Context())
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	t.Cleanup(sub.Unsubscribe)
	return sub
}

// received returns the events already delivered to ch, without waiting for more.
func received(ch <-chan model.ForecastEvent) []model.ForecastEvent {
	var events []model.ForecastEvent
	for {
		select {
		case evt, ok := <-ch:
			if !ok {
				return events
			}
			events = append(events, evt)
		default:
			return events
		}
	}
}

func eventTypes(events []model.ForecastEvent) []model.ForecastEventType {
	types := make([]model.ForecastEventType, len(events))
	for i, evt := range events {
		types[i] = evt.Type
	}
	return types
}

func snapshotRows(events []model.ForecastEvent) []model.ForecastCacheRow {
	var rows []model.ForecastCacheRow
	for _, evt := range events {
		if evt.Type == model.ForecastEventSnapshot {
			rows = append(rows, *evt.Snapshot)
		}
	}
	return rows
}

// expectEventTypes fails unless events have exactly the given types.
func expectEventTypes(t *testing.T, events []model.ForecastEvent, want ...model.ForecastEventType) {
	t.Helper()
	if got := eventTypes(events); !slices.Equal(got, want) {
		t.Fatalf("expected events %v, got %v", want, got)
	}
}

// snapshots is n Snapshot event types, for events.
func snapshots(n int) []model.ForecastEventType {
	types := make([]model.ForecastEventType, n)
	for i := range types {
		types[i] = model.ForecastEventSnapshot
	}
	return types
}

// events flattens event types and slices of them, as returned by snapshots.
func events(parts ...any) []model.ForecastEventType {
	var types []model.ForecastEventType
	for _, p := range parts {
		switch v := p.(type) {
		case model.ForecastEventType:
			types = append(types, v)
		case []model.ForecastEventType:
			types = append(types, v...)
		default:
			panic(fmt.Sprintf("unexpected event part %T", p))
		}
	}
	return types
}

// sortedRows orders rows the way they are stored.
func sortedRows(rows []model.ForecastCacheRow) []model.ForecastCacheRow {
	sorted := slices.Clone(rows)
	slices.SortFunc(sorted, func(a, b model.ForecastCacheRow) int {
		if a.Date != b.Date {
			return int(a.Date - b.Date)
		}
		return strings.Compare(a.AccountTypeID, b.AccountTypeID)
	})
	return sorted
}

func expectSameRows(t *testing.T, got, want []model.ForecastCacheRow) {
	t.Helper()
	if g, w := sortedRows(got), sortedRows(want); !slices.Equal(g, w) {
		t.Fatalf("expected rows %v, got %v", w, g)
	}
}

// emitSnapshot sends the i:th forecast point of account type typeID to h.
func emitSnapshot(h model.PredictionEventHandler, typeID string, i int) error {
	return h.Snapshot(model.PredictionBalanceSnapshot{
		ID:         typeID,
		Day:        date.Today().Add(date.Duration(i+1) * date.Day),
		Balance:    float64(1000 * (i + 1)),
		LowerBound: float64(900 * (i + 1)),
		UpperBound: float64(1100 * (i + 1)),
	})
}

// emittingCompute is a fake compute that sends n forecast points, then returns err.
func emittingCompute(n int, err error) computeFunc {
	return func(ctx context.Context, h model.PredictionEventHandler, _ model.PredictionParams) error {
		if setupErr := h.Setup(model.PredictionSetupEvent{}); setupErr != nil {
			return setupErr
		}
		for i := range n {
			if snapErr := emitSnapshot(h, "savings", i); snapErr != nil {
				return snapErr
			}
		}
		if err != nil {
			return err
		}
		return h.Close()
	}
}

func addFutureSpecialDate(t *testing.T, svc *model.Service) model.SpecialDate {
	t.Helper()
	sd, err := svc.UpsertSpecialDate(t.Context(), model.SpecialDateInput{Name: "Retirement", Date: date.Today().Add(2 * date.Year)})
	if err != nil {
		t.Fatalf("create special date: %v", err)
	}
	return sd
}

func TestDashboardForecastRunStreamsSnapshotsThenDone(t *testing.T) {
	svc, timers := newForecastTestService(t, emittingCompute(3, nil))
	addFutureSpecialDate(t, svc)
	startForecast(svc)

	sub := subscribe(t, svc)
	if sub.Status.State != model.ForecastStateIdle {
		t.Fatalf("expected idle before any Forecast Run, got %q", sub.Status.State)
	}
	timers.fire()

	got := received(sub.Events)
	expectEventTypes(t, got, events(model.ForecastEventStarted, snapshots(3), model.ForecastEventDone)...)

	after := subscribe(t, svc)
	if after.Status.State != model.ForecastStateDone {
		t.Fatalf("expected done after the Forecast Run, got %q", after.Status.State)
	}
	expectSameRows(t, after.Rows, snapshotRows(got))
}

func TestDashboardForecastRunWithTheServiceStoresTheForecast(t *testing.T) {
	svc, timers := newForecastTestService(t, nil)
	ctx := t.Context()
	at, err := svc.UpsertAccountType(ctx, model.AccountTypeInput{Name: "Savings", Color: "#00ff00"})
	if err != nil {
		t.Fatalf("create account type: %v", err)
	}
	acc, err := svc.UpsertAccount(ctx, model.AccountInput{Name: "My Savings", TypeID: at.ID})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	if _, err := svc.UpsertAccountSnapshot(ctx, acc.ID, model.AccountSnapshotInput{Date: mustParseDate("2026-01-01"), Balance: newFixedValue(10000)}); err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	if _, err := svc.UpsertAccountGrowthModel(ctx, model.AccountGrowthModelInput{
		AccountID: acc.ID, Type: "fixed", AnnualRate: newFixedValue(0.05), AnnualVolatility: newFixedValue(0), StartDate: mustParseDate("2026-01-01"),
	}); err != nil {
		t.Fatalf("create growth model: %v", err)
	}
	addFutureSpecialDate(t, svc)
	startForecast(svc)

	sub := subscribe(t, svc)
	timers.fire()

	got := received(sub.Events)
	if len(got) < 3 || got[0].Type != model.ForecastEventStarted || got[len(got)-1].Type != model.ForecastEventDone {
		t.Fatalf("expected Started, Snapshots, Done, got %v", eventTypes(got))
	}
	streamed := snapshotRows(got)
	for _, row := range streamed {
		if row.AccountTypeID == at.ID && row.Balance <= 0 {
			t.Fatalf("expected a positive balance for %s, got %f", at.ID, row.Balance)
		}
	}
	if !slices.ContainsFunc(streamed, func(row model.ForecastCacheRow) bool { return row.AccountTypeID == at.ID }) {
		t.Fatalf("expected forecast rows for account type %s, got %v", at.ID, streamed)
	}
	expectSameRows(t, subscribe(t, svc).Rows, streamed)
}

func TestDashboardForecastRunForecastsThroughTheLastSpecialDate(t *testing.T) {
	var got []model.PredictionParams
	svc, timers := newForecastTestService(t, computeFunc(func(_ context.Context, _ model.PredictionEventHandler, params model.PredictionParams) error {
		got = append(got, params)
		return nil
	}))
	ctx := t.Context()
	// Whether a Special Date is after the Forecast Start is up to the Forecast, not today.
	last := mustParseDate("2045-06-01")
	for _, d := range []date.Date{mustParseDate("2030-01-01"), last, mustParseDate("2020-01-01")} {
		if _, err := svc.UpsertSpecialDate(ctx, model.SpecialDateInput{Name: d.String(), Date: d}); err != nil {
			t.Fatalf("create special date: %v", err)
		}
	}
	if err := svc.SetForecastSamples(ctx, 123); err != nil {
		t.Fatalf("set forecast samples: %v", err)
	}
	if err := svc.SetForecastSnapshotInterval(ctx, "*-*-15"); err != nil {
		t.Fatalf("set forecast interval: %v", err)
	}
	startForecast(svc)
	timers.fire()

	// Without lower and upper bounds: a Quantile of 0.
	want := model.PredictionParams{
		Through:          last,
		Samples:          ptr[int64](123),
		Quantile:         ptr(0.0),
		SnapshotInterval: date.Cron("*-*-15"),
		GroupBy:          model.GroupByType,
	}
	if len(got) != 1 || !reflect.DeepEqual(got[0], want) {
		t.Fatalf("expected one Forecast with %+v, got %+v", want, got)
	}
}

// storeForecastRows runs one Forecast Run that stores n rows.
func storeForecastRows(t *testing.T, svc *model.Service, timers *fakeTimers, n int) {
	t.Helper()
	timers.fire()
	if rows := subscribe(t, svc).Rows; len(rows) != n {
		t.Fatalf("expected %d stored rows, got %d", n, len(rows))
	}
}

func TestDashboardForecastRunWithoutSpecialDatesClearsTheForecast(t *testing.T) {
	computed := 0
	svc, timers := newForecastTestService(t, computeFunc(func(ctx context.Context, h model.PredictionEventHandler, p model.PredictionParams) error {
		computed++
		return emittingCompute(3, nil)(ctx, h, p)
	}))
	sd := addFutureSpecialDate(t, svc)
	startForecast(svc)
	storeForecastRows(t, svc, timers, 3)

	if err := svc.DeleteSpecialDate(t.Context(), sd.ID); err != nil {
		t.Fatalf("delete special date: %v", err)
	}
	sub := subscribe(t, svc)
	timers.fire()

	expectEventTypes(t, received(sub.Events), model.ForecastEventStarted, model.ForecastEventDone)
	if rows := subscribe(t, svc).Rows; len(rows) != 0 {
		t.Fatalf("expected the Dashboard Forecast to be empty, got %v", rows)
	}
	if computed != 1 {
		t.Fatalf("expected no prediction without a Special Date, got %d predictions in total", computed)
	}
}

func TestDashboardForecastRunWithOnlySpecialDatesOnOrBeforeTheForecastStartClearsTheForecast(t *testing.T) {
	svc, timers := newForecastTestService(t, nil)
	ctx := t.Context()
	start := mustParseDate("2026-01-01")
	mustSnapshot(t, svc, mustAccount(t, svc, "Checking").ID, start, 1000)
	if err := svc.SetForecastSnapshotInterval(ctx, "*-*-01"); err != nil {
		t.Fatalf("set forecast interval: %v", err)
	}
	sd, err := svc.UpsertSpecialDate(ctx, model.SpecialDateInput{Name: "Retirement", Date: mustParseDate("2027-01-01")})
	if err != nil {
		t.Fatalf("create special date: %v", err)
	}
	startForecast(svc)
	timers.fire()
	if rows := subscribe(t, svc).Rows; len(rows) != 13 {
		t.Fatalf("expected a row on the first of every month through the Special Date, got %v", rows)
	}

	if _, err := svc.UpsertSpecialDate(ctx, model.SpecialDateInput{ID: sd.ID, Name: sd.Name, Date: start}); err != nil {
		t.Fatalf("move special date to the Forecast Start: %v", err)
	}
	if _, err := svc.UpsertSpecialDate(ctx, model.SpecialDateInput{Name: "Graduation", Date: start.Add(-date.Year)}); err != nil {
		t.Fatalf("create earlier special date: %v", err)
	}
	sub := subscribe(t, svc)
	timers.fire()

	expectEventTypes(t, received(sub.Events), model.ForecastEventStarted, model.ForecastEventDone)
	if rows := subscribe(t, svc).Rows; len(rows) != 0 {
		t.Fatalf("expected the Dashboard Forecast to be empty, got %v", rows)
	}
}

func TestDashboardForecastRunFailsOnComputeError(t *testing.T) {
	svc, timers := newForecastTestService(t, emittingCompute(2, errors.New("simulation diverged")))
	addFutureSpecialDate(t, svc)
	startForecast(svc)

	sub := subscribe(t, svc)
	timers.fire()

	got := received(sub.Events)
	expectEventTypes(t, got, events(model.ForecastEventStarted, snapshots(2), model.ForecastEventFailed)...)
	if msg := got[len(got)-1].Message; !strings.Contains(msg, "simulation diverged") {
		t.Fatalf("expected the Failed message to carry the error, got %q", msg)
	}
	expectSameRows(t, subscribe(t, svc).Rows, snapshotRows(got))
	if n := timers.pending(); n != 0 {
		t.Fatalf("expected no Forecast Run scheduled after it Failed, got %d", n)
	}
}

func TestDashboardForecastRunFailsOnComputePanic(t *testing.T) {
	svc, timers := newForecastTestService(t, computeFunc(func(ctx context.Context, h model.PredictionEventHandler, p model.PredictionParams) error {
		if err := emitSnapshot(h, "savings", 0); err != nil {
			return err
		}
		panic("negative variance")
	}))
	addFutureSpecialDate(t, svc)
	logger := startForecast(svc)

	sub := subscribe(t, svc)
	timers.fire()

	got := received(sub.Events)
	expectEventTypes(t, got, events(model.ForecastEventStarted, snapshots(1), model.ForecastEventFailed)...)
	if msg := got[len(got)-1].Message; !strings.Contains(msg, "negative variance") {
		t.Fatalf("expected the Failed message to carry the panic value, got %q", msg)
	}
	if log := logger.String(); !strings.Contains(log, "negative variance") || !strings.Contains(log, "runtime/debug.Stack") {
		t.Fatalf("expected the panic to be logged with its stack, got %q", log)
	}
	expectSameRows(t, subscribe(t, svc).Rows, snapshotRows(got))
	if n := timers.pending(); n != 0 {
		t.Fatalf("expected no Forecast Run scheduled after it Failed, got %d", n)
	}
}

func TestDashboardForecastSubscribeAfterFailedRunReturnsTheFailure(t *testing.T) {
	svc, timers := newForecastTestService(t, emittingCompute(0, errors.New("simulation diverged")))
	addFutureSpecialDate(t, svc)
	startForecast(svc)
	timers.fire()

	sub := subscribe(t, svc)
	if sub.Status.State != model.ForecastStateFailed || !strings.Contains(sub.Status.Message, "simulation diverged") {
		t.Fatalf("expected failed with the error, got %+v", sub.Status)
	}
}

// TestDashboardForecastRunPaysInterestIntoAnAccountWithoutASnapshot is the setup from issue #130,
// which used to fail the Forecast Run. Every account takes part in a Forecast, so the interest lands
// in the account without a Snapshot.
func TestDashboardForecastRunPaysInterestIntoAnAccountWithoutASnapshot(t *testing.T) {
	svc, timers := newForecastTestService(t, nil)
	ctx := t.Context()
	interestType := mustAccountType(t, svc, "Interest", "#ff0000")
	savingsType := mustAccountType(t, svc, "Savings", "#00ff00")
	destination := mustTypedAccount(t, svc, "No snapshots", interestType.ID)
	acc, err := svc.UpsertAccount(ctx, model.AccountInput{Name: "Savings", TypeID: savingsType.ID, CashFlowFrequency: "*-*-01", CashFlowDestinationID: destination.ID})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	mustSnapshot(t, svc, acc.ID, mustParseDate("2026-01-01"), 10000)
	if _, err := svc.UpsertAccountGrowthModel(ctx, model.AccountGrowthModelInput{
		AccountID: acc.ID, Type: "fixed", AnnualRate: newFixedValue(0.05), AnnualVolatility: newFixedValue(0), StartDate: mustParseDate("2026-01-01"),
	}); err != nil {
		t.Fatalf("create growth model: %v", err)
	}
	if err := svc.SetForecastSnapshotInterval(ctx, "*-*-02"); err != nil {
		t.Fatalf("set forecast interval: %v", err)
	}
	if _, err := svc.UpsertSpecialDate(ctx, model.SpecialDateInput{Name: "Retirement", Date: mustParseDate("2026-02-02")}); err != nil {
		t.Fatalf("create special date: %v", err)
	}
	startForecast(svc)

	sub := subscribe(t, svc)
	timers.fire()

	got := received(sub.Events)
	if len(got) < 2 || got[0].Type != model.ForecastEventStarted || got[len(got)-1].Type != model.ForecastEventDone {
		t.Fatalf("expected Started, forecast points, then Done, got %v", eventTypes(got))
	}
	// The Forecast Start's interest is paid out on it, then January's on February 1.
	daily := math.Pow(1.05, 1.0/365) - 1
	interest := map[string]float64{
		"2026-01-02": 10000 * daily,
		"2026-02-02": 10000*daily + 10000*(math.Pow(1+daily, 31)-1),
	}
	rows := subscribe(t, svc).Rows
	if len(rows) != 4 {
		t.Fatalf("expected a row per Account Type on 2026-01-02 and 2026-02-02, got %v", rows)
	}
	for _, row := range rows {
		switch row.AccountTypeID {
		case savingsType.ID:
			if row.Balance != 10000 {
				t.Errorf("expected the savings to keep 10000 on %s, got %f", row.Date, row.Balance)
			}
		case interestType.ID:
			if want := interest[row.Date.String()]; math.Abs(row.Balance-want) > 1e-6 {
				t.Errorf("expected the interest %f in the account without a Snapshot on %s, got %f", want, row.Date, row.Balance)
			}
		default:
			t.Errorf("unexpected row %+v", row)
		}
	}
}

// blockingCompute is a fake compute whose first prediction sends one forecast point, signals
// computing and waits to be cancelled, then ends with abandon. Later predictions send n points.
func blockingCompute(computing chan<- struct{}, n int, abandon func(ctx context.Context) error) computeFunc {
	var calls sync.Mutex
	first := true
	return func(ctx context.Context, h model.PredictionEventHandler, p model.PredictionParams) error {
		calls.Lock()
		isFirst := first
		first = false
		calls.Unlock()
		if !isFirst {
			return emittingCompute(n, nil)(ctx, h, p)
		}
		if err := emitSnapshot(h, "savings", 0); err != nil {
			return err
		}
		close(computing)
		<-ctx.Done()
		return abandon(ctx)
	}
}

func TestDashboardForecastRunSupersededByMarkStaleEmitsNoOutcome(t *testing.T) {
	for name, abandon := range map[string]func(ctx context.Context) error{
		"returning the context error": func(ctx context.Context) error { return ctx.Err() },
		"returning another error":     func(ctx context.Context) error { return errors.New("interrupted") },
		"panicking":                   func(ctx context.Context) error { panic("interrupted") },
	} {
		t.Run(name, func(t *testing.T) {
			computing := make(chan struct{})
			svc, timers := newForecastTestService(t, blockingCompute(computing, 2, abandon))
			addFutureSpecialDate(t, svc)
			startForecast(svc)

			sub := subscribe(t, svc)
			done := timers.fireInBackground()
			<-computing
			svc.DashboardForecast().MarkStale()
			<-done

			expectEventTypes(t, received(sub.Events), model.ForecastEventStarted, model.ForecastEventSnapshot)
			if status := subscribe(t, svc).Status; status.State != model.ForecastStateRunning {
				t.Fatalf("expected the superseded Forecast Run to leave the status running, got %+v", status)
			}

			timers.fire()
			expectEventTypes(t, received(sub.Events), events(model.ForecastEventStarted, snapshots(2), model.ForecastEventDone)...)
		})
	}
}

func TestDashboardForecastDebouncesMarkStaleIntoOneRun(t *testing.T) {
	computed := 0
	svc, timers := newForecastTestService(t, computeFunc(func(ctx context.Context, h model.PredictionEventHandler, p model.PredictionParams) error {
		computed++
		return emittingCompute(1, nil)(ctx, h, p)
	}))
	addFutureSpecialDate(t, svc)
	startForecast(svc)

	forecast := svc.DashboardForecast()
	forecast.MarkStale()
	forecast.MarkStale()
	forecast.MarkStale()
	if n := timers.pending(); n != 1 {
		t.Fatalf("expected one scheduled Forecast Run, got %d", n)
	}
	sub := subscribe(t, svc)
	timers.fire()

	expectEventTypes(t, received(sub.Events), model.ForecastEventStarted, model.ForecastEventSnapshot, model.ForecastEventDone)
	if computed != 1 {
		t.Fatalf("expected one prediction, got %d", computed)
	}
}

func TestDashboardForecastSchedulesNothingBeforeStart(t *testing.T) {
	svc, timers := newForecastTestService(t, nil)
	seedForecastableData(t, svc)
	svc.DashboardForecast().MarkStale()
	if n := timers.pending(); n != 0 {
		t.Fatalf("expected nothing scheduled before Start, got %d", n)
	}

	startForecast(svc)
	if n := timers.pending(); n != 1 {
		t.Fatalf("expected Start to schedule the first Forecast Run, got %d", n)
	}
}

func TestDashboardForecastSchedulesNothingAfterStop(t *testing.T) {
	svc, timers := newForecastTestService(t, nil)
	startForecast(svc)
	svc.DashboardForecast().Stop()
	if n := timers.pending(); n != 0 {
		t.Fatalf("expected Stop to cancel the scheduled Forecast Run, got %d", n)
	}

	svc.DashboardForecast().MarkStale()
	if err := svc.SetForecastSamples(t.Context(), 100); err != nil {
		t.Fatalf("set forecast samples: %v", err)
	}
	if n := timers.pending(); n != 0 {
		t.Fatalf("expected nothing scheduled after Stop, got %d", n)
	}
}

func TestDashboardForecastStopWaitsForTheRunningForecastRun(t *testing.T) {
	computing, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	svc, timers := newForecastTestService(t, computeFunc(func(ctx context.Context, h model.PredictionEventHandler, p model.PredictionParams) error {
		if err := emitSnapshot(h, "savings", 0); err != nil {
			return err
		}
		close(computing)
		<-ctx.Done()
		close(cancelled)
		<-release
		return ctx.Err()
	}))
	addFutureSpecialDate(t, svc)
	startForecast(svc)
	sub := subscribe(t, svc)

	done := timers.fireInBackground()
	<-computing
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		svc.DashboardForecast().Stop()
	}()
	<-cancelled
	select {
	case <-stopped:
		t.Fatal("Stop returned before the Forecast Run exited")
	default:
	}
	close(release)
	<-stopped
	<-done

	var got []model.ForecastEvent
	for evt := range sub.Events {
		got = append(got, evt)
	}
	expectEventTypes(t, got, model.ForecastEventStarted, model.ForecastEventSnapshot)
	sub.Unsubscribe()

	after := subscribe(t, svc)
	if _, open := <-after.Events; open {
		t.Fatal("expected subscribing after Stop to return closed events")
	}
}

func TestDashboardForecastUnsubscribeIsSafeToRepeat(t *testing.T) {
	svc, timers := newForecastTestService(t, emittingCompute(1, nil))
	addFutureSpecialDate(t, svc)
	startForecast(svc)

	sub := subscribe(t, svc)
	sub.Unsubscribe()
	sub.Unsubscribe()
	timers.fire()
	if got := received(sub.Events); len(got) != 0 {
		t.Fatalf("expected no events after Unsubscribe, got %v", eventTypes(got))
	}
	if _, open := <-sub.Events; open {
		t.Fatal("expected Unsubscribe to close the events")
	}
}

func TestDashboardForecastSubscribeMidRunReturnsRunningAndTheRowsSoFar(t *testing.T) {
	paused, resume := make(chan struct{}), make(chan struct{})
	svc, timers := newForecastTestService(t, computeFunc(func(ctx context.Context, h model.PredictionEventHandler, p model.PredictionParams) error {
		for i := range 4 {
			if i == 2 {
				close(paused)
				<-resume
			}
			if err := emitSnapshot(h, "savings", i); err != nil {
				return err
			}
		}
		return nil
	}))
	addFutureSpecialDate(t, svc)
	startForecast(svc)

	done := timers.fireInBackground()
	<-paused
	sub := subscribe(t, svc)
	close(resume)
	<-done

	if sub.Status.State != model.ForecastStateRunning {
		t.Fatalf("expected running mid-run, got %+v", sub.Status)
	}
	if len(sub.Rows) != 2 {
		t.Fatalf("expected the 2 rows stored so far, got %v", sub.Rows)
	}
	got := received(sub.Events)
	expectEventTypes(t, got, model.ForecastEventSnapshot, model.ForecastEventSnapshot, model.ForecastEventDone)
	expectSameRows(t, append(sub.Rows, snapshotRows(got)...), subscribe(t, svc).Rows)
}

func TestDashboardForecastSubscribeWhileRowsAreStoredSeesEachRowOnce(t *testing.T) {
	const n = 50 // fewer events than a subscriber buffers
	storing := make(chan struct{})
	svc, timers := newForecastTestService(t, computeFunc(func(ctx context.Context, h model.PredictionEventHandler, p model.PredictionParams) error {
		for i := range n {
			if err := emitSnapshot(h, "savings", i); err != nil {
				return err
			}
			if i == 0 {
				close(storing)
			}
		}
		return nil
	}))
	addFutureSpecialDate(t, svc)
	startForecast(svc)

	done := timers.fireInBackground()
	<-storing
	sub := subscribe(t, svc)
	<-done

	// The Forecast Run may have finished before Subscribe got its turn: then Done is in the status.
	got := received(sub.Events)
	endsDone := len(got) > 0 && got[len(got)-1].Type == model.ForecastEventDone
	if !endsDone && sub.Status.State != model.ForecastStateDone {
		t.Fatalf("expected Done in the status or the events, got %+v and %v", sub.Status, eventTypes(got))
	}
	stored := subscribe(t, svc).Rows
	if len(stored) != n {
		t.Fatalf("expected %d stored rows, got %d", n, len(stored))
	}
	expectSameRows(t, append(sub.Rows, snapshotRows(got)...), stored)
}

func TestDashboardForecastDisconnectsASubscriberThatFallsBehind(t *testing.T) {
	const n = 100
	ack, fastClosed := make(chan struct{}), make(chan struct{})
	svc, timers := newForecastTestService(t, computeFunc(func(ctx context.Context, h model.PredictionEventHandler, p model.PredictionParams) error {
		for i := range n {
			if err := emitSnapshot(h, "savings", i); err != nil {
				return err
			}
			select {
			case <-ack:
			case <-fastClosed:
				return errors.New("the fast subscriber was disconnected")
			}
		}
		return nil
	}))
	addFutureSpecialDate(t, svc)
	startForecast(svc)

	slow := subscribe(t, svc)
	fast := subscribe(t, svc)
	var fastEvents []model.ForecastEvent
	go func() {
		defer close(fastClosed)
		for evt := range fast.Events {
			fastEvents = append(fastEvents, evt)
			switch evt.Type {
			case model.ForecastEventSnapshot:
				ack <- struct{}{}
			case model.ForecastEventDone, model.ForecastEventFailed:
				return
			}
		}
	}()
	timers.fire()
	<-fastClosed

	expectEventTypes(t, fastEvents, events(model.ForecastEventStarted, snapshots(n), model.ForecastEventDone)...)
	if got := received(slow.Events); len(got) != 64 {
		t.Fatalf("expected the slow subscriber to get its 64 buffered events, got %d", len(got))
	}
	if _, open := <-slow.Events; open {
		t.Fatal("expected the slow subscriber to be disconnected")
	}
	slow.Unsubscribe()
	if status := subscribe(t, svc).Status; status.State != model.ForecastStateDone {
		t.Fatalf("expected the Forecast Run to complete, got %+v", status)
	}
}

func TestMigratingTheDashboardForecastToDaysClearsItAndTheConfidenceSetting(t *testing.T) {
	ctx := t.Context()
	conn := memdb.TestDB(t)
	old, err := model.Open(ctx, migrationsBefore(t, "0030"), "static/migrations", conn)
	if err != nil {
		t.Fatalf("open database before the Dashboard Forecast is stored in days: %v", err)
	}
	if _, err := old.DB().ExecContext(ctx, `INSERT INTO forecast_cache (date, account_type_id, median, lower_bound, upper_bound) VALUES (?, 'savings', 1000, 900, 1100)`,
		date.Today().Add(date.Day).ToStdTime().UnixMilli()); err != nil {
		t.Fatalf("store a Dashboard Forecast row in milliseconds: %v", err)
	}
	if _, err := old.DB().ExecContext(ctx, `INSERT INTO app_setting (key, value) VALUES ('forecast_confidence', '0.9')`); err != nil {
		t.Fatalf("store the forecast confidence: %v", err)
	}
	if err := old.SetForecastSamples(ctx, 500); err != nil {
		t.Fatalf("set forecast samples: %v", err)
	}
	old.DB().Close()

	svc, err := model.Open(ctx, pefigo.StaticEmbeddedFS, "static/migrations", conn)
	if err != nil {
		t.Fatalf("migrate the Dashboard Forecast to days: %v", err)
	}
	t.Cleanup(func() { svc.DB().Close() })

	if rows := subscribe(t, svc).Rows; len(rows) != 0 {
		t.Fatalf("expected the Dashboard Forecast to be cleared, got %v", rows)
	}
	var confidence int
	if err := svc.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM app_setting WHERE key = 'forecast_confidence'`).Scan(&confidence); err != nil || confidence != 0 {
		t.Fatalf("expected the forecast confidence setting to be deleted, got %d (%v)", confidence, err)
	}
	if samples, err := svc.GetForecastSamples(ctx); err != nil || samples != 500 {
		t.Fatalf("expected the other forecast settings to be kept, got %d samples (%v)", samples, err)
	}
}
