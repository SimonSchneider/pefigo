package controller_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo"
	"github.com/SimonSchneider/pefigo/internal/controller"
	"github.com/SimonSchneider/pefigo/internal/model"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
	_ "github.com/ncruces/go-sqlite3/embed"
	"github.com/ncruces/go-sqlite3/vfs/memdb"
)

// streamGuard only turns a hung stream into a test failure: the tests synchronise on the events
// they read, never on time.
const streamGuard = 10 * time.Second

// fakeTimers replaces the debounce timer, so a test fires the scheduled Forecast Run by hand.
type fakeTimers struct {
	t      *testing.T
	mu     sync.Mutex
	timers []*fakeTimer
}

type fakeTimer struct {
	owner *fakeTimers
	f     func()
	over  bool // stopped or fired
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
	if t.over {
		return false
	}
	t.over = true
	return true
}

// take marks the one pending timer fired and returns its func.
func (ts *fakeTimers) take() func() {
	ts.t.Helper()
	ts.mu.Lock()
	defer ts.mu.Unlock()
	var pending []*fakeTimer
	for _, timer := range ts.timers {
		if !timer.over {
			pending = append(pending, timer)
		}
	}
	if len(pending) != 1 {
		ts.t.Fatalf("expected exactly one pending forecast timer, got %d", len(pending))
	}
	pending[0].over = true
	return pending[0].f
}

// fire runs the scheduled Forecast Run in the caller: when it returns, every event was emitted.
func (ts *fakeTimers) fire() {
	ts.t.Helper()
	ts.take()()
}

// fireInBackground runs the scheduled Forecast Run on another goroutine. The returned channel is
// closed once it has returned.
func (ts *fakeTimers) fireInBackground() <-chan struct{} {
	ts.t.Helper()
	f := ts.take()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()
	return done
}

// computeFunc is a fake ForecastCompute.
type computeFunc func(ctx context.Context, h model.PredictionEventHandler, params model.PredictionParams) error

func (f computeFunc) RunPrediction(ctx context.Context, h model.PredictionEventHandler, params model.PredictionParams) error {
	return f(ctx, h, params)
}

// forecastRows is n forecast points of the Account Type typeID, one per day from tomorrow.
func forecastRows(typeID string, n int) []model.ForecastCacheRow {
	rows := make([]model.ForecastCacheRow, n)
	for i := range rows {
		rows[i] = model.ForecastCacheRow{
			AccountTypeID: typeID,
			BalancePoint:  model.BalancePoint{Date: date.Today().Add(date.Duration(i+1) * date.Day), Balance: float64(1000 * (i + 1))},
		}
	}
	return rows
}

func emitRow(h model.PredictionEventHandler, row model.ForecastCacheRow) error {
	return h.Snapshot(model.PredictionBalanceSnapshot{
		ID:         row.AccountTypeID,
		Day:        row.Date,
		Balance:    row.Balance,
		LowerBound: row.Balance * 0.9,
		UpperBound: row.Balance * 1.1,
	})
}

// emitting is a fake compute that sends rows as the prediction's snapshots, then returns err.
func emitting(rows []model.ForecastCacheRow, err error) computeFunc {
	return func(_ context.Context, h model.PredictionEventHandler, _ model.PredictionParams) error {
		for _, row := range rows {
			if emitErr := emitRow(h, row); emitErr != nil {
				return emitErr
			}
		}
		return err
	}
}

// streamFixture is a started Dashboard Forecast with fake timers and a fake compute, served by the
// app's handler. Savings has history and Pension has none, so Pension only gets a series when it
// has forecast rows.
type streamFixture struct {
	svc     *model.Service
	timers  *fakeTimers
	srv     *httptest.Server
	savings model.AccountType
	pension model.AccountType
	// savingsAccount is the account of Savings, with one Snapshot on historyDate.
	savingsAccount model.Account
	special        model.SpecialDate
	// compute is what each Forecast Run predicts with. Set it before firing the timer.
	compute computeFunc
}

func newStreamFixture(t *testing.T) *streamFixture {
	t.Helper()
	fx := &streamFixture{timers: &fakeTimers{t: t}}
	svc, err := model.Open(t.Context(), pefigo.StaticEmbeddedFS, "static/migrations", memdb.TestDB(t),
		model.WithForecastTimer(fx.timers.afterFunc),
		model.WithForecastCompute(computeFunc(func(ctx context.Context, h model.PredictionEventHandler, p model.PredictionParams) error {
			return fx.compute(ctx, h, p)
		})),
	)
	if err != nil {
		t.Fatalf("failed to create test db: %v", err)
	}
	t.Cleanup(func() {
		svc.DashboardForecast().Stop()
		svc.DB().Close()
	})
	fx.svc = svc

	ctx := t.Context()
	if fx.savings, err = svc.UpsertAccountType(ctx, model.AccountTypeInput{Name: "Savings", Color: "#00ff00"}); err != nil {
		t.Fatalf("create account type: %v", err)
	}
	if fx.pension, err = svc.UpsertAccountType(ctx, model.AccountTypeInput{Name: "Pension", Color: "#0000ff"}); err != nil {
		t.Fatalf("create account type: %v", err)
	}
	if fx.savingsAccount, err = svc.UpsertAccount(ctx, model.AccountInput{Name: "My Savings", TypeID: fx.savings.ID}); err != nil {
		t.Fatalf("create account: %v", err)
	}
	if _, err := svc.UpsertAccountSnapshot(ctx, fx.savingsAccount.ID, model.AccountSnapshotInput{Date: historyDate(t), Balance: uncertain.NewFixed(10000)}); err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	if fx.special, err = svc.UpsertSpecialDate(ctx, model.SpecialDateInput{Name: "Retirement", Date: date.Today().Add(2 * date.Year), Color: "#ff0000"}); err != nil {
		t.Fatalf("create special date: %v", err)
	}

	svc.DashboardForecast().Start(nil)
	fx.srv = fx.serve(t, controller.NewHandler(svc, fstest.MapFS{}))
	return fx
}

// serve serves h until the test ends. Stopping the Dashboard Forecast before closing the server
// closes every subscription, so a failing test can't hang on a stream handler that is still open.
func (fx *streamFixture) serve(t *testing.T, h http.Handler) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	t.Cleanup(fx.stop)
	return srv
}

func historyDate(t *testing.T) date.Date {
	t.Helper()
	d, err := date.ParseDate("2026-01-01")
	if err != nil {
		t.Fatalf("parse date: %v", err)
	}
	return d
}

// savingsSeries is Savings with its history point.
func (fx *streamFixture) savingsSeries(t *testing.T) model.DashboardForecastSeries {
	return model.DashboardForecastSeries{
		ID:      fx.savings.ID,
		Name:    "Savings",
		Color:   "#00ff00",
		History: []model.BalancePoint{{Date: historyDate(t), Balance: 10000}},
	}
}

// pensionSeries is Pension, which has no history points.
func (fx *streamFixture) pensionSeries() model.DashboardForecastSeries {
	return model.DashboardForecastSeries{ID: fx.pension.ID, Name: "Pension", Color: "#0000ff", History: []model.BalancePoint{}}
}

func (fx *streamFixture) connect(t *testing.T) *sseStream {
	t.Helper()
	return connect(t, fx.srv)
}

func (fx *streamFixture) stop() {
	fx.svc.DashboardForecast().Stop()
}

// marklines are the date lines of today and the Special Date.
func (fx *streamFixture) marklines() []model.Markline {
	return []model.Markline{
		{Date: date.Today(), Name: "Today"},
		{Date: fx.special.Date, Color: "#ff0000", Name: "Retirement"},
	}
}

// setup is the setup of the series, with the date lines of today and the Special Date.
func (fx *streamFixture) setup(series ...model.DashboardForecastSeries) model.ForecastDashboardData {
	return model.ForecastDashboardData{Series: series, Marklines: fx.marklines()}
}

// expectSetup expects a setup event that carries want. Every series' history must be a list, even
// an empty one.
func expectSetup(s *sseStream, want model.ForecastDashboardData) {
	s.t.Helper()
	var got model.ForecastDashboardData
	decode(s.t, s.expect("setup"), &got)
	sameSeries := func(got, want model.DashboardForecastSeries) bool {
		return got.ID == want.ID && got.Name == want.Name && got.Color == want.Color &&
			got.History != nil && slices.Equal(got.History, want.History)
	}
	if !slices.EqualFunc(got.Series, want.Series, sameSeries) || !slices.Equal(got.Marklines, want.Marklines) {
		s.t.Fatalf("expected the setup %+v, got %+v", want, got)
	}
}

type sseEvent struct {
	Name string
	Data string
}

// sseStream reads the events of one connection to the Dashboard Forecast stream.
type sseStream struct {
	t      *testing.T
	r      *bufio.Reader
	cancel context.CancelFunc
	read   []string // the names of the events read so far, for failure messages
}

func connect(t *testing.T, srv *httptest.Server) *sseStream {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), streamGuard)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/dashboard/forecast/stream", nil)
	if err != nil {
		cancel()
		t.Fatalf("new request: %v", err)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		cancel()
		t.Fatalf("connect to the stream: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		resp.Body.Close()
	})
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected status 200, got %d: %s", resp.StatusCode, body)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("expected an event stream, got content type %q", ct)
	}
	return &sseStream{t: t, r: bufio.NewReader(resp.Body), cancel: cancel}
}

// next reads the next event. ok is false when the stream ended, which happens only once the
// handler has returned.
func (s *sseStream) next() (evt sseEvent, ok bool) {
	s.t.Helper()
	hasData := false
	for {
		line, err := s.r.ReadString('\n')
		if errors.Is(err, io.EOF) && line == "" && evt.Name == "" && !hasData {
			return sseEvent{}, false
		}
		if err != nil {
			s.t.Fatalf("reading the stream after %v: %v", s.read, err)
		}
		line = strings.TrimSuffix(line, "\n")
		switch {
		case line == "":
			if evt.Name != "" || hasData {
				s.read = append(s.read, evt.Name)
				return evt, true
			}
		case strings.HasPrefix(line, "event:"):
			evt.Name = strings.TrimPrefix(strings.TrimPrefix(line, "event:"), " ")
		case strings.HasPrefix(line, "data:"):
			if hasData {
				evt.Data += "\n"
			}
			evt.Data += strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " ")
			hasData = true
		}
	}
}

// expect reads the next event and fails unless it is called one of names.
func (s *sseStream) expect(names ...string) sseEvent {
	s.t.Helper()
	evt, ok := s.next()
	if !ok {
		s.t.Fatalf("expected %q after %v, but the stream ended", names, s.read)
	}
	if !slices.Contains(names, evt.Name) {
		s.t.Fatalf("expected %q after %v, got %q", names, s.read[:len(s.read)-1], evt.Name)
	}
	return evt
}

// expectEnd fails unless the stream ends without another event.
func (s *sseStream) expectEnd() {
	s.t.Helper()
	if evt, ok := s.next(); ok {
		s.t.Fatalf("expected the stream to end after %v, got %q", s.read[:len(s.read)-1], evt.Name)
	}
}

// close disconnects the client.
func (s *sseStream) close() {
	s.cancel()
}

// expectSnapshots expects one snapshot event per row, each carrying the row's JSON.
func expectSnapshots(s *sseStream, rows ...model.ForecastCacheRow) {
	s.t.Helper()
	for _, row := range rows {
		evt := s.expect("snapshot")
		want, err := json.Marshal(row)
		if err != nil {
			s.t.Fatalf("marshal row: %v", err)
		}
		if evt.Data != string(want) {
			s.t.Fatalf("expected snapshot %s, got %s", want, evt.Data)
		}
	}
}

// expectFailed expects a failed event whose message contains msg.
func expectFailed(s *sseStream, msg string) {
	s.t.Helper()
	var got map[string]string
	decode(s.t, s.expect("failed"), &got)
	if !strings.Contains(got["message"], msg) {
		s.t.Fatalf("expected the failure message to contain %q, got %v", msg, got)
	}
}

func decode(t *testing.T, evt sseEvent, v any) {
	t.Helper()
	if err := json.Unmarshal([]byte(evt.Data), v); err != nil {
		t.Fatalf("decode %q event %q: %v", evt.Name, evt.Data, err)
	}
}

func TestDashboardForecastStreamWhenIdleSendsTheSetupUntilStopped(t *testing.T) {
	fx := newStreamFixture(t)

	stream := fx.connect(t)
	// No Forecast Run has stored a row yet, and the series is still identified by its Account Type.
	expectSetup(stream, fx.setup(fx.savingsSeries(t)))

	// Stopping closes the subscription, which ends the stream without another event.
	fx.stop()
	stream.expectEnd()
}

func TestDashboardForecastStreamForwardsAForecastRunThatIsDone(t *testing.T) {
	fx := newStreamFixture(t)
	rows := forecastRows(fx.savings.ID, 3)
	fx.compute = emitting(rows, nil)

	live := fx.connect(t)
	expectSetup(live, fx.setup(fx.savingsSeries(t)))
	fx.timers.fire()
	after := fx.connect(t)
	// Stopping ends both streams, so a missing event fails at once instead of at the guard.
	fx.stop()

	// A Forecast Run that starts while connected is preceded by the current setup.
	expectSetup(live, fx.setup(fx.savingsSeries(t)))
	live.expect("started")
	expectSnapshots(live, rows...)
	live.expect("done")
	live.expectEnd()

	// Connecting after the Forecast Run was Done sends its rows, without started or done.
	expectSetup(after, fx.setup(fx.savingsSeries(t)))
	expectSnapshots(after, rows...)
	after.expectEnd()
}

func TestDashboardForecastStreamMidRunSendsTheSetupOnceAndTheForecastRunSoFar(t *testing.T) {
	fx := newStreamFixture(t)
	rows := forecastRows(fx.pension.ID, 4)
	paused, resume := make(chan struct{}), make(chan struct{})
	fx.compute = func(ctx context.Context, h model.PredictionEventHandler, _ model.PredictionParams) error {
		for i, row := range rows {
			if i == 2 {
				close(paused)
				select {
				case <-resume:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			if err := emitRow(h, row); err != nil {
				return err
			}
		}
		return nil
	}

	ran := fx.timers.fireInBackground()
	<-paused
	stream := fx.connect(t)
	// Pension has no history, but it has forecast rows, so it gets a series too.
	expectSetup(stream, fx.setup(fx.pensionSeries(), fx.savingsSeries(t)))
	// The handler subscribed before sending the setup: it holds the first two rows, and the
	// other two arrive live.
	close(resume)
	<-ran
	fx.stop()

	// The Forecast Run started before connecting, so its started follows the setup directly.
	stream.expect("started")
	expectSnapshots(stream, rows...)
	stream.expect("done")
	stream.expectEnd()
}

func TestDashboardForecastStreamSendsAFailedForecastRun(t *testing.T) {
	fx := newStreamFixture(t)
	rows := forecastRows(fx.pension.ID, 2)
	fx.compute = emitting(rows, errors.New("simulation diverged"))

	live := fx.connect(t)
	expectSetup(live, fx.setup(fx.savingsSeries(t)))
	fx.timers.fire()
	after := fx.connect(t)
	fx.stop()

	// Pension has no account, so only stored rows give it a series, and a starting Forecast Run
	// has none yet.
	expectSetup(live, fx.setup(fx.savingsSeries(t)))
	live.expect("started")
	expectSnapshots(live, rows...)
	expectFailed(live, "simulation diverged")
	live.expectEnd()

	// Connecting after the Forecast Run Failed sends the rows it stored and the failure.
	expectSetup(after, fx.setup(fx.pensionSeries(), fx.savingsSeries(t)))
	expectSnapshots(after, rows...)
	expectFailed(after, "simulation diverged")
	after.expectEnd()
}

func TestDashboardForecastStreamReturnsWhenTheClientGoesAway(t *testing.T) {
	fx := newStreamFixture(t)
	returned := make(chan struct{})
	handler := controller.NewHandler(fx.svc, fstest.MapFS{})
	srv := fx.serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(returned)
		handler.ServeHTTP(w, r)
	}))

	stream := connect(t, srv)
	expectSetup(stream, fx.setup(fx.savingsSeries(t)))
	stream.close()

	guard, cancel := context.WithTimeout(t.Context(), streamGuard)
	defer cancel()
	select {
	case <-returned:
	case <-guard.Done():
		t.Fatal("expected the stream handler to return once the client went away")
	}
}

// expectFields fails unless the JSON object raw has exactly the fields, and returns them.
func expectFields(t *testing.T, raw json.RawMessage, fields ...string) map[string]json.RawMessage {
	t.Helper()
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	got := slices.Sorted(maps.Keys(obj))
	if want := slices.Sorted(slices.Values(fields)); !slices.Equal(got, want) {
		t.Fatalf("expected the fields %v in %s, got %v", want, raw, got)
	}
	return obj
}

// expectDay fails unless raw is a JSON string of the form YYYY-MM-DD.
func expectDay(t *testing.T, raw json.RawMessage) {
	t.Helper()
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatalf("expected a date string, got %s", raw)
	}
	if _, err := time.Parse(time.DateOnly, s); err != nil {
		t.Fatalf("expected a YYYY-MM-DD date, got %q", s)
	}
}

// decodeList decodes the JSON list raw.
func decodeList(t *testing.T, raw json.RawMessage) []json.RawMessage {
	t.Helper()
	var list []json.RawMessage
	if err := json.Unmarshal(raw, &list); err != nil {
		t.Fatalf("expected a list, got %s: %v", raw, err)
	}
	return list
}

func TestDashboardForecastStreamSendsEveryPointAsADateAndABalance(t *testing.T) {
	fx := newStreamFixture(t)
	acc, err := fx.svc.UpsertAccount(t.Context(), model.AccountInput{Name: "No type"})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	if _, err := fx.svc.UpsertAccountSnapshot(t.Context(), acc.ID, model.AccountSnapshotInput{Date: historyDate(t), Balance: uncertain.NewFixed(500)}); err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	fx.compute = emitting(append(forecastRows(fx.savings.ID, 2), forecastRows("", 2)...), nil)
	fx.timers.fire()
	stream := fx.connect(t)
	fx.stop()

	setup := expectFields(t, json.RawMessage(stream.expect("setup").Data), "series", "marklines")
	var names []string
	for _, raw := range decodeList(t, setup["series"]) {
		series := expectFields(t, raw, "id", "name", "color", "history")
		names = append(names, string(series["name"]))
		history := decodeList(t, series["history"])
		if len(history) == 0 {
			t.Fatalf("expected history points in %s", raw)
		}
		for _, p := range history {
			expectDay(t, expectFields(t, p, "date", "balance")["date"])
		}
	}
	if want := []string{`"Savings"`, `"Uncategorized"`}; !slices.Equal(names, want) {
		t.Fatalf("expected the series %v, got %v", want, names)
	}
	for _, m := range decodeList(t, setup["marklines"]) {
		expectDay(t, expectFields(t, m, "date", "color", "name")["date"])
	}
	var ids []string
	for range 4 {
		snapshot := expectFields(t, json.RawMessage(stream.expect("snapshot").Data), "id", "date", "balance")
		expectDay(t, snapshot["date"])
		ids = append(ids, string(snapshot["id"]))
	}
	if !slices.Contains(ids, `""`) {
		t.Fatalf("expected Uncategorized snapshots with the id \"\", got the ids %v", ids)
	}
	stream.expectEnd()
}

func TestDashboardForecastStreamWhenIdleSendsTheSeriesTheForecastProjects(t *testing.T) {
	fx := newStreamFixture(t)
	if _, err := fx.svc.UpsertAccount(t.Context(), model.AccountInput{Name: "Pension", TypeID: fx.pension.ID}); err != nil {
		t.Fatalf("create account: %v", err)
	}
	rows := forecastRows(fx.pension.ID, 2)
	fx.compute = emitting(rows, nil)

	stream := fx.connect(t)
	// Pension's account has no Snapshot, so Pension has no history and no stored rows yet, but a
	// Forecast projects every account: the Forecast Run will fill it.
	expectSetup(stream, fx.setup(fx.pensionSeries(), fx.savingsSeries(t)))
	fx.timers.fire()
	fx.stop()

	expectSetup(stream, fx.setup(fx.pensionSeries(), fx.savingsSeries(t)))
	stream.expect("started")
	expectSnapshots(stream, rows...)
	stream.expect("done")
	stream.expectEnd()
}

func TestDashboardForecastStreamSendsTheCurrentSetupBeforeEachLiveForecastRun(t *testing.T) {
	tests := map[string]struct {
		// change writes Forecast Inputs while the page is connected, and returns the setup the
		// Forecast Run that follows must be preceded by.
		change func(t *testing.T, fx *streamFixture) model.ForecastDashboardData
	}{
		"a recorded Snapshot adds to the Balance History": {
			change: func(t *testing.T, fx *streamFixture) model.ForecastDashboardData {
				recorded := historyDate(t).Add(31 * date.Day)
				if _, err := fx.svc.UpsertAccountSnapshot(t.Context(), fx.savingsAccount.ID, model.AccountSnapshotInput{Date: recorded, Balance: uncertain.NewFixed(12000)}); err != nil {
					t.Fatalf("create snapshot: %v", err)
				}
				savings := fx.savingsSeries(t)
				savings.History = append(savings.History, model.BalancePoint{Date: recorded, Balance: 12000})
				return fx.setup(savings)
			},
		},
		"a new Account Type with an account gets a series": {
			change: func(t *testing.T, fx *streamFixture) model.ForecastDashboardData {
				stocks, err := fx.svc.UpsertAccountType(t.Context(), model.AccountTypeInput{Name: "Stocks", Color: "#ffff00"})
				if err != nil {
					t.Fatalf("create account type: %v", err)
				}
				if _, err := fx.svc.UpsertAccount(t.Context(), model.AccountInput{Name: "My Stocks", TypeID: stocks.ID}); err != nil {
					t.Fatalf("create account: %v", err)
				}
				return fx.setup(fx.savingsSeries(t), model.DashboardForecastSeries{ID: stocks.ID, Name: "Stocks", Color: "#ffff00", History: []model.BalancePoint{}})
			},
		},
		"a renamed and recoloured Account Type has its new name and colour": {
			change: func(t *testing.T, fx *streamFixture) model.ForecastDashboardData {
				if _, err := fx.svc.UpsertAccountType(t.Context(), model.AccountTypeInput{ID: fx.savings.ID, Name: "Buffer", Color: "#00ffff"}); err != nil {
					t.Fatalf("update account type: %v", err)
				}
				savings := fx.savingsSeries(t)
				savings.Name, savings.Color = "Buffer", "#00ffff"
				return fx.setup(savings)
			},
		},
		"a new Special Date gets a markline": {
			change: func(t *testing.T, fx *streamFixture) model.ForecastDashboardData {
				house, err := fx.svc.UpsertSpecialDate(t.Context(), model.SpecialDateInput{Name: "House", Date: date.Today().Add(date.Year), Color: "#ff00ff"})
				if err != nil {
					t.Fatalf("create special date: %v", err)
				}
				setup := fx.setup(fx.savingsSeries(t))
				setup.Marklines = slices.Insert(setup.Marklines, 1, model.Markline{Date: house.Date, Color: "#ff00ff", Name: "House"})
				return setup
			},
		},
		"an Account Type without Balance History or an account loses its series": {
			change: func(t *testing.T, fx *streamFixture) model.ForecastDashboardData {
				moved := model.AccountInput{ID: fx.savingsAccount.ID, Name: fx.savingsAccount.Name, TypeID: fx.pension.ID}
				if _, err := fx.svc.UpsertAccount(t.Context(), moved); err != nil {
					t.Fatalf("update account: %v", err)
				}
				pension := fx.pensionSeries()
				pension.History = fx.savingsSeries(t).History
				return fx.setup(pension)
			},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			fx := newStreamFixture(t)
			fx.compute = emitting(nil, nil)

			stream := fx.connect(t)
			expectSetup(stream, fx.setup(fx.savingsSeries(t)))
			want := tt.change(t, fx)
			fx.timers.fire()
			fx.stop()

			expectSetup(stream, want)
			stream.expect("started")
			stream.expect("done")
			stream.expectEnd()
		})
	}
}

func TestDashboardForecastStreamEndsWhenTheSetupCannotBeRefreshed(t *testing.T) {
	fx := newStreamFixture(t)
	fx.compute = emitting(nil, nil)

	stream := fx.connect(t)
	expectSetup(stream, fx.setup(fx.savingsSeries(t)))
	// Without a database the Forecast Run starts and Fails, and reading the setup fails.
	if err := fx.svc.DB().Close(); err != nil {
		t.Fatalf("close the database: %v", err)
	}
	fx.timers.fire()

	// The page's EventSource reconnects and gets a full setup instead of a started without one.
	stream.expectEnd()
}
