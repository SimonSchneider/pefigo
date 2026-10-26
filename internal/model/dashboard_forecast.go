package model

import (
	"cmp"
	"context"
	"fmt"
	"runtime/debug"
	"slices"
	"sync"
	"time"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/pdb"
)

// BalancePoint is the balance of a dashboard forecast series on one day: a Balance History point,
// or a point of the Dashboard Forecast. Balance is the mean of the uncertain balance.
type BalancePoint struct {
	Date    date.Date `json:"date"`
	Balance float64   `json:"balance"`
}

// ForecastCacheRow is one stored point of the Dashboard Forecast: the projected balance of one
// Account Type, or of Uncategorized when AccountTypeID is "", on one day.
type ForecastCacheRow struct {
	AccountTypeID string `json:"id"`
	BalancePoint
}

// ForecastState is where the Dashboard Forecast is in its Forecast Runs.
type ForecastState string

const (
	ForecastStateIdle    ForecastState = "idle"    // no Forecast Run has started yet
	ForecastStateRunning ForecastState = "running" // a Forecast Run started and has no outcome yet
	ForecastStateDone    ForecastState = "done"    // the last Forecast Run was Done
	ForecastStateFailed  ForecastState = "failed"  // the last Forecast Run Failed
)

// ForecastStatus is the state of the Dashboard Forecast. Message says why when it Failed.
type ForecastStatus struct {
	State   ForecastState
	Message string
}

type ForecastEventType string

const (
	ForecastEventStarted  ForecastEventType = "started"  // a Forecast Run cleared the Dashboard Forecast
	ForecastEventSnapshot ForecastEventType = "snapshot" // a Forecast Run stored one row
	ForecastEventDone     ForecastEventType = "done"     // a Forecast Run was Done
	ForecastEventFailed   ForecastEventType = "failed"   // a Forecast Run Failed, see Message
)

// ForecastEvent is one change to the Dashboard Forecast. Snapshot is set for ForecastEventSnapshot
// and Message for ForecastEventFailed.
type ForecastEvent struct {
	Type     ForecastEventType
	Snapshot *ForecastCacheRow
	Message  string
}

// ForecastSubscription is the Dashboard Forecast as it was when subscribing, plus every change since:
// each stored row is either in Rows or arrives on Events, never both.
type ForecastSubscription struct {
	Status ForecastStatus
	Rows   []ForecastCacheRow
	// Events is closed when the subscriber falls too far behind, on Stop, or on Unsubscribe.
	Events <-chan ForecastEvent
	// Unsubscribe stops the events. It is safe to call more than once and after Events was closed.
	Unsubscribe func()
}

// ForecastTimer is a scheduled debounce, as returned by time.AfterFunc.
type ForecastTimer interface {
	Stop() bool
}

// ForecastCompute computes a Forecast. The Service implements it.
type ForecastCompute interface {
	RunPrediction(ctx context.Context, handler PredictionEventHandler, params PredictionParams) error
}

// ForecastLogger receives errors and panics of Forecast Runs. srvu.Logger satisfies it.
type ForecastLogger interface {
	Printf(format string, v ...any)
}

type nopForecastLogger struct{}

func (nopForecastLogger) Printf(string, ...any) {}

// WithForecastTimer replaces time.AfterFunc for scheduling Forecast Runs after the debounce.
func WithForecastTimer(afterFunc func(time.Duration, func()) ForecastTimer) ServiceOption {
	return func(s *Service) {
		s.dashboardForecast.afterFunc = afterFunc
	}
}

// WithForecastCompute replaces the Service as what computes each Forecast Run's prediction.
func WithForecastCompute(c ForecastCompute) ServiceOption {
	return func(s *Service) {
		s.dashboardForecast.compute = c
	}
}

const (
	forecastDebounce         = 5 * time.Second
	forecastSubscriberBuffer = 64
)

// DashboardForecast keeps the Dashboard Forecast up to date: every time it is marked Stale, it
// starts a new Forecast Run after a debounce, abandoning the one in progress. Subscribers get the
// stored Dashboard Forecast and every change to it.
type DashboardForecast struct {
	afterFunc func(time.Duration, func()) ForecastTimer
	compute   ForecastCompute
	svc       *Service

	// mu guards scheduling. MarkStale takes it inside SQLite's update hook, so it is never held
	// across a database call or while waiting for anything.
	mu        sync.Mutex
	logger    ForecastLogger
	started   bool
	stopped   bool
	timer     ForecastTimer
	scheduled uint64 // identifies the latest scheduled timer
	cancelRun context.CancelFunc
	runs      sync.WaitGroup

	// runMu lets only one Forecast Run execute at a time.
	runMu sync.Mutex

	// subMu guards the status and the subscribers. It is held while a row is stored and broadcast,
	// and while Subscribe reads the rows and registers, so each row reaches a subscriber once.
	subMu       sync.Mutex
	status      ForecastStatus
	subscribers map[*forecastSubscriber]struct{}
	closed      bool
}

type forecastSubscriber struct {
	ch chan ForecastEvent
}

func newDashboardForecast() *DashboardForecast {
	return &DashboardForecast{
		afterFunc: func(d time.Duration, f func()) ForecastTimer {
			return time.AfterFunc(d, f)
		},
		status:      ForecastStatus{State: ForecastStateIdle},
		subscribers: make(map[*forecastSubscriber]struct{}),
	}
}

// DashboardForecast is the Dashboard Forecast, which the Service keeps up to date once started.
func (s *Service) DashboardForecast() *DashboardForecast {
	return s.dashboardForecast
}

// Start schedules the first Forecast Run, and lets every later MarkStale schedule one. Errors and
// panics of Forecast Runs go to logger, if it isn't nil.
func (f *DashboardForecast) Start(logger ForecastLogger) {
	if logger == nil {
		logger = nopForecastLogger{}
	}
	f.mu.Lock()
	f.logger = logger
	f.started = true
	f.mu.Unlock()
	f.MarkStale()
}

// MarkStale restarts the debounce before the next Forecast Run and abandons the one in progress.
// It runs inside SQLite's update hook, so it never uses the database nor waits for a Forecast Run.
func (f *DashboardForecast) MarkStale() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.started || f.stopped {
		return
	}
	if f.cancelRun != nil {
		f.cancelRun()
	}
	if f.timer != nil {
		f.timer.Stop()
	}
	f.scheduled++
	scheduled := f.scheduled
	f.timer = f.afterFunc(forecastDebounce, func() { f.runScheduled(scheduled) })
}

// Stop cancels the scheduled and the running Forecast Run, waits for the running one to exit and
// closes every subscription. Nothing is scheduled afterwards.
func (f *DashboardForecast) Stop() {
	f.mu.Lock()
	f.stopped = true
	if f.timer != nil {
		f.timer.Stop()
	}
	if f.cancelRun != nil {
		f.cancelRun()
	}
	f.mu.Unlock()
	// No run can be added any more: runScheduled adds itself under mu only while not stopped.
	f.runs.Wait()

	f.subMu.Lock()
	defer f.subMu.Unlock()
	f.closed = true
	for sub := range f.subscribers {
		f.dropLocked(sub)
	}
}

// Subscribe returns the status, the stored rows and the changes after them, read atomically.
func (f *DashboardForecast) Subscribe(ctx context.Context) (ForecastSubscription, error) {
	f.subMu.Lock()
	defer f.subMu.Unlock()
	rows, err := f.svc.listDashboardForecastRows(ctx)
	if err != nil {
		return ForecastSubscription{}, err
	}
	sub := &forecastSubscriber{ch: make(chan ForecastEvent, forecastSubscriberBuffer)}
	if f.closed {
		close(sub.ch)
	} else {
		f.subscribers[sub] = struct{}{}
	}
	return ForecastSubscription{
		Status:      f.status,
		Rows:        rows,
		Events:      sub.ch,
		Unsubscribe: func() { f.unsubscribe(sub) },
	}, nil
}

func (f *DashboardForecast) unsubscribe(sub *forecastSubscriber) {
	f.subMu.Lock()
	defer f.subMu.Unlock()
	f.dropLocked(sub)
}

// dropLocked closes sub's channel unless it was dropped before. subMu must be held.
func (f *DashboardForecast) dropLocked(sub *forecastSubscriber) {
	if _, ok := f.subscribers[sub]; !ok {
		return
	}
	delete(f.subscribers, sub)
	close(sub.ch)
}

// broadcastLocked sends evt to every subscriber without waiting: a subscriber whose buffer is full
// has fallen behind and is disconnected. subMu must be held.
func (f *DashboardForecast) broadcastLocked(evt ForecastEvent) {
	for sub := range f.subscribers {
		select {
		case sub.ch <- evt:
		default:
			f.dropLocked(sub)
		}
	}
}

// runScheduled is what the debounce timer runs.
func (f *DashboardForecast) runScheduled(scheduled uint64) {
	f.mu.Lock()
	if f.stopped || scheduled != f.scheduled {
		f.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	f.cancelRun = cancel
	logger := f.logger
	f.runs.Add(1)
	f.mu.Unlock()
	defer f.runs.Done()
	defer cancel()

	f.runMu.Lock()
	defer f.runMu.Unlock()
	if ctx.Err() != nil {
		return
	}
	f.run(ctx, logger)
}

// run is one Forecast Run. It ends Done or Failed, even when the computation panics, unless ctx is
// cancelled: then a newer Forecast Run follows, or the Dashboard Forecast was stopped.
func (f *DashboardForecast) run(ctx context.Context, logger ForecastLogger) {
	defer func() {
		if r := recover(); r != nil {
			logger.Printf("Dashboard Forecast Run panicked: %v\n%s", r, debug.Stack())
			f.finish(ctx, fmt.Errorf("panic: %v", r))
		}
	}()
	err := f.computeForecast(ctx)
	if err != nil && ctx.Err() == nil {
		logger.Printf("Dashboard Forecast Run failed: %v", err)
	}
	f.finish(ctx, err)
}

func (f *DashboardForecast) computeForecast(ctx context.Context) error {
	if err := f.start(ctx); err != nil {
		return err
	}
	params, ok, err := f.svc.dashboardForecastParams(ctx)
	if err != nil || !ok {
		return err
	}
	return f.compute.RunPrediction(ctx, &dashboardForecastHandler{ctx: ctx, f: f}, params)
}

// start sets the status to running, clears the stored rows and emits Started, atomically.
func (f *DashboardForecast) start(ctx context.Context) error {
	f.subMu.Lock()
	defer f.subMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	f.status = ForecastStatus{State: ForecastStateRunning}
	err := f.svc.q.DeleteAllForecastCache(ctx)
	f.broadcastLocked(ForecastEvent{Type: ForecastEventStarted})
	if err != nil {
		return fmt.Errorf("deleting the stored Dashboard Forecast: %w", err)
	}
	return nil
}

// store stores row and emits it as a Snapshot, atomically.
func (f *DashboardForecast) store(ctx context.Context, row ForecastCacheRow) error {
	f.subMu.Lock()
	defer f.subMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := f.svc.q.InsertForecastCache(ctx, pdb.InsertForecastCacheParams{
		Date:          int64(row.Date),
		AccountTypeID: row.AccountTypeID,
		Balance:       row.Balance,
	}); err != nil {
		return fmt.Errorf("storing Dashboard Forecast row: %w", err)
	}
	f.broadcastLocked(ForecastEvent{Type: ForecastEventSnapshot, Snapshot: &row})
	return nil
}

// finish records the outcome of the Forecast Run and emits Done, or Failed when err is set. An
// abandoned Forecast Run has no outcome: it leaves the status running for the newer one.
func (f *DashboardForecast) finish(ctx context.Context, err error) {
	f.subMu.Lock()
	defer f.subMu.Unlock()
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		f.status = ForecastStatus{State: ForecastStateFailed, Message: err.Error()}
		f.broadcastLocked(ForecastEvent{Type: ForecastEventFailed, Message: err.Error()})
		return
	}
	f.status = ForecastStatus{State: ForecastStateDone}
	f.broadcastLocked(ForecastEvent{Type: ForecastEventDone})
}

// dashboardForecastHandler stores and emits each point of a Forecast Run's Forecast.
type dashboardForecastHandler struct {
	ctx context.Context
	f   *DashboardForecast
}

func (h *dashboardForecastHandler) Setup(PredictionSetupEvent) error {
	return nil
}

func (h *dashboardForecastHandler) Snapshot(snap PredictionBalanceSnapshot) error {
	return h.f.store(h.ctx, ForecastCacheRow{
		AccountTypeID: snap.ID,
		BalancePoint:  BalancePoint{Date: snap.Day, Balance: snap.Balance},
	})
}

func (h *dashboardForecastHandler) Close() error {
	return nil
}

// dashboardForecastParams builds the Forecast of a Forecast Run from the Forecast Settings: it runs
// through the last Special Date, grouped by Account Type, without lower and upper bounds. ok is false
// without a Special Date, which leaves the Dashboard Forecast empty. The Forecast is empty too when the
// last Special Date is on or before the Forecast Start.
func (s *Service) dashboardForecastParams(ctx context.Context) (params PredictionParams, ok bool, err error) {
	specialDates, err := s.ListSpecialDates(ctx)
	if err != nil {
		return params, false, fmt.Errorf("listing special dates: %w", err)
	}
	if len(specialDates) == 0 {
		return params, false, nil
	}
	last := slices.MaxFunc(specialDates, func(a, b SpecialDate) int { return cmp.Compare(a.Date, b.Date) }).Date

	samples, err := s.GetForecastSamples(ctx)
	if err != nil {
		return params, false, fmt.Errorf("getting forecast samples: %w", err)
	}
	snapshotInterval, err := s.GetForecastSnapshotInterval(ctx)
	if err != nil {
		return params, false, fmt.Errorf("getting forecast snapshot interval: %w", err)
	}
	return PredictionParams{
		Through:          last,
		Samples:          samples,
		SnapshotInterval: date.Cron(snapshotInterval),
		GroupBy:          GroupByType,
	}, true, nil
}

func (s *Service) listDashboardForecastRows(ctx context.Context) ([]ForecastCacheRow, error) {
	rows, err := s.q.ListForecastCache(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing the stored Dashboard Forecast: %w", err)
	}
	result := make([]ForecastCacheRow, len(rows))
	for i, r := range rows {
		result[i] = ForecastCacheRow{
			AccountTypeID: r.AccountTypeID,
			BalancePoint:  BalancePoint{Date: date.Date(r.Date), Balance: r.Balance},
		}
	}
	return result, nil
}

// DashboardForecastSeries is one line of the dashboard forecast chart: an Account Type, or
// Uncategorized when ID is "", with its Balance History. Its forecast points are the
// ForecastCacheRows with its ID.
type DashboardForecastSeries struct {
	ID      string         `json:"id"`
	Name    string         `json:"name"`
	Color   string         `json:"color"`
	History []BalancePoint `json:"history"`
}

// ForecastDashboardData is what the dashboard chart starts from: one series per Account Type and
// the date lines of today and the Special Dates.
type ForecastDashboardData struct {
	Series    []DashboardForecastSeries `json:"series"`
	Marklines []Markline                `json:"marklines"`
}

// GetDashboardForecastHistory returns a series for Uncategorized and every Account Type that has
// Balance History or forecast points, in the order of the Account Types with Uncategorized last.
// Forecast points are in rows, or still to come for every Account Type with an account when there
// is a Forecast, which projects every account. A series holds its Balance History only: its
// forecast points are rows.
func (s *Service) GetDashboardForecastHistory(ctx context.Context, rows []ForecastCacheRow) (*ForecastDashboardData, error) {
	accountTypes, err := s.ListAccountTypes(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing account types: %w", err)
	}
	today := date.Today()
	history, err := s.balanceHistory(ctx, today)
	if err != nil {
		return nil, fmt.Errorf("building balance history: %w", err)
	}
	projected, err := s.q.ListProjectedAccountTypeIDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing projected account types: %w", err)
	}
	forecast := make(map[string]bool)
	for _, typeID := range projected {
		forecast[typeID] = true
	}
	for _, row := range rows {
		forecast[row.AccountTypeID] = true
	}

	series := make([]DashboardForecastSeries, 0, len(accountTypes)+1)
	for _, at := range append(accountTypes, uncategorized) {
		points, recorded := history[at.ID]
		if !recorded && !forecast[at.ID] {
			continue
		}
		if points == nil {
			points = []BalancePoint{}
		}
		series = append(series, DashboardForecastSeries{ID: at.ID, Name: at.Name, Color: at.Color, History: points})
	}

	specialDates, err := s.ListSpecialDates(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing special dates: %w", err)
	}
	return &ForecastDashboardData{
		Series:    series,
		Marklines: dateLines(specialDates, today),
	}, nil
}
