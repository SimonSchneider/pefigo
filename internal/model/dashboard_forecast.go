package model

import (
	"context"
	"fmt"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/pdb"
)

// ForecastCacheRow is one stored point of the Dashboard Forecast: the projected balance of one
// Account Type on one day.
type ForecastCacheRow struct {
	Date          int64
	AccountTypeID string
	Median        float64
	LowerBound    float64
	UpperBound    float64
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

// ForecastCompute computes a prediction. The Service implements it.
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
		Date:          row.Date,
		AccountTypeID: row.AccountTypeID,
		Median:        row.Median,
		LowerBound:    row.LowerBound,
		UpperBound:    row.UpperBound,
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

// dashboardForecastHandler stores and emits each snapshot of a Forecast Run's prediction.
type dashboardForecastHandler struct {
	ctx context.Context
	f   *DashboardForecast
}

func (h *dashboardForecastHandler) Setup(PredictionSetupEvent) error {
	return nil
}

func (h *dashboardForecastHandler) Snapshot(snap PredictionBalanceSnapshot) error {
	return h.f.store(h.ctx, ForecastCacheRow{
		Date:          snap.Day,
		AccountTypeID: snap.ID,
		Median:        snap.Balance,
		LowerBound:    snap.LowerBound,
		UpperBound:    snap.UpperBound,
	})
}

func (h *dashboardForecastHandler) Close() error {
	return nil
}

// dashboardForecastParams builds the prediction of a Forecast Run from the Forecast Settings: it
// ends at the last Special Date, grouped by Account Type. ok is false when no Special Date is
// after today, which leaves the Dashboard Forecast empty.
func (s *Service) dashboardForecastParams(ctx context.Context) (params PredictionParams, ok bool, err error) {
	specialDates, err := s.ListSpecialDates(ctx)
	if err != nil {
		return params, false, fmt.Errorf("listing special dates: %w", err)
	}
	if len(specialDates) == 0 {
		return params, false, nil
	}
	sort.Slice(specialDates, func(i, j int) bool {
		return specialDates[i].Date.Before(specialDates[j].Date)
	})
	endDate := specialDates[len(specialDates)-1].Date
	today := date.Today()
	if !endDate.After(today) {
		return params, false, nil
	}

	confidence, err := s.GetForecastConfidence(ctx)
	if err != nil {
		return params, false, fmt.Errorf("getting forecast confidence: %w", err)
	}
	samples, err := s.GetForecastSamples(ctx)
	if err != nil {
		return params, false, fmt.Errorf("getting forecast samples: %w", err)
	}
	snapshotInterval, err := s.GetForecastSnapshotInterval(ctx)
	if err != nil {
		return params, false, fmt.Errorf("getting forecast snapshot interval: %w", err)
	}
	return PredictionParams{
		Duration:         endDate.Sub(today),
		Samples:          samples,
		Quantile:         confidence,
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
			Date:          r.Date,
			AccountTypeID: r.AccountTypeID,
			Median:        r.Median,
			LowerBound:    r.LowerBound,
			UpperBound:    r.UpperBound,
		}
	}
	return result, nil
}

// ForecastDashboardData is what the dashboard chart starts from: one entity per Account Type and the
// marklines of today and the Special Dates.
type ForecastDashboardData struct {
	Entities  []PredictionFinancialEntity `json:"entities"`
	Marklines []Markline                  `json:"marklines"`
}

// GetDashboardForecastHistory returns one entity per Account Type that has history points or appears
// in rows, identified by the Account Type ID and holding the history points only: the forecast
// points are rows. A history series of accounts without an Account Type has the ID "".
func (s *Service) GetDashboardForecastHistory(ctx context.Context, rows []ForecastCacheRow) (*ForecastDashboardData, error) {
	accountTypes, err := s.ListAccountTypes(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing account types: %w", err)
	}
	history, err := s.buildSnapshotHistoryChart(ctx, accountTypes)
	if err != nil {
		return nil, fmt.Errorf("building snapshot history: %w", err)
	}

	byTypeID := make(map[string]*PredictionFinancialEntity)
	for _, series := range history.Series {
		entity := &PredictionFinancialEntity{
			ID:        series.typeID,
			Name:      series.Name,
			Color:     series.Color,
			Snapshots: make([]PredictionBalanceSnapshot, 0, len(history.Dates)),
		}
		for i, dateStr := range history.Dates {
			d, err := date.ParseDate(dateStr)
			if err != nil {
				continue
			}
			entity.Snapshots = append(entity.Snapshots, PredictionBalanceSnapshot{
				Day:     d.ToStdTime().UnixMilli(),
				Balance: series.Data[i],
			})
		}
		byTypeID[series.typeID] = entity
	}
	typesByID := KeyBy(accountTypes, func(at AccountType) string { return at.ID })
	for _, row := range rows {
		at, exists := typesByID[row.AccountTypeID]
		if _, has := byTypeID[row.AccountTypeID]; has || !exists {
			continue
		}
		byTypeID[at.ID] = &PredictionFinancialEntity{
			ID:        at.ID,
			Name:      at.Name,
			Color:     at.Color,
			Snapshots: []PredictionBalanceSnapshot{},
		}
	}

	entities := make([]PredictionFinancialEntity, 0, len(byTypeID))
	for _, at := range accountTypes {
		if entity, ok := byTypeID[at.ID]; ok {
			entities = append(entities, *entity)
		}
	}
	if entity, ok := byTypeID[""]; ok {
		entities = append(entities, *entity)
	}

	specialDates, err := s.ListSpecialDates(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing special dates: %w", err)
	}
	marklines := make([]Markline, 0, len(specialDates)+1)
	marklines = append(marklines, Markline{
		Date: date.Today().ToStdTime().UnixMilli(),
		Name: "Today",
	})
	for _, sd := range specialDates {
		marklines = append(marklines, Markline{
			Date:  sd.Date.ToStdTime().UnixMilli(),
			Color: sd.Color,
			Name:  sd.Name,
		})
	}

	return &ForecastDashboardData{
		Entities:  entities,
		Marklines: marklines,
	}, nil
}
