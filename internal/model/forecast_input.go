package model

import (
	"sync/atomic"

	"github.com/ncruces/go-sqlite3"
)

// notForecastInputs are the tables whose writes don't make the Dashboard Forecast Stale. The
// forecast run and the caches write to them while the forecast is computed, so counting them would
// loop. Every other table is a Forecast Input, including tables added later. See ADR 0001.
var notForecastInputs = map[string]bool{
	"forecast_cache": true,
	"api_cache":      true,
	"favicon_cache":  true,
	"user":           true,
}

// forecastInputWatcher learns about every row written to a Forecast Input table from SQLite's update
// hook, so Service mutations never need to know the Dashboard Forecast exists.
type forecastInputWatcher struct {
	generation atomic.Uint64
	runner     atomic.Pointer[ForecastRunner]
}

// register is the per-connection init that installs the update hook on a new pooled connection.
func (w *forecastInputWatcher) register(c *sqlite3.Conn) error {
	c.UpdateHook(w.rowChanged)
	return nil
}

// rowChanged runs inside SQLite while the row is written, so it must never use the database.
func (w *forecastInputWatcher) rowChanged(_ sqlite3.AuthorizerActionCode, _, table string, _ int64) {
	if notForecastInputs[table] {
		return
	}
	w.generation.Add(1)
	if runner := w.runner.Load(); runner != nil {
		runner.Invalidate()
	}
}
