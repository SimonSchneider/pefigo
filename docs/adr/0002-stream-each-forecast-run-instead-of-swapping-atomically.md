# Stream each Forecast Run instead of swapping the Dashboard Forecast atomically

A Forecast Run clears the stored Dashboard Forecast when it starts, then stores and broadcasts each Snapshot as soon as it is computed, so the dashboard chart fills in while the run is still going. We chose this over building the whole Dashboard Forecast first and replacing the stored one in a single transaction. The swap would mean viewers never see a partial forecast and a Failed run keeps the last good one, but it gives up the progressive fill. A partial forecast after a Failed run is acceptable: viewers are shown that the run Failed, and the next Forecast Input write starts a new Forecast Run.

## Consequences

- A Failed or abandoned Forecast Run leaves the Dashboard Forecast partly computed until the next Forecast Run starts. Don't replace this with an atomic swap without giving up the progressive fill on purpose.
- Viewers must keep history and forecast points apart, because a starting Forecast Run clears only the forecast.
- Subscribing must read the stored Dashboard Forecast and register for updates with no Snapshot stored in between. Otherwise that Snapshot reaches the viewer twice.
