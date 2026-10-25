# Detect Forecast Input changes with SQLite's update hook

The Dashboard Forecast must become Stale whenever a Forecast Input changes. Previously every Service mutation had to remember to call `invalidateForecast()`, and 12 of them forgot. Instead, SQLite's update hook is registered on every connection: SQLite reports the table of every row it changes, however the SQL was issued, and the hook marks the Dashboard Forecast Stale. Mutations no longer know the forecast exists.

## Considered Options

- **Triggers on Forecast Input tables** bumping a generation row: declares the inputs in SQL, but needs about three triggers per table, and Go is never told about a write, so something has to poll.
- **Wrapping `pdb.DBTX`**: sqlc runs upserts with `RETURNING` through `QueryRowContext`, so the wrapper would have to classify writes by SQL text.

## Consequences

- Every table counts except a short deny-list: `forecast_cache`, `api_cache`, `favicon_cache` and `user`. New tables count by default; an edit to a table that isn't really a Forecast Input costs one unnecessary forecast run, which is cheaper than a missed one. The forecast run and exchange-rate refreshes write to the cache tables, so counting those would loop.
- Only writes make the Dashboard Forecast Stale. A day passing does not, and neither does an exchange rate expiring: rebuilding just because the date moved would mostly drop Transfers that have probably already happened, until the next Snapshot is recorded.
- The hook doesn't see `WITHOUT ROWID` tables or a `DELETE` with no `WHERE` (SQLite's truncate optimisation). Don't use either on Forecast Input tables.
- The hook fires before commit, so a rolled-back transaction costs at most one unnecessary forecast run.
- The hook runs inside SQLite and must never touch the database itself.
