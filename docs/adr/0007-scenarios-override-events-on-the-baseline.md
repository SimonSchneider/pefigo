# Scenarios override Events on the Baseline instead of copying the Forecast Inputs

A Scenario stores only overrides: for each Event it mentions, the date it puts the Event at, or that it leaves the Event out. Every other Event is as in the Baseline, and nothing outside Events can differ. We chose this over giving each Scenario its own copy of the Forecast Inputs, because copies drift. A raise, a new Transfer Template or a corrected growth model entered in the Baseline would never reach a copy, so every Scenario would need re-copying before it could be trusted. With overrides, every Scenario picks up every Baseline change. Planning or unplanning an Event later never breaks a Scenario either.

## Considered Options

- **A full copy of the Forecast Inputs per Scenario**: anything can be edited freely, but each copy drifts from the Baseline from the moment it is made.
- **A JSON file of the Forecast Inputs per Scenario**, loaded and run without touching the stored data: a copy, plus a file format to keep versioned as migrations change the schema. JSON export and import was split out to #155 as a feature of its own.
- **Changes as add, move or remove per Event**: just as expressive, but a Scenario that "adds" an Event breaks once that Event is planned, and a "move" breaks once it is unplanned. An override (at a date, or left out) doesn't care whether the Event is planned.

## Consequences

- Anything a Scenario varies must be inside an Event, so a single change is an Event with one change. A Scenario never changes a Snapshot, so every Scenario has the same Forecast Start.
- The Baseline and every Scenario apply an Event's changes at a date through one shared path. Entry types added to Events later then work in Scenarios without touching Scenarios.
- Every Scenario Forecast is kept up to date in the background, like the Dashboard Forecast. Every Scenario builds on the Baseline, so a Forecast Input write makes all of them Stale. Each write then starts one Forecast Run per Scenario, plus the Baseline's. They run concurrently, up to the host's CPU core count. This extends [ADR 0001](0001-detect-forecast-input-changes-with-sqlite-update-hook.md) (one update hook, now making every forecast Stale) and [ADR 0002](0002-stream-each-forecast-run-instead-of-swapping-atomically.md) (each forecast streams its own Forecast Runs) from one forecast to many. With a handful of Scenarios that cost is accepted. Making only the edited Scenario's forecast Stale is an optimisation, not a requirement.
