# A Forecast can start on a configured day after the latest Snapshot

Snapshots are often recorded on payday after that payday's Transfers, while the Transfer Templates spread one monthly cycle over the next days: the salary and its splits on the 25th, loans and saving on the 26th, spending and the sweep of what is left to savings on the 27th. A Forecast that starts on the latest Snapshot's day charges that cycle a second time, although the Snapshots already hold it. So the Forecast Start is the first of the Forecast Start Days on or after the latest Snapshot, and the Transfers before it are taken to be in the Snapshots. By default every day is a Forecast Start Day, which keeps the start of ADR 0004.

## Considered Options

- **A Snapshot is the balance at the end of its day**: only the Snapshot's day is skipped, so the cycle's later bills are charged against spending accounts recorded at 0, and a 100% sweep then pulls the shortfall out of savings.
- **A Snapshot covers the next N days**: breaks whenever payday moves, such as to the Friday before a weekend.
- **Date Snapshots ahead by hand**, to the day after the cycle: works without a setting, but the Balance History hides Snapshots dated after today, and it is easy to forget.

## Consequences

- Starting Balances are still the Snapshots as recorded. Transfers, growth, interest and ISK between the latest Snapshot and the Forecast Start are not counted.
- Without a Forecast Start Day within four years of the latest Snapshot, the Forecast starts on the latest Snapshot's day.
- A Special Date between the latest Snapshot and the Forecast Start isn't after the Forecast Start, so when it is the last one the Dashboard Forecast is empty.
- The Transfers page doesn't use the Forecast Start Days: it starts on the day it is asked for.
