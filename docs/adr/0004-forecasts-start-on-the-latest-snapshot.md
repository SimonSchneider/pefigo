# A Forecast starts on the latest Snapshot, not today

A Forecast starts on the day of the latest Snapshot of any account, and every account begins there at its Starting Balance: its latest Snapshot as recorded, or 0 without one. Previously a Forecast started the day after today, and each account last recorded before then was first carried forward from its own Snapshot with growth, interest and tax, but no Transfers. Every Forecast depended on the day it was computed, and the engine reported stale accounts on days the other accounts were missing, so grouped totals had to filter those days out. Stale accounts are the exception, so they now start as recorded.

## Considered Options

- **Start the day after today and carry stale accounts forward**: the previous behavior. A Forecast changes as days pass although no Forecast Input changed, and every account needs its own start day.
- **Start the day after today with stale accounts as recorded**: no carrying forward, but the Forecast still depends on the date, and the Transfers between the latest Snapshot and today are never charged.

## Consequences

- The date doesn't change a Forecast: computing it again on a later day from the same Forecast Inputs gives the same Forecast (bills in other currencies aside, through exchange rates). Today is only the Today line on a chart.
- When nothing was recorded lately, a Forecast starts in the past. Its points before today are forecast, with that stretch's Transfers charged.
- A stale account isn't grown to the Forecast Start: a stock account last recorded two years before the latest Snapshot starts at that old balance.
- Growth, interest waiting to be paid out and ISK quarters start counting on the Forecast Start, so an account's first interest payout or ISK tax leaves out the time before it.
- Without any Snapshot there is no Forecast, and the Dashboard Forecast is empty.
- Don't reintroduce a start day per account. Every day a Forecast reports holds every account only because they all start together.
