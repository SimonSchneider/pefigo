# PeFi

Personal finance: tracking account balances and projecting how wealth develops over time.

## Language

### Dates

**Start Date**:
The first day a dated entry, such as a Transfer Template or a Bill Amount, applies.
_Avoid_: effective from, valid from

**End Date**:
The first day a dated entry no longer applies: it applies up to, but not on, its End Date. Without an End Date it applies indefinitely.
_Avoid_: effective to, until, last day

### Accounts

**Snapshot**:
An account's balance recorded at the start of a day, before that day's Transfers. Only recorded balances are Snapshots: the balances a Forecast reports are not.
_Avoid_: balance record

**Startup Share Account**:
An account holding shares in a startup. It has no Snapshots: its balance on a day is derived from its latest investment round with shares and a price per share, and its share changes, up to that day. Without such a round it is 0.

**Uncategorized**:
The Account Type that accounts without one are grouped under.
_Avoid_: unknown, no type

**Balance History**:
The balance per Account Type on each day, up to today, that an account's balance was recorded, with every account counting at its latest recorded balance.
_Avoid_: snapshot history, historic snapshots

### Forecasting

**Forecast**:
A projection of every account's balance from the Forecast Start, beginning at each account's Starting Balance and applying growth, interest, tax and Transfers day by day. Without any Snapshot there is no Forecast.
_Avoid_: simulation, prediction

**Forecast Start**:
The first day a Forecast projects: the day of the latest Snapshot of any account, whatever day today is.

**Forecast Interval**:
The days a Forecast reports balances on, such as the 28th of every month. Every day it reports holds every account of the Forecast.
_Avoid_: snapshot interval

**Starting Balance**:
An account's balance at the start of a day: its latest Snapshot on or before that day as it was recorded, or 0 without one. Nothing between that Snapshot and the day is counted. A Startup Share Account's Starting Balance is its derived balance on that day.
_Avoid_: catch-up, estimated balance

**Dashboard Forecast**:
The Forecast of balances per Account Type from the Forecast Start to the last Special Date, kept up to date in the background using the Forecast Settings. It is empty when there is no Forecast or no Special Date is after the Forecast Start.
_Avoid_: forecast cache, cached forecast

**Forecast Run**:
One computation of the Dashboard Forecast from the current Forecast Inputs. It ends Done or Failed, unless the Dashboard Forecast becomes Stale while it runs: then it is abandoned without an outcome and a newer Forecast Run follows.
_Avoid_: forecast job, recompute

**Done**:
The outcome of a Forecast Run that computed the whole Dashboard Forecast.

**Failed**:
The outcome of a Forecast Run that stopped on an error, leaving the Dashboard Forecast partly computed.

**Forecast Input**:
Any stored data or setting the Dashboard Forecast is computed from.

**Forecast Input Generation**:
A number that advances every time a Forecast Input is written. It is kept in memory only, and nothing records which generation a Dashboard Forecast was computed from.

**Stale**:
A Dashboard Forecast is stale when a Forecast Input has changed since it was computed.
_Avoid_: invalidated, dirty, outdated
