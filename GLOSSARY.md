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

**Uncategorized**:
The Account Type that accounts without one are grouped under.
_Avoid_: unknown, no type

**Balance History**:
The balance per Account Type on each day, up to today, that an account's balance was recorded, with every account counting at its latest recorded balance.
_Avoid_: snapshot history, historic snapshots

### Forecasting

**Dashboard Forecast**:
The projection of balances per Account Type, from the day it was computed to the last Special Date, kept up to date in the background using the Forecast Settings. It is empty when no Special Date is after today.
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
