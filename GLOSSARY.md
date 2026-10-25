# PeFi

Personal finance: tracking account balances and projecting how wealth develops over time.

## Language

### Forecasting

**Dashboard Forecast**:
The projection of balances per Account Type, from the day it was computed to the last Special Date, kept up to date in the background using the Forecast Settings.
_Avoid_: forecast cache, cached forecast

**Forecast Input**:
Any stored data or setting the Dashboard Forecast is computed from.

**Stale**:
A Dashboard Forecast is stale when a Forecast Input has changed since it was computed.
_Avoid_: invalidated, dirty, outdated
