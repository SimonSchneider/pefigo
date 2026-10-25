-- The Dashboard Forecast stores each point's day as a date.Date and only its mean balance, and the
-- Confidence Interval setting is gone. The Dashboard Forecast is a cache: the next Forecast Run
-- refills it.
-- migrate:up
DROP TABLE forecast_cache;
CREATE TABLE forecast_cache (
    date INTEGER NOT NULL,
    account_type_id TEXT NOT NULL,
    balance REAL NOT NULL,
    PRIMARY KEY (date, account_type_id)
);
DELETE FROM app_setting WHERE key = 'forecast_confidence';
