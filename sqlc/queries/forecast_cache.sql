-- name: ListForecastCache :many
SELECT date, account_type_id, balance
FROM forecast_cache
ORDER BY date, account_type_id;

-- name: DeleteAllForecastCache :exec
DELETE FROM forecast_cache;

-- name: InsertForecastCache :exec
INSERT INTO forecast_cache (date, account_type_id, balance)
VALUES (?, ?, ?);
