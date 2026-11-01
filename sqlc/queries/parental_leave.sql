-- name: ListPartialParentalLeaves :many
SELECT *
FROM partial_parental_leave
WHERE salary_id = ?
ORDER BY id;

-- name: UpsertPartialParentalLeave :one
INSERT INTO partial_parental_leave (
    id,
    salary_id,
    start_date,
    end_date,
    sjuk_days_per_year,
    lagsta_days_per_year,
    skipped_work_days_per_year,
    event_id,
    created_at,
    updated_at
  )
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO
UPDATE
SET salary_id = EXCLUDED.salary_id,
  start_date = EXCLUDED.start_date,
  end_date = EXCLUDED.end_date,
  sjuk_days_per_year = EXCLUDED.sjuk_days_per_year,
  lagsta_days_per_year = EXCLUDED.lagsta_days_per_year,
  skipped_work_days_per_year = EXCLUDED.skipped_work_days_per_year,
  event_id = EXCLUDED.event_id,
  updated_at = EXCLUDED.updated_at
RETURNING *;

-- name: DeletePartialParentalLeave :exec
DELETE FROM partial_parental_leave
WHERE id = ?;

-- name: ListAllPartialParentalLeaves :many
SELECT *
FROM partial_parental_leave
ORDER BY salary_id, id;

-- name: ListFullParentalLeaves :many
SELECT *
FROM full_parental_leave
WHERE salary_id = ?
ORDER BY id;

-- name: UpsertFullParentalLeave :one
INSERT INTO full_parental_leave (
    id,
    salary_id,
    start_date,
    end_date,
    sjuk_days_per_week,
    event_id,
    created_at,
    updated_at
  )
VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO
UPDATE
SET salary_id = EXCLUDED.salary_id,
  start_date = EXCLUDED.start_date,
  end_date = EXCLUDED.end_date,
  sjuk_days_per_week = EXCLUDED.sjuk_days_per_week,
  event_id = EXCLUDED.event_id,
  updated_at = EXCLUDED.updated_at
RETURNING *;

-- name: DeleteFullParentalLeave :exec
DELETE FROM full_parental_leave
WHERE id = ?;

-- name: ListAllFullParentalLeaves :many
SELECT *
FROM full_parental_leave
ORDER BY salary_id, id;

-- name: GetPartialParentalLeave :one
SELECT *
FROM partial_parental_leave
WHERE id = ?;

-- name: GetFullParentalLeave :one
SELECT *
FROM full_parental_leave
WHERE id = ?;
