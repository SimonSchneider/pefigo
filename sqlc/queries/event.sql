-- name: ListEvents :many
SELECT *
FROM event
ORDER BY date,
  name,
  id;
-- name: GetEvent :one
SELECT *
FROM event
WHERE id = ?;
-- name: UpsertEvent :one
INSERT INTO event (id, name, date, planned, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO
UPDATE
SET name = EXCLUDED.name,
  date = EXCLUDED.date,
  planned = EXCLUDED.planned,
  updated_at = EXCLUDED.updated_at
RETURNING *;
-- name: DeleteEvent :exec
DELETE FROM event
WHERE id = ?;
-- name: SetAccountEvent :exec
UPDATE account
SET event_id = ?
WHERE id = ?;
