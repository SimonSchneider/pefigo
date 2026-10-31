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
-- name: ListTransferTemplateEnds :many
SELECT *
FROM event_transfer_template_end
ORDER BY event_id,
  transfer_template_id;
-- name: UpsertTransferTemplateEnd :exec
INSERT INTO event_transfer_template_end (event_id, transfer_template_id, end_offset)
VALUES (?, ?, ?) ON CONFLICT (event_id, transfer_template_id) DO
UPDATE
SET end_offset = EXCLUDED.end_offset;
-- name: DeleteTransferTemplateEnd :exec
DELETE FROM event_transfer_template_end
WHERE event_id = ?
  AND transfer_template_id = ?;
-- name: ListGrowthModelEnds :many
SELECT *
FROM event_growth_model_end
ORDER BY event_id,
  growth_model_id;
-- name: UpsertGrowthModelEnd :exec
INSERT INTO event_growth_model_end (event_id, growth_model_id, end_offset)
VALUES (?, ?, ?) ON CONFLICT (event_id, growth_model_id) DO
UPDATE
SET end_offset = EXCLUDED.end_offset;
-- name: DeleteGrowthModelEnd :exec
DELETE FROM event_growth_model_end
WHERE event_id = ?
  AND growth_model_id = ?;
