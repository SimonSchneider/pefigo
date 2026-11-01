-- name: ListBillAccounts :many
SELECT *
FROM bill_account
ORDER BY name, id;

-- name: GetBillAccount :one
SELECT *
FROM bill_account
WHERE id = ?;

-- name: UpsertBillAccount :one
INSERT INTO bill_account (
    id,
    name,
    from_account_id,
    recurrence,
    priority,
    enabled,
    created_at,
    updated_at
  )
VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO
UPDATE
SET name = EXCLUDED.name,
  from_account_id = EXCLUDED.from_account_id,
  recurrence = EXCLUDED.recurrence,
  priority = EXCLUDED.priority,
  enabled = EXCLUDED.enabled,
  updated_at = EXCLUDED.updated_at
RETURNING *;

-- name: DeleteBillAccount :exec
DELETE FROM bill_account
WHERE id = ?;

-- name: ListBills :many
SELECT *
FROM bill
WHERE bill_account_id = ?
ORDER BY name, id;

-- name: ListAllBills :many
SELECT *
FROM bill
ORDER BY bill_account_id, name, id;

-- name: GetBill :one
SELECT *
FROM bill
WHERE id = ?;

-- name: UpsertBill :one
INSERT INTO bill (
    id,
    bill_account_id,
    name,
    budget_category_id,
    enabled,
    notes,
    url,
    event_id,
    created_at,
    updated_at
  )
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO
UPDATE
SET bill_account_id = EXCLUDED.bill_account_id,
  name = EXCLUDED.name,
  budget_category_id = EXCLUDED.budget_category_id,
  enabled = EXCLUDED.enabled,
  notes = EXCLUDED.notes,
  url = EXCLUDED.url,
  event_id = EXCLUDED.event_id,
  updated_at = EXCLUDED.updated_at
RETURNING *;

-- name: DeleteBill :exec
DELETE FROM bill
WHERE id = ?;

-- name: ListBillAmounts :many
SELECT *
FROM bill_amount
WHERE bill_id = ?
ORDER BY id;

-- name: ListAllBillAmounts :many
SELECT *
FROM bill_amount
ORDER BY bill_id, id;

-- name: UpsertBillAmount :one
INSERT INTO bill_amount (
    id,
    bill_id,
    amount,
    period,
    currency,
    start_date,
    end_date,
    event_id,
    created_at,
    updated_at
  )
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO
UPDATE
SET bill_id = EXCLUDED.bill_id,
  amount = EXCLUDED.amount,
  period = EXCLUDED.period,
  currency = EXCLUDED.currency,
  start_date = EXCLUDED.start_date,
  end_date = EXCLUDED.end_date,
  event_id = EXCLUDED.event_id,
  updated_at = EXCLUDED.updated_at
RETURNING *;

-- name: DeleteBillAmount :exec
DELETE FROM bill_amount
WHERE id = ?;

-- name: GetBillAmount :one
SELECT *
FROM bill_amount
WHERE id = ?;

-- name: ListAllBillEnds :many
SELECT *
FROM bill_end
ORDER BY bill_id, id;

-- name: GetBillEnd :one
SELECT *
FROM bill_end
WHERE id = ?;

-- name: UpsertBillEnd :one
INSERT INTO bill_end (id, bill_id, date, event_id, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO
UPDATE
SET bill_id = EXCLUDED.bill_id,
  date = EXCLUDED.date,
  event_id = EXCLUDED.event_id,
  updated_at = EXCLUDED.updated_at
RETURNING *;

-- name: DeleteBillEnd :exec
DELETE FROM bill_end
WHERE id = ?;
