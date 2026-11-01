-- name: ListSalaries :many
SELECT *
FROM salary
ORDER BY name, id;

-- name: GetSalary :one
SELECT *
FROM salary
WHERE id = ?;

-- name: UpsertSalary :one
INSERT INTO salary (
    id,
    name,
    to_account_id,
    pension_account_id,
    priority,
    recurrence,
    budget_category_id,
    enabled,
    kommun,
    forsamling,
    church_member,
    is_gross,
    event_id,
    created_at,
    updated_at
  )
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO
UPDATE
SET name = EXCLUDED.name,
  to_account_id = EXCLUDED.to_account_id,
  pension_account_id = EXCLUDED.pension_account_id,
  priority = EXCLUDED.priority,
  recurrence = EXCLUDED.recurrence,
  budget_category_id = EXCLUDED.budget_category_id,
  enabled = EXCLUDED.enabled,
  kommun = EXCLUDED.kommun,
  forsamling = EXCLUDED.forsamling,
  church_member = EXCLUDED.church_member,
  is_gross = EXCLUDED.is_gross,
  event_id = EXCLUDED.event_id,
  updated_at = EXCLUDED.updated_at
RETURNING *;

-- name: DeleteSalary :exec
DELETE FROM salary
WHERE id = ?;

-- name: ListSalaryAmounts :many
SELECT *
FROM salary_amount
WHERE salary_id = ?
ORDER BY id;

-- name: GetSalaryAmount :one
SELECT *
FROM salary_amount
WHERE id = ?;

-- name: UpsertSalaryAmount :one
INSERT INTO salary_amount (
    id,
    salary_id,
    amount,
    start_date,
    event_id,
    created_at,
    updated_at
  )
VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO
UPDATE
SET salary_id = EXCLUDED.salary_id,
  amount = EXCLUDED.amount,
  start_date = EXCLUDED.start_date,
  event_id = EXCLUDED.event_id,
  updated_at = EXCLUDED.updated_at
RETURNING *;

-- name: DeleteSalaryAmount :exec
DELETE FROM salary_amount
WHERE id = ?;

-- name: ListAllSalaryAmounts :many
SELECT *
FROM salary_amount
ORDER BY salary_id, id;

-- name: ListSalaryAdjustments :many
SELECT *
FROM salary_adjustment
WHERE salary_id = ?
ORDER BY id;

-- name: UpsertSalaryAdjustment :one
INSERT INTO salary_adjustment (
    id,
    salary_id,
    valid_from,
    vacation_days_per_year,
    sick_days_per_occasion,
    sick_occasions_per_year,
    vab_days_per_year,
    event_id,
    created_at,
    updated_at
  )
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO
UPDATE
SET salary_id = EXCLUDED.salary_id,
  valid_from = EXCLUDED.valid_from,
  vacation_days_per_year = EXCLUDED.vacation_days_per_year,
  sick_days_per_occasion = EXCLUDED.sick_days_per_occasion,
  sick_occasions_per_year = EXCLUDED.sick_occasions_per_year,
  vab_days_per_year = EXCLUDED.vab_days_per_year,
  event_id = EXCLUDED.event_id,
  updated_at = EXCLUDED.updated_at
RETURNING *;

-- name: DeleteSalaryAdjustment :exec
DELETE FROM salary_adjustment
WHERE id = ?;

-- name: ListAllSalaryAdjustments :many
SELECT *
FROM salary_adjustment
ORDER BY salary_id, id;

-- name: ListAllSalaryEnds :many
SELECT *
FROM salary_end
ORDER BY salary_id, id;

-- name: GetSalaryEnd :one
SELECT *
FROM salary_end
WHERE id = ?;

-- name: UpsertSalaryEnd :one
INSERT INTO salary_end (id, salary_id, date, event_id, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO
UPDATE
SET salary_id = EXCLUDED.salary_id,
  date = EXCLUDED.date,
  event_id = EXCLUDED.event_id,
  updated_at = EXCLUDED.updated_at
RETURNING *;

-- name: DeleteSalaryEnd :exec
DELETE FROM salary_end
WHERE id = ?;

-- name: GetSalaryAdjustment :one
SELECT *
FROM salary_adjustment
WHERE id = ?;
