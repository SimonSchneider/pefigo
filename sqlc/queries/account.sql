-- name: GetAccount :one
SELECT *
FROM account
WHERE id = ?;
-- name: UpdateAccount :one
UPDATE account
SET name = ?,
  updated_at = ?,
  balance_upper_limit = ?,
  cash_flow_frequency = ?,
  cash_flow_destination_id = ?,
  type_id = ?,
  budget_category_id = ?,
  is_isk = ?
WHERE id = ?
RETURNING *;
-- name: DeleteAccount :one
DELETE FROM account
WHERE id = ?
RETURNING *;
-- name: ListAccounts :many
SELECT *
FROM account
ORDER BY name,
  id;
-- name: CreateAccount :one
INSERT INTO account (
    id,
    name,
    balance_upper_limit,
    cash_flow_frequency,
    cash_flow_destination_id,
    type_id,
    budget_category_id,
    is_isk,
    created_at,
    updated_at
  )
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING *;
-- name: GetSnapshotsByAccount :many
SELECT *
FROM account_snapshot
WHERE account_id = ?;
-- name: GetSnapshotsByAccounts :many
SELECT *
FROM account_snapshot
WHERE account_id IN (sqlc.slice('ids'))
ORDER BY date,
  account_id;
-- name: GetSnapshot :one
SELECT *
FROM account_snapshot
WHERE account_id = ?
  AND date = ?;
-- name: UpsertSnapshot :one
INSERT INTO account_snapshot (account_id, date, balance)
VALUES (?, ?, ?) ON CONFLICT (account_id, date) DO
UPDATE
SET balance = EXCLUDED.balance
RETURNING *;
-- name: DeleteSnapshot :exec
DELETE FROM account_snapshot
WHERE account_id = ?
  AND date = ?;
-- name: GetGrowthModelsByAccount :many
SELECT *
FROM growth_model
WHERE account_id = ?;
-- name: GetGrowthModel :one
SELECT *
FROM growth_model
WHERE id = ?;
-- name: UpsertGrowthModel :one
INSERT INTO growth_model (
    id,
    account_id,
    model_type,
    annual_growth_rate,
    annual_volatility,
    start_date,
    end_date,
    created_at,
    updated_at
  )
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO
UPDATE
SET model_type = EXCLUDED.model_type,
  annual_growth_rate = EXCLUDED.annual_growth_rate,
  annual_volatility = EXCLUDED.annual_volatility,
  start_date = EXCLUDED.start_date,
  end_date = EXCLUDED.end_date,
  updated_at = EXCLUDED.updated_at
RETURNING *;
-- name: DeleteGrowthModel :exec
DELETE FROM growth_model
WHERE id = ?;
-- name: GetTransferTemplates :many
SELECT *
FROM transfer_template
ORDER BY recurrence,
  priority,
  name,
  start_date,
  end_date,
  created_at;
-- name: GetTransferTemplate :one
SELECT *
FROM transfer_template
WHERE id = ?;
-- name: UpsertTransferTemplate :one
INSERT INTO transfer_template (
    id,
    name,
    from_account_id,
    to_account_id,
    amount_type,
    amount_fixed,
    amount_percent,
    priority,
    recurrence,
    start_date,
    end_date,
    enabled,
    budget_category_id,
    created_at,
    updated_at
  )
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO
UPDATE
SET name = EXCLUDED.name,
  from_account_id = EXCLUDED.from_account_id,
  to_account_id = EXCLUDED.to_account_id,
  amount_type = EXCLUDED.amount_type,
  amount_fixed = EXCLUDED.amount_fixed,
  amount_percent = EXCLUDED.amount_percent,
  priority = EXCLUDED.priority,
  recurrence = EXCLUDED.recurrence,
  start_date = EXCLUDED.start_date,
  end_date = EXCLUDED.end_date,
  enabled = EXCLUDED.enabled,
  budget_category_id = EXCLUDED.budget_category_id,
  updated_at = EXCLUDED.updated_at
RETURNING *;
-- name: DeleteTransferTemplate :exec
DELETE FROM transfer_template
WHERE id = ?;
-- name: ListLatestSnapshotPerAccount :many
SELECT s.*
FROM account_snapshot s
  INNER JOIN (
    SELECT account_id,
      MAX(date) AS max_date
    FROM account_snapshot
    GROUP BY account_id
  ) latest ON s.account_id = latest.account_id
  AND s.date = latest.max_date;

-- name: ListSnapshotHistoryWithType :many
-- Returns all snapshots with account type_id for grouping (full history).
SELECT s.account_id,
  s.date,
  s.balance,
  COALESCE(a.type_id, '') AS type_id
FROM account_snapshot s
  JOIN account a ON a.id = s.account_id
ORDER BY s.date,
  a.type_id,
  s.account_id;
-- name: ListStartupShareSnapshotHistory :many
-- Returns investment rounds joined with startup share config and account type for balance history.
-- valuation = pre_money_valuation; post_money = valuation + investment; post_money_shares derived in app.
SELECT ir.account_id,
  ir.date,
  ir.valuation,
  ir.pre_money_shares,
  ir.investment,
  ssa.tax_rate,
  ssa.valuation_discount_factor,
  COALESCE(a.type_id, '') AS type_id
FROM investment_round ir
  JOIN startup_share_account ssa ON ssa.account_id = ir.account_id
  JOIN account a ON a.id = ir.account_id
ORDER BY ir.date,
  a.type_id,
  ir.account_id;
-- name: ListGrowthModels :many
SELECT *
FROM growth_model
ORDER BY start_date, id;
-- name: UpdateSnapshotDate :many
UPDATE account_snapshot
SET date = ?
WHERE date = ?
RETURNING *;
-- name: ListAccountTypes :many
SELECT *
FROM account_type
ORDER BY name,
  id;
-- name: GetAccountType :one
SELECT *
FROM account_type
WHERE id = ?;
-- name: UpsertAccountType :one
INSERT INTO account_type (id, name, color)
VALUES (?, ?, ?) ON CONFLICT (id) DO
UPDATE
SET name = EXCLUDED.name,
  color = EXCLUDED.color
RETURNING *;
-- name: DeleteAccountType :exec
DELETE FROM account_type
WHERE id = ?;
-- name: GetSpecialDates :many
SELECT *
FROM special_date
ORDER BY date,
  name,
  id;
-- name: GetSpecialDate :one
SELECT *
FROM special_date
WHERE id = ?;
-- name: UpsertSpecialDate :one
INSERT INTO special_date (id, name, date, color)
VALUES (?, ?, ?, ?) ON CONFLICT (id) DO
UPDATE
SET name = EXCLUDED.name,
  date = EXCLUDED.date,
  color = EXCLUDED.color
RETURNING *;
-- name: DeleteSpecialDate :exec
DELETE FROM special_date
WHERE id = ?;
-- name: ListTransferTemplateCategories :many
SELECT *
FROM transfer_template_category
ORDER BY name,
  id;
-- name: GetTransferTemplateCategory :one
SELECT *
FROM transfer_template_category
WHERE id = ?;
-- name: UpsertTransferTemplateCategory :one
INSERT INTO transfer_template_category (id, name, color, created_at, updated_at)
VALUES (?, ?, ?, ?, ?) ON CONFLICT (id) DO
UPDATE
SET name = EXCLUDED.name,
  color = EXCLUDED.color,
  updated_at = EXCLUDED.updated_at
RETURNING *;
-- name: DeleteTransferTemplateCategory :exec
DELETE FROM transfer_template_category
WHERE id = ?;
-- name: UpsertStartupShareAccount :one
INSERT INTO startup_share_account (account_id, tax_rate, valuation_discount_factor)
VALUES (?, ?, ?) ON CONFLICT (account_id) DO
UPDATE
SET tax_rate = EXCLUDED.tax_rate,
  valuation_discount_factor = EXCLUDED.valuation_discount_factor
RETURNING *;
-- name: GetStartupShareAccount :one
SELECT *
FROM startup_share_account
WHERE account_id = ?;
-- name: DeleteStartupShareAccount :exec
DELETE FROM startup_share_account
WHERE account_id = ?;
-- name: UpsertShareChange :one
INSERT INTO share_change (
    id,
    account_id,
    date,
    delta_shares,
    total_price,
    created_at,
    updated_at
  )
VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO
UPDATE
SET account_id = EXCLUDED.account_id,
  date = EXCLUDED.date,
  delta_shares = EXCLUDED.delta_shares,
  total_price = EXCLUDED.total_price,
  updated_at = EXCLUDED.updated_at
RETURNING *;
-- name: GetShareChange :one
SELECT *
FROM share_change
WHERE id = ?;
-- name: ListShareChanges :many
SELECT *
FROM share_change
WHERE account_id = ?
ORDER BY date ASC,
  id;
-- name: DeleteShareChange :exec
DELETE FROM share_change
WHERE id = ?;
-- name: UpsertInvestmentRound :one
INSERT INTO investment_round (
    id,
    account_id,
    date,
    valuation,
    pre_money_shares,
    investment,
    created_at,
    updated_at
  )
VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (account_id, date) DO
UPDATE
SET valuation = EXCLUDED.valuation,
  pre_money_shares = EXCLUDED.pre_money_shares,
  investment = EXCLUDED.investment,
  updated_at = EXCLUDED.updated_at
RETURNING *;
-- name: GetInvestmentRound :one
SELECT *
FROM investment_round
WHERE id = ?;
-- name: ListInvestmentRounds :many
SELECT *
FROM investment_round
WHERE account_id = ?
ORDER BY date DESC,
  id;
-- name: GetLatestInvestmentRound :one
SELECT *
FROM investment_round
WHERE account_id = ?
  AND date <= ?
ORDER BY date DESC
LIMIT 1;
-- name: DeleteInvestmentRound :exec
DELETE FROM investment_round
WHERE id = ?;
-- name: UpsertStartupShareOption :one
INSERT INTO startup_share_option (
    id,
    account_id,
    source_account_id,
    shares,
    strike_price_per_share,
    grant_date,
    end_date,
    created_at,
    updated_at
  )
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO
UPDATE
SET account_id = EXCLUDED.account_id,
  source_account_id = EXCLUDED.source_account_id,
  shares = EXCLUDED.shares,
  strike_price_per_share = EXCLUDED.strike_price_per_share,
  grant_date = EXCLUDED.grant_date,
  end_date = EXCLUDED.end_date,
  updated_at = EXCLUDED.updated_at
RETURNING *;
-- name: GetStartupShareOption :one
SELECT *
FROM startup_share_option
WHERE id = ?;
-- name: ListStartupShareOptions :many
SELECT *
FROM startup_share_option
WHERE account_id = ?
ORDER BY grant_date,
  id;
-- name: DeleteStartupShareOption :exec
DELETE FROM startup_share_option
WHERE id = ?;
-- name: GetBudgetAccounts :many
SELECT *
FROM account
WHERE budget_category_id IS NOT NULL
ORDER BY name,
  id;