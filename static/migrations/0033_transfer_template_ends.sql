-- A Transfer Template ends as a Salary or Bill does: at the earliest of its ends, each a dated part in
-- an Event or in none, so an Event can end a Transfer Template outside it. Each stored End Date becomes
-- an end in the Transfer Template's own Event, or in none, with the same stored days, so every Transfer
-- Template still transfers on the same days. The End Date column goes: a Transfer Template has one way
-- to end. Deleting a Transfer Template or an Event deletes its ends.
-- migrate:up
CREATE TABLE transfer_template_end (
    id                   TEXT    NOT NULL PRIMARY KEY,
    transfer_template_id TEXT    NOT NULL REFERENCES transfer_template (id) ON DELETE CASCADE,
    date                 INTEGER NOT NULL,
    event_id             TEXT REFERENCES event (id) ON DELETE CASCADE,
    created_at           INTEGER NOT NULL,
    updated_at           INTEGER NOT NULL
);
CREATE INDEX idx_transfer_template_end_transfer_template ON transfer_template_end (transfer_template_id);
CREATE INDEX idx_transfer_template_end_event ON transfer_template_end (event_id);

INSERT INTO transfer_template_end (id, transfer_template_id, date, event_id, created_at, updated_at)
SELECT lower(hex(randomblob(16))), id, end_date, event_id, created_at, updated_at
FROM transfer_template
WHERE end_date IS NOT NULL;

ALTER TABLE transfer_template DROP COLUMN end_date;
