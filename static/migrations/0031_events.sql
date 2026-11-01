-- Events group related changes to the plans for the future around one date. An account, growth model
-- or Transfer Template belongs to at most one Event, and while it does, its Start and End Dates are
-- stored as days after the Event's date, so moving the Event moves them. Deleting an Event deletes the
-- entries it adds.
-- migrate:up
CREATE TABLE event (
    id         TEXT    NOT NULL PRIMARY KEY,
    name       TEXT    NOT NULL,
    date       INTEGER NOT NULL,
    planned    BOOLEAN NOT NULL DEFAULT TRUE,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

ALTER TABLE account ADD COLUMN event_id TEXT REFERENCES event (id) ON DELETE CASCADE;
ALTER TABLE growth_model ADD COLUMN event_id TEXT REFERENCES event (id) ON DELETE CASCADE;
ALTER TABLE transfer_template ADD COLUMN event_id TEXT REFERENCES event (id) ON DELETE CASCADE;

CREATE INDEX idx_account_event ON account (event_id);
CREATE INDEX idx_growth_model_event ON growth_model (event_id);
CREATE INDEX idx_transfer_template_event ON transfer_template (event_id);
