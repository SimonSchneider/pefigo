-- Events also add Salaries and Bills, and dated parts to Salaries, Bills and Startup Share accounts
-- outside them. A Salary, Bill, Salary Amount, salary adjustment, parental leave, Bill Amount,
-- investment round or share change belongs to at most one Event, and while it does, its dates are
-- stored as days after the Event's date. A Salary or Bill in an Event is left out with it, and so is
-- every part of it. An end is a dated part too: a Salary or Bill pays nothing from the earliest of its
-- ends. Deleting an Event deletes the entries it adds.
--
-- An investment round's date is an offset in an Event, so two Events may each add one at +0 to the
-- same account: the rounds are no longer unique by account and date.
-- migrate:up
ALTER TABLE salary ADD COLUMN event_id TEXT REFERENCES event (id) ON DELETE CASCADE;
ALTER TABLE salary_amount ADD COLUMN event_id TEXT REFERENCES event (id) ON DELETE CASCADE;
ALTER TABLE salary_adjustment ADD COLUMN event_id TEXT REFERENCES event (id) ON DELETE CASCADE;
ALTER TABLE partial_parental_leave ADD COLUMN event_id TEXT REFERENCES event (id) ON DELETE CASCADE;
ALTER TABLE full_parental_leave ADD COLUMN event_id TEXT REFERENCES event (id) ON DELETE CASCADE;
ALTER TABLE bill ADD COLUMN event_id TEXT REFERENCES event (id) ON DELETE CASCADE;
ALTER TABLE bill_amount ADD COLUMN event_id TEXT REFERENCES event (id) ON DELETE CASCADE;
ALTER TABLE share_change ADD COLUMN event_id TEXT REFERENCES event (id) ON DELETE CASCADE;

CREATE INDEX idx_salary_event ON salary (event_id);
CREATE INDEX idx_salary_amount_event ON salary_amount (event_id);
CREATE INDEX idx_salary_adjustment_event ON salary_adjustment (event_id);
CREATE INDEX idx_partial_parental_leave_event ON partial_parental_leave (event_id);
CREATE INDEX idx_full_parental_leave_event ON full_parental_leave (event_id);
CREATE INDEX idx_bill_event ON bill (event_id);
CREATE INDEX idx_bill_amount_event ON bill_amount (event_id);
CREATE INDEX idx_share_change_event ON share_change (event_id);

CREATE TABLE investment_round_new (
    id               TEXT    NOT NULL PRIMARY KEY,
    account_id       TEXT    NOT NULL,
    date             INTEGER NOT NULL,
    valuation        REAL    NOT NULL,
    created_at       INTEGER NOT NULL,
    updated_at       INTEGER NOT NULL,
    pre_money_shares REAL    NOT NULL DEFAULT 0,
    investment       REAL    NOT NULL DEFAULT 0,
    event_id         TEXT REFERENCES event (id) ON DELETE CASCADE,
    FOREIGN KEY (account_id) REFERENCES account (id) ON DELETE CASCADE
);
INSERT INTO investment_round_new (id, account_id, date, valuation, created_at, updated_at, pre_money_shares, investment)
SELECT id, account_id, date, valuation, created_at, updated_at, pre_money_shares, investment
FROM investment_round;
DROP TABLE investment_round;
ALTER TABLE investment_round_new RENAME TO investment_round;
CREATE INDEX idx_investment_round_account_date ON investment_round (account_id, date);
CREATE INDEX idx_investment_round_event ON investment_round (event_id);

CREATE TABLE salary_end (
    id         TEXT    NOT NULL PRIMARY KEY,
    salary_id  TEXT    NOT NULL REFERENCES salary (id) ON DELETE CASCADE,
    date       INTEGER NOT NULL,
    event_id   TEXT REFERENCES event (id) ON DELETE CASCADE,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX idx_salary_end_salary ON salary_end (salary_id);
CREATE INDEX idx_salary_end_event ON salary_end (event_id);

CREATE TABLE bill_end (
    id         TEXT    NOT NULL PRIMARY KEY,
    bill_id    TEXT    NOT NULL REFERENCES bill (id) ON DELETE CASCADE,
    date       INTEGER NOT NULL,
    event_id   TEXT REFERENCES event (id) ON DELETE CASCADE,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX idx_bill_end_bill ON bill_end (bill_id);
CREATE INDEX idx_bill_end_event ON bill_end (event_id);
