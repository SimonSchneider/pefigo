-- End Dates are now exclusive (ADR 0003): move each stored End Date to the day after, so every entry
-- still applies on the day that was its last. Parental leave was already exclusive.
-- migrate:up
UPDATE transfer_template SET end_date = end_date + 1 WHERE end_date IS NOT NULL;
UPDATE bill_amount SET end_date = end_date + 1 WHERE end_date IS NOT NULL;
UPDATE growth_model SET end_date = end_date + 1 WHERE end_date IS NOT NULL;
