# Event entries store their dates as offsets from the Event's date

An Account, growth model or Transfer Template in an Event stores every date it has as the number of days after the Event's date: a growth model's or Transfer Template's Start and End Dates, and the date of a one-off Transfer Template, whose recurrence is a single date such as `2028-03-01`. The ends an Event declares store their End Dates the same way. An entry outside an Event stores days after day 0, which are its absolute dates. Moving an Event is then one write to the Event, never to its entries, and placing an Event elsewhere, as a Scenario does ([ADR 0007](0007-scenarios-override-events-on-the-baseline.md)), adds a different date to the same offsets. A recurrence that repeats, such as `*-*-25`, isn't a date and doesn't change: a monthly Transfer Template that starts on the Event's date first charges on its next recurrence day.

A one-off's date is stored as a date that many days after day 0, such as `1970-02-01` for 31 days after the Event's date, so the recurrence column always holds a date pattern.

## Considered Options

- **Absolute dates, rewritten when an Event moves**: every move writes every entry, and a Scenario that places an Event elsewhere would need its own copy of the entries' dates.
- **A one-off in an Event keeps its absolute recurrence**: its Start Date would move with the Event but the day it charges would not, so moving the Event would drop the one-off.

## Consequences

- Leaving an Event out leaves out its entries, and with its Accounts every entry outside it that uses one: a Transfer Template from or to it, and a growth model of it. A cash flow paid out to one of its Accounts stays in its own Account, as without a destination. Otherwise an unplanned Event would still change the Baseline.
- An entry joining or leaving an Event keeps the days it applies on: its dates are rebased from the Event's date to day 0, or the other way.
- Read an Event entry's dates through the model, which places them at a date. The stored values are offsets and mean nothing on their own.
- The forms give an entry in an Event its dates as days after the Event's date, such as `+20`, as they're stored. A date typed there is turned into days, and days given outside an Event are rejected.
- A Transfer Template saved without a recurrence is a one-off on its Start Date, so a one-off in an Event is given by its Start alone, such as `+5`.
