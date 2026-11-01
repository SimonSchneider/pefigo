# A Salary or Bill ends at the earliest of its Ends, each a dated part an Event can add

A Salary or Bill isn't given an End Date directly. It has Ends: dated parts like its Salary Amounts or Bill Amounts, each in an Event or not, and its End Date is the earliest End that is placed. An Event that ends a Salary, such as a New job, adds an End to it, just as a Second child adds parental leave to it, so an Event still only adds things. Several Events can each end the same Salary, so alternative New job Events, each used by a different Scenario ([ADR 0007](0007-scenarios-override-events-on-the-baseline.md)), all work: whichever a Scenario places earliest applies.

An End is edited on its Salary's or Bill's own page, with the Event select its other parts have. The Event's page lists the Ends an Event adds, as it lists everything the Event adds, but doesn't remove them. What the user found confusing in #157 was adding ends from the Event's page, not an Event ending something.

This extends [ADR 0008](0008-event-entries-store-their-dates-as-offsets.md) beyond Accounts, growth models and Transfer Templates. A Salary or Bill, each of its dated parts (Salary Amounts, adjustments, parental leave, Bill Amounts and Ends), and each investment round and share change belongs to at most one Event. Each stores its dates as days after its own Event's date, which may differ from its Salary's or Bill's. A one-off Salary's recurrence, such as `2027-06-01`, is stored as a one-off Transfer Template's is.

## Considered Options

- **One End Date column on the Salary or Bill, set by an Event**: only one Event could end a Salary, so two alternative New job Events could never both exist.
- **Ending entries from the Event's page**, as #157 first tried for Transfer Templates and growth models: dropped there because the user found adding ends on the Event's page confusing, and a Salary's page wouldn't show when it ends.
- **A Salary Amount of 0 from the Event's date**: the Salary would still be paid, at 0, with a Salary Breakdown and Pension Contribution of 0, and a Bill Amount of 0 would still charge.

## Consequences

- An End in a left-out Event doesn't count, so an unplanned Event that ends a Salary leaves the Baseline as without it.
- An End outside every Event is an absolute date, as any part outside an Event is: an End leaving its Event keeps the day it applies on. That also lets a Salary or Bill in the Baseline have a plain End Date, a small addition beyond #158 that the user chose to keep.
- An End leaves its Event from its own form, never from the Event's page.
- A Salary or Bill joining, leaving or changing Event brings along its parts that are in its old Event, or in none, keeping their days. Parts other Events add to it stay where they are.
- The Salary and Bill pages list every part at its own Event's date. What they show the Salary paying or the Bill charging is the Baseline's, where only placed Ends count. A Salary or Bill in an unplanned Event shows what it would pay or charge with its Event at its date.
