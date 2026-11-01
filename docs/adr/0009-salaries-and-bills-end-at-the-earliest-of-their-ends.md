# A Salary or Bill ends at the earliest of its ends, each a dated part an Event can add

A Salary or Bill has no End Date of its own. It has ends instead: dated parts, like its Salary Amounts or Bill Amounts, each in an Event or not, and it pays nothing from the earliest end that is placed. An Event that ends a Salary, such as a New job, adds an end to it, just as a Second child adds parental leave to it, so an Event still only adds things ([ADR 0008](0008-event-entries-store-their-dates-as-offsets.md)). The end is edited on the Salary's or Bill's own page, with the Event select its other parts have, never from the Event's page. Several Events can each end the same Salary, so alternative New job Events, each used by a different Scenario ([ADR 0007](0007-scenarios-override-events-on-the-baseline.md)), all work: whichever a Scenario places earliest applies.

## Considered Options

- **One End Date column on the Salary or Bill, set by an Event**: only one Event could end a Salary, so two alternative New job Events could never both exist.
- **An Event's page listing the entries it ends**, as #157 first tried for Transfer Templates and growth models: dropped there because ending entries from the Event's page was confusing, and a Salary's page wouldn't show when it ends.
- **A Salary Amount of 0 from the Event's date**: the Salary would still be paid, at 0, with a Salary Breakdown and Pension Contribution of 0, and a Bill Amount of 0 would still charge.

## Consequences

- An end in a left-out Event doesn't count, so an unplanned Event that ends a Salary leaves the Baseline as without it.
- An end outside every Event is an absolute date, as any part outside an Event is: an end leaving its Event keeps the day it applies on.
- The Salary and Bill pages list every end at its own Event's date, but what they show the Salary paying or the Bill charging is the Baseline's, where only placed ends count.
