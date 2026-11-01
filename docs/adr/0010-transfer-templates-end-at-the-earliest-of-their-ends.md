# A Transfer Template ends at the earliest of its Ends, each a dated part an Event can add

A Transfer Template isn't given an End Date directly. It has Ends, as a Salary or Bill does ([ADR 0009](0009-salaries-and-bills-end-at-the-earliest-of-their-ends.md)): dated parts, each in an Event or not, and its End Date is the earliest End that is placed. An End's Event is its own, independent of the Transfer Template's, so an Event can end a Transfer Template outside it without its Start moving: a "Move abroad" Event ends Rent paid since 2025, and moving the Event moves only the End. Several Events can each end the same Transfer Template, and whichever a Scenario places earliest applies.

An End is edited on its Transfer Template's own page, with the Event select its form has. The Event's page lists the Ends an Event adds, as it lists Salary and Bill Ends, but doesn't remove them.

## Considered Options

- **Keep the End Date column, with an Event select of its own for it**: a second Event could then end a Transfer Template, but only one, so two alternative Events, each used by a different Scenario, couldn't both end it. It is also a second, different way to end something beside Salary and Bill Ends.
- **Keep the End Date column beside Ends**: two ways to end a Transfer Template, and the form would have to say which wins.
- **Put the Transfer Template in the Event that ends it**: its Start becomes an offset from that Event too, so moving the Event moves when it starts.

## Consequences

- Each End Date stored before this became an End in the Transfer Template's own Event, or in none, with the same stored days, so every Transfer Template transfers on the same days. The End Date column is gone.
- The Transfer Template form has a Start only. A new Transfer Template is saved first, then given Ends on its page.
- An End on or before a Transfer Template's Start isn't rejected, as the two may be in different Events: the Transfer Template then never applies. For a one-off, an End on or before its date means it never transfers.
- An End in a left-out Event doesn't count. A Transfer Template that is left out, listed apart on the Transfer Templates page, shows the End it would have with its own Event planned: its Ends in that Event, at its own date, and those placed.
- A Transfer Template joining, leaving or changing Event brings along its Ends in its old Event, or in none, keeping their days. Ends other Events add to it stay where they are.
- Duplicating a Transfer Template copies its Ends, each in the same Event.
- Deleting a Transfer Template, or the Event or Account it is deleted with, deletes its Ends; the Event's delete page lists those in other Events.
- `TransferTemplate.EndDate` remains as the End Date the Forecast reads: for a stored Transfer Template it is the earliest of the Ends it is loaded with, and saving it doesn't read it.
