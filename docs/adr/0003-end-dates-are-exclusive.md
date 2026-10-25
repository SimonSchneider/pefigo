# End Dates are exclusive

Every dated entry applies from its Start Date up to, but not on, its End Date. Salary and Bill Amounts are split into segments where each one ends on the day the next starts, so this is the only rule under which a price change on a recurrence day charges the new amount once. Previously the forecast engine and the Transfer Template page counted the End Date as a day the entry applies, and the transfers calculator counted neither the Start Date nor the End Date. On the day a Bill's price changed, the first two charged both amounts and the calculator charged nothing.

## Considered Options

- **Inclusive End Dates everywhere**, with generated segments ending the day before the next one starts: keeps the meaning of End Dates already typed into the app, but generated segments would have to subtract a day, and parental leave already treats its End Date as exclusive.
- **Exclusive internally, inclusive in the UI**: generated segments would then display an End Date that means something different from a typed one.

## Consequences

- A migration adds one day to every stored Transfer Template, Bill Amount and growth model End Date, so existing forecasts don't move. An End Date entered before the migration shows one day later than it was typed.
- An End Date on or before its Start Date is rejected, because the entry would never apply.
- A Startup Share Option's end date is the day it is exercised, not the end of a range, so this rule doesn't apply to it.
- Don't add a check that treats an End Date as inclusive. The rule lives in one place, and the forecast and the Transfer Template pages read it from there.
