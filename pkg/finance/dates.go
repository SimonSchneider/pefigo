package finance

import "github.com/SimonSchneider/goslu/date"

// appliesOn reports whether a dated entry applies on day: from its Start Date up to, but not on, its
// End Date, and indefinitely without an End Date. It is the only comparison of a day against a Start
// Date or End Date, so don't add another. See ADR 0003.
func appliesOn(start date.Date, end *date.Date, day date.Date) bool {
	return !day.Before(start) && (end == nil || day.Before(*end))
}
