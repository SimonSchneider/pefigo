package model

import (
	"fmt"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/pkg/finance"
)

// ErrEndDateNotAfterStartDate is returned when saving an End Date on or before its Start Date, since the
// entry would then never apply. See ADR 0003.
var ErrEndDateNotAfterStartDate = fmt.Errorf("%w: end date must be after the start date", ErrInvalidInput)

func checkEndDate(start date.Date, end *date.Date) error {
	if end != nil && !end.After(start) {
		return fmt.Errorf("%w: %s is not after %s", ErrEndDateNotAfterStartDate, end, start)
	}
	return nil
}

// earliest is the earlier of two End Dates, where nil is none.
func earliest(a, b *date.Date) *date.Date {
	if a == nil || (b != nil && b.Before(*a)) {
		return b
	}
	return a
}

// firstEnd is the End Date of a Salary or Bill with ends: the earliest day of its ends, from which
// nothing of it applies, or nil without any.
func firstEnd[E any](ends []E, day func(E) date.Date) *date.Date {
	var end *date.Date
	for _, e := range ends {
		end = earliest(end, ptr(day(e)))
	}
	return end
}

// endedBy reports whether what ends on end, or never for nil, has ended by day: an entry from day up to
// end doesn't apply on day, by the one rule of ADR 0003.
func endedBy(end *date.Date, day date.Date) bool {
	return !finance.AppliesOn(day, end, day)
}
