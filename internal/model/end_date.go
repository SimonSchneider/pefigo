package model

import (
	"fmt"

	"github.com/SimonSchneider/goslu/date"
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
