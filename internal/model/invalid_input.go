package model

import "errors"

// ErrInvalidInput is wrapped by every error that rejects what a caller asked to save, as opposed to a
// failure to save it.
var ErrInvalidInput = errors.New("invalid input")
