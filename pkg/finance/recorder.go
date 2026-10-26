package finance

import (
	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

// ReportRecorder receives a Forecast's reports.
type ReportRecorder interface {
	// OnReport receives the balance of every entity at the end of a Forecast Interval day, by entity ID.
	// The map is the recorder's to keep.
	OnReport(day date.Date, balances map[string]uncertain.Value) error
}

type TransferRecorder interface {
	OnTransfer(sourceAccountID, destinationAccountID string, day date.Date, amount uncertain.Value) error
}

type TransferRecorderFunc func(sourceAccountID, destinationAccountID string, day date.Date, amount uncertain.Value) error

func (f TransferRecorderFunc) OnTransfer(sourceAccountID, destinationAccountID string, day date.Date, amount uncertain.Value) error {
	return f(sourceAccountID, destinationAccountID, day, amount)
}
func EmptyTransferRecorder() TransferRecorder {
	return TransferRecorderFunc(func(sourceAccountID, destinationAccountID string, day date.Date, amount uncertain.Value) error {
		return nil
	})
}

type Recorder interface {
	ReportRecorder
	TransferRecorder
}

// CompositeRecorder is a Recorder made of a ReportRecorder and a TransferRecorder. Either may be nil.
type CompositeRecorder struct {
	ReportRecorder
	TransferRecorder
}

func (r CompositeRecorder) OnReport(day date.Date, balances map[string]uncertain.Value) error {
	if r.ReportRecorder == nil {
		return nil
	}
	return r.ReportRecorder.OnReport(day, balances)
}

func (r CompositeRecorder) OnTransfer(sourceAccountID, destinationAccountID string, day date.Date, amount uncertain.Value) error {
	if r.TransferRecorder == nil {
		return nil
	}
	return r.TransferRecorder.OnTransfer(sourceAccountID, destinationAccountID, day, amount)
}
