package finance

import (
	"fmt"
	"math"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

type TransferAmountType string

const (
	AmountFixed   TransferAmountType = "fixed"
	AmountPercent TransferAmountType = "percent"
)

type TransferTemplate struct {
	ID   string
	Name string

	FromAccountID string
	ToAccountID   string

	AmountType    TransferAmountType
	AmountFixed   TransferFixed
	AmountPercent TransferPercent
	Priority      int64     // lower number = happens earlier
	Recurrence    date.Cron // e.g. "*-*-25"

	EffectiveFrom date.Date
	EffectiveTo   *date.Date
	Enabled       bool
}

// AppliesOn reports whether day is from the Transfer Template's Start Date up to, but not on, its End
// Date. It says nothing about Enabled or the recurrence.
func (t TransferTemplate) AppliesOn(day date.Date) bool {
	return appliesOn(t.EffectiveFrom, t.EffectiveTo, day)
}

// ChargedOn reports whether the Transfer Template moves money on day: it is enabled, applies on day
// and its recurrence matches day.
func (t TransferTemplate) ChargedOn(day date.Date) bool {
	return t.Enabled && t.AppliesOn(day) && t.Recurrence.Matches(day)
}

type TransferFixed struct {
	Amount uncertain.Value
}
type TransferPercent struct {
	Percent float64 // e.g. 0.1 for 10%
}

func applyDailyTransfers(ucfg *uncertain.Config, accounts map[string]*ModeledEntity, transfers []TransferTemplate, day date.Date, recorder TransferRecorder) error {
	// TODO: transfers of equal priority should be applied at the same time
	// All transfers in the same priority will run simultaneously and % will be based on balance before the priority group
	// We could have a flag to allow transfers to draw money from the target account if the source account is negative
	// ie. A1=-100, A2=200,
	// T1: A1 -> A2, 50% -> A1=-50, A2=150
	// T2: A2 -> A1, 50% -> A1= 25, A2= 75

	// TODO: Do we need to support transfers which can understand if the destination account requires money?
	// For example we have a negative account and want to transfer money to it if it is negative but not if it is positive. (doesn't this solve the above question too?)
	// This is useful for loan payments and fill ups of salary accounts from saving accounts if they can't cover basic expenses.

	// TODO: Transfers will need to calculate uncertain values correctly in case of emptying accounts.
	// make sure that two probabilistic transfers don't cause inconsistent results on money in and out.
	var currentPriority int64 = math.MinInt64
	priorityBalances := make(map[string]float64)
	for _, transfer := range transfers {
		if transfer.Priority != currentPriority {
			currentPriority = transfer.Priority
			for _, account := range accounts {
				priorityBalances[account.ID] = account.balance.Sample(ucfg)
			}
		}

		fromAccountBalance, okFromBalance := priorityBalances[transfer.FromAccountID]
		fromAccount, okFrom := accounts[transfer.FromAccountID]
		toAccount, okTo := accounts[transfer.ToAccountID]

		sourceBalance := fromAccountBalance
		if !okFromBalance && okFrom {
			sourceBalance = fromAccount.balance.Sample(ucfg)
		}

		// These transfers will transfer as given, no uncertainty here, for empirical they are just changed by the transfer amount
		var transferAmount uncertain.Value
		switch transfer.AmountType {
		case AmountFixed:
			transferAmount = transfer.AmountFixed.Amount
		case AmountPercent:
			transferAmount = uncertain.NewFixed(sourceBalance * transfer.AmountPercent.Percent)
		default:
			continue // Unknown amount type
		}

		amount := transferAmount
		if okTo && toAccount.BalanceLimit.Upper.Valid() {
			destTransferLimit := toAccount.BalanceLimit.Upper.Sub(ucfg, toAccount.balance)
			amount = uncertain.NewMapped(func(cfg *uncertain.Config) float64 {
				return math.Min(destTransferLimit.Sample(cfg), transferAmount.Sample(cfg))
			})
		}

		if err := recorder.OnTransfer(transfer.FromAccountID, transfer.ToAccountID, day, amount); err != nil {
			return fmt.Errorf("failed to record transfer %s from %s to %s on %s: %w", transfer.ID, transfer.FromAccountID, transfer.ToAccountID, day, err)
		}

		if okFrom {
			// Perform the transfer
			fromAccount.balance = fromAccount.balance.Sub(ucfg, amount)
		}
		if okTo {
			toAccount.balance = toAccount.balance.Add(ucfg, amount)
			toAccount.dayDeposits = toAccount.dayDeposits.Add(ucfg, amount)
		}
	}
	return nil
}
