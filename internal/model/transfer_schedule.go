package model

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/pkg/finance"
)

// TransferSchedule is every Transfer Template: the stored ones and those generated from Salaries and
// Bills. It decides which of them apply on a day and for how much, so its callers never compare a day
// against a Start Date or End Date, or check Enabled or a recurrence, themselves. See ADR 0003.
type TransferSchedule struct {
	templates []TransferTemplate // sorted by recurrence, priority, name and Start Date
}

func (s *Service) TransferSchedule(ctx context.Context) (*TransferSchedule, error) {
	templates, err := s.ListTransferTemplates(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing DB transfer templates: %w", err)
	}
	salaryTemplates, err := s.generateSalaryTransferTemplates(ctx)
	if err != nil {
		return nil, fmt.Errorf("generating salary transfer templates: %w", err)
	}
	billTemplates, err := s.generateBillTransferTemplates(ctx)
	if err != nil {
		return nil, fmt.Errorf("generating bill transfer templates: %w", err)
	}
	all := append(templates, salaryTemplates...)
	all = append(all, billTemplates...)
	sortTransferTemplates(all)
	return &TransferSchedule{templates: all}, nil
}

// ForecastTransfers returns the schedule for the forecast engine, which charges each Transfer Template
// on the days TransferScheduleDay.Charged would.
func (ts *TransferSchedule) ForecastTransfers() []finance.TransferTemplate {
	transfers := make([]finance.TransferTemplate, len(ts.templates))
	for i, t := range ts.templates {
		transfers[i] = t.ToFinanceTransferTemplate()
	}
	return transfers
}

// Recurring reports whether the Transfer Template repeats, rather than being charged on one day.
func (t TransferTemplate) Recurring() bool {
	return strings.Contains(string(t.Recurrence), "*")
}

// TransferKind says where a Transfer Template's money comes from and goes to.
type TransferKind string

const (
	// TransferIncome comes from outside the Accounts.
	TransferIncome TransferKind = "income"
	// TransferExpense leaves the Accounts.
	TransferExpense TransferKind = "expense"
	// TransferInternal moves between two Accounts.
	TransferInternal TransferKind = "internal"
)

func (t TransferTemplate) Kind() TransferKind {
	switch {
	case t.FromAccountID == "":
		return TransferIncome
	case t.ToAccountID == "":
		return TransferExpense
	default:
		return TransferInternal
	}
}

// TransferState says whether a Transfer Template applies on a day.
type TransferState string

const (
	// TransferActive: enabled, and the day is from its Start Date up to, but not on, its End Date.
	TransferActive TransferState = "Active"
	// TransferInactive: enabled, but the day is before its Start Date or on or after its End Date.
	TransferInactive TransferState = "Inactive"
	TransferDisabled TransferState = "Disabled"
)

// ScheduledTransfer is a Transfer Template evaluated on a day.
type ScheduledTransfer struct {
	TransferTemplate
	// Amount is what the Transfer Template moves when it is Active on the day, and 0 otherwise. The
	// Active Transfer Templates are evaluated in groups of one recurrence and priority, in schedule
	// order, starting from empty Accounts: a percent amount is taken from the balance its source
	// Account has before its group.
	Amount float64
	state  TransferState
}

func (t ScheduledTransfer) ActiveState() TransferState {
	return t.state
}

// HasDifferentAmount reports whether Amount differs from the configured fixed amount.
func (t ScheduledTransfer) HasDifferentAmount() bool {
	return math.Abs(t.Amount-t.AmountFixed.Mean()) > 1
}

// TransferScheduleDay is the Transfer Schedule evaluated on one day.
type TransferScheduleDay struct {
	Day       date.Date
	transfers []ScheduledTransfer // every Transfer Template, in schedule order
}

func (ts *TransferSchedule) On(day date.Date) *TransferScheduleDay {
	type balance struct {
		beforeGroup float64
		current     float64
	}
	type groupKey struct {
		recurrence date.Cron
		priority   int64
	}
	balances := make(map[string]balance)
	var group *groupKey
	d := &TransferScheduleDay{Day: day, transfers: make([]ScheduledTransfer, len(ts.templates))}
	for i, t := range ts.templates {
		st := ScheduledTransfer{TransferTemplate: t, state: transferStateOn(t, day)}
		d.transfers[i] = st
		if st.state != TransferActive {
			continue
		}
		if k := (groupKey{t.Recurrence, t.Priority}); group == nil || *group != k {
			group = &k
			for id, b := range balances {
				b.beforeGroup = b.current
				balances[id] = b
			}
		}
		switch t.AmountType {
		case "fixed":
			st.Amount = t.AmountFixed.Mean()
		case "percent":
			st.Amount = t.AmountPercent * balances[t.FromAccountID].beforeGroup
		}
		from := balances[t.FromAccountID]
		from.current -= st.Amount
		balances[t.FromAccountID] = from
		to := balances[t.ToAccountID]
		to.current += st.Amount
		balances[t.ToAccountID] = to
		d.transfers[i] = st
	}
	return d
}

func transferStateOn(t TransferTemplate, day date.Date) TransferState {
	if !t.Enabled {
		return TransferDisabled
	}
	if !t.ToFinanceTransferTemplate().AppliesOn(day) {
		return TransferInactive
	}
	return TransferActive
}

// Transfers returns every Transfer Template in schedule order, with its state and amount on the day.
func (d *TransferScheduleDay) Transfers() []ScheduledTransfer {
	return d.transfers
}

// Charged returns the Transfer Templates that move money on the day: the Active ones whose recurrence
// matches it.
func (d *TransferScheduleDay) Charged() []ScheduledTransfer {
	var charged []ScheduledTransfer
	for _, t := range d.transfers {
		if t.ToFinanceTransferTemplate().ChargedOn(d.Day) {
			charged = append(charged, t)
		}
	}
	return charged
}

// Monthly returns the Active recurring Transfer Templates: one month of recurring transfers as of the
// day.
func (d *TransferScheduleDay) Monthly() []ScheduledTransfer {
	var monthly []ScheduledTransfer
	for _, t := range d.transfers {
		if t.state == TransferActive && t.Recurring() {
			monthly = append(monthly, t)
		}
	}
	return monthly
}

// TransferScheduleRow is one row of the Transfer Template table: a Transfer Template, or a group of the
// Transfer Templates that share a name, priority, recurrence and Accounts, such as the segments of a
// Bill. A group spans its Members' dates, is Active when one of them is, is Disabled when all of them
// are, and its Amount is their total.
type TransferScheduleRow struct {
	ScheduledTransfer
	Members []ScheduledTransfer
}

func (r TransferScheduleRow) IsGroup() bool {
	return len(r.Members) > 0
}

// Rows groups the evaluated Transfer Templates for display, so grouping changes no amount.
func (d *TransferScheduleDay) Rows() []TransferScheduleRow {
	type groupKey struct {
		name       string
		priority   int64
		recurrence date.Cron
		from, to   string
	}
	groups := make(map[groupKey][]ScheduledTransfer)
	var order []groupKey
	for _, t := range d.transfers {
		k := groupKey{name: t.Name, priority: t.Priority, recurrence: t.Recurrence, from: t.FromAccountID, to: t.ToAccountID}
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], t)
	}
	rows := make([]TransferScheduleRow, 0, len(order))
	for _, k := range order {
		if members := groups[k]; len(members) == 1 {
			rows = append(rows, TransferScheduleRow{ScheduledTransfer: members[0]})
		} else {
			rows = append(rows, groupRow(members))
		}
	}
	return rows
}

func groupRow(members []ScheduledTransfer) TransferScheduleRow {
	first := members[0]
	group := ScheduledTransfer{
		TransferTemplate: TransferTemplate{
			ID:               "group-" + first.ID,
			Name:             first.Name,
			FromAccountID:    first.FromAccountID,
			ToAccountID:      first.ToAccountID,
			Priority:         first.Priority,
			Recurrence:       first.Recurrence,
			StartDate:        first.StartDate,
			EndDate:          first.EndDate,
			BudgetCategoryID: first.BudgetCategoryID,
			Source:           first.Source,
		},
		state: TransferDisabled,
	}
	for _, m := range members {
		group.Amount += m.Amount
		group.Enabled = group.Enabled || m.Enabled
		if m.StartDate < group.StartDate {
			group.StartDate = m.StartDate
		}
		if group.EndDate != nil && (m.EndDate == nil || *m.EndDate > *group.EndDate) {
			group.EndDate = m.EndDate
		}
		if m.state == TransferActive || (m.state == TransferInactive && group.state == TransferDisabled) {
			group.state = m.state
		}
	}
	return TransferScheduleRow{ScheduledTransfer: group, Members: members}
}
