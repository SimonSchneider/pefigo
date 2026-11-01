package model

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/pkg/finance"
)

// TransferSchedule is every Transfer Template of the plans for the future with the Events placed: the
// stored ones and those generated from Salaries and Bills. It decides which of them apply on a day and
// for how much, so its callers never compare a day against a Start Date or End Date, or check Enabled
// or a recurrence, themselves. See ADR 0003.
type TransferSchedule struct {
	templates []TransferTemplate // sorted by recurrence, priority, name and Start Date
	// leftOut are the Transfer Templates the Events leave out: those of a left-out Event, at its own
	// date, and those that use one of its Accounts.
	leftOut []TransferTemplate
}

// TransferSchedule is the Baseline's Transfer Schedule.
func (s *Service) TransferSchedule(ctx context.Context) (*TransferSchedule, error) {
	p, err := s.placeEvents(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("placing the events: %w", err)
	}
	return s.transferSchedule(ctx, p)
}

// transferSchedule is the Transfer Schedule of p: its Transfer Templates with those generated from
// Salaries and Bills, but those that use an Account p leaves out.
func (s *Service) transferSchedule(ctx context.Context, p *plans) (*TransferSchedule, error) {
	salaryTemplates, err := s.salaryTransferTemplates(ctx, p.salaries)
	if err != nil {
		return nil, fmt.Errorf("generating salary transfer templates: %w", err)
	}
	billTemplates, err := s.generateBillTransferTemplates(ctx)
	if err != nil {
		return nil, fmt.Errorf("generating bill transfer templates: %w", err)
	}
	ts := &TransferSchedule{
		templates: slices.Clone(p.transferTemplates),
		leftOut:   slices.Clone(p.leftOutTransferTemplates),
	}
	for _, t := range append(salaryTemplates, billTemplates...) {
		if p.usesLeftOutAccount(t) {
			ts.leftOut = append(ts.leftOut, t)
		} else {
			ts.templates = append(ts.templates, t)
		}
	}
	sortTransferTemplates(ts.templates)
	sortTransferTemplates(ts.leftOut)
	return ts, nil
}

// LeftOut returns the Transfer Templates the Events leave out of the schedule, sorted as the
// schedule is.
func (ts *TransferSchedule) LeftOut() []TransferTemplate {
	return ts.leftOut
}

// checkRecurrences fails on the first Transfer Template whose recurrence isn't a valid date pattern,
// naming the stored row it is from. Only charging a day matches recurrences, so a Forecast checks them
// when it loads, while listing the schedule shows them, to be fixed.
func (ts *TransferSchedule) checkRecurrences() error {
	for _, t := range ts.templates {
		if !validDatePattern(t.Recurrence) {
			return fmt.Errorf("%s has invalid recurrence %q", t.recurrenceRow(), t.Recurrence)
		}
	}
	return nil
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
	// Amount is what the Transfer Template moves when it is Active on the day, and 0 otherwise. In
	// Transfers, Monthly and Rows every Active Transfer Template is evaluated, as one month of
	// transfers; in Charged only those charged on the day are. See evaluate.
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
	d := &TransferScheduleDay{Day: day, transfers: make([]ScheduledTransfer, len(ts.templates))}
	for i, t := range ts.templates {
		d.transfers[i] = ScheduledTransfer{TransferTemplate: t, state: transferStateOn(t, day)}
	}
	evaluate(d.transfers, monthGroup)
	return d
}

type evaluationGroup struct {
	recurrence date.Cron
	priority   int64
}

// monthGroup groups a month of transfers by recurrence and priority, since different recurrences
// happen on different days.
func monthGroup(t ScheduledTransfer) evaluationGroup {
	return evaluationGroup{recurrence: t.Recurrence, priority: t.Priority}
}

// dayGroup groups one day's transfers by priority, as the forecast engine applies them.
func dayGroup(t ScheduledTransfer) evaluationGroup {
	return evaluationGroup{priority: t.Priority}
}

// evaluate sets the Amount of the Active transfers in order, starting from empty Accounts. A run of
// transfers in the same group happens at the same time: a percent amount is taken from the balance
// its source Account has before its group.
func evaluate(transfers []ScheduledTransfer, groupOf func(ScheduledTransfer) evaluationGroup) {
	type balance struct {
		beforeGroup float64
		current     float64
	}
	balances := make(map[string]balance)
	var group *evaluationGroup
	for i := range transfers {
		t := &transfers[i]
		t.Amount = 0
		if t.state != TransferActive {
			continue
		}
		if k := groupOf(*t); group == nil || *group != k {
			group = &k
			for id, b := range balances {
				b.beforeGroup = b.current
				balances[id] = b
			}
		}
		switch t.AmountType {
		case TransferAmountFixed:
			t.Amount = t.AmountFixed.Mean()
		case TransferAmountPercent:
			t.Amount = t.AmountPercent * balances[t.FromAccountID].beforeGroup
		}
		from := balances[t.FromAccountID]
		from.current -= t.Amount
		balances[t.FromAccountID] = from
		to := balances[t.ToAccountID]
		to.current += t.Amount
		balances[t.ToAccountID] = to
	}
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
// matches it, in priority order and evaluated on their own in priority groups, as the forecast charges
// them.
func (d *TransferScheduleDay) Charged() []ScheduledTransfer {
	var charged []ScheduledTransfer
	for _, t := range d.transfers {
		if t.ToFinanceTransferTemplate().ChargedOn(d.Day) {
			charged = append(charged, t)
		}
	}
	slices.SortStableFunc(charged, func(a, b ScheduledTransfer) int { return cmp.Compare(a.Priority, b.Priority) })
	evaluate(charged, dayGroup)
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

// TransferScheduleRow is one row of the Transfer Template table: a Transfer Template, or a group of
// rows. The Transfer Templates that share a name, priority, recurrence, Accounts, Event and the Salary
// or Bill they are generated from, such as the segments of a Bill, are one group, and the Bills of a Bill Account are one group of those rows. A
// group spans its Members' dates, is Active when one of them is, is Disabled when all of them are, and
// its Amount is their total.
type TransferScheduleRow struct {
	ScheduledTransfer
	Members []TransferScheduleRow
}

func (r TransferScheduleRow) IsGroup() bool {
	return len(r.Members) > 0
}

// isBillAccount reports whether the row is the group of a Bill Account's Bills.
func (r TransferScheduleRow) isBillAccount() bool {
	return r.Source.Type == TransferSourceBill && r.Source.EntityID == ""
}

// Rows groups the evaluated Transfer Templates for display, so grouping changes no amount.
func (d *TransferScheduleDay) Rows() []TransferScheduleRow {
	type groupKey struct {
		name       string
		priority   int64
		recurrence date.Cron
		from, to   string
		eventID    string
		// source is the Salary or Bill the Transfer Template is generated from: one Bill's segments
		// group, two Bills of one name don't.
		source TransferTemplateSource
	}
	groups := make(map[groupKey][]TransferScheduleRow)
	var order []groupKey
	for _, t := range d.transfers {
		k := groupKey{
			name: t.Name, priority: t.Priority, recurrence: t.Recurrence, from: t.FromAccountID, to: t.ToAccountID, eventID: t.EventID,
			source: TransferTemplateSource{Type: t.Source.Type, EntityID: t.Source.EntityID},
		}
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], TransferScheduleRow{ScheduledTransfer: t})
	}
	rows := make([]TransferScheduleRow, 0, len(order))
	for _, k := range order {
		if members := groups[k]; len(members) == 1 {
			rows = append(rows, members[0])
		} else {
			rows = append(rows, groupRow("group-"+members[0].ID, members))
		}
	}
	return foldBillAccounts(rows)
}

// foldBillAccounts puts the rows of each Bill Account's Bills in one group, named after the Bill
// Account, where its first Bill was.
func foldBillAccounts(rows []TransferScheduleRow) []TransferScheduleRow {
	bills := make(map[string][]TransferScheduleRow)
	for _, row := range rows {
		if row.Source.Type == TransferSourceBill {
			bills[row.Source.BillAccountID] = append(bills[row.Source.BillAccountID], row)
		}
	}
	folded := make([]TransferScheduleRow, 0, len(rows))
	for _, row := range rows {
		if row.Source.Type != TransferSourceBill {
			folded = append(folded, row)
			continue
		}
		members, ok := bills[row.Source.BillAccountID]
		if !ok {
			continue
		}
		delete(bills, row.Source.BillAccountID)
		group := groupRow("bill-account-"+row.Source.BillAccountID, members)
		group.Name = row.Source.BillAccountName
		group.BudgetCategoryID = nil
		group.Source = TransferTemplateSource{Type: TransferSourceBill, Label: row.Source.BillAccountName, BillAccountID: row.Source.BillAccountID}
		folded = append(folded, group)
	}
	return folded
}

// groupRow is the group id of members, named and placed as its first member.
func groupRow(id string, members []TransferScheduleRow) TransferScheduleRow {
	first := members[0]
	group := ScheduledTransfer{
		TransferTemplate: TransferTemplate{
			ID:               id,
			Name:             first.Name,
			FromAccountID:    first.FromAccountID,
			ToAccountID:      first.ToAccountID,
			Priority:         first.Priority,
			Recurrence:       first.Recurrence,
			StartDate:        first.StartDate,
			EndDate:          first.EndDate,
			BudgetCategoryID: first.BudgetCategoryID,
			EventID:          first.EventID,
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
