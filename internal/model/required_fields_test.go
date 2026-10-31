package model_test

import (
	"context"
	"errors"
	"testing"

	"github.com/SimonSchneider/pefigo/internal/model"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

// parentRows are the rows the entities of TestSavingWithoutARequiredFieldIsInvalidInput link to.
type parentRows struct {
	account, sourceAccount, salary, billAccount, bill string
}

func mustParentRows(t *testing.T, svc *model.Service) parentRows {
	t.Helper()
	ctx := t.Context()
	salary, err := svc.UpsertSalary(ctx, model.Salary{Name: "Acme"})
	if err != nil {
		t.Fatalf("create salary: %v", err)
	}
	billAccount, err := svc.UpsertBillAccount(ctx, model.BillAccount{Name: "Bills"})
	if err != nil {
		t.Fatalf("create bill account: %v", err)
	}
	bill, err := svc.UpsertBill(ctx, model.Bill{BillAccountID: billAccount.ID, Name: "Rent"})
	if err != nil {
		t.Fatalf("create bill: %v", err)
	}
	return parentRows{
		account:       mustAccount(t, svc, "Startup").ID,
		sourceAccount: mustAccount(t, svc, "Checking").ID,
		salary:        salary.ID,
		billAccount:   billAccount.ID,
		bill:          bill.ID,
	}
}

// requiredField is an entity saved with one of its required fields left empty.
type requiredField struct {
	name string
	// save saves the entity, with the field left empty when empty is true.
	save func(ctx context.Context, svc *model.Service, p parentRows, empty bool) error
	// stored is how many of the entity are stored.
	stored func(ctx context.Context, svc *model.Service, p parentRows) (int, error)
}

// requiredFieldsOf are the required fields of the entity saved by save: valid is the entity with every
// one given, and each of empty leaves one of them empty.
func requiredFieldsOf[T any](
	save func(context.Context, *model.Service, T) error,
	stored func(context.Context, *model.Service, parentRows) (int, error),
	valid func(parentRows) T,
	empty map[string]func(*T),
) []requiredField {
	var fields []requiredField
	for name, leaveEmpty := range empty {
		fields = append(fields, requiredField{
			name: name,
			save: func(ctx context.Context, svc *model.Service, p parentRows, empty bool) error {
				inp := valid(p)
				if empty {
					leaveEmpty(&inp)
				}
				return save(ctx, svc, inp)
			},
			stored: stored,
		})
	}
	return fields
}

// saved adapts a Service upsert to the save of requiredFieldsOf.
func saved[T, R any](upsert func(*model.Service, context.Context, T) (R, error)) func(context.Context, *model.Service, T) error {
	return func(ctx context.Context, svc *model.Service, inp T) error {
		_, err := upsert(svc, ctx, inp)
		return err
	}
}

// listed counts what list returns for every one of parentIDs, including rows saved with an empty
// parent link.
func listed[T any](list func(*model.Service, context.Context, string) ([]T, error), parentIDs func(parentRows) []string) func(context.Context, *model.Service, parentRows) (int, error) {
	return func(ctx context.Context, svc *model.Service, p parentRows) (int, error) {
		n := 0
		for _, id := range append(parentIDs(p), "") {
			rows, err := list(svc, ctx, id)
			if err != nil {
				return 0, err
			}
			n += len(rows)
		}
		return n, nil
	}
}

// listedAll counts what list returns.
func listedAll[T any](list func(*model.Service, context.Context) ([]T, error)) func(context.Context, *model.Service, parentRows) (int, error) {
	return func(ctx context.Context, svc *model.Service, _ parentRows) (int, error) {
		rows, err := list(svc, ctx)
		return len(rows), err
	}
}

var (
	requiredDay = mustParseDate("2026-01-01")
	laterDay    = mustParseDate("2026-06-01")
)

func ofAccount(p parentRows) []string { return []string{p.account} }
func ofSalary(p parentRows) []string  { return []string{p.salary} }

// snapshotInput is what UpsertAccountSnapshot and UpsertOrDeleteSnapshot save.
type snapshotInput struct {
	accountID string
	model.AccountSnapshotInput
}

var storedSnapshots = func(ctx context.Context, svc *model.Service, p parentRows) (int, error) {
	snaps, err := svc.ListAccountsSnapshots(ctx, []string{p.account, ""})
	return len(snaps), err
}

var requiredFields = map[string][]requiredField{
	"UpsertAccount": requiredFieldsOf(saved((*model.Service).UpsertAccount),
		listedAll((*model.Service).ListAccounts),
		func(parentRows) model.AccountInput { return model.AccountInput{Name: "Savings"} },
		map[string]func(*model.AccountInput){"name": func(a *model.AccountInput) { a.Name = "" }},
	),
	"UpsertAccountWithStartupShares": requiredFieldsOf(
		func(ctx context.Context, svc *model.Service, inp model.AccountInput) error {
			_, err := svc.UpsertAccountWithStartupShares(ctx, inp, &model.StartupShareAccountInput{})
			return err
		},
		listedAll((*model.Service).ListAccounts),
		func(parentRows) model.AccountInput { return model.AccountInput{Name: "Startup 2"} },
		map[string]func(*model.AccountInput){"name": func(a *model.AccountInput) { a.Name = "" }},
	),
	"UpsertAccountType": requiredFieldsOf(saved((*model.Service).UpsertAccountType),
		listedAll((*model.Service).ListAccountTypes),
		func(parentRows) model.AccountTypeInput { return model.AccountTypeInput{Name: "Savings"} },
		map[string]func(*model.AccountTypeInput){"name": func(a *model.AccountTypeInput) { a.Name = "" }},
	),
	"UpsertSpecialDate": requiredFieldsOf(saved((*model.Service).UpsertSpecialDate),
		listedAll((*model.Service).ListSpecialDates),
		func(parentRows) model.SpecialDateInput {
			return model.SpecialDateInput{Name: "Retirement", Date: requiredDay}
		},
		map[string]func(*model.SpecialDateInput){
			"name": func(s *model.SpecialDateInput) { s.Name = "" },
			"date": func(s *model.SpecialDateInput) { s.Date = 0 },
		},
	),
	"UpsertEvent": requiredFieldsOf(saved((*model.Service).UpsertEvent),
		listedAll((*model.Service).ListEvents),
		func(parentRows) model.EventInput { return model.EventInput{Name: "Apartment", Date: requiredDay} },
		map[string]func(*model.EventInput){
			"name": func(e *model.EventInput) { e.Name = "" },
			"date": func(e *model.EventInput) { e.Date = 0 },
		},
	),
	"UpsertAccountGrowthModel": requiredFieldsOf(saved((*model.Service).UpsertAccountGrowthModel),
		listed((*model.Service).ListAccountGrowthModels, ofAccount),
		func(p parentRows) model.AccountGrowthModelInput {
			return model.AccountGrowthModelInput{AccountID: p.account, Type: model.GrowthModelFixed, AnnualRate: newFixedValue(0.05), AnnualVolatility: newFixedValue(0), StartDate: requiredDay}
		},
		map[string]func(*model.AccountGrowthModelInput){
			"account":    func(g *model.AccountGrowthModelInput) { g.AccountID = "" },
			"start date": func(g *model.AccountGrowthModelInput) { g.StartDate = 0 },
		},
	),
	"UpsertStartupShareAccount": requiredFieldsOf(saved((*model.Service).UpsertStartupShareAccount),
		func(ctx context.Context, svc *model.Service, p parentRows) (int, error) {
			n := 0
			for _, id := range []string{p.account, ""} {
				if _, err := svc.GetStartupShareAccount(ctx, id); err == nil {
					n++
				}
			}
			return n, nil
		},
		func(p parentRows) model.StartupShareAccountInput {
			return model.StartupShareAccountInput{AccountID: p.account}
		},
		map[string]func(*model.StartupShareAccountInput){"account": func(s *model.StartupShareAccountInput) { s.AccountID = "" }},
	),
	"UpsertInvestmentRound": requiredFieldsOf(saved((*model.Service).UpsertInvestmentRound),
		listed((*model.Service).ListInvestmentRounds, ofAccount),
		func(p parentRows) model.InvestmentRoundInput {
			return model.InvestmentRoundInput{AccountID: p.account, Date: requiredDay, Valuation: 1e6, PreMoneyShares: 1000}
		},
		map[string]func(*model.InvestmentRoundInput){
			"account": func(i *model.InvestmentRoundInput) { i.AccountID = "" },
			"date":    func(i *model.InvestmentRoundInput) { i.Date = 0 },
		},
	),
	"UpsertShareChange": requiredFieldsOf(saved((*model.Service).UpsertShareChange),
		listed((*model.Service).ListShareChanges, ofAccount),
		func(p parentRows) model.ShareChangeInput {
			return model.ShareChangeInput{AccountID: p.account, Date: requiredDay, DeltaShares: 10, TotalPrice: 100}
		},
		map[string]func(*model.ShareChangeInput){
			"account": func(s *model.ShareChangeInput) { s.AccountID = "" },
			"date":    func(s *model.ShareChangeInput) { s.Date = 0 },
		},
	),
	"UpsertStartupShareOption": requiredFieldsOf(saved((*model.Service).UpsertStartupShareOption),
		listed((*model.Service).ListStartupShareOptions, ofAccount),
		func(p parentRows) model.StartupShareOptionInput {
			return model.StartupShareOptionInput{AccountID: p.account, SourceAccountID: p.sourceAccount, Shares: 10, StrikePricePerShare: 10, GrantDate: requiredDay, EndDate: laterDay}
		},
		map[string]func(*model.StartupShareOptionInput){
			"account":        func(o *model.StartupShareOptionInput) { o.AccountID = "" },
			"source account": func(o *model.StartupShareOptionInput) { o.SourceAccountID = "" },
			"grant date":     func(o *model.StartupShareOptionInput) { o.GrantDate = 0 },
			"expiry date":    func(o *model.StartupShareOptionInput) { o.EndDate = 0 },
		},
	),
	"UpsertAccountSnapshot": requiredFieldsOf(
		func(ctx context.Context, svc *model.Service, inp snapshotInput) error {
			_, err := svc.UpsertAccountSnapshot(ctx, inp.accountID, inp.AccountSnapshotInput)
			return err
		},
		storedSnapshots,
		func(p parentRows) snapshotInput {
			return snapshotInput{p.account, model.AccountSnapshotInput{Date: requiredDay, Balance: newFixedValue(1000)}}
		},
		map[string]func(*snapshotInput){
			"account": func(s *snapshotInput) { s.accountID = "" },
			"date":    func(s *snapshotInput) { s.Date = 0 },
		},
	),
	"UpsertOrDeleteSnapshot": requiredFieldsOf(
		func(ctx context.Context, svc *model.Service, inp snapshotInput) error {
			_, err := svc.UpsertOrDeleteSnapshot(ctx, inp.accountID, inp.AccountSnapshotInput)
			return err
		},
		storedSnapshots,
		func(p parentRows) snapshotInput {
			return snapshotInput{p.account, model.AccountSnapshotInput{Date: requiredDay, Balance: newFixedValue(1000)}}
		},
		map[string]func(*snapshotInput){
			"account": func(s *snapshotInput) { s.accountID = "" },
			"date":    func(s *snapshotInput) { s.Date = 0 },
		},
	),
	"UpsertTransferTemplate": requiredFieldsOf(saved((*model.Service).UpsertTransferTemplate),
		listedAll((*model.Service).ListTransferTemplates),
		func(p parentRows) model.TransferTemplate {
			return model.TransferTemplate{Name: "Rent", FromAccountID: p.sourceAccount, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(1000), Recurrence: "*-*-25", StartDate: requiredDay, Enabled: true}
		},
		map[string]func(*model.TransferTemplate){
			"name":       func(t *model.TransferTemplate) { t.Name = "" },
			"start date": func(t *model.TransferTemplate) { t.StartDate = 0 },
		},
	),
	"UpsertCategory": requiredFieldsOf(saved((*model.Service).UpsertCategory),
		listedAll((*model.Service).ListCategories),
		func(parentRows) model.TransferTemplateCategoryInput {
			return model.TransferTemplateCategoryInput{Name: "Housing"}
		},
		map[string]func(*model.TransferTemplateCategoryInput){"name": func(c *model.TransferTemplateCategoryInput) { c.Name = "" }},
	),
	"UpsertSalary": requiredFieldsOf(saved((*model.Service).UpsertSalary),
		listedAll((*model.Service).ListSalaries),
		func(parentRows) model.Salary { return model.Salary{Name: "Globex"} },
		map[string]func(*model.Salary){"name": func(s *model.Salary) { s.Name = "" }},
	),
	"UpsertSalaryAmount": requiredFieldsOf(saved((*model.Service).UpsertSalaryAmount),
		listed((*model.Service).ListSalaryAmounts, ofSalary),
		func(p parentRows) model.SalaryAmount {
			return model.SalaryAmount{SalaryID: p.salary, Amount: newFixedValue(30000), StartDate: requiredDay}
		},
		map[string]func(*model.SalaryAmount){
			"salary":     func(a *model.SalaryAmount) { a.SalaryID = "" },
			"start date": func(a *model.SalaryAmount) { a.StartDate = 0 },
		},
	),
	"UpsertSalaryAdjustment": requiredFieldsOf(saved((*model.Service).UpsertSalaryAdjustment),
		listed((*model.Service).ListSalaryAdjustments, ofSalary),
		func(p parentRows) model.SalaryAdjustment {
			return model.SalaryAdjustment{SalaryID: p.salary, ValidFrom: requiredDay, VacationDaysPerYear: 25}
		},
		map[string]func(*model.SalaryAdjustment){
			"salary":     func(a *model.SalaryAdjustment) { a.SalaryID = "" },
			"valid from": func(a *model.SalaryAdjustment) { a.ValidFrom = 0 },
		},
	),
	"UpsertPartialParentalLeave": requiredFieldsOf(saved((*model.Service).UpsertPartialParentalLeave),
		listed((*model.Service).ListPartialParentalLeaves, ofSalary),
		func(p parentRows) model.PartialParentalLeave {
			return model.PartialParentalLeave{SalaryID: p.salary, StartDate: requiredDay, EndDate: laterDay, SjukDaysPerYear: 50}
		},
		map[string]func(*model.PartialParentalLeave){
			"salary":     func(l *model.PartialParentalLeave) { l.SalaryID = "" },
			"start date": func(l *model.PartialParentalLeave) { l.StartDate = 0 },
			"end date":   func(l *model.PartialParentalLeave) { l.EndDate = 0 },
		},
	),
	"UpsertFullParentalLeave": requiredFieldsOf(saved((*model.Service).UpsertFullParentalLeave),
		listed((*model.Service).ListFullParentalLeaves, ofSalary),
		func(p parentRows) model.FullParentalLeave {
			return model.FullParentalLeave{SalaryID: p.salary, StartDate: requiredDay, EndDate: laterDay, SjukDaysPerWeek: 5}
		},
		map[string]func(*model.FullParentalLeave){
			"salary":     func(l *model.FullParentalLeave) { l.SalaryID = "" },
			"start date": func(l *model.FullParentalLeave) { l.StartDate = 0 },
			"end date":   func(l *model.FullParentalLeave) { l.EndDate = 0 },
		},
	),
	"UpsertSweYearlyParams": requiredFieldsOf(saved((*model.Service).UpsertSweYearlyParams),
		listedAll((*model.Service).ListSweYearlyParams),
		func(parentRows) model.SweYearlyParams {
			return model.SweYearlyParams{Amount: 80600, Prisbasbelopp: 58800, ValidFrom: mustParseDate("2099-01-01")}
		},
		map[string]func(*model.SweYearlyParams){"valid from": func(s *model.SweYearlyParams) { s.ValidFrom = 0 }},
	),
	"UpsertBillAccount": requiredFieldsOf(saved((*model.Service).UpsertBillAccount),
		listedAll((*model.Service).ListBillAccounts),
		func(parentRows) model.BillAccount { return model.BillAccount{Name: "Subscriptions"} },
		map[string]func(*model.BillAccount){"name": func(b *model.BillAccount) { b.Name = "" }},
	),
	"UpsertBill": requiredFieldsOf(saved((*model.Service).UpsertBill),
		listed((*model.Service).ListBills, func(p parentRows) []string { return []string{p.billAccount} }),
		func(p parentRows) model.Bill { return model.Bill{BillAccountID: p.billAccount, Name: "Streaming"} },
		map[string]func(*model.Bill){
			"bill account": func(b *model.Bill) { b.BillAccountID = "" },
			"name":         func(b *model.Bill) { b.Name = "" },
		},
	),
	"UpsertBillAmount": requiredFieldsOf(saved((*model.Service).UpsertBillAmount),
		listed((*model.Service).ListBillAmounts, func(p parentRows) []string { return []string{p.bill} }),
		func(p parentRows) model.BillAmount {
			return model.BillAmount{BillID: p.bill, Amount: newFixedValue(100), StartDate: requiredDay}
		},
		map[string]func(*model.BillAmount){
			"bill":       func(a *model.BillAmount) { a.BillID = "" },
			"start date": func(a *model.BillAmount) { a.StartDate = 0 },
		},
	),
}

func TestSavingWithoutARequiredFieldIsInvalidInput(t *testing.T) {
	for upsert, fields := range requiredFields {
		for _, field := range fields {
			t.Run(upsert+"/"+field.name, func(t *testing.T) {
				t.Parallel()
				svc := newTestService(t)
				ctx := t.Context()
				p := mustParentRows(t, svc)
				before, err := field.stored(ctx, svc, p)
				if err != nil {
					t.Fatalf("count stored: %v", err)
				}

				if err := field.save(ctx, svc, p, true); !errors.Is(err, model.ErrInvalidInput) {
					t.Fatalf("expected ErrInvalidInput without the %s, got %v", field.name, err)
				}
				if after, err := field.stored(ctx, svc, p); err != nil || after != before {
					t.Fatalf("expected nothing stored without the %s, got %d stored instead of %d (%v)", field.name, after, before, err)
				}

				// Given, it saves.
				if err := field.save(ctx, svc, p, false); err != nil {
					t.Fatalf("expected it to save with the %s: %v", field.name, err)
				}
				if after, err := field.stored(ctx, svc, p); err != nil || after != before+1 {
					t.Fatalf("expected one stored with the %s, got %d stored instead of %d (%v)", field.name, after, before+1, err)
				}
			})
		}
	}
}

func TestMovingSnapshotsToOrFromAnEmptyDateIsInvalidInput(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	acc := mustAccount(t, svc, "Checking")
	mustSnapshot(t, svc, acc.ID, requiredDay, 1000)

	for name, move := range map[string]func() (*model.SnapshotsRow, error){
		"from": func() (*model.SnapshotsRow, error) { return svc.ModifySnapshotDateRow(ctx, 0, laterDay) },
		"to":   func() (*model.SnapshotsRow, error) { return svc.ModifySnapshotDateRow(ctx, requiredDay, 0) },
	} {
		if _, err := move(); !errors.Is(err, model.ErrInvalidInput) {
			t.Errorf("moving %s an empty date: expected ErrInvalidInput, got %v", name, err)
		}
	}
	snaps, err := svc.ListAccountSnapshots(ctx, acc.ID)
	if err != nil {
		t.Fatalf("list snapshots: %v", err)
	}
	if len(snaps) != 1 || snaps[0].Date != requiredDay {
		t.Fatalf("expected the Snapshot to stay on %s, got %+v", requiredDay, snaps)
	}
}

// Saving a field that may be empty without it saves none, not a default.
func TestSavingAnOptionalFieldEmptySavesNone(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	p := mustParentRows(t, svc)

	t.Run("account cash flow and budget category", func(t *testing.T) {
		acc, err := svc.UpsertAccount(ctx, model.AccountInput{Name: "Savings"})
		if err != nil {
			t.Fatalf("save account: %v", err)
		}
		got, err := svc.GetAccount(ctx, acc.ID)
		if err != nil {
			t.Fatalf("get account: %v", err)
		}
		if got.CashFlowFrequency != "" || got.CashFlowDestinationID != "" || got.BudgetCategoryID != nil {
			t.Fatalf("expected no cash flow and no budget category, got %+v", got)
		}
	})
	t.Run("growth model end date", func(t *testing.T) {
		gm, err := svc.UpsertAccountGrowthModel(ctx, model.AccountGrowthModelInput{AccountID: p.account, Type: model.GrowthModelFixed, AnnualRate: newFixedValue(0.05), AnnualVolatility: newFixedValue(0), StartDate: requiredDay})
		if err != nil {
			t.Fatalf("save growth model: %v", err)
		}
		got, err := svc.GetGrowthModel(ctx, gm.ID)
		if err != nil {
			t.Fatalf("get growth model: %v", err)
		}
		if got.EndDate != nil {
			t.Fatalf("expected no End Date, got %s", got.EndDate)
		}
	})
	t.Run("transfer template accounts, end date and budget category", func(t *testing.T) {
		tt, err := svc.UpsertTransferTemplate(ctx, model.TransferTemplate{Name: "Gift", AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(100), Recurrence: "*-*-25", StartDate: requiredDay, Enabled: true})
		if err != nil {
			t.Fatalf("save transfer template: %v", err)
		}
		got, err := svc.GetTransferTemplate(ctx, tt.ID)
		if err != nil {
			t.Fatalf("get transfer template: %v", err)
		}
		if got.FromAccountID != "" || got.ToAccountID != "" || got.EndDate != nil || got.BudgetCategoryID != nil {
			t.Fatalf("expected External accounts, no End Date and no budget category, got %+v", got)
		}
	})
	t.Run("salary accounts and budget category", func(t *testing.T) {
		got, err := svc.GetSalary(ctx, p.salary)
		if err != nil {
			t.Fatalf("get salary: %v", err)
		}
		if got.ToAccountID != "" || got.PensionAccountID != "" || got.BudgetCategoryID != nil {
			t.Fatalf("expected an External to-account, no pension account and no budget category, got %+v", got)
		}
	})
	t.Run("bill budget category", func(t *testing.T) {
		got, err := svc.GetBill(ctx, p.bill)
		if err != nil {
			t.Fatalf("get bill: %v", err)
		}
		if got.BudgetCategoryID != nil {
			t.Fatalf("expected no budget category, got %v", *got.BudgetCategoryID)
		}
	})
	t.Run("bill amount currency and end date", func(t *testing.T) {
		if _, err := svc.UpsertBillAmount(ctx, model.BillAmount{BillID: p.bill, Amount: newFixedValue(100), StartDate: requiredDay}); err != nil {
			t.Fatalf("save bill amount: %v", err)
		}
		amounts, err := svc.ListBillAmounts(ctx, p.bill)
		if err != nil || len(amounts) != 1 {
			t.Fatalf("expected one bill amount, got %+v (%v)", amounts, err)
		}
		if amounts[0].Currency != "" || amounts[0].EndDate != nil {
			t.Fatalf("expected the default currency and no End Date, got %+v", amounts[0])
		}
	})
}

// An amount that isn't given (the zero uncertain.Value an empty field maps to) saves 0, as every other
// number does.
func TestSavingAnAmountNotGivenSavesZero(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	p := mustParentRows(t, svc)
	var notGiven uncertain.Value

	expectZero := func(t *testing.T, what string, got uncertain.Value) {
		t.Helper()
		if !got.IsFixed() || got.Mean() != 0 {
			t.Errorf("expected the %s to be a fixed 0, got %s", what, got)
		}
	}
	gm, err := svc.UpsertAccountGrowthModel(ctx, model.AccountGrowthModelInput{AccountID: p.account, Type: model.GrowthModelFixed, AnnualRate: notGiven, AnnualVolatility: notGiven, StartDate: requiredDay})
	if err != nil {
		t.Fatalf("save growth model: %v", err)
	}
	expectZero(t, "annual rate", gm.AnnualRate)
	expectZero(t, "annual volatility", gm.AnnualVolatility)

	tt, err := svc.UpsertTransferTemplate(ctx, model.TransferTemplate{Name: "Saving", FromAccountID: p.sourceAccount, AmountType: model.TransferAmountPercent, AmountFixed: notGiven, AmountPercent: 0.1, Recurrence: "*-*-25", StartDate: requiredDay})
	if err != nil {
		t.Fatalf("save transfer template: %v", err)
	}
	expectZero(t, "transfer template amount", tt.AmountFixed)

	sa, err := svc.UpsertSalaryAmount(ctx, model.SalaryAmount{SalaryID: p.salary, Amount: notGiven, StartDate: requiredDay})
	if err != nil {
		t.Fatalf("save salary amount: %v", err)
	}
	expectZero(t, "salary amount", sa.Amount)

	ba, err := svc.UpsertBillAmount(ctx, model.BillAmount{BillID: p.bill, Amount: notGiven, StartDate: requiredDay})
	if err != nil {
		t.Fatalf("save bill amount: %v", err)
	}
	expectZero(t, "bill amount", ba.Amount)

	snap, err := svc.UpsertAccountSnapshot(ctx, p.account, model.AccountSnapshotInput{Date: requiredDay, Balance: notGiven})
	if err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	expectZero(t, "snapshot balance", snap.Balance)
}
