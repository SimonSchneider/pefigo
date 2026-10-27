package model_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/SimonSchneider/pefigo/internal/model"
)

// startupShareAccountUpserts are the two Service calls that save a Startup Share Account's Tax Rate and
// Valuation Discount. Each saves inp for a new account and returns that account's ID.
var startupShareAccountUpserts = []struct {
	name   string
	upsert func(ctx context.Context, svc *model.Service, t *testing.T, inp model.StartupShareAccountInput) (string, error)
}{
	{"UpsertStartupShareAccount", func(ctx context.Context, svc *model.Service, t *testing.T, inp model.StartupShareAccountInput) (string, error) {
		inp.AccountID = mustAccount(t, svc, "Startup").ID
		_, err := svc.UpsertStartupShareAccount(ctx, inp)
		return inp.AccountID, err
	}},
	{"UpsertAccountWithStartupShares", func(ctx context.Context, svc *model.Service, t *testing.T, inp model.StartupShareAccountInput) (string, error) {
		acc, err := svc.UpsertAccountWithStartupShares(ctx, model.AccountInput{Name: "Startup"}, &inp)
		return acc.ID, err
	}},
}

func TestAStartupShareAccountWithoutATaxRateOrValuationDiscountGetsTheDefaults(t *testing.T) {
	for _, u := range startupShareAccountUpserts {
		t.Run(u.name, func(t *testing.T) {
			svc := newTestService(t)
			id, err := u.upsert(t.Context(), svc, t, model.StartupShareAccountInput{})
			if err != nil {
				t.Fatalf("save startup share account: %v", err)
			}
			ssa, err := svc.GetStartupShareAccount(t.Context(), id)
			if err != nil {
				t.Fatalf("get startup share account: %v", err)
			}
			if ssa.TaxRate != 0.15 {
				t.Errorf("expected the Tax Rate to default to 15%%, got %v", ssa.TaxRate)
			}
			if ssa.ValuationDiscountFactor != 0.50 {
				t.Errorf("expected the Valuation Discount to default to 50%%, got %v", ssa.ValuationDiscountFactor)
			}
		})
	}
}

func TestAStartupShareAccountKeepsAGivenTaxRateAndValuationDiscount(t *testing.T) {
	for _, u := range startupShareAccountUpserts {
		for _, given := range []float64{0, 0.4, 1} {
			t.Run(fmt.Sprintf("%s: %v", u.name, given), func(t *testing.T) {
				svc := newTestService(t)
				id, err := u.upsert(t.Context(), svc, t, model.StartupShareAccountInput{TaxRate: ptr(given), ValuationDiscountFactor: ptr(given)})
				if err != nil {
					t.Fatalf("save startup share account: %v", err)
				}
				ssa, err := svc.GetStartupShareAccount(t.Context(), id)
				if err != nil {
					t.Fatalf("get startup share account: %v", err)
				}
				if ssa.TaxRate != given || ssa.ValuationDiscountFactor != given {
					t.Errorf("expected a Tax Rate and Valuation Discount of %v, got %v and %v", given, ssa.TaxRate, ssa.ValuationDiscountFactor)
				}
			})
		}
	}
}

func TestATaxRateOrValuationDiscountOutside0To100PercentIsInvalid(t *testing.T) {
	invalid := map[string]model.StartupShareAccountInput{
		"Tax Rate below 0%":             {TaxRate: ptr(-0.01)},
		"Tax Rate above 100%":           {TaxRate: ptr(1.01)},
		"Valuation Discount below 0%":   {ValuationDiscountFactor: ptr(-0.01)},
		"Valuation Discount above 100%": {ValuationDiscountFactor: ptr(1.01)},
	}
	for _, u := range startupShareAccountUpserts {
		for name, inp := range invalid {
			t.Run(u.name+": "+name, func(t *testing.T) {
				svc := newTestService(t)
				id, err := u.upsert(t.Context(), svc, t, inp)
				if !errors.Is(err, model.ErrInvalidInput) {
					t.Fatalf("expected ErrInvalidInput, got %v", err)
				}
				if id == "" {
					return
				}
				if _, err := svc.GetStartupShareAccount(t.Context(), id); err == nil {
					t.Error("expected no startup share account to be saved")
				}
			})
		}
	}
}

func TestAnAccountWithAnInvalidTaxRateIsNotSaved(t *testing.T) {
	svc := newTestService(t)
	if _, err := svc.UpsertAccountWithStartupShares(t.Context(), model.AccountInput{Name: "Startup"}, &model.StartupShareAccountInput{TaxRate: ptr(1.5)}); !errors.Is(err, model.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput, got %v", err)
	}
	accs, err := svc.ListAccounts(t.Context())
	if err != nil {
		t.Fatalf("list accounts: %v", err)
	}
	if len(accs) != 0 {
		t.Errorf("expected no account to be saved, got %v", accs)
	}
}
