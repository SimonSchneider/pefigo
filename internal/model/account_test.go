package model_test

import (
	"testing"

	"github.com/SimonSchneider/pefigo/internal/model"
)

func TestAccountFormMode(t *testing.T) {
	t.Run("standard when no startup share account", func(t *testing.T) {
		v := &model.AccountEditView{}
		if got := v.AccountFormMode(); got != "standard" {
			t.Errorf("expected 'standard', got %q", got)
		}
	})

	t.Run("startup when startup share account exists", func(t *testing.T) {
		v := &model.AccountEditView{
			StartupShareAccount: &model.StartupShareAccount{AccountID: "a1"},
		}
		if got := v.AccountFormMode(); got != "startup" {
			t.Errorf("expected 'startup', got %q", got)
		}
	})
}

func TestDerivedStartupShareFormatting(t *testing.T) {
	v := &model.AccountEditView{
		DerivedStartupShareSummary: &model.DerivedStartupShareSummary{
			SharesOwned:              12345.67,
			TotalShares:              1000000.00,
			AvgPurchasePricePerShare: 0.0012345678,
		},
	}

	t.Run("shares owned uses thousand separator", func(t *testing.T) {
		got := v.GetStartupShareSharesOwned()
		if got != "12,346" {
			t.Errorf("expected '12,346', got %q", got)
		}
	})

	t.Run("total shares uses thousand separator", func(t *testing.T) {
		got := v.GetStartupShareTotalShares()
		if got != "1,000,000" {
			t.Errorf("expected '1,000,000', got %q", got)
		}
	})

	t.Run("avg purchase price keeps full precision", func(t *testing.T) {
		got := v.GetStartupSharePurchasePrice()
		if got != "0.0012345678" {
			t.Errorf("expected '0.0012345678', got %q", got)
		}
	})
}
