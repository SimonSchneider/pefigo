package model

import (
	"context"
	"fmt"

	"github.com/SimonSchneider/goslu/sid"
	"github.com/SimonSchneider/pefigo/internal/pdb"
	"github.com/SimonSchneider/pefigo/pkg/ui"
)

type AccountType struct {
	ID    string
	Name  string
	Color string
}

// uncategorized is the Account Type that accounts without one are grouped under.
var uncategorized = AccountType{ID: "", Name: "Uncategorized", Color: "#999999"}

type AccountTypeInput struct {
	ID    string
	Name  string
	Color string
}

type AccountTypeWithFilter struct {
	AccountType
	Exclude bool
}

type AccountTypesWithFilter []AccountTypeWithFilter

func (a AccountTypesWithFilter) GetAccountType(typeID string) AccountTypeWithFilter {
	if typeID == "" {
		return AccountTypeWithFilter{}
	}
	for _, at := range a {
		if at.ID == typeID {
			return at
		}
	}
	return AccountTypeWithFilter{}
}

func accountTypeFromDB(at pdb.AccountType) AccountType {
	return AccountType{
		ID:    at.ID,
		Name:  at.Name,
		Color: ui.OrDefault(at.Color),
	}
}

func (s *Service) GetAccountType(ctx context.Context, id string) (AccountType, error) {
	at, err := s.q.GetAccountType(ctx, id)
	if err != nil {
		return AccountType{}, fmt.Errorf("failed to get account type: %w", err)
	}
	return accountTypeFromDB(at), nil
}

func (s *Service) UpsertAccountType(ctx context.Context, inp AccountTypeInput) (AccountType, error) {
	if err := checkRequired("name", inp.Name); err != nil {
		return AccountType{}, err
	}
	id := inp.ID
	if id == "" {
		id = sid.MustNewString(15)
	}
	at, err := s.q.UpsertAccountType(ctx, pdb.UpsertAccountTypeParams{
		ID:    id,
		Name:  inp.Name,
		Color: ui.WithDefaultNull(inp.Color),
	})
	if err != nil {
		return AccountType{}, fmt.Errorf("failed to upsert account type: %w", err)
	}
	return accountTypeFromDB(at), nil
}

func (s *Service) DeleteAccountType(ctx context.Context, id string) error {
	err := s.q.DeleteAccountType(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to delete account type: %w", err)
	}
	return nil
}

func (s *Service) ListAccountTypes(ctx context.Context) ([]AccountType, error) {
	ats, err := s.q.ListAccountTypes(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]AccountType, len(ats))
	for i := range ats {
		result[i] = accountTypeFromDB(ats[i])
	}
	return result, nil
}

func (s *Service) accountTypesWithExclusions(ctx context.Context, excludedTypeIDs []string) (AccountTypesWithFilter, error) {
	accountTypes, err := s.ListAccountTypes(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing account types: %w", err)
	}
	excludeSet := make(map[string]bool, len(excludedTypeIDs))
	for _, id := range excludedTypeIDs {
		excludeSet[id] = true
	}
	result := make(AccountTypesWithFilter, 0, len(accountTypes))
	for _, at := range accountTypes {
		result = append(result, AccountTypeWithFilter{
			AccountType: at,
			Exclude:     excludeSet[at.ID],
		})
	}
	return result, nil
}
