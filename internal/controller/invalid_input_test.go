package controller_test

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/SimonSchneider/pefigo/internal/model"
)

func TestSavingAnEndDateBeforeItsStartDateIsABadRequest(t *testing.T) {
	svc, srv := newSettingsServer(t)
	account, err := svc.UpsertAccount(t.Context(), model.AccountInput{Name: "Savings"})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}

	resp, err := srv.Client().PostForm(srv.URL+"/growth-models/", url.Values{
		"account_id": {account.ID},
		"type":       {"fixed"},
		"start_date": {"2025-03-01"},
		"end_date":   {"2025-02-01"},
	})
	if err != nil {
		t.Fatalf("save growth model: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), "end date must be after the start date") {
		t.Errorf("expected the response to say why, got %q", body)
	}
}

func TestSavingASalaryAmountWithoutAStartDateIsABadRequest(t *testing.T) {
	svc, srv := newSettingsServer(t)
	salary, err := svc.UpsertSalary(t.Context(), model.Salary{Name: "Acme"})
	if err != nil {
		t.Fatalf("create salary: %v", err)
	}

	resp, err := srv.Client().PostForm(srv.URL+"/salary-amounts/", url.Values{
		"salary_id":  {salary.ID},
		"amount":     {"30000"},
		"start_date": {""},
	})
	if err != nil {
		t.Fatalf("save salary amount: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), "start date is required") {
		t.Errorf("expected the response to say why, got %q", body)
	}
	if amounts, err := svc.ListSalaryAmounts(t.Context(), salary.ID); err != nil || len(amounts) != 0 {
		t.Fatalf("expected no salary amount saved, got %+v (%v)", amounts, err)
	}
}
