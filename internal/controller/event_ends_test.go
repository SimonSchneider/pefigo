package controller_test

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo/internal/model"
	"github.com/SimonSchneider/pefigo/pkg/uncertain"
)

func TestTheEventPageListsTheEndsAnEventMakesWithoutRemovingThem(t *testing.T) {
	svc, srv := newSettingsServer(t)
	ctx := t.Context()
	checking, err := svc.UpsertAccount(ctx, model.AccountInput{Name: "Checking"})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	newJob, err := svc.UpsertEvent(ctx, model.EventInput{Name: "New job", Date: mustDate(t, "2027-09-01")})
	if err != nil {
		t.Fatalf("create event: %v", err)
	}
	salary, err := svc.UpsertSalary(ctx, model.Salary{Name: "Salary A", ToAccountID: checking.ID, Recurrence: "*-*-25", Enabled: true})
	if err != nil {
		t.Fatalf("create salary: %v", err)
	}
	atTheEvent := date.Duration(0)
	end, err := svc.UpsertSalaryEnd(ctx, model.SalaryEnd{SalaryID: salary.ID, EventID: newJob.ID, DateOffset: &atTheEvent})
	if err != nil {
		t.Fatalf("create salary end: %v", err)
	}
	amount, err := svc.UpsertSalaryAmount(ctx, model.SalaryAmount{SalaryID: salary.ID, EventID: newJob.ID, StartOffset: &atTheEvent, Amount: uncertain.NewFixed(1000)})
	if err != nil {
		t.Fatalf("create salary amount: %v", err)
	}
	endEntry := model.EventEntry{Kind: model.EventEntrySalaryEnd, ID: end.ID}.String()
	amountEntry := model.EventEntry{Kind: model.EventEntrySalaryAmount, ID: amount.ID}.String()

	page := getPage(t, srv, "/events/"+newJob.ID+"/edit")
	if !strings.Contains(page, "Salary end") {
		t.Errorf("expected the Event page to list the end of Salary A")
	}
	if strings.Contains(page, `value="`+endEntry+`"`) {
		t.Errorf("expected no Remove for the end: an end is edited on its Salary's page")
	}
	if !strings.Contains(page, `value="`+amountEntry+`"`) {
		t.Errorf("expected a Remove for the Salary Amount")
	}

	resp, err := srv.Client().PostForm(srv.URL+"/events/"+newJob.ID+"/entries/remove", url.Values{"entry": {endEntry}})
	if err != nil {
		t.Fatalf("remove end: %v", err)
	}
	defer resp.Body.Close()
	if body, _ := io.ReadAll(resp.Body); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected removing an end from its Event to be a bad request, got %d: %s", resp.StatusCode, body)
	}
	salaryPage, err := svc.GetSalaryEditPageData(ctx, salary.ID)
	if err != nil {
		t.Fatalf("salary page: %v", err)
	}
	if ends := salaryPage.Salary.Ends; len(ends) != 1 || ends[0].EventID != newJob.ID {
		t.Errorf("expected the end still in the New job, got %+v", ends)
	}
}
