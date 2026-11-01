package controller_test

import (
	"net/url"
	"strings"
	"testing"

	"github.com/SimonSchneider/pefigo/internal/model"
	"github.com/SimonSchneider/pefigo/pkg/swe"
)

func TestSalaryFormsPutASalaryAndItsPartsInAnEventInDaysAfterItsDate(t *testing.T) {
	svc, srv := newSettingsServer(t, model.WithSweOptions(swe.WithTaxData(swe.MemoryTaxData{})))
	ctx := t.Context()
	child, err := svc.UpsertEvent(ctx, model.EventInput{Name: "Second child", Date: mustDate(t, "2027-06-01")})
	if err != nil {
		t.Fatalf("create event: %v", err)
	}
	if page := getPage(t, srv, "/salaries/new?event_id="+child.ID); !strings.Contains(page, `value="`+child.ID+`" selected`) {
		t.Errorf("expected a new Salary from an Event's page to select the Event")
	}
	location := postForm(t, srv, "/salaries/", url.Values{
		"name": {"Salary B"}, "recurrence": {"*-*-25"}, "event_id": {child.ID}, "enabled": {"on"}, "is_gross": {"on"}, "kommun": {"STOCKHOLM"}, "forsamling": {"TEST"},
	})
	salaryID := strings.Split(location, "/")[2]
	if sal, err := svc.GetSalary(ctx, salaryID); err != nil || sal.EventID != child.ID {
		t.Fatalf("expected Salary B in the Second child, got %+v (%v)", sal, err)
	}

	for path, form := range map[string]url.Values{
		"/salary-amounts/":          {"amount": {"30000"}, "start_date": {"+0"}},
		"/salary-adjustments/":      {"valid_from": {"+10"}, "vab_days_per_year": {"10"}},
		"/partial-parental-leaves/": {"start_date": {"+200"}, "end_date": {"+300"}, "sjuk_days_per_year": {"40"}},
		"/full-parental-leaves/":    {"start_date": {"+20"}, "end_date": {"+200"}, "sjuk_days_per_week": {"7"}},
		"/salary-ends/":             {"date": {"+400"}},
	} {
		form.Set("salary_id", salaryID)
		form.Set("event_id", child.ID)
		postForm(t, srv, path, form)
	}

	sal, err := svc.GetSalary(ctx, salaryID)
	if err != nil {
		t.Fatalf("get salary: %v", err)
	}
	if len(sal.Amounts) != 1 || sal.Amounts[0].StartDate != child.Date || sal.Amounts[0].EventID != child.ID {
		t.Errorf("expected a Salary Amount from the Second child's date, got %+v", sal.Amounts)
	}
	if len(sal.FullParentalLeaves) != 1 || sal.FullParentalLeaves[0].StartDate != mustDate(t, "2027-06-21") || sal.FullParentalLeaves[0].EndDate != mustDate(t, "2027-12-18") {
		t.Errorf("expected full parental leave from 2027-06-21 to 2027-12-18, got %+v", sal.FullParentalLeaves)
	}
	if len(sal.Ends) != 1 || sal.Ends[0].Date != mustDate(t, "2028-07-05") {
		t.Errorf("expected an end on 2028-07-05, got %+v", sal.Ends)
	}
	page := getPage(t, srv, location)
	for _, want := range []string{`value="+0"`, `value="+10"`, `value="+200"`, `value="+300"`, `value="+20"`, `value="+400"`, `name="offset_event_id" value="` + child.ID + `"`} {
		if !strings.Contains(page, want) {
			t.Errorf("expected the Salary page to give %s", want)
		}
	}

	// The Second child adds parental leave to a Salary outside it.
	other := postForm(t, srv, "/salaries/", url.Values{"name": {"Salary A"}, "recurrence": {"*-*-25"}, "enabled": {"on"}})
	otherID := strings.Split(other, "/")[2]
	postForm(t, srv, "/full-parental-leaves/", url.Values{"salary_id": {otherID}, "event_id": {child.ID}, "start_date": {"+0"}, "end_date": {"+183"}, "sjuk_days_per_week": {"7"}})
	if sal, err := svc.GetSalary(ctx, otherID); err != nil || sal.EventID != "" || len(sal.FullParentalLeaves) != 1 || sal.FullParentalLeaves[0].EventID != child.ID {
		t.Fatalf("expected the Second child's leave on Salary A, outside it, got %+v (%v)", sal, err)
	}

	list := getPage(t, srv, "/salaries")
	if !strings.Contains(list, "Second child") {
		t.Errorf("expected the Salaries list to show Salary B's Event")
	}
	events := getPage(t, srv, "/events")
	for _, want := range []string{"Salary B", "Full parental leave", "Salary end", "+400 days"} {
		if !strings.Contains(events, want) {
			t.Errorf("expected the Events page to list %q", want)
		}
	}
}

func TestBillFormsPutABillAndItsPartsInAnEventInDaysAfterItsDate(t *testing.T) {
	svc, srv := newSettingsServer(t)
	ctx := t.Context()
	move, err := svc.UpsertEvent(ctx, model.EventInput{Name: "Move", Date: mustDate(t, "2027-06-01")})
	if err != nil {
		t.Fatalf("create event: %v", err)
	}
	accountLocation := postForm(t, srv, "/bills/", url.Values{"name": {"Household"}, "recurrence": {"*-*-01"}, "enabled": {"on"}})
	billAccountID := strings.Split(accountLocation, "/")[2]
	location := postForm(t, srv, "/bill-items/", url.Values{"bill_account_id": {billAccountID}, "name": {"Rent"}, "event_id": {move.ID}, "enabled": {"on"}})
	billID := strings.Split(location, "/")[2]
	postForm(t, srv, "/bill-amounts/", url.Values{"bill_id": {billID}, "amount": {"9000"}, "event_id": {move.ID}, "start_date": {"+0"}, "end_date": {"+365"}})
	postForm(t, srv, "/bill-ends/", url.Values{"bill_id": {billID}, "event_id": {move.ID}, "date": {"+180"}})

	edit, err := svc.GetBillEditPageData(ctx, billID)
	if err != nil {
		t.Fatalf("bill edit page: %v", err)
	}
	if edit.Bill.EventID != move.ID || len(edit.Bill.Amounts) != 1 || edit.Bill.Amounts[0].StartDate != move.Date || len(edit.Bill.Ends) != 1 || edit.Bill.Ends[0].Date != mustDate(t, "2027-11-28") {
		t.Fatalf("expected Rent in the Move from its date, ending on 2027-11-28, got %+v", edit.Bill)
	}
	page := getPage(t, srv, location)
	for _, want := range []string{`value="+0"`, `value="+365"`, `value="+180"`, `name="offset_event_id" value="` + move.ID + `"`, `value="` + move.ID + `" selected`} {
		if !strings.Contains(page, want) {
			t.Errorf("expected the Bill page to give %s", want)
		}
	}
	if list := getPage(t, srv, "/bills"); !strings.Contains(list, "Move") {
		t.Errorf("expected the Bills list to show Rent's Event")
	}
}

func TestStartupShareFormsPutRoundsAndShareChangesInAnEventInDaysAfterItsDate(t *testing.T) {
	svc, srv := newSettingsServer(t)
	ctx := t.Context()
	seriesA, err := svc.UpsertEvent(ctx, model.EventInput{Name: "Series A", Date: mustDate(t, "2027-06-01")})
	if err != nil {
		t.Fatalf("create event: %v", err)
	}
	location := postForm(t, srv, "/accounts/", url.Values{"name": {"Startup Co"}, "account_form_mode": {"startup"}})
	accountID := strings.Split(location, "/")[2]
	postForm(t, srv, "/investment-rounds/", url.Values{"account_id": {accountID}, "event_id": {seriesA.ID}, "date": {"+0"}, "valuation": {"4000000"}, "pre_money_shares": {"10000"}, "investment": {"1000000"}})
	postForm(t, srv, "/share-changes/", url.Values{"account_id": {accountID}, "event_id": {seriesA.ID}, "date": {"+30"}, "delta_shares": {"100"}, "total_price": {"1000"}})

	rounds, err := svc.ListInvestmentRounds(ctx, accountID)
	if err != nil || len(rounds) != 1 || rounds[0].Date != seriesA.Date || rounds[0].EventID != seriesA.ID {
		t.Fatalf("expected a round in the Series A on its date, got %+v (%v)", rounds, err)
	}
	changes, err := svc.ListShareChanges(ctx, accountID)
	if err != nil || len(changes) != 1 || changes[0].Date != mustDate(t, "2027-07-01") {
		t.Fatalf("expected a share change on 2027-07-01, got %+v (%v)", changes, err)
	}
	page := getPage(t, srv, location)
	for _, want := range []string{`value="+0"`, `value="+30"`, `name="offset_event_id" value="` + seriesA.ID + `"`} {
		if !strings.Contains(page, want) {
			t.Errorf("expected the account page to give %s", want)
		}
	}
}
