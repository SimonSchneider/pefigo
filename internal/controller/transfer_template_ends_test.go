package controller_test

import (
	"net/url"
	"strings"
	"testing"

	"github.com/SimonSchneider/pefigo/internal/model"
)

func TestTransferTemplateFormsEndATransferTemplateFromAnEventInDaysAfterItsDate(t *testing.T) {
	svc, srv := newSettingsServer(t)
	ctx := t.Context()
	abroad, err := svc.UpsertEvent(ctx, model.EventInput{Name: "Move abroad", Date: mustDate(t, "2027-09-01")})
	if err != nil {
		t.Fatalf("create event: %v", err)
	}
	location := postForm(t, srv, "/transfers/", url.Values{
		"name": {"Rent"}, "amount_type": {"fixed"}, "amount_fixed": {"10000"}, "recurrence": {"*-*-01"}, "start_date": {"2025-01-01"}, "enabled": {"on"},
	})
	rentID := strings.Split(location, "/")[2]
	edit := "/transfer-templates/" + rentID + "/edit"
	if page := getPage(t, srv, edit); strings.Contains(page, `name="end_date"`) {
		t.Errorf("expected the Transfer Template form to have no End Date: its Ends are saved on their own")
	}

	if got := postForm(t, srv, "/transfer-template-ends/", url.Values{"transfer_template_id": {rentID}, "event_id": {abroad.ID}, "date": {"+0"}}); got != edit {
		t.Errorf("expected saving an End to redirect to the Transfer Template's page, got %q", got)
	}

	rent, err := svc.GetTransferTemplate(ctx, rentID)
	if err != nil || rent.EventID != "" || rent.StartDate != mustDate(t, "2025-01-01") || len(rent.Ends) != 1 || rent.Ends[0].EventID != abroad.ID || rent.Ends[0].Date != abroad.Date {
		t.Fatalf("expected the Rent outside every Event from 2025-01-01, ended by the Move abroad on its date, got %+v (%v)", rent, err)
	}
	page := getPage(t, srv, edit)
	for _, want := range []string{"Ends", `value="+0"`, `name="offset_event_id" value="` + abroad.ID + `"`, `value="` + abroad.ID + `" selected`} {
		if !strings.Contains(page, want) {
			t.Errorf("expected the Rent's page to give %s", want)
		}
	}
	events := getPage(t, srv, "/events/"+abroad.ID+"/edit")
	for _, want := range []string{"Transfer Template end", "Rent", `href="` + edit + `"`} {
		if !strings.Contains(events, want) {
			t.Errorf("expected the Move abroad's page to list %q", want)
		}
	}

	if got := postForm(t, srv, "/transfer-template-ends/"+rent.Ends[0].ID+"/delete", nil); got != edit {
		t.Errorf("expected deleting an End to redirect to the Transfer Template's page, got %q", got)
	}
	if rent, err := svc.GetTransferTemplate(ctx, rentID); err != nil || len(rent.Ends) != 0 {
		t.Errorf("expected the Rent without an End, got %+v (%v)", rent.Ends, err)
	}
}
