package model_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/SimonSchneider/goslu/date"
	"github.com/SimonSchneider/pefigo"
	"github.com/SimonSchneider/pefigo/internal/model"
	"github.com/ncruces/go-sqlite3/vfs/memdb"
)

func mustTransferTemplateEnd(t *testing.T, svc *model.Service, end model.TransferTemplateEnd) model.TransferTemplateEnd {
	t.Helper()
	saved, err := svc.UpsertTransferTemplateEnd(t.Context(), end)
	if err != nil {
		t.Fatalf("save transfer template end: %v", err)
	}
	return saved
}

func mustGetTransferTemplateEnd(t *testing.T, svc *model.Service, id string) model.TransferTemplateEnd {
	t.Helper()
	end, err := svc.GetTransferTemplateEnd(t.Context(), id)
	if err != nil {
		t.Fatalf("get transfer template end: %v", err)
	}
	return end
}

// mustEndTransferTemplate ends the Transfer Template id on day, with an End outside every Event.
func mustEndTransferTemplate(t *testing.T, svc *model.Service, id string, day date.Date) model.TransferTemplateEnd {
	t.Helper()
	return mustTransferTemplateEnd(t, svc, model.TransferTemplateEnd{TransferTemplateID: id, Date: day})
}

// monthlyRent is a Transfer Template of 10 000 from checking on the 1st of every month from start, in
// the Event eventID or in none.
func monthlyRent(t *testing.T, svc *model.Service, checkingID, start, eventID string) model.TransferTemplate {
	t.Helper()
	tt := model.TransferTemplate{
		Name: "Rent", FromAccountID: checkingID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(10000),
		Recurrence: "*-*-01", Enabled: true, EventID: eventID,
	}
	if eventID == "" {
		tt.StartDate = mustParseDate(start)
	} else {
		tt.StartOffset = days(0)
	}
	return mustTransferTemplate(t, svc, tt)
}

// paysOn reports whether the Baseline charges the Transfer Template name on each of the days.
func paysOn(t *testing.T, svc *model.Service, name string, days ...string) map[string]bool {
	t.Helper()
	pays := make(map[string]bool, len(days))
	for _, day := range days {
		_, pays[day] = chargedOn(t, svc, day)[name]
	}
	return pays
}

func expectPays(t *testing.T, svc *model.Service, name string, want map[string]bool) {
	t.Helper()
	days := make([]string, 0, len(want))
	for day := range want {
		days = append(days, day)
	}
	for day, got := range paysOn(t, svc, name, days...) {
		if got != want[day] {
			t.Errorf("on %s: expected %s charged %v, got %v", day, name, want[day], got)
		}
	}
}

func TestAnEventEndsATransferTemplateOutsideIt(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	rent := monthlyRent(t, svc, checking.ID, "2025-01-01", "")
	abroad := mustEvent(t, svc, model.EventInput{Name: "Move abroad", Date: mustParseDate("2027-09-01")})

	end := mustTransferTemplateEnd(t, svc, model.TransferTemplateEnd{TransferTemplateID: rent.ID, EventID: abroad.ID, DateOffset: days(0)})

	if end.Date != abroad.Date {
		t.Fatalf("expected the end on the Event's date, got %s", end.Date)
	}
	expectPays(t, svc, "Rent", map[string]bool{"2025-01-01": true, "2027-08-01": true, "2027-09-01": false, "2028-01-01": false})
	page, err := svc.GetTransferTemplatesPageData(t.Context())
	if err != nil {
		t.Fatalf("transfer templates page: %v", err)
	}
	if len(page.Rows) != 1 || page.Rows[0].ID != rent.ID || dateOf(page.Rows[0].EndDate) != "2027-09-01" {
		t.Errorf("expected the Transfer Templates page to list the Rent ending on 2027-09-01, got %+v", page.Rows)
	}

	moveEvent(t, svc, abroad, "2028-01-01")

	expectPays(t, svc, "Rent", map[string]bool{"2027-09-01": true, "2027-12-01": true, "2028-01-01": false})
	if got := mustGetTransferTemplate(t, svc, rent.ID); got.StartDate != mustParseDate("2025-01-01") || got.EventID != "" {
		t.Errorf("expected the Rent to stay outside every Event from 2025-01-01, got %s in %q", got.StartDate, got.EventID)
	}
}

func TestATransferTemplatesEndsAreEditedOnItsOwnPageAndListedOnTheEvents(t *testing.T) {
	svc := newTestService(t)
	ctx := t.Context()
	checking := mustAccount(t, svc, "Checking")
	rent := monthlyRent(t, svc, checking.ID, "2025-01-01", "")
	abroad := mustEvent(t, svc, model.EventInput{Name: "Move abroad", Date: mustParseDate("2027-09-01")})
	house := mustEvent(t, svc, model.EventInput{Name: "Buy a house", Date: mustParseDate("2028-06-01"), Planned: ptr(false)})
	inAbroad := mustTransferTemplateEnd(t, svc, model.TransferTemplateEnd{TransferTemplateID: rent.ID, EventID: abroad.ID, DateOffset: days(0)})
	mustTransferTemplateEnd(t, svc, model.TransferTemplateEnd{TransferTemplateID: rent.ID, EventID: house.ID, DateOffset: days(30)})

	edit, err := svc.GetTransferTemplateEditPageData(ctx, rent.ID)
	if err != nil {
		t.Fatalf("transfer template edit page: %v", err)
	}
	var ends []string
	for _, e := range edit.TransferTemplate.Ends {
		ends = append(ends, e.EventID+" "+e.Date.String())
	}
	if want := []string{abroad.ID + " 2027-09-01", house.ID + " 2028-07-01"}; !slices.Equal(ends, want) {
		t.Errorf("expected the Rent's edit page to list its Ends at their Events' dates %v, got %v", want, ends)
	}
	if edit.PartEventID() != "" {
		t.Errorf("expected a new End of the Rent outside every Event, got %q", edit.PartEventID())
	}

	page, err := svc.GetEventEditPageData(ctx, abroad.ID)
	if err != nil {
		t.Fatalf("event edit page: %v", err)
	}
	if len(page.Event.Parts) != 1 {
		t.Fatalf("expected the Move abroad to list the Rent's End, got %+v", page.Event.Parts)
	}
	part := page.Event.Parts[0]
	if part.Kind != model.EventEntryTransferTemplateEnd || part.OwnerID != rent.ID || part.OwnerName != "Rent" || dateOf(part.StartDate) != "2027-09-01" || part.StartOffset() != 0 {
		t.Errorf("expected the Rent's End at +0 on 2027-09-01, got %+v", part)
	}
	if part.Kind.RemovableFromEvent() {
		t.Error("expected the Event's page not to remove a Transfer Template end")
	}
	if err := svc.RemoveFromEvent(ctx, model.EventEntry{Kind: model.EventEntryTransferTemplateEnd, ID: inAbroad.ID}); !errors.Is(err, model.ErrInvalidInput) {
		t.Errorf("expected removing a Transfer Template end from the Event's page to be invalid input, got %v", err)
	}

	if err := svc.DeleteTransferTemplateEnd(ctx, inAbroad.ID); err != nil {
		t.Fatalf("delete transfer template end: %v", err)
	}
	expectPays(t, svc, "Rent", map[string]bool{"2027-09-01": true, "2028-07-01": true})
}

func TestDeletingAnEventListsTheEndsDeletedWithItsTransferTemplates(t *testing.T) {
	svc := newTestService(t)
	lease := mustEvent(t, svc, model.EventInput{Name: "New lease", Date: mustParseDate("2026-01-01")})
	abroad := mustEvent(t, svc, model.EventInput{Name: "Move abroad", Date: mustParseDate("2027-09-01")})
	flat := mustEventAccount(t, svc, "Flat", lease.ID)
	checking := mustAccount(t, svc, "Checking")
	rent := monthlyRent(t, svc, checking.ID, "", lease.ID)
	upkeep := monthlyRent(t, svc, flat.ID, "2026-01-01", "")
	upkeep.Name = "Upkeep"
	upkeep = mustTransferTemplate(t, svc, upkeep)
	for _, tt := range []model.TransferTemplate{rent, upkeep} {
		mustTransferTemplateEnd(t, svc, model.TransferTemplateEnd{TransferTemplateID: tt.ID, EventID: abroad.ID, DateOffset: days(0)})
	}
	// An End in the New lease itself is listed as what the Event adds, not as also deleted.
	mustTransferTemplateEnd(t, svc, model.TransferTemplateEnd{TransferTemplateID: rent.ID, EventID: lease.ID, DateOffset: days(700)})

	view, err := svc.GetEventDeletePageData(t.Context(), lease.ID)
	if err != nil {
		t.Fatalf("event delete page: %v", err)
	}
	var also []string
	for _, e := range view.AlsoDeleted {
		also = append(also, e.Name+" in "+e.EventName)
	}
	slices.Sort(also)
	want := []string{
		"Transfer Template end of Rent from 2027-09-01 in Move abroad",
		"Transfer Template end of Upkeep from 2027-09-01 in Move abroad",
		"Upkeep in ",
	}
	if !slices.Equal(also, want) {
		t.Errorf("expected deleting the New lease to also delete %v, got %v", want, also)
	}
}

func TestATransferTemplateOfAnUnplannedEventShowsTheEndsItWouldHaveWithItsEventPlanned(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	sabbatical := mustEvent(t, svc, model.EventInput{Name: "Sabbatical", Date: mustParseDate("2028-01-01"), Planned: ptr(false)})
	offer := mustEvent(t, svc, model.EventInput{Name: "Job offer", Date: mustParseDate("2028-02-01"), Planned: ptr(false)})
	graduation := mustEvent(t, svc, model.EventInput{Name: "Graduation", Date: mustParseDate("2028-12-01")})
	fees := monthlyRent(t, svc, checking.ID, "", sabbatical.ID)
	fees.Name = "Course fees"
	fees = mustTransferTemplate(t, svc, fees)
	for _, end := range []model.TransferTemplateEnd{
		{EventID: sabbatical.ID, DateOffset: days(180)},
		{EventID: offer.ID, DateOffset: days(0)},
		{EventID: graduation.ID, DateOffset: days(0)},
	} {
		end.TransferTemplateID = fees.ID
		mustTransferTemplateEnd(t, svc, end)
	}
	leftOutEnd := func() string {
		t.Helper()
		page, err := svc.GetTransferTemplatesPageData(t.Context())
		if err != nil {
			t.Fatalf("transfer templates page: %v", err)
		}
		for _, tt := range page.LeftOut {
			if tt.ID == fees.ID {
				return dateOf(tt.EndDate)
			}
		}
		t.Fatalf("expected the Course fees left out with the Sabbatical, got %+v", page.LeftOut)
		return ""
	}

	// Its End in the Sabbatical counts, at the Sabbatical's date; the Job offer's, another unplanned
	// Event's, doesn't.
	if got := leftOutEnd(); got != "2028-06-29" {
		t.Errorf("expected the left-out Course fees to end 180 days after the Sabbatical, on 2028-06-29, got %s", got)
	}
	moveEvent(t, svc, graduation, "2028-04-01")
	if got := leftOutEnd(); got != "2028-04-01" {
		t.Errorf("expected the left-out Course fees to end on the planned Graduation, 2028-04-01, got %s", got)
	}
}

func TestDuplicatingATransferTemplateCopiesItsEnds(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	rent := monthlyRent(t, svc, checking.ID, "2025-01-01", "")
	abroad := mustEvent(t, svc, model.EventInput{Name: "Move abroad", Date: mustParseDate("2027-09-01")})
	mustTransferTemplateEnd(t, svc, model.TransferTemplateEnd{TransferTemplateID: rent.ID, EventID: abroad.ID, DateOffset: days(0)})
	mustEndTransferTemplate(t, svc, rent.ID, mustParseDate("2030-01-01"))

	copied, err := svc.DuplicateTransferTemplate(t.Context(), rent.ID)
	if err != nil {
		t.Fatalf("duplicate transfer template: %v", err)
	}

	ends := func(tt model.TransferTemplate) []string {
		var got []string
		for _, e := range tt.Ends {
			got = append(got, e.EventID+" "+e.Date.String())
		}
		return got
	}
	original := mustGetTransferTemplate(t, svc, rent.ID)
	duplicate := mustGetTransferTemplate(t, svc, copied.ID)
	if copied.ID == rent.ID || !slices.Equal(ends(duplicate), ends(original)) || len(duplicate.Ends) != 2 || duplicate.Ends[0].ID == original.Ends[0].ID {
		t.Errorf("expected the copy to have its own Ends %v, got %+v", ends(original), duplicate.Ends)
	}
	moveEvent(t, svc, abroad, "2028-01-01")
	if got := mustGetTransferTemplate(t, svc, copied.ID); dateOf(got.EndDate) != "2028-01-01" {
		t.Errorf("expected the copy's End in the Move abroad to move with it, got %s", dateOf(got.EndDate))
	}
}

func TestAnEndInAnUnplannedEventCountsOnlyWhereTheEventIsPlaced(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	mustSnapshot(t, svc, checking.ID, mustParseDate("2028-01-01"), 1e6)
	rent := monthlyRent(t, svc, checking.ID, "2025-01-01", "")
	abroad := mustEvent(t, svc, model.EventInput{Name: "Move abroad", Date: mustParseDate("2028-02-15"), Planned: ptr(false)})
	mustTransferTemplateEnd(t, svc, model.TransferTemplateEnd{TransferTemplateID: rent.ID, EventID: abroad.ID, DateOffset: days(0)})

	expectPays(t, svc, "Rent", map[string]bool{"2028-02-01": true, "2028-03-01": true, "2028-04-01": true})

	baseline := balanceOn(t, forecastThrough(t, svc, "2028-04-30", nil), checking.ID, "2028-04-28")
	for at, saved := range map[string]float64{"2028-02-15": 20000, "2028-03-15": 10000, "2028-04-15": 0} {
		rec := forecastThrough(t, svc, "2028-04-30", model.EventPlacements{abroad.ID: {Date: mustParseDate(at)}})
		if got := balanceOn(t, rec, checking.ID, "2028-04-28") - baseline; got != saved {
			t.Errorf("with the Move abroad placed on %s: expected Checking %v above the Baseline on 2028-04-28, got %v", at, saved, got)
		}
	}
}

func TestTheEarliestPlacedEndOfATransferTemplateApplies(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	rent := monthlyRent(t, svc, checking.ID, "2025-01-01", "")
	abroad := mustEvent(t, svc, model.EventInput{Name: "Move abroad", Date: mustParseDate("2027-09-01")})
	house := mustEvent(t, svc, model.EventInput{Name: "Buy a house", Date: mustParseDate("2027-06-01")})
	mustTransferTemplateEnd(t, svc, model.TransferTemplateEnd{TransferTemplateID: rent.ID, EventID: abroad.ID, DateOffset: days(0)})
	mustTransferTemplateEnd(t, svc, model.TransferTemplateEnd{TransferTemplateID: rent.ID, EventID: house.ID, DateOffset: days(0)})

	expectPays(t, svc, "Rent", map[string]bool{"2027-05-01": true, "2027-06-01": false, "2027-09-01": false})

	mustEvent(t, svc, model.EventInput{ID: house.ID, Name: house.Name, Date: house.Date, Planned: ptr(false)})

	expectPays(t, svc, "Rent", map[string]bool{"2027-06-01": true, "2027-08-01": true, "2027-09-01": false})
}

func TestATransferTemplateInOneEventEndedByAnother(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	lease := mustEvent(t, svc, model.EventInput{Name: "New lease", Date: mustParseDate("2026-01-01")})
	abroad := mustEvent(t, svc, model.EventInput{Name: "Move abroad", Date: mustParseDate("2027-09-01")})
	rent := monthlyRent(t, svc, checking.ID, "", lease.ID)
	mustTransferTemplateEnd(t, svc, model.TransferTemplateEnd{TransferTemplateID: rent.ID, EventID: abroad.ID, DateOffset: days(0)})

	expectPays(t, svc, "Rent", map[string]bool{"2025-12-01": false, "2026-01-01": true, "2027-08-01": true, "2027-09-01": false})

	moveEvent(t, svc, lease, "2026-03-01")
	expectPays(t, svc, "Rent", map[string]bool{"2026-02-01": false, "2026-03-01": true, "2027-08-01": true, "2027-09-01": false})

	moveEvent(t, svc, abroad, "2027-12-01")
	expectPays(t, svc, "Rent", map[string]bool{"2026-02-01": false, "2026-03-01": true, "2027-11-01": true, "2027-12-01": false})
}

func TestMigratingEndDatesToEndsKeepsTheDaysATransferTemplateTransfersOn(t *testing.T) {
	ctx := t.Context()
	conn := memdb.TestDB(t)
	old, err := model.Open(ctx, migrationsBefore(t, "0033"), "static/migrations", conn)
	if err != nil {
		t.Fatalf("open database before Transfer Template ends: %v", err)
	}
	// The rows are stored as the schema with an End Date column holds them: in an Event as days after
	// its date.
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := old.DB().ExecContext(ctx, query, args...); err != nil {
			t.Fatalf("seed the database before Transfer Template ends: %v", err)
		}
	}
	amount, err := newFixedValue(1000).Encode()
	if err != nil {
		t.Fatalf("encode amount: %v", err)
	}
	apartment := mustParseDate("2027-09-01")
	exec(`INSERT INTO account (id, name, created_at, updated_at) VALUES ('checking', 'Checking', 0, 0)`)
	exec(`INSERT INTO event (id, name, date, planned, created_at, updated_at) VALUES ('apartment', 'Apartment', ?, TRUE, 0, 0)`, int64(apartment))
	insert := func(id, name string, start, end int64, eventID *string) {
		t.Helper()
		exec(`INSERT INTO transfer_template (id, name, from_account_id, amount_type, amount_fixed, amount_percent, priority, recurrence, start_date, end_date, enabled, event_id, created_at, updated_at)
			VALUES (?, ?, 'checking', 'fixed', ?, 0, 0, '*-*-01', ?, ?, TRUE, ?, 0, 0)`, id, name, amount, start, end, eventID)
	}
	insert("gym", "Gym", int64(mustParseDate("2025-01-01")), int64(mustParseDate("2026-06-01")), nil)
	// Amortization starts on the Apartment's date and ends 92 days after it, on 2027-12-02.
	insert("amortization", "Amortization", 0, 92, ptr("apartment"))
	exec(`INSERT INTO transfer_template (id, name, from_account_id, amount_type, amount_fixed, amount_percent, priority, recurrence, start_date, enabled, created_at, updated_at)
		VALUES ('rent', 'Rent', 'checking', 'fixed', ?, 0, 0, '*-*-01', ?, TRUE, 0, 0)`, amount, int64(mustParseDate("2025-01-01")))
	old.DB().Close()

	svc, err := model.Open(ctx, pefigo.StaticEmbeddedFS, "static/migrations", conn)
	if err != nil {
		t.Fatalf("migrate to Transfer Template ends: %v", err)
	}
	t.Cleanup(func() { svc.DB().Close() })

	expectPays(t, svc, "Gym", map[string]bool{"2025-01-01": true, "2026-05-01": true, "2026-06-01": false})
	expectPays(t, svc, "Amortization", map[string]bool{"2027-08-01": false, "2027-09-01": true, "2027-12-01": true, "2028-01-01": false})
	expectPays(t, svc, "Rent", map[string]bool{"2025-01-01": true, "2040-01-01": true})
	for id, want := range map[string]struct{ eventID, day string }{"gym": {"", "2026-06-01"}, "amortization": {"apartment", "2027-12-02"}} {
		tt := mustGetTransferTemplate(t, svc, id)
		if len(tt.Ends) != 1 || tt.Ends[0].EventID != want.eventID || tt.Ends[0].Date != mustParseDate(want.day) {
			t.Errorf("expected %s to end on %s with an End in %q, got %+v", tt.Name, want.day, want.eventID, tt.Ends)
		}
	}
	if rent := mustGetTransferTemplate(t, svc, "rent"); len(rent.Ends) != 0 || rent.EndDate != nil {
		t.Errorf("expected Rent without an End, got %+v", rent.Ends)
	}

	// Amortization's End moves with the Apartment, as its End Date did: 92 days after 2027-10-01.
	moveEvent(t, svc, model.Event{ID: "apartment", Name: "Apartment", Date: apartment, Planned: true}, "2027-10-01")
	expectPays(t, svc, "Amortization", map[string]bool{"2027-09-01": false, "2027-10-01": true, "2027-12-01": true, "2028-01-01": false})
	if end := mustGetTransferTemplate(t, svc, "amortization").EndDate; dateOf(end) != "2028-01-01" {
		t.Errorf("expected Amortization to end 92 days after the moved Apartment, on 2028-01-01, got %s", dateOf(end))
	}
}

func TestAnEndOnOrBeforeATransferTemplatesStartMeansItNeverTransfers(t *testing.T) {
	svc := newTestService(t)
	checking := mustAccount(t, svc, "Checking")
	bonus := mustTransferTemplate(t, svc, model.TransferTemplate{
		Name: "Bonus", ToAccountID: checking.ID, AmountType: model.TransferAmountFixed, AmountFixed: newFixedValue(20000),
		StartDate: mustParseDate("2027-03-01"), Enabled: true,
	})
	rent := monthlyRent(t, svc, checking.ID, "2027-03-01", "")
	for _, tt := range []model.TransferTemplate{bonus, rent} {
		mustEndTransferTemplate(t, svc, tt.ID, mustParseDate("2027-03-01"))
	}

	expectPays(t, svc, "Bonus", map[string]bool{"2027-03-01": false})
	expectPays(t, svc, "Rent", map[string]bool{"2027-03-01": false, "2027-04-01": false})
}
