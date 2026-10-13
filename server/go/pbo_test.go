package pbo_test

import (
	"strings"
	"testing"
	"time"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tests"
	"github.com/pocketbase/pocketbase/tools/types"

	pbo "github.com/HenningH87/pb-offline/server/go"
)

const (
	projectsCollection = "pbo_projects"
	tasksCollection    = "pbo_tasks"
	looseCollection    = "pbo_loose" // deliberately NOT registered
)

// newApp returns a bootstrapped test app carrying the fixture collections the
// suite syncs, plus the id of a user to own their records.
func newApp(t *testing.T) (*tests.TestApp, string) {
	t.Helper()

	app, err := tests.NewTestApp()
	if err != nil {
		t.Fatalf("new test app: %v", err)
	}
	t.Cleanup(app.Cleanup)

	users, err := app.FindCollectionByNameOrId("users")
	if err != nil {
		t.Fatalf("find users collection: %v", err)
	}

	projects := newSyncedCollection(t, app, projectsCollection, users.Id, "")
	newSyncedCollection(t, app, tasksCollection, users.Id, projects.Id)
	newSyncedCollection(t, app, looseCollection, users.Id, "")

	owners, err := app.FindAllRecords("users")
	if err != nil || len(owners) == 0 {
		t.Fatalf("no fixture users available: %v", err)
	}

	return app, owners[0].Id
}

// newSyncedCollection creates a base collection shaped the way SPEC §2.6
// requires of anything the client mirrors: autodate `updated`, an owner relation.
// A non-empty parentID adds a cascading relation to that collection.
func newSyncedCollection(t *testing.T, app core.App, name, usersID, parentID string) *core.Collection {
	t.Helper()

	c := core.NewBaseCollection(name)
	c.Fields.Add(&core.TextField{Name: "title"})
	c.Fields.Add(&core.RelationField{Name: "owner", CollectionId: usersID, MaxSelect: 1})
	if parentID != "" {
		c.Fields.Add(&core.RelationField{Name: "parent", CollectionId: parentID, MaxSelect: 1, CascadeDelete: true})
	}
	c.Fields.Add(&core.AutodateField{Name: "created", OnCreate: true, System: true})
	c.Fields.Add(&core.AutodateField{Name: "updated", OnCreate: true, OnUpdate: true, System: true})

	if err := app.Save(c); err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	return c
}

func newRecord(t *testing.T, app core.App, collection, title, owner string, extra map[string]any) *core.Record {
	t.Helper()

	c, err := app.FindCollectionByNameOrId(collection)
	if err != nil {
		t.Fatalf("find %s: %v", collection, err)
	}

	r := core.NewRecord(c)
	r.Set("title", title)
	r.Set("owner", owner)
	for k, v := range extra {
		r.Set(k, v)
	}
	if err := app.Save(r); err != nil {
		t.Fatalf("save %s record: %v", collection, err)
	}
	return r
}

func tombstones(t *testing.T, app core.App) []*core.Record {
	t.Helper()

	records, err := app.FindAllRecords(pbo.DefaultDeletionsCollection)
	if err != nil {
		t.Fatalf("list tombstones: %v", err)
	}
	return records
}

func register(t *testing.T, app core.App, opts pbo.Options) {
	t.Helper()

	if err := pbo.Register(app, opts); err != nil {
		t.Fatalf("Register: %v", err)
	}
}

// ── schema ───────────────────────────────────────────────────────────────────

func TestEnsureSchemaCreatesTheTombstoneCollection(t *testing.T) {
	app, _ := newApp(t)

	register(t, app, pbo.Options{Collections: []string{projectsCollection, tasksCollection}})

	c, err := app.FindCollectionByNameOrId(pbo.DefaultDeletionsCollection)
	if err != nil {
		t.Fatalf("deletions collection was not created: %v", err)
	}

	if _, ok := c.Fields.GetByName(pbo.FieldCollection).(*core.TextField); !ok {
		t.Errorf("field %q is not a text field", pbo.FieldCollection)
	}
	if _, ok := c.Fields.GetByName(pbo.FieldRecordID).(*core.TextField); !ok {
		t.Errorf("field %q is not a text field", pbo.FieldRecordID)
	}

	owner, ok := c.Fields.GetByName(pbo.DefaultOwnerField).(*core.RelationField)
	if !ok {
		t.Fatalf("field %q is not a relation field", pbo.DefaultOwnerField)
	}
	users, _ := app.FindCollectionByNameOrId("users")
	if owner.CollectionId != users.Id {
		t.Errorf("owner relation points at %q, want the auth collection %q", owner.CollectionId, users.Id)
	}

	// `updated` is the client's cursor: without OnUpdate the collection is
	// invisible to the sync engine (SPEC §2.6 convention 3).
	updated, ok := c.Fields.GetByName("updated").(*core.AutodateField)
	if !ok || !updated.OnCreate || !updated.OnUpdate {
		t.Errorf("updated autodate = %+v, want OnCreate and OnUpdate", updated)
	}
	created, ok := c.Fields.GetByName("created").(*core.AutodateField)
	if !ok || !created.OnCreate {
		t.Errorf("created autodate = %+v, want OnCreate", created)
	}

	rule := pbo.DefaultOwnerField + " = @request.auth.id"
	if c.ListRule == nil || *c.ListRule != rule {
		t.Errorf("listRule = %v, want %q", c.ListRule, rule)
	}
	if c.ViewRule == nil || *c.ViewRule != rule {
		t.Errorf("viewRule = %v, want %q", c.ViewRule, rule)
	}
	for name, r := range map[string]*string{"create": c.CreateRule, "update": c.UpdateRule, "delete": c.DeleteRule} {
		if r != nil {
			t.Errorf("%sRule = %q, want nil (hooks write as superuser)", name, *r)
		}
	}

	// The (owner, updated, id) index is what the client's cursor pull sorts and
	// filters on.
	if idx := c.Indexes.String(); !strings.Contains(idx, "`owner`, `updated`, `id`") {
		t.Errorf("indexes %s are missing the (owner, updated, id) composite", idx)
	}
}

// The migration is documented "idempotent and additive", and that was true of
// the FIELDS and false of the rules: all five were reassigned on every boot, so
// an operator who deliberately tightened `deletions` had it reverted at the
// next restart with no log line. Rules are the security boundary of the
// collection; resetting one somebody changed on purpose is a downgrade.
func TestEnsureSchemaKeepsRulesTheOperatorWrote(t *testing.T) {
	app, _ := newApp(t)

	// First boot creates the collection with pb-offline's rules.
	register(t, app, pbo.Options{Collections: []string{projectsCollection}})

	c, err := app.FindCollectionByNameOrId(pbo.DefaultDeletionsCollection)
	if err != nil {
		t.Fatalf("deletions collection was not created: %v", err)
	}

	// The operator then narrows list/view and opens delete to the owner.
	tightened := pbo.DefaultOwnerField + " = @request.auth.id && @request.auth.verified = true"
	ownerRule := pbo.DefaultOwnerField + " = @request.auth.id"
	c.ListRule = &tightened
	c.ViewRule = &tightened
	c.DeleteRule = &ownerRule
	if err := app.Save(c); err != nil {
		t.Fatalf("save the operator's rules: %v", err)
	}

	// Next boot. Every one of them survives it.
	register(t, app, pbo.Options{Collections: []string{projectsCollection}})

	after, err := app.FindCollectionByNameOrId(pbo.DefaultDeletionsCollection)
	if err != nil {
		t.Fatalf("find deletions collection: %v", err)
	}
	if after.ListRule == nil || *after.ListRule != tightened {
		t.Errorf("listRule = %v, want the operator's %q", after.ListRule, tightened)
	}
	if after.ViewRule == nil || *after.ViewRule != tightened {
		t.Errorf("viewRule = %v, want the operator's %q", after.ViewRule, tightened)
	}
	if after.DeleteRule == nil || *after.DeleteRule != ownerRule {
		t.Errorf("deleteRule = %v, want the operator's %q", after.DeleteRule, ownerRule)
	}
	// The two they did not touch stay superuser-only, as this helper set them.
	if after.CreateRule != nil || after.UpdateRule != nil {
		t.Errorf("create/updateRule = %v/%v, want nil", after.CreateRule, after.UpdateRule)
	}
}

// A rule this helper has never set is still set: preserving the operator's
// answer must not turn into refusing to configure a half-built collection.
func TestEnsureSchemaSetsRulesThatAreUnset(t *testing.T) {
	app, _ := newApp(t)

	// A tombstone collection the operator created by hand, rules left at PB's
	// default for a new base collection: nil everywhere.
	existing := core.NewBaseCollection(pbo.DefaultDeletionsCollection)
	if err := app.Save(existing); err != nil {
		t.Fatalf("create the collection by hand: %v", err)
	}

	register(t, app, pbo.Options{Collections: []string{projectsCollection}})

	c, err := app.FindCollectionByNameOrId(pbo.DefaultDeletionsCollection)
	if err != nil {
		t.Fatalf("find deletions collection: %v", err)
	}
	rule := pbo.DefaultOwnerField + " = @request.auth.id"
	if c.ListRule == nil || *c.ListRule != rule {
		t.Errorf("listRule = %v, want %q", c.ListRule, rule)
	}
	if c.ViewRule == nil || *c.ViewRule != rule {
		t.Errorf("viewRule = %v, want %q", c.ViewRule, rule)
	}
}

func TestEnsureSchemaFalseCreatesNothing(t *testing.T) {
	app, _ := newApp(t)

	register(t, app, pbo.Options{
		Collections:  []string{projectsCollection},
		EnsureSchema: pbo.BoolPtr(false),
	})

	if _, err := app.FindCollectionByNameOrId(pbo.DefaultDeletionsCollection); err == nil {
		t.Fatal("deletions collection exists, want it left to the application's own migrations")
	}
}

func TestRegisterRejectsEmptyCollections(t *testing.T) {
	app, _ := newApp(t)

	if err := pbo.Register(app, pbo.Options{}); err == nil {
		t.Fatal("Register with no collections succeeded, want an error")
	}
	if err := pbo.Register(app, pbo.Options{Collections: []string{"  "}}); err == nil {
		t.Fatal("Register with only blank collection names succeeded, want an error")
	}
}

func TestRegisterRejectsTheTombstoneCollectionAsASource(t *testing.T) {
	app, _ := newApp(t)

	err := pbo.Register(app, pbo.Options{Collections: []string{projectsCollection, "deletions"}})
	if err == nil {
		t.Fatal("Register accepted the tombstone collection as a synced collection")
	}
}

// ── tombstones ───────────────────────────────────────────────────────────────

func TestTombstoneWrittenOnDeleteWithTheRecordsOwner(t *testing.T) {
	app, owner := newApp(t)
	register(t, app, pbo.Options{Collections: []string{projectsCollection, tasksCollection}})

	record := newRecord(t, app, projectsCollection, "alpha", owner, nil)
	if err := app.Delete(record); err != nil {
		t.Fatalf("delete: %v", err)
	}

	rows := tombstones(t, app)
	if len(rows) != 1 {
		t.Fatalf("got %d tombstones, want 1", len(rows))
	}
	got := rows[0]
	if got.GetString(pbo.FieldCollection) != projectsCollection {
		t.Errorf("collection = %q, want %q", got.GetString(pbo.FieldCollection), projectsCollection)
	}
	if got.GetString(pbo.FieldRecordID) != record.Id {
		t.Errorf("record_id = %q, want %q", got.GetString(pbo.FieldRecordID), record.Id)
	}
	if got.GetString(pbo.DefaultOwnerField) != owner {
		t.Errorf("owner = %q, want the deleted record's owner %q", got.GetString(pbo.DefaultOwnerField), owner)
	}
	// The client pulls tombstones with the same (updated, id) cursor as anything
	// else, so both autodates have to be populated.
	if got.GetDateTime("updated").IsZero() || got.GetDateTime("created").IsZero() {
		t.Errorf("tombstone is missing created/updated: created=%v updated=%v",
			got.GetDateTime("created"), got.GetDateTime("updated"))
	}
}

func TestNoTombstoneForAnUnregisteredCollection(t *testing.T) {
	app, owner := newApp(t)
	register(t, app, pbo.Options{Collections: []string{projectsCollection, tasksCollection}})

	record := newRecord(t, app, looseCollection, "not synced", owner, nil)
	if err := app.Delete(record); err != nil {
		t.Fatalf("delete: %v", err)
	}

	if rows := tombstones(t, app); len(rows) != 0 {
		t.Fatalf("got %d tombstones for an unregistered collection, want 0", len(rows))
	}
}

func TestCascadeDeleteWritesATombstonePerRecord(t *testing.T) {
	app, owner := newApp(t)
	register(t, app, pbo.Options{Collections: []string{projectsCollection, tasksCollection}})

	parent := newRecord(t, app, projectsCollection, "parent", owner, nil)
	child := newRecord(t, app, tasksCollection, "child", owner, map[string]any{"parent": parent.Id})

	if err := app.Delete(parent); err != nil {
		t.Fatalf("delete parent: %v", err)
	}

	seen := map[string]string{}
	for _, row := range tombstones(t, app) {
		seen[row.GetString(pbo.FieldRecordID)] = row.GetString(pbo.FieldCollection)
	}
	if seen[parent.Id] != projectsCollection {
		t.Errorf("no tombstone for the parent: %v", seen)
	}
	if seen[child.Id] != tasksCollection {
		t.Errorf("no tombstone for the cascade-deleted child: %v", seen)
	}
}

func TestTombstoneWrittenForARecordWithNoOwner(t *testing.T) {
	app, _ := newApp(t)
	register(t, app, pbo.Options{Collections: []string{projectsCollection}})

	record := newRecord(t, app, projectsCollection, "unowned", "", nil)
	if err := app.Delete(record); err != nil {
		t.Fatalf("delete: %v", err)
	}

	rows := tombstones(t, app)
	if len(rows) != 1 {
		t.Fatalf("got %d tombstones, want 1 even without an owner", len(rows))
	}
	if rows[0].GetString(pbo.DefaultOwnerField) != "" {
		t.Errorf("owner = %q, want empty", rows[0].GetString(pbo.DefaultOwnerField))
	}
}

func TestCustomOwnerFieldAndDeletionsCollection(t *testing.T) {
	app, owner := newApp(t)

	users, _ := app.FindCollectionByNameOrId("users")
	c := core.NewBaseCollection("pbo_custom")
	c.Fields.Add(&core.RelationField{Name: "account", CollectionId: users.Id, MaxSelect: 1})
	c.Fields.Add(&core.AutodateField{Name: "created", OnCreate: true, System: true})
	c.Fields.Add(&core.AutodateField{Name: "updated", OnCreate: true, OnUpdate: true, System: true})
	if err := app.Save(c); err != nil {
		t.Fatalf("create pbo_custom: %v", err)
	}

	register(t, app, pbo.Options{
		Collections:         []string{"pbo_custom"},
		DeletionsCollection: "graves",
		OwnerField:          "account",
	})

	record := core.NewRecord(c)
	record.Set("account", owner)
	if err := app.Save(record); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := app.Delete(record); err != nil {
		t.Fatalf("delete: %v", err)
	}

	rows, err := app.FindAllRecords("graves")
	if err != nil {
		t.Fatalf("list graves: %v", err)
	}
	if len(rows) != 1 || rows[0].GetString("account") != owner {
		t.Fatalf("got %d rows in graves (owner %v), want 1 owned by %q", len(rows), rows, owner)
	}
}

// ── idempotency ──────────────────────────────────────────────────────────────

func TestRegisterIsIdempotent(t *testing.T) {
	app, owner := newApp(t)

	opts := pbo.Options{Collections: []string{projectsCollection, tasksCollection}}
	register(t, app, opts)
	register(t, app, opts)
	register(t, app, opts)

	record := newRecord(t, app, projectsCollection, "once", owner, nil)
	if err := app.Delete(record); err != nil {
		t.Fatalf("delete: %v", err)
	}

	if rows := tombstones(t, app); len(rows) != 1 {
		t.Fatalf("got %d tombstones after 3 Registers, want exactly 1", len(rows))
	}

	jobs := 0
	for _, job := range app.Cron().Jobs() {
		if job.Id() == "pbo-prune-tombstones" {
			jobs++
		}
	}
	if jobs != 1 {
		t.Fatalf("got %d prune cron jobs, want 1", jobs)
	}
}

func TestEnsureSchemaIsIdempotentAndPreservesExistingTombstones(t *testing.T) {
	app, owner := newApp(t)

	opts := pbo.Options{Collections: []string{projectsCollection}}
	register(t, app, opts)

	record := newRecord(t, app, projectsCollection, "survivor", owner, nil)
	if err := app.Delete(record); err != nil {
		t.Fatalf("delete: %v", err)
	}
	before, _ := app.FindCollectionByNameOrId(pbo.DefaultDeletionsCollection)

	register(t, app, opts)

	after, err := app.FindCollectionByNameOrId(pbo.DefaultDeletionsCollection)
	if err != nil {
		t.Fatalf("deletions collection vanished: %v", err)
	}
	if after.Id != before.Id {
		t.Errorf("collection id changed %q → %q; it must be patched, never recreated", before.Id, after.Id)
	}
	if rows := tombstones(t, app); len(rows) != 1 {
		t.Fatalf("got %d tombstones after re-registering, want the 1 that existed", len(rows))
	}
}

// ── retention ────────────────────────────────────────────────────────────────

func TestRetentionDefaultsAndReportsTheConfiguredWindow(t *testing.T) {
	app, _ := newApp(t)

	if got := pbo.Retention(app); got != pbo.DefaultRetention {
		t.Errorf("Retention before Register = %v, want the documented default %v", got, pbo.DefaultRetention)
	}

	register(t, app, pbo.Options{Collections: []string{projectsCollection}, Retention: 72 * time.Hour})
	if got := pbo.Retention(app); got != 72*time.Hour {
		t.Errorf("Retention = %v, want 72h", got)
	}

	// Zero means "unset", not "keep nothing".
	register(t, app, pbo.Options{Collections: []string{projectsCollection}})
	if got := pbo.Retention(app); got != pbo.DefaultRetention {
		t.Errorf("Retention after a zero-value Options = %v, want %v", got, pbo.DefaultRetention)
	}
}

func TestPruneRemovesOnlyRowsOlderThanRetention(t *testing.T) {
	app, owner := newApp(t)

	retention := 24 * time.Hour
	register(t, app, pbo.Options{
		Collections: []string{projectsCollection},
		Retention:   retention,
		// Every minute, so the job is real and schedulable; the test calls it
		// directly rather than waiting for the clock.
		PruneCron: "* * * * *",
	})

	fresh := newRecord(t, app, projectsCollection, "fresh", owner, nil)
	stale := newRecord(t, app, projectsCollection, "stale", owner, nil)
	edge := newRecord(t, app, projectsCollection, "edge", owner, nil)
	for _, r := range []*core.Record{fresh, stale, edge} {
		if err := app.Delete(r); err != nil {
			t.Fatalf("delete: %v", err)
		}
	}

	now := types.NowDateTime()
	backdate(t, app, stale.Id, now.Add(-retention-time.Hour))
	// Just inside the window: a prune that keys on >= instead of > would take it.
	backdate(t, app, edge.Id, now.Add(-retention+time.Minute))

	runPruneJob(t, app)

	remaining := map[string]bool{}
	for _, row := range tombstones(t, app) {
		remaining[row.GetString(pbo.FieldRecordID)] = true
	}
	if remaining[stale.Id] {
		t.Errorf("the tombstone older than the retention window survived the prune")
	}
	if !remaining[fresh.Id] {
		t.Errorf("a fresh tombstone was pruned")
	}
	if !remaining[edge.Id] {
		t.Errorf("a tombstone just inside the retention window was pruned")
	}
}

func TestPruneHandlesMoreRowsThanOneBatch(t *testing.T) {
	app, owner := newApp(t)

	retention := time.Hour
	register(t, app, pbo.Options{
		Collections: []string{projectsCollection},
		Retention:   retention,
		PruneCron:   "* * * * *",
	})

	const total = 620 // > the 500-row prune batch
	ids := make([]string, 0, total)
	for i := 0; i < total; i++ {
		r := newRecord(t, app, projectsCollection, "bulk", owner, nil)
		if err := app.Delete(r); err != nil {
			t.Fatalf("delete: %v", err)
		}
		ids = append(ids, r.Id)
	}

	old := types.NowDateTime().Add(-retention - time.Hour)
	for _, id := range ids {
		backdate(t, app, id, old)
	}

	runPruneJob(t, app)

	if rows := tombstones(t, app); len(rows) != 0 {
		t.Fatalf("got %d tombstones after pruning %d expired rows, want 0", len(rows), total)
	}
}

func TestRegisterRejectsAnInvalidCronExpression(t *testing.T) {
	app, _ := newApp(t)

	err := pbo.Register(app, pbo.Options{
		Collections: []string{projectsCollection},
		PruneCron:   "not a cron expression",
	})
	if err == nil {
		t.Fatal("Register accepted an invalid cron expression")
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

// backdate rewrites a tombstone's `created` directly in SQL — the autodate field
// would otherwise overwrite any value the record API is given.
func backdate(t *testing.T, app core.App, recordID string, at types.DateTime) {
	t.Helper()

	_, err := app.DB().NewQuery(
		"UPDATE {{deletions}} SET [[created]] = {:at} WHERE [[record_id]] = {:rid}",
	).Bind(dbx.Params{"at": at.String(), "rid": recordID}).Execute()
	if err != nil {
		t.Fatalf("backdate %s: %v", recordID, err)
	}
}

// runPruneJob runs the registered cron job's function, which is the same code
// path the scheduler takes.
func runPruneJob(t *testing.T, app core.App) {
	t.Helper()

	for _, job := range app.Cron().Jobs() {
		if job.Id() == "pbo-prune-tombstones" {
			job.Run()
			return
		}
	}
	t.Fatal("prune cron job was never registered")
}
