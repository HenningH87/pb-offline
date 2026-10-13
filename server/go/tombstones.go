package pbo

import (
	"fmt"

	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tools/hook"
)

// Tombstone field names. These are part of the client contract (SPEC §2.6) and
// are not configurable — a client that had to be told the field names would need
// a discovery round trip before its first sync.
const (
	FieldCollection = "collection"
	FieldRecordID   = "record_id"
)

// bindDeleteHook installs OnRecordAfterDeleteSuccess for the configured
// collections.
//
// One handler covers all of them: the hook is tagged with every configured
// collection name, so a re-Register with a different set replaces the whole
// binding instead of leaving a stale per-collection handler behind.
//
// Notes that shaped this:
//   - AfterDeleteSuccess fires only once the delete has committed, so a rolled
//     back delete leaves no tombstone.
//   - A cascade delete fires it per deleted record, which is what the client
//     needs: one tombstone each.
//   - A superuser delete fires it too. That matters — an admin deleting a row in
//     the dashboard must not leave clients holding it forever.
//   - e.App is read rather than the captured app so the write joins the enclosing
//     transaction when there is one.
func bindDeleteHook(app core.App, opts Options) {
	app.OnRecordAfterDeleteSuccess(opts.Collections...).Bind(&hook.Handler[*core.RecordEvent]{
		Id: deleteHookID,
		Func: func(e *core.RecordEvent) error {
			owner := recordOwner(e.Record, opts.OwnerField)
			if owner == "" {
				// Still write it. An owner-filtered client will not see this row,
				// but a delete that was never recorded at all cannot be recovered
				// by anything short of a full resync.
				e.App.Logger().Warn(
					"pbo: deleted record has no owner; writing an unowned tombstone",
					"collection", e.Record.Collection().Name,
					"record", e.Record.Id,
					"ownerField", opts.OwnerField,
				)
			}

			if err := writeTombstone(e.App, opts, e.Record.Collection().Name, e.Record.Id, owner); err != nil {
				// The delete has already committed; this cannot be rolled back,
				// and it silently costs a client its consistency. It is the one
				// error in this package that deserves noise.
				e.App.Logger().Error(
					"pbo: FAILED to write a delete tombstone — offline clients will not learn about this delete",
					"collection", e.Record.Collection().Name,
					"record", e.Record.Id,
					"error", err,
				)
			}

			return e.Next()
		},
	})
}

// writeTombstone inserts one tombstone row. It runs outside the API rules
// (a direct model save is never rule-checked), which is why the tombstone
// collection's create rule is nil.
func writeTombstone(app core.App, opts Options, collection string, recordID string, owner string) error {
	deletions, err := app.FindCollectionByNameOrId(opts.DeletionsCollection)
	if err != nil {
		return fmt.Errorf("tombstone collection %q not found: %w", opts.DeletionsCollection, err)
	}

	tombstone := core.NewRecord(deletions)
	tombstone.Set(FieldCollection, collection)
	tombstone.Set(FieldRecordID, recordID)
	tombstone.Set(opts.OwnerField, owner)

	return app.Save(tombstone)
}

// recordOwner reads the owner id off a record, tolerating both shapes a
// PocketBase field can hold it in: a single relation (string) and a multiple
// relation (slice), where the first entry is taken.
func recordOwner(record *core.Record, ownerField string) string {
	if record == nil {
		return ""
	}

	switch v := record.GetRaw(ownerField).(type) {
	case string:
		return v
	case []string:
		if len(v) > 0 {
			return v[0]
		}
	}

	return ""
}
