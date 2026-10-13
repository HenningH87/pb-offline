package pbo

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/pocketbase/pocketbase/core"
)

// ensureSchema creates or patches the tombstone collection.
//
// Shape (SPEC §2.6, and the client depends on every line of it):
//
//	collection   text,     indexed      — the source collection's name
//	record_id    text,     indexed      — the deleted record's id
//	<ownerField> relation to the auth collection, indexed
//	created      autodate  (on create)
//	updated      autodate  (on create AND update)   ← REQUIRED
//
// `updated` is required because the client pulls tombstones with exactly the
// same (updated, id) cursor it uses for every other collection. A tombstone
// collection without it is invisible to the sync engine.
//
// Composite index: (owner, updated, id) — the pull's filter and sort.
//
// Rules:
//
//	listRule, viewRule                  = "<ownerField> = @request.auth.id"
//	createRule, updateRule, deleteRule  = nil  (hooks write as superuser)
//
// It is idempotent and additive: an existing collection is patched in place and
// never dropped, because dropping it would destroy the tombstones that
// long-offline clients still need.
func ensureSchema(app core.App, opts Options) error {
	ownerCollectionID, err := resolveOwnerCollectionID(app, opts)
	if err != nil {
		return err
	}

	collection, _ := app.FindCollectionByNameOrId(opts.DeletionsCollection)

	created := false
	if collection == nil {
		// A pinned id keeps repeated boots and separate environments agreeing on
		// one collection identity, which matters for relations and for exported
		// snapshots diffed across environments.
		collection = core.NewBaseCollection(opts.DeletionsCollection, pinnedCollectionID(opts.DeletionsCollection))
		created = true
	} else if !collection.IsBase() {
		return fmt.Errorf(
			"pbo: collection %q already exists with type %q; the tombstone collection must be a base collection",
			opts.DeletionsCollection, collection.Type,
		)
	}

	if err := ensureTextField(collection, FieldCollection); err != nil {
		return err
	}
	if err := ensureTextField(collection, FieldRecordID); err != nil {
		return err
	}
	if err := ensureOwnerField(collection, opts.OwnerField, ownerCollectionID); err != nil {
		return err
	}
	if err := ensureAutodateField(collection, "created", true, false); err != nil {
		return err
	}
	// `updated` must fire on update as well as create: it is the client's cursor.
	if err := ensureAutodateField(collection, "updated", true, true); err != nil {
		return err
	}

	prefix := "idx_pbo_" + sanitiseIdentifier(opts.DeletionsCollection)
	owner := "`" + opts.OwnerField + "`"
	collection.AddIndex(prefix+"_collection", false, "`"+FieldCollection+"`", "")
	collection.AddIndex(prefix+"_record_id", false, "`"+FieldRecordID+"`", "")
	collection.AddIndex(prefix+"_owner", false, owner, "")
	collection.AddIndex(prefix+"_cursor", false, owner+", `updated`, `id`", "")

	ownerRule := opts.OwnerField + " = @request.auth.id"
	collection.ListRule = chooseRule(app, collection.Name, created, "listRule", collection.ListRule, &ownerRule)
	collection.ViewRule = chooseRule(app, collection.Name, created, "viewRule", collection.ViewRule, &ownerRule)
	collection.CreateRule = chooseRule(app, collection.Name, created, "createRule", collection.CreateRule, nil)
	collection.UpdateRule = chooseRule(app, collection.Name, created, "updateRule", collection.UpdateRule, nil)
	collection.DeleteRule = chooseRule(app, collection.Name, created, "deleteRule", collection.DeleteRule, nil)

	if err := app.Save(collection); err != nil {
		return fmt.Errorf("pbo: failed to save the %q collection: %w", opts.DeletionsCollection, err)
	}

	if created {
		app.Logger().Info("pbo: created the tombstone collection", "collection", opts.DeletionsCollection)
	}

	return nil
}

// chooseRule decides what one API rule of the tombstone collection should be,
// and it is the operator's answer that wins.
//
// "Idempotent and additive" was true of the FIELDS and false of the rules: the
// old code reassigned all five on every boot, so an operator who deliberately
// tightened `deletions` had it silently reverted at the next restart, with no
// log line. Rules are the security boundary of a collection, and resetting a
// boundary somebody changed on purpose is a downgrade, not a migration.
//
// So: a rule this helper has never set (nil) is set; a rule that already says
// what we would have said is left alone; anything else is KEPT and named in a
// warning. On a collection this call is creating there is nothing to preserve
// and the prescribed rule is written outright.
func chooseRule(app core.App, collectionName string, created bool, ruleName string, current, prescribed *string) *string {
	if created || current == nil {
		return prescribed
	}
	if prescribed != nil && *current == *prescribed {
		return current
	}
	app.Logger().Warn(
		"pbo: keeping the existing API rule on the tombstone collection; pb-offline would have set a different one",
		"collection", collectionName,
		"rule", ruleName,
		"existing", ruleText(current),
		"prescribed", ruleText(prescribed),
	)
	return current
}

// ruleText renders a PocketBase rule pointer for a log line. nil is not empty:
// an empty rule is "anyone", nil is "superusers only", and confusing the two in
// a warning about API rules would be its own security bug.
func ruleText(rule *string) string {
	if rule == nil {
		return "nil (superusers only)"
	}
	if *rule == "" {
		return `"" (public)`
	}
	return *rule
}

// resolveOwnerCollectionID decides which auth collection the tombstone's owner
// relation points at, by copying the answer from the collections being synced
// rather than asking the operator to write it down a second time.
func resolveOwnerCollectionID(app core.App, opts Options) (string, error) {
	for _, name := range opts.Collections {
		collection, err := app.FindCollectionByNameOrId(name)
		if err != nil || collection == nil {
			continue
		}
		if rel, ok := collection.Fields.GetByName(opts.OwnerField).(*core.RelationField); ok && rel.CollectionId != "" {
			return rel.CollectionId, nil
		}
	}

	// An existing tombstone collection already carries the answer.
	if existing, err := app.FindCollectionByNameOrId(opts.DeletionsCollection); err == nil && existing != nil {
		if rel, ok := existing.Fields.GetByName(opts.OwnerField).(*core.RelationField); ok && rel.CollectionId != "" {
			return rel.CollectionId, nil
		}
	}

	authCollections, err := app.FindAllCollections(core.CollectionTypeAuth)
	if err != nil {
		return "", fmt.Errorf("pbo: failed to list auth collections: %w", err)
	}

	candidates := make([]*core.Collection, 0, len(authCollections))
	for _, c := range authCollections {
		if c.System {
			continue // _superusers and friends are never the owner of user data
		}
		if c.Name == "users" {
			return c.Id, nil
		}
		candidates = append(candidates, c)
	}
	if len(candidates) == 1 {
		return candidates[0].Id, nil
	}

	return "", fmt.Errorf(
		"pbo: cannot infer the auth collection for the %q relation — none of %v has a %q relation field and there is no unambiguous auth collection; create the %q collection yourself and register with EnsureSchema false",
		opts.OwnerField, opts.Collections, opts.OwnerField, opts.DeletionsCollection,
	)
}

func ensureTextField(collection *core.Collection, name string) error {
	switch f := collection.Fields.GetByName(name).(type) {
	case nil:
		collection.Fields.Add(&core.TextField{Name: name, Required: true})
	case *core.TextField:
		f.Required = true
		f.Hidden = false
	default:
		return incompatibleField(collection, name, "text", f.Type())
	}
	return nil
}

func ensureOwnerField(collection *core.Collection, name string, ownerCollectionID string) error {
	switch f := collection.Fields.GetByName(name).(type) {
	case nil:
		collection.Fields.Add(&core.RelationField{
			Name:         name,
			CollectionId: ownerCollectionID,
			MaxSelect:    1,
			// Not required: a collection configured `filter: null` (SPEC §2.3)
			// has no owner, and a tombstone with an empty owner is still better
			// than a delete nobody records.
			Required: false,
			// Never cascade: deleting a user must not erase the evidence that
			// their records were deleted.
			CascadeDelete: false,
		})
	case *core.RelationField:
		if f.CollectionId != "" && f.CollectionId != ownerCollectionID {
			return fmt.Errorf(
				"pbo: collection %q already has a %q relation to collection id %q, but the synced collections point at %q",
				collection.Name, name, f.CollectionId, ownerCollectionID,
			)
		}
		f.CollectionId = ownerCollectionID
		f.MaxSelect = 1
		f.Hidden = false
	default:
		return incompatibleField(collection, name, "relation", f.Type())
	}
	return nil
}

func ensureAutodateField(collection *core.Collection, name string, onCreate, onUpdate bool) error {
	switch f := collection.Fields.GetByName(name).(type) {
	case nil:
		collection.Fields.Add(&core.AutodateField{Name: name, OnCreate: onCreate, OnUpdate: onUpdate, System: true})
	case *core.AutodateField:
		f.OnCreate = onCreate
		f.OnUpdate = onUpdate
		f.Hidden = false
	default:
		return incompatibleField(collection, name, "autodate", f.Type())
	}
	return nil
}

func incompatibleField(collection *core.Collection, name, want, got string) error {
	return fmt.Errorf(
		"pbo: collection %q already has a field %q of type %q, but the tombstone contract requires %q; rename or remove it rather than letting pbo coerce it",
		collection.Name, name, got, want,
	)
}

// pinnedCollectionID derives a stable 15-character lowercase-alphanumeric id
// from the collection name, which is the id shape PocketBase requires.
func pinnedCollectionID(name string) string {
	sum := sha256.Sum256([]byte("pb-offline/deletions/" + name))
	return "pbo" + hex.EncodeToString(sum[:])[:12]
}

func sanitiseIdentifier(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r + ('a' - 'A'))
		default:
			b.WriteRune('_')
		}
	}
	return b.String()
}
