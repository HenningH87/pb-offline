// Package pbo installs the server-side half of the offline sync engine into an
// embedded PocketBase application: delete tombstones, the collection that holds
// them, and the cron that prunes them.
//
// It is deliberately small. PocketBase already provides everything else the
// client needs — the autodate `updated` column is the sync clock, API rules are
// the ownership boundary, and SSE is the freshness channel. The one thing a
// client cannot synthesise is the fact that a hard-deleted record ever existed.
//
// See SPEC.md §2.6 and §8. The JSVM equivalent in pb_hooks/pb-offline.pb.js MUST
// stay behaviourally identical; a divergence is a bug in whichever one is wrong.
//
// Usage:
//
//	app := pocketbase.New()
//	if err := pbo.Register(app, pbo.Options{Collections: []string{"projects", "tasks"}}); err != nil {
//		log.Fatal(err)
//	}
//	app.Start()
package pbo

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tools/hook"
)

// Default configuration values.
const (
	DefaultDeletionsCollection = "deletions"
	DefaultOwnerField          = "owner"
	DefaultRetention           = 30 * 24 * time.Hour
	DefaultPruneCron           = "0 3 * * *"

	// Cron job id. Namespaced so it cannot collide with an application's own.
	pruneJobID = "pbo-prune-tombstones"

	// Hook handler ids. Binding with an explicit id makes Register idempotent:
	// PocketBase's hook.Bind replaces a handler whose id already exists rather
	// than appending a second one.
	deleteHookID    = "pbo-tombstone-write"
	bootstrapHookID = "pbo-ensure-schema"
)

// Options configures Register.
type Options struct {
	// Collections to install tombstone hooks on. Required, non-empty.
	// A collection absent from this list is not offline-syncable: its deletes
	// are invisible to a disconnected client.
	Collections []string

	// Tombstone collection name. Default "deletions".
	DeletionsCollection string

	// Field naming the owner on the source collections and on the tombstone.
	// Default "owner". A source record without this field yields a tombstone
	// with an empty owner, which no client's owner-filtered pull will see — so
	// the hook logs it loudly rather than writing a tombstone nobody can read.
	OwnerField string

	// Retention window for tombstones. Default 30 days.
	//
	// This value is a CONTRACT WITH CLIENTS: a client whose tombstone cursor is
	// older than it has provably missed deletes and must full-resync. Shortening
	// it in production silently widens the window in which a long-offline client
	// diverges. Expose it to clients (see Retention) rather than hardcoding the
	// same number on both sides.
	Retention time.Duration

	// Prune cron expression. Default "0 3 * * *".
	PruneCron string

	// Create or patch the deletions collection at boot. Default true.
	// Set false (BoolPtr(false)) when the schema is owned by the application's
	// own migrations.
	//
	// SPEC §2.6 types this field `bool` and specifies a default of true, which a
	// Go bool cannot express — its zero value is false, so a caller who omitted
	// the field would get the opposite of the documented default. A *bool is the
	// same shape PocketBase itself uses for collection rules for the same reason:
	// nil means "unset, use the default".
	EnsureSchema *bool
}

// BoolPtr returns a pointer to v, for the optional bool fields of Options.
func BoolPtr(v bool) *bool { return &v }

// DefaultOptions returns Options for the given collections with every other
// field at its documented default.
func DefaultOptions(collections ...string) Options {
	return Options{
		Collections:         collections,
		DeletionsCollection: DefaultDeletionsCollection,
		OwnerField:          DefaultOwnerField,
		Retention:           DefaultRetention,
		PruneCron:           DefaultPruneCron,
		EnsureSchema:        BoolPtr(true),
	}
}

// registry remembers the retention window per app so Retention can answer
// without the caller having to carry the Options around. Keyed on the app
// instance, which for an embedded PocketBase is a single long-lived value.
var registry sync.Map // core.App -> time.Duration

func normalize(opts Options) (Options, error) {
	out := opts

	cleaned := make([]string, 0, len(opts.Collections))
	seen := make(map[string]struct{}, len(opts.Collections))
	for _, name := range opts.Collections {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}
		cleaned = append(cleaned, name)
	}
	if len(cleaned) == 0 {
		return out, errors.New("pbo: Options.Collections must list at least one collection")
	}
	out.Collections = cleaned

	if out.DeletionsCollection = strings.TrimSpace(out.DeletionsCollection); out.DeletionsCollection == "" {
		out.DeletionsCollection = DefaultDeletionsCollection
	}
	if out.OwnerField = strings.TrimSpace(out.OwnerField); out.OwnerField == "" {
		out.OwnerField = DefaultOwnerField
	}
	if out.Retention <= 0 {
		out.Retention = DefaultRetention
	}
	if out.PruneCron = strings.TrimSpace(out.PruneCron); out.PruneCron == "" {
		out.PruneCron = DefaultPruneCron
	}
	if out.EnsureSchema == nil {
		out.EnsureSchema = BoolPtr(true)
	}

	if _, clash := seen[out.DeletionsCollection]; clash {
		return out, fmt.Errorf(
			"pbo: %q is both a synced collection and the tombstone collection; deleting a tombstone would write a tombstone",
			out.DeletionsCollection,
		)
	}

	return out, nil
}

// Register installs the tombstone hooks, the deletions collection migration and
// the prune cron.
//
// It MUST be called before app.Start(). It is idempotent: calling it twice
// installs one set of hooks and one cron, and the second call's Options win.
func Register(app core.App, opts Options) error {
	if app == nil {
		return errors.New("pbo: app is nil")
	}

	opts, err := normalize(opts)
	if err != nil {
		return err
	}

	registry.Store(app, opts.Retention)

	if *opts.EnsureSchema {
		// The normal embedded path calls Register before app.Start(), when the
		// database is not open yet, so the schema work has to wait for bootstrap.
		// A test app (tests.NewTestApp) is already bootstrapped and will never
		// fire the hook again, so run it directly in that case.
		app.OnBootstrap().Bind(&hook.Handler[*core.BootstrapEvent]{
			Id: bootstrapHookID,
			Func: func(e *core.BootstrapEvent) error {
				if err := e.Next(); err != nil {
					return err
				}
				return ensureSchema(e.App, opts)
			},
			Priority: 100, // after the collections cache is loaded (priority 99)
		})

		if app.IsBootstrapped() {
			if err := ensureSchema(app, opts); err != nil {
				return err
			}
		}
	}

	bindDeleteHook(app, opts)

	if err := bindPruneCron(app, opts); err != nil {
		return err
	}

	return nil
}

// Retention returns the configured window, so an application can hand it to
// clients (an /api/app/sync-info route, a settings record) instead of having the
// same number written down twice.
//
// It returns DefaultRetention for an app that was never registered, which is the
// value the client assumes when the server tells it nothing (SPEC §11 trigger 1).
func Retention(app core.App) time.Duration {
	if v, ok := registry.Load(app); ok {
		if d, ok := v.(time.Duration); ok {
			return d
		}
	}
	return DefaultRetention
}
