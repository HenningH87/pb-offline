package pbo

import (
	"fmt"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tools/types"
)

// pruneBatchSize bounds one DELETE statement. A chatty application can
// accumulate a lot of rows, and a single unbounded DELETE holds the SQLite
// write lock for as long as it takes.
const pruneBatchSize = 500

// bindPruneCron registers the retention job.
//
// The window is a contract with clients (see Options.Retention). A client whose
// tombstone cursor predates it MUST full-resync rather than silently diverge —
// that is the client's obligation, and this cron is what makes the obligation
// necessary.
func bindPruneCron(app core.App, opts Options) error {
	err := app.Cron().Add(pruneJobID, opts.PruneCron, func() {
		pruned, err := prune(app, opts)
		if err != nil {
			app.Logger().Error("pbo: tombstone prune failed", "error", err)
			return
		}
		if pruned > 0 {
			app.Logger().Info(
				"pbo: pruned expired tombstones",
				"collection", opts.DeletionsCollection,
				"count", pruned,
				"retention", opts.Retention.String(),
			)
		}
	})
	if err != nil {
		return fmt.Errorf("pbo: invalid PruneCron %q: %w", opts.PruneCron, err)
	}
	return nil
}

// prune deletes tombstones older than the retention window and reports how many
// went.
//
// It keys on `created`, not `updated`: a tombstone is never updated, and keying
// on a column nothing writes twice keeps the arithmetic obvious.
//
// The rows are deleted with SQL rather than through the record API on purpose —
// a tombstone has no delete hooks worth firing, and a per-record delete of an
// expired backlog is orders of magnitude slower.
func prune(app core.App, opts Options) (int, error) {
	collection, err := app.FindCollectionByNameOrId(opts.DeletionsCollection)
	if err != nil {
		return 0, fmt.Errorf("tombstone collection %q not found: %w", opts.DeletionsCollection, err)
	}

	cutoff := types.NowDateTime().Add(-opts.Retention)

	total := 0
	for {
		var deleted int64
		err := app.RunInTransaction(func(txApp core.App) error {
			// The subselect is what bounds the batch: SQLite's DELETE has no
			// LIMIT clause unless it was compiled with SQLITE_ENABLE_UPDATE_DELETE_LIMIT.
			res, err := txApp.DB().NewQuery(fmt.Sprintf(
				"DELETE FROM {{%s}} WHERE [[id]] IN (SELECT [[id]] FROM {{%s}} WHERE [[created]] < {:cutoff} LIMIT {:limit})",
				collection.Name, collection.Name,
			)).Bind(dbx.Params{
				"cutoff": cutoff.String(),
				"limit":  pruneBatchSize,
			}).Execute()
			if err != nil {
				return err
			}

			deleted, err = res.RowsAffected()
			return err
		})
		if err != nil {
			return total, fmt.Errorf("failed to prune %q: %w", opts.DeletionsCollection, err)
		}

		total += int(deleted)
		if deleted < pruneBatchSize {
			return total, nil
		}
	}
}
