/**
 * Tombstones (SPEC §8).
 *
 * PocketBase hard-deletes, so a deleted row simply stops appearing in pull
 * pages — indistinguishable from "unchanged". The server helper writes one row
 * per delete into the tombstone collection; the client pulls it with the same
 * cursor mechanism as anything else.
 *
 * Tombstones are pulled LAST in every pass, so a delete cannot be resurrected by
 * a record page fetched afterwards in the same pass.
 */
import type { Table } from 'dexie';
import type { Cursor, PBRecord, PendingEntry } from '../types.js';
import type { PBODexie } from '../db/schema.js';
import { classifySdkError, PBOError } from '../errors.js';
import { clearMeta, pendingFor, refreshState, writeMeta } from '../db/meta.js';
import { advanceCursor, buildPullFilter, PULL_SORT } from '../util/cursor.js';
import { defaultFilter, expandFilter } from '../config.js';
import { isAuthCondition, readCursor, readCursorRow, writeCursor, type CollectionPullStats, type PullDeps } from './pull.js';

export interface Tombstone {
  id: string;
  collection: string;
  record_id: string;
  updated: string;
  [k: string]: unknown;
}

/** 30 days — the documented default of `pbo.Options.Retention` (SPEC §2.6). */
export const DEFAULT_RETENTION_MS = 30 * 24 * 60 * 60 * 1000;

/**
 * True when the tombstone cursor predates the server's retention window, so
 * deletes have provably been missed and a full resync is required (SPEC §8.6).
 *
 * A client that has never pulled tombstones is NOT stale: its null cursor asks
 * for everything the server still holds, which is the whole window.
 */
export function isCursorStale(lastPullAt: number | null, retentionMs: number, now: number): boolean {
  if (lastPullAt === null || retentionMs <= 0) return false;
  return now - lastPullAt > retentionMs;
}

function tableOf(db: PBODexie, collection: string): Table<PBRecord, string> {
  return db.table(collection) as Table<PBRecord, string>;
}

/**
 * Apply one tombstone. Also the path an SSE `delete` action takes (SPEC §9.6).
 *
 * Returns true when it actually removed something locally.
 */
export async function applyTombstone(deps: PullDeps, t: Tombstone): Promise<boolean> {
  const collection = String(t.collection ?? '');
  const recordId = String(t.record_id ?? '');
  if (collection === '' || recordId === '') return false;
  // A tombstone for a collection this client does not mirror is inert.
  if (!deps.cfg.collections.some((c) => c.name === collection)) return false;

  const db = deps.db;
  // A holder rather than two `let`s: TypeScript does not narrow assignments made
  // inside a callback, and a cast at the read site would hide a real mistake.
  const out: { conflicted: PBRecord | null; removed: boolean } = { conflicted: null, removed: false };

  await db.transaction('rw', tableOf(db, collection), db.pbo.pending, db.pbo.meta, db.pbo.deadletter, async () => {
    // SPEC §8.7: a tombstone OLDER than the incarnation this mirror holds is a
    // previous incarnation's. The id was deleted and made again (§8.3), the new
    // row's create was confirmed, and only then did the old delete arrive — on
    // the next pull pass, or late over SSE. Applying it would drop a row the
    // server still holds, past the record cursor, for good.
    const held = await db.pbo.meta.get([collection, recordId]);
    if (typeof t.updated === 'string' && t.updated !== '' && typeof held?.serverUpdated === 'string' && held.serverUpdated > t.updated) {
      return;
    }
    const entries: PendingEntry[] = await pendingFor(db, collection, recordId);
    const queued = entries.filter((e) => e.state === 'queued');
    // SPEC §8.3: a queued create is PRESERVED — id reuse after a delete is legal
    // and the user's create is newer intent than a remote delete of a previous
    // incarnation. Every other queued entry is dead on arrival.
    const keepsCreate = queued.some((e) => e.op === 'create');
    const cancel = queued.filter((e) => e.op !== 'create').map((e) => e.seq);
    if (cancel.length > 0) await db.pbo.pending.bulkDelete(cancel);

    const row = await tableOf(db, collection).get(recordId);
    // SPEC §8.4: a dirty record loses its edit LOUDLY. A delete is not
    // mergeable, and losing it silently is worse than losing an edit.
    if (entries.length > 0) out.conflicted = row ?? ({ id: recordId } as PBRecord);

    if (row) {
      await tableOf(db, collection).delete(recordId);
      out.removed = true;
    }
    if (keepsCreate) {
      // The record's server incarnation is gone; the queued create is not.
      await writeMeta(db, collection, recordId, {
        serverUpdated: null,
        lastPushedUpdated: null,
        lastError: null,
      });
      await refreshState(db, collection, recordId);
    } else {
      const meta = await db.pbo.meta.get([collection, recordId]);
      if (meta) out.removed = true;
      await clearMeta(db, collection, recordId);
    }
  });

  if (out.conflicted !== null) {
    deps.emit('conflict', {
      collection,
      id: recordId,
      local: out.conflicted,
      remote: { id: recordId },
      resolution: 'remote',
    });
  }
  return out.removed;
}

/**
 * Pull and apply tombstones.
 *
 * The tombstone collection is an ORDINARY synced collection for pull purposes
 * (SPEC §8.2): same `(updated,id)` cursor, same paging, its own `_cursors` row.
 * It is never mirrored — a tombstone is applied and discarded.
 */
export async function pullTombstones(deps: PullDeps): Promise<CollectionPullStats> {
  const name = deps.cfg.deletionsCollection;
  const stats: CollectionPullStats = { applied: 0, skipped: 0, deleted: 0, pages: 0 };
  const owner = deps.getOwnerId();
  // An anonymous session has no tombstones to see: the filter would be
  // unsatisfiable and the request pointless.
  if (owner === null) return stats;
  if (deps.halted.has(name)) return { ...stats, halted: true };

  const userFilter = expandFilter(defaultFilter(deps.cfg.ownerField), owner);

  for (;;) {
    if (deps.abort.aborted) break;
    const cursor: Cursor = await readCursor(deps.db, name);
    let items: Tombstone[];
    try {
      const filter = buildPullFilter(userFilter, cursor);
      const options: Record<string, unknown> = { sort: PULL_SORT, skipTotal: true, requestKey: null };
      if (filter !== undefined) options['filter'] = filter;
      const page = (await deps.pb.collection(name).getList(1, deps.cfg.pullPageSize, options)) as {
        items?: Tombstone[];
      };
      items = Array.isArray(page?.items) ? page.items : [];
      deps.observeRequest?.(true, false);
    } catch (err) {
      const e = classifySdkError(err, { phase: 'pull', collection: name });
      const status = Number((e.context?.['status'] as number | undefined) ?? 0);
      deps.observeRequest?.(status > 0, status === 0);
      if (e.code === 'link/offline' || e.code === 'link/timeout') return { ...stats, linkDown: true };
      // SPEC §5.10 — a dead session is a link condition, not a §6.5 halt.
      if (isAuthCondition(deps, status)) {
        deps.onAuthLost?.();
        return { ...stats, linkDown: true };
      }
      if (e.code === 'pull/denied') {
        deps.halted.set(name, e.toShape());
        deps.emit('error', { error: e.toShape() });
        return { ...stats, halted: true };
      }
      deps.emit('error', { error: e.toShape() });
      return { ...stats, resync: true };
    }

    const next = advanceCursor(items);
    // Same termination hazard as a record page, and §2.6.3 warns about exactly
    // this: a tombstone collection created by hand without an `updated` autodate.
    // Nothing is applied from a page that cannot advance the cursor.
    if (items.length > 0 && next === null) {
      const e = new PBOError(
        'pull/malformed',
        `a page of "${name}" ended in a tombstone with no usable "updated" — the deletions collection is missing the autodate field required by SPEC §2.6.3`,
        { context: { collection: name, id: items[items.length - 1]?.id ?? null } },
      );
      deps.emit('error', { error: e.toShape() });
      return { ...stats, resync: true };
    }

    for (const t of items) {
      if (await applyTombstone(deps, t)) stats.deleted += 1;
      stats.applied += 1;
    }

    if (next) {
      const at = (deps.now ?? Date.now)();
      await deps.db.transaction('rw', deps.db.pbo.cursors, async () => {
        await writeCursor(deps.db, name, next, at);
      });
    } else if (items.length === 0) {
      // Nothing to consume, but the pass DID reach the server: record that, or
      // the retention check below would call a healthy client stale (SPEC §8.6).
      const at = (deps.now ?? Date.now)();
      await deps.db.transaction('rw', deps.db.pbo.cursors, async () => {
        await writeCursor(deps.db, name, cursor, at);
      });
    }

    stats.pages += 1;
    if (items.length < deps.cfg.pullPageSize) break;
  }

  const cursor = await readCursor(deps.db, name);
  deps.emit('pull', { collection: name, applied: stats.applied, deleted: stats.deleted, cursor });
  return stats;
}

/** The tombstone cursor row, for the SPEC §11.1 retention check. */
export async function tombstoneCursorAge(db: PBODexie, deletionsCollection: string): Promise<number | null> {
  const row = await readCursorRow(db, deletionsCollection);
  return row?.lastPullAt ?? null;
}
