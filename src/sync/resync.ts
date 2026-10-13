/**
 * Full resync (SPEC §11) — the escape hatch that bounds the worst case of any
 * bug in the sync mechanics.
 *
 * Automatic triggers: a tombstone cursor older than the server's retention
 * window; `staleAfterMs` elapsed since the last successful pull; a cursor the
 * server rejects as malformed. Manual: `fullResync()`.
 */
import type { Table } from 'dexie';
import type { DeadLetter, PBRecord, PendingEntry } from '../types.js';
import { toPBOError } from '../errors.js';
import { writeMeta } from '../db/meta.js';
import { clearCursors, pull, readCursorRow, type PullDeps } from './pull.js';
import { DEFAULT_RETENTION_MS, isCursorStale } from './tombstones.js';

export type ResyncReason =
  | 'cursor-stale'
  | 'stale-after'
  | 'malformed-cursor'
  | 'manual'
  | string;

function tableOf(deps: PullDeps, collection: string): Table<PBRecord, string> {
  return deps.db.table(collection) as Table<PBRecord, string>;
}

/**
 * Check the automatic triggers. Runs before every pull pass (SPEC §11).
 *
 * The retention window is taken from `staleAfterMs` when the application
 * configured one, and otherwise from the helper's documented 30-day default.
 */
export async function shouldResync(deps: PullDeps): Promise<ResyncReason | null> {
  const now = (deps.now ?? Date.now)();
  const retention = deps.cfg.staleAfterMs > 0 ? deps.cfg.staleAfterMs : DEFAULT_RETENTION_MS;

  const tombstones = await readCursorRow(deps.db, deps.cfg.deletionsCollection);
  if (isCursorStale(tombstones?.lastPullAt ?? null, retention, now)) return 'cursor-stale';

  if (deps.cfg.staleAfterMs > 0) {
    let newest: number | null = null;
    for (const c of deps.cfg.collections) {
      const row = await readCursorRow(deps.db, c.name);
      if (row?.lastPullAt != null && (newest === null || row.lastPullAt > newest)) newest = row.lastPullAt;
    }
    if (newest !== null && now - newest > deps.cfg.staleAfterMs) return 'stale-after';
  }
  return null;
}

/**
 * Re-apply what the user did that the server does not hold (SPEC §11.5): every
 * dead letter, then every surviving `_pending` entry.
 *
 * So a `live()` subscriber does not watch the user's unsent work vanish, and so
 * the drain that follows pushes a mirror that agrees with the queue. A DEAD
 * LETTER is re-applied too, and keeps `state: 'dead'` with its error (§5.6
 * invariant 13): the 4xx left the row as the user left it, and a resync must not
 * be the thing that drops it — a refused create exists nowhere but here. Dead
 * letters go first (they are older), so a later queued entry for the same record
 * wins.
 */
export async function reapplyPending(deps: PullDeps): Promise<number> {
  const mirrored = new Set(deps.cfg.collections.map((c) => c.name));
  const dead: DeadLetter[] = (await deps.db.pbo.deadletter.orderBy('at').toArray()).filter((d) =>
    mirrored.has(d.collection),
  );
  const entries: PendingEntry[] = await deps.db.pbo.pending.orderBy('seq').toArray();
  if (entries.length === 0 && dead.length === 0) return 0;
  const names = new Set([...entries, ...dead].map((e) => e.collection));
  const scope = [
    ...[...names].map((n) => deps.db.table(n) as unknown as Table<unknown, unknown>),
    deps.db.pbo.meta as unknown as Table<unknown, unknown>,
    deps.db.pbo.pending as unknown as Table<unknown, unknown>,
    deps.db.pbo.deadletter as unknown as Table<unknown, unknown>,
  ];

  await deps.db.transaction('rw', scope, async () => {
    for (const d of dead) {
      const t = tableOf(deps, d.collection);
      if (d.op === 'delete') {
        await t.delete(d.recordId);
        continue;
      }
      const existing = await t.get(d.recordId);
      // An update's payload is the changed fields only: over a row the server
      // no longer has it would be a stub, not the user's record.
      if (d.op === 'update' && !existing) continue;
      await t.put({ ...(existing ?? {}), ...(d.payload ?? {}), id: d.recordId } as PBRecord);
      await writeMeta(deps.db, d.collection, d.recordId, {
        clientUpdatedAt: d.at,
        state: 'dead',
        lastError: d.error,
      });
    }
    for (const e of entries) {
      const t = tableOf(deps, e.collection);
      if (e.op === 'delete') {
        await t.delete(e.recordId);
        continue;
      }
      const existing = await t.get(e.recordId);
      await t.put({ ...(existing ?? {}), ...(e.payload ?? {}), id: e.recordId } as PBRecord);
      await writeMeta(deps.db, e.collection, e.recordId, {
        clientUpdatedAt: e.clientUpdatedAt,
        state: 'dirty',
      });
    }
  });
  return entries.length + dead.length;
}

/**
 * Wipe the mirror and refetch.
 *
 * The queue is PRESERVED throughout: local intent is not what is being resynced.
 * Step 3's transaction clears the collection tables, `_cursors` and `_meta` and
 * NOTHING else — the tempting bug is `db.tables.forEach(t => t.clear())`, and
 * TESTPLAN §1.10 asserts against it explicitly.
 *
 * Returns whether the procedure reached `{phase:'done'}`. The caller needs the
 * answer: a resync that emitted `{phase:'failed'}` must not stamp `lastSyncedAt`,
 * or a UI renders "last synced 2 seconds ago" over a mirror that never refilled.
 */
export async function fullResync(deps: PullDeps, reason: ResyncReason): Promise<boolean> {
  deps.emit('full-resync', { phase: 'start', reason: String(reason) });
  try {
    // 2 · drain first if the link is up. If it cannot drain, the queue survives.
    if (deps.drain && (deps.isOnline?.() ?? false)) {
      await deps.drain();
    }

    // 3 · one transaction: collection tables + _cursors + _meta.
    const scope = [
      ...deps.cfg.collections.map((c) => deps.db.table(c.name) as unknown as Table<unknown, unknown>),
      deps.db.pbo.cursors as unknown as Table<unknown, unknown>,
      deps.db.pbo.meta as unknown as Table<unknown, unknown>,
    ];
    await deps.db.transaction('rw', scope, async () => {
      for (const c of deps.cfg.collections) await deps.db.table(c.name).clear();
      await clearCursors(deps.db);
      await deps.db.pbo.meta.clear();
    });

    // 4 · pull everything from a null cursor, tombstones last. Tombstones from
    //     before the wipe are harmless no-ops against an empty mirror.
    await pull(deps);

    // 5 · re-apply what is still queued, then drain it.
    await reapplyPending(deps);
    if (deps.drain && (deps.isOnline?.() ?? false)) {
      await deps.drain();
    }

    deps.emit('full-resync', { phase: 'done', reason: String(reason) });
    return true;
  } catch (err) {
    // The mirror is left empty-but-consistent; the next reconnect retries.
    deps.emit('full-resync', { phase: 'failed', reason: String(reason) });
    deps.emit('error', { error: toPBOError(err).toShape() });
    return false;
  }
}
