/**
 * Dead letters (SPEC §5.6, §12).
 *
 * A 4xx is a permanent rejection of one specific mutation. The engine parks it,
 * surfaces it, and CONTINUES with the next entry — halting the whole queue for
 * one bad write would strand every unrelated write behind it.
 *
 * It is never silently dropped, and the local mirror row is never reverted: the
 * library does not undo a user's data behind their back. What to do about a dead
 * letter is the application's decision.
 */
import type { DeadLetter, PendingEntry, PBOErrorShape, Readable } from '../types.js';
import type { PBODexie } from '../db/schema.js';
import { fromLiveQuery } from '../store/readable.js';
import { refreshState, writeMeta } from '../db/meta.js';

/**
 * Move a rejected entry to `_deadletter`, in one transaction with its removal.
 *
 * The mirror row is deliberately untouched (SPEC §5.6): the user's data stays as
 * the user left it, and the application decides whether to revert it.
 */
export async function park(db: PBODexie, entry: PendingEntry, error: PBOErrorShape): Promise<DeadLetter> {
  return db.transaction('rw', db.pbo.pending, db.pbo.deadletter, db.pbo.meta, async () => {
    await db.pbo.pending.delete(entry.seq);
    const row = {
      collection: entry.collection,
      recordId: entry.recordId,
      op: entry.op,
      payload: entry.payload,
      error,
      at: Date.now(),
    };
    const id = (await db.pbo.deadletter.add(row as DeadLetter)) as number;
    await writeMeta(db, entry.collection, entry.recordId, { state: 'dead', lastError: error });
    return { ...row, id };
  });
}

/** Reactive list, for a UI that surfaces failed writes. Newest last. */
export function liveDeadLetters(getDb: () => PBODexie | undefined, onError?: (err: unknown) => void): Readable<DeadLetter[]> {
  return fromLiveQuery<DeadLetter[]>(
    async () => {
      const db = getDb();
      if (!db) return [];
      return db.pbo.deadletter.orderBy('at').toArray();
    },
    [],
    onError,
  );
}

export async function list(db: PBODexie): Promise<DeadLetter[]> {
  return db.pbo.deadletter.orderBy('at').toArray();
}

/**
 * Re-enqueue a dead letter at the TAIL of `_pending`.
 *
 * Tail, not its original position: the world has moved on, and inserting into
 * the middle of an ordered log would reorder writes that already succeeded.
 * `_pending`'s `++seq` gives the tail for free.
 */
export async function retry(db: PBODexie, id: number): Promise<void> {
  await db.transaction('rw', db.pbo.deadletter, db.pbo.pending, db.pbo.meta, async () => {
    const entry = await db.pbo.deadletter.get(id);
    if (!entry) return;
    await db.pbo.deadletter.delete(id);
    await db.pbo.pending.add({
      collection: entry.collection,
      recordId: entry.recordId,
      op: entry.op,
      payload: entry.payload,
      clientUpdatedAt: Date.now(),
      state: 'queued',
      attempts: 0,
      nextAttemptAt: 0,
      lastError: null,
    } as PendingEntry);
    await writeMeta(db, entry.collection, entry.recordId, { lastError: null });
    await refreshState(db, entry.collection, entry.recordId);
  });
}

/** Drop a dead letter permanently. The mirror row is left as it is. */
export async function discard(db: PBODexie, id: number): Promise<void> {
  await db.transaction('rw', db.pbo.deadletter, db.pbo.meta, db.pbo.pending, async () => {
    const entry = await db.pbo.deadletter.get(id);
    if (!entry) return;
    await db.pbo.deadletter.delete(id);
    await writeMeta(db, entry.collection, entry.recordId, { lastError: null });
    await refreshState(db, entry.collection, entry.recordId);
  });
}
