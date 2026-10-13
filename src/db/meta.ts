/**
 * `_meta` — per-record sync bookkeeping (SPEC §4.2, §7, §10).
 *
 * Three fields carry the whole conflict story:
 *   serverUpdated     — the last SERVER-minted `updated` seen for this record.
 *                       The only value ever compared for ordering.
 *   lastPushedUpdated — what the server returned for THIS device's last push of
 *                       the record. Echo suppression compares against it.
 *   clientUpdatedAt   — a client wall clock, for ordering the local queue and for
 *                       diagnostics. NEVER an input to a conflict decision.
 */
import type { MetaRow, PendingEntry, RecordMeta, RecordSyncState } from '../types.js';
import type { PBODexie } from './schema.js';

export function emptyMeta(collection: string, recordId: string): MetaRow {
  return {
    collection,
    recordId,
    serverUpdated: null,
    clientUpdatedAt: 0,
    lastPushedUpdated: null,
    state: 'synced',
    lastError: null,
  };
}

export async function readMeta(
  db: PBODexie,
  collection: string,
  recordId: string,
): Promise<MetaRow | undefined> {
  return db.pbo.meta.get([collection, recordId]);
}

/**
 * Upsert meta fields.
 *
 * MUST be called inside the caller's transaction — it never opens one of its
 * own, because every meta write is part of a larger atomic step (SPEC §4.3,
 * §5.5). Dexie joins the ambient transaction automatically.
 */
export async function writeMeta(
  db: PBODexie,
  collection: string,
  recordId: string,
  patch: Partial<Omit<MetaRow, 'collection' | 'recordId'>>,
): Promise<void> {
  const existing = (await db.pbo.meta.get([collection, recordId])) ?? emptyMeta(collection, recordId);
  await db.pbo.meta.put({ ...existing, ...patch, collection, recordId });
}

/** Queued + inflight entries for one record, in `seq` order. */
export async function pendingFor(
  db: PBODexie,
  collection: string,
  recordId: string,
): Promise<PendingEntry[]> {
  const rows = await db.pbo.pending.where('[collection+recordId]').equals([collection, recordId]).toArray();
  return rows.sort((a, b) => a.seq - b.seq);
}

async function deadLetterCount(db: PBODexie, collection: string, recordId: string): Promise<number> {
  return db.pbo.deadletter
    .where('recordId')
    .equals(recordId)
    .filter((d) => d.collection === collection)
    .count();
}

/**
 * Derive the record's sync state from the queue and the meta row.
 *
 *   dead       — a dead-letter entry exists for the record
 *   conflicted — a conflict was recorded and not yet resolved
 *   pending    — a `_pending` entry exists and is inflight
 *   dirty      — a `_pending` entry exists and is queued
 *   synced     — otherwise
 */
export async function deriveState(
  db: PBODexie,
  collection: string,
  recordId: string,
): Promise<RecordSyncState> {
  if ((await deadLetterCount(db, collection, recordId)) > 0) return 'dead';
  const row = await readMeta(db, collection, recordId);
  if (row?.state === 'conflicted') return 'conflicted';
  const entries = await pendingFor(db, collection, recordId);
  if (entries.some((e) => e.state === 'inflight')) return 'pending';
  if (entries.length > 0) return 'dirty';
  return 'synced';
}

/** The public projection of a meta row. */
export function toPublicMeta(row: MetaRow, pendingOps: number): RecordMeta {
  return {
    id: row.recordId,
    state: row.state,
    serverUpdated: row.serverUpdated,
    clientUpdatedAt: row.clientUpdatedAt,
    pendingOps,
    lastError: row.lastError,
  };
}

/** Recompute and persist the derived state. Caller supplies the transaction. */
export async function refreshState(db: PBODexie, collection: string, recordId: string): Promise<RecordSyncState> {
  const state = await deriveState(db, collection, recordId);
  const existing = await readMeta(db, collection, recordId);
  if (existing && existing.state !== state) {
    await db.pbo.meta.put({ ...existing, state });
  }
  return state;
}

/** Drop meta for a record. Used on tombstone apply and on delete success. */
export async function clearMeta(db: PBODexie, collection: string, recordId: string): Promise<void> {
  await db.pbo.meta.delete([collection, recordId]);
}
