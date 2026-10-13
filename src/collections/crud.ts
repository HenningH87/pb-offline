/**
 * Local-first writes (SPEC §4.3).
 *
 * Every write applies the mirror row, the `_meta` row and the `_pending` entry in
 * ONE Dexie read-write transaction, and resolves when that transaction commits.
 * Nothing here awaits the network, and nothing here is gated on `online`.
 */
import type { Table } from 'dexie';
import type { PBRecord, BatchOp, WriteOpts, PendingEntry, PendingOp } from '../types.js';
import type { PBODexie } from '../db/schema.js';
import type { CollectionDeps } from './define.js';
import { assertValidId, generateId } from '../ids.js';
import { PBOError } from '../errors.js';
import { clearMeta, pendingFor, writeMeta } from '../db/meta.js';

/** Fields the client never authors: the server owns the sync clock (SPEC §10). */
const SERVER_OWNED = ['created', 'updated'] as const;

export function requireDb(deps: CollectionDeps): PBODexie {
  const db = deps.getDb();
  if (!db) {
    throw new PBOError('internal/invariant', `collection "${deps.collectionCfg.name}" used before start()`, {
      context: { collection: deps.collectionCfg.name },
    });
  }
  return db;
}

/** SPEC §14.3: with `anonymousMode: 'disabled'` and no session, writes throw. */
export function assertWritable(deps: CollectionDeps): void {
  if (deps.cfg.anonymousMode === 'disabled' && deps.getOwnerId() === null) {
    throw new PBOError('auth/unauthenticated', 'a write requires a session (anonymousMode: "disabled")', {
      context: { collection: deps.collectionCfg.name },
    });
  }
}

function table<T extends PBRecord>(db: PBODexie, name: string): Table<T, string> {
  return db.table(name) as Table<T, string>;
}

/** Strip the id and the server-owned clock from a caller-supplied object. */
function sanitise(input: Record<string, unknown>): Record<string, unknown> {
  const out = { ...input };
  delete out['id'];
  for (const f of SERVER_OWNED) delete out[f];
  return out;
}

function newEntry(
  collection: string,
  recordId: string,
  op: PendingOp,
  payload: Record<string, unknown> | null,
  now: number,
): Omit<PendingEntry, 'seq'> {
  return {
    collection,
    recordId,
    op,
    payload,
    clientUpdatedAt: now,
    state: 'queued',
    attempts: 0,
    nextAttemptAt: 0,
    lastError: null,
  };
}

async function enqueue(db: PBODexie, entry: Omit<PendingEntry, 'seq'>): Promise<void> {
  await db.pbo.pending.add(entry as PendingEntry);
}

/**
 * Decide whether a new update may merge into an existing queued entry.
 *
 * Eligible iff: the entry is `queued` (never `inflight` — that payload is on the
 * wire), its op is `create` or `update` (a create never coalesces into a delete
 * and vice versa, SPEC §4.3), and it was last touched inside the window.
 * Merging keeps the original `seq` — cross-record ordering is preserved — and
 * advances `clientUpdatedAt`.
 *
 * This is where the reference implementation's per-entity 800 ms save debounce
 * lives, moved from the write path to the queue so the local write itself stays
 * immediate.
 */
export function canCoalesce(
  entry: { state: string; op: string; clientUpdatedAt: number },
  windowMs: number,
  now: number,
): boolean {
  if (windowMs <= 0) return false; // 0 disables coalescing entirely
  if (entry.state !== 'queued') return false;
  if (entry.op !== 'create' && entry.op !== 'update') return false;
  return now - entry.clientUpdatedAt <= windowMs;
}

// ─────────────────────────── internals (no nudge) ───────────────────────────

async function createIn<T extends PBRecord>(
  deps: CollectionDeps,
  data: Omit<T, 'id' | 'created' | 'updated'> & { id?: string },
): Promise<T> {
  assertWritable(deps);
  const name = deps.collectionCfg.name;
  const supplied = (data as { id?: string }).id;
  // Validation precedes every Dexie touch: a bad id must never reach the queue.
  if (supplied !== undefined) assertValidId(supplied, name, deps.collectionCfg.idFormat);
  const db = requireDb(deps);

  const id = supplied ?? generateId(deps.collectionCfg.idFormat);
  const fields = sanitise(data as Record<string, unknown>);
  const owner = deps.getOwnerId();
  // The session owner is FORCED, not defaulted.
  //
  // Filling the field in only when the caller left it blank made the owner a
  // caller-supplied value, and the ordinary way a "duplicate this record"
  // feature gets written is to spread a record that already carries one. The
  // queued create would then name whatever owner that object held and the only
  // thing standing between it and the server is the consuming app's createRule
  // — a boundary this library prescribes but does not install. A create for
  // somebody else is never what the caller meant, so it is an error and not a
  // silent correction.
  if (owner !== null) {
    const requested = fields[deps.cfg.ownerField];
    if (requested !== undefined && requested !== owner) {
      throw new PBOError(
        'internal/invariant',
        `create() may not assign "${deps.cfg.ownerField}" to another account`,
        { context: { collection: name, requested, owner } },
      );
    }
    fields[deps.cfg.ownerField] = owner;
  }
  const record = { ...fields, id } as T;
  const now = Date.now();

  await db.transaction('rw', table(db, name), db.pbo.pending, db.pbo.meta, async () => {
    await table<T>(db, name).put(record);
    await writeMeta(db, name, id, {
      clientUpdatedAt: now,
      state: 'dirty',
      serverUpdated: null,
      lastPushedUpdated: null,
      lastError: null,
    });
    await enqueue(db, newEntry(name, id, 'create', { ...record }, now));
  });
  return record;
}

async function updateIn<T extends PBRecord>(
  deps: CollectionDeps,
  id: string,
  patch: Partial<T>,
  opts?: WriteOpts,
): Promise<T> {
  assertWritable(deps);
  const db = requireDb(deps);
  const name = deps.collectionCfg.name;
  const fields = sanitise(patch as Record<string, unknown>);
  // Reassigning the owner is a HAND-OFF, not an edit, and there is no legitimate
  // client-side reason to do it: the record leaves this account the moment the
  // patch lands. The server rule prescribed in SPEC §2.6 refuses it too
  // (`@request.body.owner:isset = false`), so a patch carrying this field would
  // otherwise queue locally, apply to the mirror, and dead-letter on the wire —
  // the worst of the three places to find out.
  if (deps.cfg.ownerField in fields) {
    throw new PBOError('internal/invariant', `update() may not change "${deps.cfg.ownerField}"`, {
      context: { collection: name, id, hint: 'record ownership is the server\'s; it is not an editable field' },
    });
  }
  const now = Date.now();
  let merged!: T;

  await db.transaction('rw', table(db, name), db.pbo.pending, db.pbo.meta, async () => {
    const t = table<T>(db, name);
    const existing = await t.get(id);
    if (!existing) {
      // NOT `push/vanished`. SPEC §12 defines that code as a PUSH outcome — a
      // 404 on an update, dead-lettered by the engine — and using it for a
      // synchronous local rejection left an application unable to tell "you gave
      // me an id I do not have" from "the server lost your record", with a
      // `retryable` flag that means nothing in the local case.
      throw new PBOError('internal/invariant', `record "${id}" is not in the local mirror of "${name}"`, {
        context: { collection: name, id, hint: 'update() takes an id that exists locally; create() it first' },
      });
    }
    merged = { ...existing, ...fields, id } as T;
    await t.put(merged);

    if (opts?.transient) {
      // SPEC §4.3: mirror + clientUpdatedAt only. No queue entry, no dirty flag —
      // the next remote version of the record overwrites this silently.
      await writeMeta(db, name, id, { clientUpdatedAt: now });
      return;
    }

    const entries = await pendingFor(db, name, id);
    const last = entries.length > 0 ? entries[entries.length - 1] : undefined;
    if (last && !opts?.noCoalesce && canCoalesce(last, deps.cfg.writeCoalesceMs, now)) {
      const row = await db.pbo.pending.get(last.seq);
      if (row) {
        await db.pbo.pending.put({
          ...row,
          payload: { ...(row.payload ?? {}), ...fields },
          clientUpdatedAt: now,
        });
      }
    } else {
      await enqueue(db, newEntry(name, id, 'update', fields, now));
    }
    await writeMeta(db, name, id, { clientUpdatedAt: now, state: 'dirty', lastError: null });
  });
  return merged;
}

async function removeIn(deps: CollectionDeps, id: string): Promise<void> {
  assertWritable(deps);
  const db = requireDb(deps);
  const name = deps.collectionCfg.name;
  const now = Date.now();

  await db.transaction('rw', table(db, name), db.pbo.pending, db.pbo.meta, async () => {
    const t = table(db, name);
    const entries = await pendingFor(db, name, id);
    const queued = entries.filter((e) => e.state === 'queued');
    const row = await t.get(id);

    // An unpushed create means the server has never heard of this record: the
    // whole queue for it collapses and no request is ever made (SPEC §4.3).
    const unpushedCreate = queued.some((e) => e.op === 'create');
    if (unpushedCreate) {
      await db.pbo.pending.bulkDelete(queued.map((e) => e.seq));
      await t.delete(id);
      await clearMeta(db, name, id);
      return;
    }

    if (!row && queued.length === 0 && entries.length === 0) return; // idempotent

    // Queued updates for a record being deleted are dead on arrival.
    await db.pbo.pending.bulkDelete(queued.filter((e) => e.op !== 'delete').map((e) => e.seq));
    await t.delete(id);
    if (!queued.some((e) => e.op === 'delete')) {
      await enqueue(db, newEntry(name, id, 'delete', null, now));
    }
    await writeMeta(db, name, id, { clientUpdatedAt: now, state: 'dirty', lastError: null });
  });
}

// ─────────────────────────────── public ────────────────────────────────────

/**
 * Create.
 *
 * The id is minted (or a caller-supplied one validated) BEFORE the transaction,
 * and `created`/`updated` are never written locally — those are the server's and
 * stay absent until a push confirms them (SPEC §4.4, §10).
 */
export async function create<T extends PBRecord>(
  deps: CollectionDeps,
  data: Omit<T, 'id' | 'created' | 'updated'> & { id?: string },
): Promise<T> {
  const record = await createIn<T>(deps, data);
  deps.nudge();
  return record;
}

/** Update. Shallow field merge; `transient` writes the mirror and nothing else. */
export async function update<T extends PBRecord>(
  deps: CollectionDeps,
  id: string,
  patch: Partial<T>,
  opts?: WriteOpts,
): Promise<T> {
  const merged = await updateIn<T>(deps, id, patch, opts);
  if (!opts?.transient) deps.nudge();
  return merged;
}

/** Delete, with the queue-collapse rules of SPEC §4.3. */
export async function remove(deps: CollectionDeps, id: string): Promise<void> {
  await removeIn(deps, id);
  deps.nudge();
}

/**
 * Batch. One transaction, contiguous `seq` values, same per-op rules.
 *
 * The per-op helpers open Dexie sub-transactions whose scope is a subset of this
 * one, so Dexie joins them into it — one commit, and no other tab's entries can
 * interleave with this batch's.
 */
export async function batch<T extends PBRecord>(deps: CollectionDeps, ops: BatchOp<T>[]): Promise<void> {
  if (ops.length === 0) return;
  assertWritable(deps);
  const db = requireDb(deps);
  const name = deps.collectionCfg.name;

  await db.transaction('rw', table(db, name), db.pbo.pending, db.pbo.meta, async () => {
    for (const op of ops) {
      if (op.op === 'create') await createIn<T>(deps, op.data);
      else if (op.op === 'update') await updateIn<T>(deps, op.id, op.patch, op.opts);
      else await removeIn(deps, op.id);
    }
  });
  deps.nudge();
}
