/**
 * Push — draining `_pending` (SPEC §5).
 *
 * Only the drainer tab runs this. Strictly ascending `seq`, one request at a
 * time, no parallelism: the queue is a log of user intent and reordering it can
 * violate referential integrity the application relies on.
 */
import type PocketBase from 'pocketbase';
import type { Table } from 'dexie';
import type { PBOEvents, PBRecord, PendingEntry, PushResult, ResolvedConfig } from '../types.js';
import type { PBODexie } from '../db/schema.js';
import type { Backoff } from '../net/backoff.js';
import { PBOError, classifySdkError, isIdTakenError as sdkIdTaken } from '../errors.js';
import { clearMeta, pendingFor, refreshState, writeMeta } from '../db/meta.js';
import { park } from './deadletter.js';

export interface PushDeps {
  pb: PocketBase;
  db: PBODexie;
  cfg: ResolvedConfig;
  isDrainer: () => boolean;
  isOnline: () => boolean;
  abort: AbortSignal;
  emit: <E extends keyof PBOEvents>(event: E, payload: PBOEvents[E]) => void;
  /** Feed the link watcher — it, and only it, decides the link is down (§18.1). */
  observeRequest?: (ok: boolean, networkFailure: boolean) => void;
  /** SPEC §5.10: is the token still one the server would accept? */
  isAuthValid?: () => boolean;
  /** SPEC §5.10: the session died; the engine emits `offline` reason 'auth'. */
  onAuthLost?: () => void;
  backoff?: Backoff;
  now?: () => number;
}

/** True when a 400 body identifies the failure as a taken `id` (SPEC §5.7). */
export const isIdTakenError = sdkIdTaken;

function tableOf(db: PBODexie, collection: string): Table<PBRecord, string> {
  return db.table(collection) as Table<PBRecord, string>;
}

/**
 * Convert a create entry into an update in place (SPEC §5.7). `state` is what the
 * converted entry is left as: `queued` after a refused create, `inflight` when
 * the conversion happens on the way to the wire (§5.12's probe), so nothing can
 * coalesce into an entry that is about to be sent.
 */
export async function convertCreateToUpdate(
  db: PBODexie,
  entry: PendingEntry,
  ownerField?: string,
  state: PendingEntry['state'] = 'queued',
): Promise<void> {
  await db.transaction('rw', db.pbo.pending, async () => {
    const row = await db.pbo.pending.get(entry.seq);
    if (!row) return;
    const payload = { ...(row.payload ?? {}) };
    // The id identifies the record in the URL now, not in the body — and PB
    // rejects an `id` in an update body.
    delete payload['id'];
    // So does the owner, under the rule set SPEC §2.6 prescribes: an update
    // body that so much as MENTIONS the owner field is refused
    // (`@request.body.owner:isset = false`), because that is the only way to
    // stop a record being handed to another account. A create payload always
    // carries the owner, so the conversion has to drop it — the record already
    // exists and its owner is not changing.
    if (ownerField !== undefined) delete payload[ownerField];
    await db.pbo.pending.put({
      ...row,
      op: 'update',
      payload,
      state,
      attempts: row.attempts + 1,
      nextAttemptAt: 0,
    });
  });
}

/** Perform the request for one entry (SPEC §5.3). */
async function request(deps: PushDeps, entry: PendingEntry): Promise<unknown> {
  const c = deps.pb.collection(entry.collection);
  if (entry.op === 'create') return c.create(entry.payload ?? {});
  if (entry.op === 'update') return c.update(entry.recordId, entry.payload ?? {});
  return c.delete(entry.recordId);
}

/**
 * SPEC §5.12 — did an earlier attempt of this create already land?
 *
 * A create is AMBIGUOUS when a request for it may have reached the server and no
 * answer was recorded: the entry was found `inflight` (the tab that sent it died
 * or navigated away mid-request) or an earlier attempt failed (a lost response,
 * a timeout, a 5xx). Re-sending it blind relies on the server answering the
 * taken id with §5.7's 400 — and a create rule that refuses the SHAPE first (an
 * app's "one of these per account" hook, which counts the record the lost
 * request made) answers 403 instead, which reads as a rules rejection.
 *
 * So the entry asks first. A list read filtered on the id, because it answers
 * 200 with zero or one rows either way (a `getOne` miss would be a 404 in every
 * console). `exists` → the create is converted to an update before it is sent;
 * `absent` → the create goes out as it is; `link` → the link failed, the drain
 * stops as for any request; `unknown` (the read was refused) → the create goes
 * out as before this check existed and §5.7 still applies.
 */
type Probe = { k: 'exists' } | { k: 'absent' } | { k: 'unknown' } | { k: 'link'; error: PBOError };

async function probeCreate(deps: PushDeps, entry: PendingEntry): Promise<Probe> {
  const id = entry.recordId;
  const filter =
    typeof deps.pb.filter === 'function'
      ? deps.pb.filter('id = {:id}', { id })
      : `id = "${id.replace(/["\\]/g, '\\$&')}"`;
  try {
    const page = (await deps.pb
      .collection(entry.collection)
      .getList(1, 1, { filter, skipTotal: true, requestKey: null })) as { items?: PBRecord[] };
    deps.observeRequest?.(true, false);
    const items = Array.isArray(page?.items) ? page.items : [];
    return items.some((row) => row?.id === id) ? { k: 'exists' } : { k: 'absent' };
  } catch (err) {
    const e = classifySdkError(err, { phase: 'push', collection: entry.collection });
    const status = Number((e.context?.['status'] as number | undefined) ?? 0);
    deps.observeRequest?.(status > 0, status === 0);
    if (status === 0 || status === 429 || status >= 500 || e.code === 'link/timeout') return { k: 'link', error: e };
    return { k: 'unknown' };
  }
}

/**
 * SPEC §5.5 — the success transaction.
 *
 * The subtle clause is "only if the record is not dirty from a LATER queue
 * entry": a newer `_pending` entry for the same record means the user has moved
 * on, so the server's authored fields merge in but the queued local fields win.
 */
async function applySuccess(
  db: PBODexie,
  entry: PendingEntry,
  response: unknown,
): Promise<{ updated: string | null }> {
  const record = (response !== null && typeof response === 'object' ? response : {}) as PBRecord;
  const updated = typeof record.updated === 'string' ? record.updated : null;
  const c = entry.collection;
  const id = entry.recordId;

  await db.transaction('rw', tableOf(db, c), db.pbo.pending, db.pbo.meta, db.pbo.deadletter, async () => {
    await db.pbo.pending.delete(entry.seq);

    const later = await pendingFor(db, c, id);

    if (entry.op === 'delete') {
      // §5.5's "not dirty from a LATER queue entry" applies to a delete too, and
      // id reuse after a delete is explicitly legal (§8.3). If the user recreated
      // the record while the delete was on the wire, the mirror row and the meta
      // row belong to the QUEUED CREATE, not to the delete that just succeeded —
      // removing them here makes the record vanish from every `live()` query
      // while the only copy of its data sits in `_pending`, and it stays vanished
      // if the link drops before that create drains.
      const recreated = later.some((e) => e.op === 'create');
      if (!recreated) {
        await tableOf(db, c).delete(id);
        await clearMeta(db, c, id);
        return;
      }
      // The record's server incarnation is gone; the queued create is not. Same
      // bookkeeping as the §8.3 tombstone branch.
      await writeMeta(db, c, id, { serverUpdated: null, lastPushedUpdated: null, lastError: null });
      await refreshState(db, c, id);
      return;
    }

    const supersededByDelete = later.some((e) => e.op === 'delete');
    if (!supersededByDelete) {
      const local = await tableOf(db, c).get(id);
      const queued: Record<string, unknown> = {};
      for (const e of later) Object.assign(queued, e.payload ?? {});
      const row: PBRecord =
        later.length === 0
          ? { ...record, id }
          : { ...(local ?? {}), ...record, ...queued, id };
      await tableOf(db, c).put(row);
    }

    // SPEC §7.1: serverUpdated AND lastPushedUpdated, in the same transaction.
    await writeMeta(db, c, id, {
      serverUpdated: updated,
      lastPushedUpdated: updated,
      lastError: null,
    });
    await refreshState(db, c, id);
  });

  return { updated };
}

/** Leave the entry queued and schedule its retry (SPEC §5.9). */
async function requeue(db: PBODexie, entry: PendingEntry, delayMs: number, error: PBOError, now: number): Promise<void> {
  await db.transaction('rw', db.pbo.pending, db.pbo.meta, db.pbo.deadletter, async () => {
    const row = await db.pbo.pending.get(entry.seq);
    if (!row) return;
    await db.pbo.pending.put({
      ...row,
      state: 'queued',
      attempts: row.attempts + 1,
      nextAttemptAt: now + delayMs,
      lastError: error.toShape(),
    });
    await writeMeta(db, entry.collection, entry.recordId, { lastError: error.toShape() });
    await refreshState(db, entry.collection, entry.recordId);
  });
}

/** SPEC §5, invariant 2 — `inflight` is cleared on EVERY exit path. */
async function clearInflight(db: PBODexie, seq: number): Promise<void> {
  await db.transaction('rw', db.pbo.pending, async () => {
    const row = await db.pbo.pending.get(seq);
    if (!row || row.state !== 'inflight') return;
    await db.pbo.pending.put({ ...row, state: 'queued' });
  });
}

type StepResult =
  | { k: 'ok'; updated: string | null }
  | { k: 'dead' }
  | { k: 'retry-entry' }
  | { k: 'link'; error: PBOError }
  | { k: 'auth' };

/**
 * Drain until empty, dead-lettered, or stopped.
 *
 * Re-entrancy (invariant 11) is the engine's: it caches the in-flight promise
 * and hands it back to a concurrent `push()`. This function assumes it is alone.
 */
export async function drain(deps: PushDeps): Promise<PushResult> {
  const { db } = deps;
  const now = (): number => (deps.now ?? Date.now)();
  let pushed = 0;
  let failed = 0;
  let deadLettered = 0;
  /** Entries already converted create→update, so §5.7's single retry is single. */
  const converted = new Set<number>();

  const finish = async (stoppedBy: PushResult['stoppedBy']): Promise<PushResult> => ({
    pushed,
    failed,
    deadLettered,
    remaining: await db.pbo.pending.count(),
    stoppedBy,
  });

  for (;;) {
    // SPEC §3.4 / §5.9: a link that went down cancels the drain at the ENTRY
    // boundary — never mid-request, whose result still has to be recorded.
    if (deps.abort.aborted || !deps.isDrainer()) return finish('stopped');
    if (!deps.isOnline()) return finish('link');

    const head = await db.pbo.pending.orderBy('seq').first();
    if (!head) return finish('empty');
    // Strict order: a head that is waiting out its backoff stops the drain. The
    // entries behind it MUST NOT be attempted — they may depend on it.
    if (head.nextAttemptAt > now()) return finish('link');

    // Invariant 2: re-read immediately before the request (another tab may have
    // coalesced into it) and mark it inflight.
    // An entry already `inflight` here was on the wire when its sender stopped
    // without recording an answer (§5.12).
    let wasInflight = false;
    const entry = await db.transaction('rw', db.pbo.pending, async (): Promise<PendingEntry | undefined> => {
      const fresh = await db.pbo.pending.get(head.seq);
      if (!fresh) return undefined;
      wasInflight = fresh.state === 'inflight';
      const marked: PendingEntry = { ...fresh, state: 'inflight' };
      await db.pbo.pending.put(marked);
      return marked;
    });
    if (!entry) continue;

    let step: StepResult;
    try {
      step = await attempt(deps, entry, converted, now(), wasInflight);
    } catch (err) {
      // A fault in the engine itself, not in the request. Never lose the entry.
      await clearInflight(db, entry.seq);
      throw err;
    }
    await clearInflight(db, entry.seq);

    if (step.k === 'ok') {
      pushed += 1;
      deps.emit('push', {
        collection: entry.collection,
        op: entry.op,
        id: entry.recordId,
        ok: true,
        attempt: entry.attempts + 1,
      });
      if (step.updated !== null && entry.op !== 'delete') {
        deps.emit('record-synced', { collection: entry.collection, id: entry.recordId, updated: step.updated });
      }
      continue;
    }

    failed += 1;
    if (step.k === 'retry-entry') continue;
    if (step.k === 'dead') {
      deadLettered += 1;
      continue;
    }
    if (step.k === 'auth') return finish('auth');
    return finish('link');
  }
}

/**
 * One request and its consequences.
 *
 * Every branch here is one of SPEC §5's numbered invariants; the numbers are in
 * the comments so a reader can check them off against the spec.
 */
async function attempt(
  deps: PushDeps,
  sent: PendingEntry,
  converted: Set<number>,
  now: number,
  wasInflight = false,
): Promise<StepResult> {
  const { db } = deps;
  let entry = sent;
  const emitFailure = (): void =>
    deps.emit('push', {
      collection: entry.collection,
      op: entry.op,
      id: entry.recordId,
      ok: false,
      attempt: entry.attempts + 1,
    });

  // 12 · an ambiguous create asks whether it already landed before it is re-sent.
  if (entry.op === 'create' && !converted.has(entry.seq) && (wasInflight || entry.attempts > 0)) {
    const probe = await probeCreate(deps, entry);
    if (probe.k === 'link') {
      // 9's handling: the link failed, the entry stays queued, the drain stops.
      await requeue(db, entry, deps.backoff?.next() ?? deps.cfg.backoff.min, probe.error, now);
      emitFailure();
      return { k: 'link', error: probe.error };
    }
    if (probe.k === 'exists') {
      converted.add(entry.seq);
      await convertCreateToUpdate(db, entry, deps.cfg.ownerField, 'inflight');
      const row = await db.pbo.pending.get(entry.seq);
      if (!row) return { k: 'retry-entry' };
      entry = row;
    }
  }

  let response: unknown;
  try {
    response = await request(deps, entry);
  } catch (err) {
    const e = classifySdkError(err, { phase: 'push', collection: entry.collection });
    const status = Number((e.context?.['status'] as number | undefined) ?? 0);
    deps.observeRequest?.(status > 0, status === 0);

    // 9 · 5xx / 429 / network / timeout ⇒ stop the drain, keep the entry.
    //
    // DEVIATION from SPEC §5.9: a 429 does NOT honour `Retry-After`, because the
    // SDK exposes no response headers — `ClientResponseError.response` is the
    // parsed JSON body and the `Response` never leaves the SDK. The jittered
    // ladder is the wait for every stop-the-drain condition alike.
    if (status === 0 || status === 429 || status >= 500 || e.code === 'link/timeout') {
      const delay = deps.backoff?.next() ?? deps.cfg.backoff.min;
      await requeue(db, entry, delay, e, now);
      emitFailure();
      return { k: 'link', error: e };
    }

    // 10 · 401/403 — a dead session is a link condition; a live one is rules.
    if (status === 401 || status === 403) {
      const valid = deps.isAuthValid?.() ?? true;
      if (!valid) {
        await requeue(db, entry, 0, e, now);
        emitFailure();
        deps.onAuthLost?.();
        return { k: 'auth' };
      }
      return deadLetter(deps, entry, new PBOError('push/rejected', e.message, { cause: err, context: e.context }), emitFailure);
    }

    // 7 · a create whose id is already taken becomes an update, once.
    if (entry.op === 'create' && isIdTakenError(err)) {
      if (!converted.has(entry.seq)) {
        converted.add(entry.seq);
        await convertCreateToUpdate(db, entry, deps.cfg.ownerField);
        emitFailure();
        return { k: 'retry-entry' };
      }
      return deadLetter(deps, entry, new PBOError('push/conflict', e.message, { cause: err, context: e.context }), emitFailure);
    }
    if (entry.op === 'update' && converted.has(entry.seq) && status === 400) {
      // The §5.7 retry itself failed with a plain 400.
      return deadLetter(deps, entry, new PBOError('push/conflict', e.message, { cause: err, context: e.context }), emitFailure);
    }

    // 8 · 404 — the record is already gone server-side.
    if (status === 404) {
      if (entry.op === 'delete') {
        const { updated } = await applySuccess(db, entry, null);
        return { k: 'ok', updated };
      }
      return deadLetter(deps, entry, new PBOError('push/vanished', e.message, { cause: err, context: e.context }), emitFailure);
    }

    // 6 · every other 4xx is a permanent rejection of THIS mutation.
    return deadLetter(deps, entry, e.code === 'push/rejected' ? e : new PBOError('push/rejected', e.message, { cause: err, context: e.context }), emitFailure);
  }

  deps.observeRequest?.(true, false);
  const { updated } = await applySuccess(db, entry, response);
  return { k: 'ok', updated };
}

async function deadLetter(
  deps: PushDeps,
  entry: PendingEntry,
  error: PBOError,
  emitFailure: () => void,
): Promise<StepResult> {
  const shape = error.toShape();
  const parked = await park(deps.db, entry, shape);
  emitFailure();
  deps.emit('dead-letter', { entry: parked });
  deps.emit('error', { error: shape });
  return { k: 'dead' };
}
