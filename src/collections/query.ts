/**
 * Reactive reads over the mirror (SPEC §2.3, §2.5).
 *
 * Everything here is Dexie's `liveQuery` wrapped in the Svelte store contract.
 * `liveQuery` observes IndexedDB writes CROSS-TAB, which is what makes a pull
 * applied by the drainer tab re-render every other tab with no extra machinery.
 *
 * The library's headline claim is that a subscription re-emits on ANY source of
 * change: local write, queue-drain reconciliation, cursor pull, SSE event,
 * tombstone delete. All five write to Dexie, so all five are covered by one
 * mechanism — do not add a second notification path.
 */
import type { IndexableType, Table } from 'dexie';
import type { PBRecord, LocalQuery, Readable, RecordMeta, RecordSyncState } from '../types.js';
import type { PBODexie } from '../db/schema.js';
import { fromLiveQuery, type Revision } from '../store/readable.js';
import { deriveState, pendingFor, readMeta, toPublicMeta } from '../db/meta.js';

type DbGetter = () => PBODexie | undefined;

/**
 * Where a failed reactive read goes. A live query that throws (quota, a closed
 * database, a caller's own `liveRaw` function) must not vanish silently — SPEC
 * §12 requires every surfaced fault to carry a code, and the store contract has
 * no error channel of its own.
 */
export type ErrorSink = ((err: unknown) => void) | undefined;

/**
 * The mirror's identity signal (SPEC §2.3, added v0.1 after first-consumer
 * feedback). Every reactive read here resolves `db()` at query time, so a query
 * that ran before `start()` opened the database read NO table and Dexie recorded
 * an empty observability set for it — permanently dead. Handing the revision to
 * `fromLiveQuery` is what re-runs it when a mirror opens, closes or is swapped
 * for another owner's. Optional so an internal caller with a fixed, already-open
 * database (`db/state.ts`, `index.ts`'s attach) can leave it out.
 */
export type LiveRevision = Revision | undefined;

function tableOf<T extends PBRecord>(db: PBODexie, collection: string): Table<T, string> {
  return db.table(collection) as Table<T, string>;
}

/** Values IndexedDB can key on. Anything else forces a scan. */
function isIndexable(v: unknown): v is IndexableType {
  return (
    typeof v === 'string' ||
    typeof v === 'number' ||
    v instanceof Date ||
    v instanceof ArrayBuffer ||
    ArrayBuffer.isView(v) ||
    Array.isArray(v)
  );
}

function isIndexed<T extends PBRecord>(table: Table<T, string>, field: string): boolean {
  const schema = table.schema;
  if (schema.primKey.keyPath === field) return true;
  return schema.indexes.some((i) => i.keyPath === field);
}

function compare(a: unknown, b: unknown): number {
  if (a === b) return 0;
  if (a === undefined || a === null) return -1;
  if (b === undefined || b === null) return 1;
  if (typeof a === 'number' && typeof b === 'number') return a - b;
  return String(a) < String(b) ? -1 : String(a) > String(b) ? 1 : 0;
}

// ── the local ordering key (SPEC §2.3, `sort.localFallback`) ────────────────
//
// The problem it solves: the sync clock is the SERVER's. A record created
// offline has no `created` and no `updated` until a push comes back, so a list
// sorted on either scatters the app's own newest writes — which is exactly when
// the user is looking at them. The engine will not fake a server stamp (§10
// depends on `created`/`updated` being server-minted and nothing else), so the
// substitute is the one clock the client already keeps honestly: `_meta.
// clientUpdatedAt`, the wall-clock ms of the last local mutation.

/** Absent for sorting purposes. `0` and `false` are values, not absences. */
function isEmptyKey(v: unknown): boolean {
  return v === undefined || v === null || v === '';
}

/**
 * `YYYY-MM-DD`, optionally with a time part. Matches both PocketBase's own
 * `2026-09-05 07:14:56.123Z` and ISO-8601's `2026-09-05T07:14:56.123Z`.
 */
const DATE_SHAPED = /^\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?)?$/;

/** Milliseconds for a date-shaped string, or NaN. PB's space separator is normalised. */
function parseDateish(v: string): number {
  return Date.parse(v.replace(' ', 'T'));
}

function isDateShaped(v: unknown): v is string {
  return typeof v === 'string' && DATE_SHAPED.test(v) && Number.isFinite(parseDateish(v));
}

/** The Dexie instance behind a table, when it is one of ours. */
function pboOf<T extends PBRecord>(table: Table<T, string>): PBODexie | undefined {
  const db = (table as unknown as { db?: unknown }).db as PBODexie | undefined;
  return db && (db as { pbo?: unknown }).pbo !== undefined ? db : undefined;
}

/**
 * `recordId → clientUpdatedAt` for one collection.
 *
 * Read inside the caller's `liveQuery` function on purpose: that makes `_meta`
 * part of the query's observed set, so a write that moves a record's
 * `clientUpdatedAt` re-emits the list even in the (theoretical) case where the
 * record row itself did not change. Only ever called when a fallback is asked
 * for, so no existing query pays for it.
 */
async function clientStamps<T extends PBRecord>(table: Table<T, string>): Promise<Map<string, number>> {
  const db = pboOf(table);
  if (!db) return new Map();
  const rows = await db.pbo.meta.where('collection').equals(table.name).toArray();
  return new Map(rows.map((r) => [r.recordId, r.clientUpdatedAt]));
}

/** Ids break every tie, so a fallback list is fully deterministic. */
function byId(a: PBRecord, b: PBRecord): number {
  return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;
}

/**
 * Sort with `localFallback: 'clientUpdatedAt'`. Semantics are normative — SPEC §2.3.
 *
 *  1. A row's key is `record[field]`, unless that is `undefined`, `null` or `''`
 *     — a FALLBACK ROW, whose key is `_meta.clientUpdatedAt` (0 when the record
 *     has no meta row at all).
 *  2. If every row that HAS a value carries a date-shaped string, the whole sort
 *     runs in the millisecond domain: those values are `Date.parse`d and the
 *     fallback rows use their `clientUpdatedAt` as-is. Fallback rows therefore
 *     INTERLEAVE with mirrored rows in true time order — an offline edit made
 *     ten minutes ago sits ten minutes back in a `created` list, not at an end.
 *  3. Otherwise the two groups are not comparable (a title against a timestamp
 *     is nothing), so they are kept apart: valued rows sort among themselves by
 *     the field, fallback rows among themselves by `clientUpdatedAt`, and the
 *     fallback group goes LAST in `asc` — which `dir: 'desc'` reverses to first,
 *     the "newest first, my unsynced writes on top" list an app actually builds.
 *  4. `dir` reverses the comparison as a whole, groups included.
 *  5. Equal keys break by `id`, ascending, in both directions.
 */
async function sortWithFallback<T extends PBRecord>(
  table: Table<T, string>,
  rows: T[],
  sort: NonNullable<LocalQuery<T>['sort']>,
): Promise<T[]> {
  const dir = sort.dir === 'desc' ? -1 : 1;
  const field = sort.field;
  const stamps = await clientStamps(table);
  const raw = (r: T): unknown => (r as Record<string, unknown>)[field];
  const stampOf = (r: T): number => stamps.get(r.id) ?? 0;

  const valued = rows.filter((r) => !isEmptyKey(raw(r)));

  // One domain, one comparison — the interleaving case (rule 2).
  if (valued.length > 0 && valued.every((r) => isDateShaped(raw(r)))) {
    const key = (r: T): number => (isEmptyKey(raw(r)) ? stampOf(r) : parseDateish(raw(r) as string));
    return [...rows].sort((a, b) => dir * (key(a) - key(b)) || byId(a, b));
  }

  // Two domains, kept apart (rule 3).
  const group = (r: T): number => (isEmptyKey(raw(r)) ? 1 : 0);
  return [...rows].sort((a, b) => {
    const g = group(a) - group(b);
    if (g !== 0) return dir * g;
    if (group(a) === 1) return dir * (stampOf(a) - stampOf(b)) || byId(a, b);
    return dir * compare(raw(a), raw(b)) || byId(a, b);
  });
}

/**
 * Translate a LocalQuery into Dexie work.
 *
 * v0 is deliberately thin (equality `where`, one sort key, limit, offset). An
 * indexed equality field narrows the read; everything else is a scan and a
 * filter. Anything harder belongs in `liveRaw` (SPEC §2.3).
 *
 * ONE implementation, shared by `find`, `count`, `live` and `liveCount`, so
 * `sort.localFallback` cannot mean two different things in a list and in the
 * `limit`ed count of that same list.
 */
export async function runQuery<T extends PBRecord>(
  table: Table<T, string>,
  query?: LocalQuery<T>,
): Promise<T[]> {
  const where = query?.where ?? {};
  const keys = Object.keys(where);
  const narrow = keys.find((k) => isIndexed(table, k) && isIndexable(where[k as keyof typeof where]));

  let rows: T[];
  if (narrow !== undefined) {
    rows = await table.where(narrow).equals(where[narrow as keyof typeof where] as IndexableType).toArray();
  } else {
    rows = await table.toArray();
  }

  if (keys.length > 0) {
    rows = rows.filter((r) => keys.every((k) => (r as Record<string, unknown>)[k] === where[k as keyof typeof where]));
  }

  const sort = query?.sort;
  if (sort) {
    if (sort.localFallback === 'clientUpdatedAt') {
      rows = await sortWithFallback(table, rows, sort);
    } else {
      const dir = sort.dir === 'desc' ? -1 : 1;
      rows = [...rows].sort(
        (a, b) => dir * compare((a as Record<string, unknown>)[sort.field], (b as Record<string, unknown>)[sort.field]),
      );
    }
  }

  const offset = query?.offset ?? 0;
  const limit = query?.limit;
  if (offset > 0 || limit !== undefined) {
    rows = rows.slice(offset, limit === undefined ? undefined : offset + limit);
  }
  return rows;
}

export function live<T extends PBRecord>(
  db: DbGetter,
  collection: string,
  query?: LocalQuery<T>,
  onError?: ErrorSink,
  revision?: LiveRevision,
): Readable<T[]> {
  return fromLiveQuery<T[]>(async () => {
    const d = db();
    if (!d) return [];
    return runQuery(tableOf<T>(d, collection), query);
  }, [], onError, revision);
}

export function liveOne<T extends PBRecord>(
  db: DbGetter,
  collection: string,
  id: string,
  onError?: ErrorSink,
  revision?: LiveRevision,
): Readable<T | undefined> {
  return fromLiveQuery<T | undefined>(async () => {
    const d = db();
    if (!d) return undefined;
    return tableOf<T>(d, collection).get(id);
  }, undefined, onError, revision);
}

export function liveCount<T extends PBRecord>(
  db: DbGetter,
  collection: string,
  query?: LocalQuery<T>,
  onError?: ErrorSink,
  revision?: LiveRevision,
): Readable<number> {
  return fromLiveQuery<number>(async () => {
    const d = db();
    if (!d) return 0;
    // Sorting cannot change a count, so a query that only sorts still takes the
    // cheap path — including one carrying `localFallback`, which then costs no
    // `_meta` read. `limit`/`offset` DO depend on the order, and those go
    // through `runQuery`, which applies the fallback.
    if (!query || (query.where === undefined && query.limit === undefined && query.offset === undefined)) {
      return tableOf<T>(d, collection).count();
    }
    return (await runQuery(tableOf<T>(d, collection), query)).length;
  }, 0, onError, revision);
}

export function liveRaw<T extends PBRecord, R>(
  db: DbGetter,
  collection: string,
  fn: (table: Table<T, string>) => Promise<R>,
  onError?: ErrorSink,
  revision?: LiveRevision,
): Readable<R> {
  // The declared empty value for arbitrary work is `undefined` — a consumer of
  // liveRaw must handle the pre-first-read frame (SPEC §2.5.1).
  return fromLiveQuery<R>(async () => {
    const d = db();
    if (!d) return undefined as unknown as R;
    return fn(tableOf<T>(d, collection));
  }, undefined as unknown as R, onError, revision);
}

/** Per-record sync meta, reactive. */
export function liveMeta(
  db: DbGetter,
  collection: string,
  id: string,
  onError?: ErrorSink,
  revision?: LiveRevision,
): Readable<RecordMeta | undefined> {
  return fromLiveQuery<RecordMeta | undefined>(async () => {
    const d = db();
    if (!d) return undefined;
    const row = await readMeta(d, collection, id);
    if (!row) return undefined;
    const entries = await pendingFor(d, collection, id);
    const state = await deriveState(d, collection, id);
    return toPublicMeta({ ...row, state }, entries.length);
  }, undefined, onError, revision);
}

/** Every record in the collection with pending or dead-lettered mutations. */
export function liveDirty(
  db: DbGetter,
  collection: string,
  onError?: ErrorSink,
  revision?: LiveRevision,
): Readable<Array<{ id: string; state: RecordSyncState }>> {
  return fromLiveQuery<Array<{ id: string; state: RecordSyncState }>>(async () => {
    const d = db();
    if (!d) return [];
    const pending = await d.pbo.pending.where('collection').equals(collection).toArray();
    const dead = await d.pbo.deadletter.where('collection').equals(collection).toArray();

    const states = new Map<string, RecordSyncState>();
    for (const e of pending) {
      const current = states.get(e.recordId);
      if (e.state === 'inflight') states.set(e.recordId, 'pending');
      else if (current === undefined) states.set(e.recordId, 'dirty');
    }
    for (const e of dead) states.set(e.recordId, 'dead');

    return [...states.entries()]
      .map(([id, state]) => ({ id, state }))
      .sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
  }, [], onError, revision);
}
