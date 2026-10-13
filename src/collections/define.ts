/**
 * `defineCollection<T>` — the typed collection handle (SPEC §2.3, §8 of the brief).
 *
 * A handle is a thin, typed facade over three things: the Dexie table (reads),
 * `collections/crud.ts` (writes) and `collections/query.ts` (reactive reads). It
 * holds no state of its own, so it is safe to construct before `start()` and to
 * keep across an auth change — it resolves the current database lazily.
 *
 * That promise covers the REACTIVE reads too, and for v0 it did not: a `live()`
 * built before the mirror opened emitted its empty value and then never anything
 * else, because Dexie re-runs a querier only for tables that querier read last
 * time and this one had read none. `deps.revision` (SPEC §2.3) is what makes the
 * docstring true — the store rebuilds its `liveQuery` when the mirror opens,
 * closes, or is swapped for another owner's.
 */
import type {
  BatchOp,
  Collection,
  LocalQuery,
  PBRecord,
  Readable,
  RecordMeta,
  RecordSyncState,
  ResolvedCollectionConfig,
  ResolvedConfig,
  WriteOpts,
} from '../types.js';
import type { Table } from 'dexie';
import type { PBODexie } from '../db/schema.js';
import * as crud from './crud.js';
import * as query from './query.js';
import { deriveState, pendingFor, readMeta, toPublicMeta } from '../db/meta.js';
import type { Revision } from '../store/readable.js';

export interface CollectionDeps {
  /** Current database, or undefined before start()/between auth changes. */
  getDb: () => PBODexie | undefined;
  /**
   * The mirror's identity signal (SPEC §2.3, added v0.1 after first-consumer
   * feedback). Without it a reactive read built before `start()` — or kept across
   * an account switch — is a permanently dead subscription rather than an empty
   * one, because Dexie only re-runs a querier for tables it read LAST time.
   * Optional: a caller with a database that never changes may omit it.
   */
  revision?: Revision;
  cfg: ResolvedConfig;
  collectionCfg: ResolvedCollectionConfig;
  /** Owner id to stamp on creates, or null in anonymous mode. */
  getOwnerId: () => string | null;
  /** Wake the drainer after a local write (SPEC §13.3). */
  nudge: () => void;
  /** Where a failed reactive read is reported. Default: dropped. */
  onError?: (err: unknown) => void;
}

/**
 * Build a typed handle.
 *
 * Every method resolves `getDb()` at call time rather than capturing a database
 * that a later auth change invalidates — that is what makes a handle safe to
 * construct before `start()` and to keep across a sign-in.
 */
export function defineCollection<T extends PBRecord = PBRecord>(deps: CollectionDeps): Collection<T> {
  const name = deps.collectionCfg.name;

  /**
   * SPEC §14.3: with `anonymousMode: 'disabled'` and no session, reads return
   * empty rather than throwing — only writes throw.
   */
  const readsBlocked = (): boolean => deps.cfg.anonymousMode === 'disabled' && deps.getOwnerId() === null;

  const db = (): PBODexie | undefined => (readsBlocked() ? undefined : deps.getDb());
  const table = (): Table<T, string> => crud.requireDb(deps).table(name) as Table<T, string>;

  return {
    name,

    async get(id) {
      if (readsBlocked()) return undefined;
      return table().get(id);
    },
    async getAll() {
      if (readsBlocked()) return [];
      return table().toArray();
    },
    async find(q?: LocalQuery<T>) {
      if (readsBlocked()) return [];
      return query.runQuery(table(), q);
    },
    async count(q?: LocalQuery<T>) {
      if (readsBlocked()) return 0;
      if (!q) return table().count();
      return (await query.runQuery(table(), q)).length;
    },

    live: (q?: LocalQuery<T>) => query.live<T>(db, name, q, deps.onError, deps.revision),
    liveOne: (id: string) => query.liveOne<T>(db, name, id, deps.onError, deps.revision),
    liveCount: (q?: LocalQuery<T>) => query.liveCount<T>(db, name, q, deps.onError, deps.revision),
    liveRaw: <R>(fn: (t: Table<T, string>) => Promise<R>): Readable<R> =>
      query.liveRaw<T, R>(db, name, fn, deps.onError, deps.revision),

    create: (data) => crud.create<T>(deps, data),
    update: (id: string, patch: Partial<T>, opts?: WriteOpts) => crud.update<T>(deps, id, patch, opts),
    delete: (id: string) => crud.remove(deps, id),
    batch: (ops: BatchOp<T>[]) => crud.batch<T>(deps, ops),

    async meta(id: string): Promise<RecordMeta | undefined> {
      const d = db();
      if (!d) return undefined;
      const row = await readMeta(d, name, id);
      if (!row) return undefined;
      const entries = await pendingFor(d, name, id);
      return toPublicMeta({ ...row, state: await deriveState(d, name, id) }, entries.length);
    },
    liveMeta: (id: string) => query.liveMeta(db, name, id, deps.onError, deps.revision),
    /**
     * `readsBlocked()` is NOT snapshotted here. It used to be — the handle
     * returned a `constant([])` when it was built with no session — which made
     * the store dead in exactly the same way the pre-`start()` live query was:
     * signing in could never wake it. The check belongs inside `db()`, which
     * every re-run consults (SPEC §14.3).
     */
    liveDirty: (): Readable<Array<{ id: string; state: RecordSyncState }>> =>
      query.liveDirty(db, name, deps.onError, deps.revision),
  };
}
