/**
 * Dexie schema (SPEC §4.2).
 *
 * One table per configured collection, plus five engine tables. The collection
 * tables are keyed on `id`, which is always the PocketBase record id (SPEC §4.4).
 */
import Dexie, { type Table } from 'dexie';
import type { ResolvedConfig, PendingEntry, CursorRow, DeadLetter, MetaRow } from '../types.js';

/** Engine table names. A configured collection MUST NOT use one of these. */
export const ENGINE_TABLES = ['_pending', '_cursors', '_deadletter', '_meta', '_state'] as const;

export const SCHEMA_VERSION = 1;

/** Dexie index declarations for the engine tables. Do not reorder — Dexie diffs them. */
export const ENGINE_SCHEMA: Record<(typeof ENGINE_TABLES)[number], string> = {
  _pending: '++seq, [collection+recordId], recordId, collection, state',
  _cursors: 'collection',
  _deadletter: '++id, collection, recordId, at',
  _meta: '[collection+recordId], collection, state',
  _state: 'key',
};

/**
 * `_state` rows, by key.
 *
 * `sync` is the observable one every tab reads (SPEC §4.2, §13.4). The other
 * two are LOCAL BOOKKEEPING about the database itself rather than about the
 * sync: they live here because a fact about a mirror belongs inside that
 * mirror, where it is deleted with it and cannot be left behind pointing at a
 * database that no longer exists (which is exactly what a localStorage key
 * would do).
 *
 *   `signout`  — a sign-out drained short and left the mirror in place; the
 *                next `start()` for this owner owes it a retry (SPEC §15).
 *   `opened`   — when this mirror was last opened, for `staleOwnerDays`.
 */
export type StateKey = 'sync' | 'signout' | 'opened';

export interface StateRow {
  key: StateKey;
  /** The SyncState shape, written by the drainer, read reactively by every tab. */
  state: unknown;
  drainerTabId: string | null;
  heartbeatAt: number;
}

/**
 * The engine tables, typed.
 *
 * They are reached through `db.pbo.*` rather than `db._state` and friends
 * because Dexie owns several underscore-prefixed properties on its own instances
 * (`_state` among them) — the IndexedDB table names still match SPEC §4.2
 * exactly, only the TypeScript accessor differs.
 */
export interface PBOTables {
  pending: Table<PendingEntry, number>;
  cursors: Table<CursorRow, string>;
  deadletter: Table<DeadLetter, number>;
  meta: Table<MetaRow, [string, string]>;
  state: Table<StateRow, string>;
}

export interface PBODexie extends Dexie {
  readonly pbo: PBOTables;
}

/**
 * Build the full `version(n).stores({...})` map for a config.
 *
 * Collection tables are `'id'` plus whatever `CollectionConfig.indexes`
 * declares. The tombstone collection gets NO table: tombstones are applied and
 * discarded, so mirroring them would only cost quota (SPEC §8).
 */
export function buildStores(cfg: ResolvedConfig): Record<string, string> {
  const stores: Record<string, string> = { ...ENGINE_SCHEMA };
  for (const c of cfg.collections) {
    stores[c.name] = ['id', ...c.indexes].join(', ');
  }
  return stores;
}

/**
 * Declare the schema on a Dexie instance.
 *
 * The version is a parameter rather than a constant because the store map is
 * derived from application config: a collection added after first run needs a
 * version bump or IndexedDB silently keeps the old object stores. `db/open.ts`
 * probes the existing database and passes the right number.
 */
export function declareSchema(db: Dexie, cfg: ResolvedConfig, version: number = SCHEMA_VERSION): PBODexie {
  db.version(version).stores(buildStores(cfg));
  const pbo: PBOTables = {
    pending: db.table('_pending'),
    cursors: db.table('_cursors'),
    deadletter: db.table('_deadletter'),
    meta: db.table('_meta'),
    state: db.table('_state'),
  };
  Object.defineProperty(db, 'pbo', { value: pbo, enumerable: false, configurable: true });
  return db as PBODexie;
}
