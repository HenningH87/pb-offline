/**
 * Per-owner database lifecycle (SPEC §4.1).
 *
 * ONE IndexedDB database per owner, named `${BRAND.dbPrefix}_${appId}_${ownerKey}`.
 * Not a shared database with an owner column — see SPEC §4.1 for why (atomic
 * sign-out wipe, no query can leak, free account switching, natural lock scoping).
 */
import Dexie from 'dexie';
import type { ResolvedConfig } from '../types.js';
import { BRAND, dbNameFor, dbPrefixFor, ownerKeyFromDbName } from '../brand.js';
import { PBOError } from '../errors.js';
import { createRevision, type Revision } from '../store/readable.js';
import { buildStores, declareSchema, SCHEMA_VERSION, type PBODexie } from './schema.js';
import { readOpenedAt, stampOpenedAt } from './state.js';

export interface OpenHandle {
  db: PBODexie;
  dbName: string;
  ownerKey: string;
}

function mapOpenError(err: unknown, dbName: string): PBOError {
  if (err instanceof PBOError) return err;
  const name = (err as { name?: string } | null)?.name ?? '';
  const message = (err as { message?: string } | null)?.message ?? String(err);
  const context = { dbName, dexieError: name };
  if (name === 'VersionError' || name === 'UpgradeError' || name === 'BlockedError') {
    return new PBOError('db/version-conflict', message, { cause: err, context });
  }
  if (name === 'QuotaExceededError' || name === 'QuotaError') {
    return new PBOError('db/quota', message, { cause: err, context });
  }
  return new PBOError('db/open-failed', message, { cause: err, context });
}

/**
 * The schema version this database must be opened at.
 *
 * A store map derived from application config can grow (a collection added
 * between releases), and IndexedDB only creates new object stores during a
 * version change — so an existing database whose object stores do not match the
 * wanted map is opened one version higher.
 */
async function resolveVersion(dbName: string, stores: Record<string, string>): Promise<number> {
  if (!(await Dexie.exists(dbName))) return SCHEMA_VERSION;
  const probe = new Dexie(dbName);
  try {
    await probe.open(); // no version declared ⇒ Dexie opens whatever exists
    const present = new Set(probe.tables.map((t) => t.name));
    const wanted = Object.keys(stores);
    const same = wanted.length === present.size && wanted.every((n) => present.has(n));
    return same ? Math.max(SCHEMA_VERSION, probe.verno) : Math.max(SCHEMA_VERSION, probe.verno) + 1;
  } finally {
    probe.close();
  }
}

/**
 * Open the database for one owner.
 *
 * Callers that own an engine's lifetime should use `createDbManager`, which
 * enforces the "at most one open instance, previous closed first" rule of
 * SPEC §4.1.
 */
export async function openForOwner(cfg: ResolvedConfig, ownerKey: string): Promise<OpenHandle> {
  const dbName = dbNameFor(cfg.appId, ownerKey);
  const stores = buildStores(cfg);
  try {
    const version = await resolveVersion(dbName, stores);
    const db = declareSchema(new Dexie(dbName), cfg, version);
    await db.open();
    return { db, dbName, ownerKey };
  } catch (err) {
    throw mapOpenError(err, dbName);
  }
}

/** Close without deleting. Used on account switch — the outgoing db survives. */
export async function close(handle: OpenHandle): Promise<void> {
  handle.db.close();
}

/**
 * How long to wait for the other tabs to let go before giving up on a delete.
 *
 * `indexedDB.deleteDatabase` BLOCKS — indefinitely, with no error — while any
 * other connection to that database is open, and multi-tab is a first-class
 * feature here (SPEC §13), so "the user has a second tab open" is the expected
 * state rather than an edge case. Unbounded, `signOut()`'s promise would simply
 * never settle and the application's sign-out flow would hang with nothing to
 * report.
 */
export const DELETE_TIMEOUT_MS = 5_000;

/**
 * `Dexie.delete`, bounded.
 *
 * A timeout is reported as `db/version-conflict` — the code §12 already assigns
 * to "another tab holds this database" — so the caller can tell the user which
 * cooperation is missing instead of waiting for ever.
 */
export async function deleteDatabase(dbName: string, timeoutMs: number = DELETE_TIMEOUT_MS): Promise<void> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      Dexie.delete(dbName),
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(
          () =>
            reject(
              new PBOError(
                'db/version-conflict',
                `deleting "${dbName}" is blocked by another open connection to it; close the other tabs and retry`,
                { context: { dbName, timeoutMs } },
              ),
            ),
          timeoutMs,
        );
      }),
    ]);
  } catch (err) {
    throw mapOpenError(err, dbName);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

/**
 * Close and DELETE the database.
 *
 * Only `signOut()` calls this (SPEC §14.2, §15). An account switch must not.
 * `Dexie.delete` is what makes the wipe atomic and total: there is no sweep to
 * interrupt and therefore no row that can survive into the next session.
 */
export async function wipe(handle: OpenHandle, timeoutMs?: number): Promise<void> {
  handle.db.close();
  await deleteDatabase(handle.dbName, timeoutMs);
}

/** Delete one owner's database by key, whether or not it is open here. */
export async function wipeOwner(cfg: ResolvedConfig, ownerKey: string, timeoutMs?: number): Promise<void> {
  await deleteDatabase(dbNameFor(cfg.appId, ownerKey), timeoutMs);
}

/** Names of this app's databases present on the device. Used by promotion (§14). */
export async function listOwnerDatabases(cfg: ResolvedConfig): Promise<string[]> {
  const prefix = dbPrefixFor(cfg.appId);
  const idb = globalThis.indexedDB as (IDBFactory & { databases?: () => Promise<{ name?: string }[]> }) | undefined;
  let names: string[];
  if (idb && typeof idb.databases === 'function') {
    names = (await idb.databases()).map((d) => d.name ?? '');
  } else {
    names = await Dexie.getDatabaseNames();
  }
  return names.filter((n) => n.startsWith(prefix)).sort();
}

/** Owner keys with a database on this device. */
export async function listOwnerKeys(cfg: ResolvedConfig): Promise<string[]> {
  const names = await listOwnerDatabases(cfg);
  return names.map((n) => ownerKeyFromDbName(cfg.appId, n)).filter((k): k is string => k !== null);
}

export interface SweepResult {
  /** Owner keys whose mirror was deleted. */
  deleted: string[];
  /** Owner keys inspected but kept, with why. */
  kept: { ownerKey: string; reason: 'fresh' | 'unstamped' | 'blocked' }[];
}

/**
 * Delete other owners' mirrors that have gone unopened for too long
 * (`staleOwnerDays`, SPEC §14.2).
 *
 * An account switch closes the outgoing database and never wipes it, so a
 * device accumulates one full unencrypted copy of every account that has signed
 * in on it — client names, birth data, notes, indefinitely, readable by anything
 * with devtools or the filesystem. This is the opt-in eviction for that.
 *
 * Three things it deliberately does NOT do:
 *
 *   - It never touches the CURRENT owner's mirror, whatever its stamp says.
 *   - It never touches the anonymous mirror. `promoteAnonymousData()` is the
 *     only thing entitled to consume that data and the engine cannot know
 *     whether the application still intends to (SPEC §14.4).
 *   - It never deletes a mirror with no `opened` stamp: that is a database from
 *     before this option existed, and "unknown age" is not "old". It stamps it
 *     instead, which starts the clock from this boot.
 *
 * Best-effort by construction: a database another tab still holds open cannot
 * be deleted (`deleteDatabase` is bounded and throws), and that is reported as
 * `blocked` rather than raised — a stale mirror surviving one more boot must
 * never be the reason `start()` fails.
 */
export async function sweepStaleOwners(
  cfg: ResolvedConfig,
  currentOwnerKey: string,
  now: number = Date.now(),
): Promise<SweepResult> {
  const result: SweepResult = { deleted: [], kept: [] };
  if (cfg.staleOwnerDays === null) return result;
  const staleAfterMs = cfg.staleOwnerDays * 24 * 60 * 60 * 1000;

  const keys = await listOwnerKeys(cfg);
  for (const ownerKey of keys) {
    if (ownerKey === currentOwnerKey || ownerKey === BRAND.anonOwner) continue;

    let handle: OpenHandle | undefined;
    let openedAt: number | null = null;
    try {
      handle = await openForOwner(cfg, ownerKey);
      openedAt = await readOpenedAt(handle.db);
      if (openedAt === null) await stampOpenedAt(handle.db, now);
    } catch {
      // An unreadable mirror is not a mirror we may delete on a guess.
      result.kept.push({ ownerKey, reason: 'blocked' });
      continue;
    } finally {
      handle?.db.close();
    }

    if (openedAt === null) {
      result.kept.push({ ownerKey, reason: 'unstamped' });
      continue;
    }
    if (now - openedAt < staleAfterMs) {
      result.kept.push({ ownerKey, reason: 'fresh' });
      continue;
    }

    try {
      await deleteDatabase(dbNameFor(cfg.appId, ownerKey));
      result.deleted.push(ownerKey);
    } catch {
      result.kept.push({ ownerKey, reason: 'blocked' });
    }
  }
  return result;
}

export interface DbManager {
  /** The open handle, or undefined before the first `open()`. */
  current(): OpenHandle | undefined;
  /**
   * Steps every time `current()` changes IDENTITY — the first open, an account
   * switch (both halves: the close and the open), a `close()`, a `wipe()`.
   *
   * Reactive reads hang off this (SPEC §2.3). Dexie's `liveQuery` keys its
   * observability on the database NAME, so a query that ran with no database
   * open, or against the outgoing owner's, is dead until something rebuilds it;
   * this is that something. Re-opening the SAME owner is not an identity change
   * and does not step.
   */
  readonly revision: Revision;
  /** Open `ownerKey`, closing any other open owner FIRST (SPEC §4.1). */
  open(ownerKey: string): Promise<OpenHandle>;
  /** Close the current handle, if any. The database survives. */
  close(): Promise<void>;
  /**
   * Close and delete the current handle's database.
   *
   * Bounded: rejects with `db/version-conflict` when another tab still holds the
   * database open past `timeoutMs` rather than hanging on the blocked delete.
   */
  wipe(timeoutMs?: number): Promise<void>;
}

/**
 * Holds at most one open Dexie instance per engine and closes the previous one
 * before opening the next. Concurrent `open()` calls serialise on one chain, so
 * an auth change racing `start()` cannot leave two databases open.
 */
export function createDbManager(cfg: ResolvedConfig): DbManager {
  let handle: OpenHandle | undefined;
  let chain: Promise<unknown> = Promise.resolve();
  const revision = createRevision();

  /** Assign the handle and announce it. One place, so no path can forget. */
  const setHandle = (next: OpenHandle | undefined): void => {
    if (handle === next) return;
    handle = next;
    revision.step();
  };

  const serialise = <R>(fn: () => Promise<R>): Promise<R> => {
    const next = chain.then(fn, fn);
    chain = next.then(
      () => undefined,
      () => undefined,
    );
    return next;
  };

  return {
    current: () => handle,
    revision,
    open: (ownerKey) =>
      serialise(async () => {
        if (handle?.ownerKey === ownerKey) return handle;
        if (handle) {
          await close(handle);
          // Stepped here as well as after the open: between these two lines the
          // outgoing owner's rows are no longer readable, and a reactive read
          // that kept showing them would be showing the wrong account's data.
          setHandle(undefined);
        }
        const opened = await openForOwner(cfg, ownerKey);
        // The `staleOwnerDays` clock (SPEC §14.2). Stamped here and not in
        // `openForOwner`, which the retention sweep uses to READ this value.
        await stampOpenedAt(opened.db);
        setHandle(opened);
        return opened;
      }),
    close: () =>
      serialise(async () => {
        if (!handle) return;
        await close(handle);
        setHandle(undefined);
      }),
    wipe: (timeoutMs?: number) =>
      serialise(async () => {
        if (!handle) return;
        // Dropped BEFORE the await: `wipe` closes the instance either way, so a
        // blocked delete must not leave the manager handing out a closed handle.
        const doomed = handle;
        setHandle(undefined);
        await wipe(doomed, timeoutMs);
      }),
  };
}
