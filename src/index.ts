/**
 * Offline-first sync engine for PocketBase.
 *
 * Public entry point. Nothing but `createPBOffline` and type re-exports lives
 * here (SPEC §1.1).
 */
import type PocketBase from 'pocketbase';
import type {
  Collection,
  DeadLetter,
  PBOffline,
  PBOfflineConfig,
  PBRecord,
  PromotionResult,
  PullResult,
  PushResult,
  SignOutResult,
  SyncState,
  Unsubscriber,
} from './types.js';
import { BRAND } from './brand.js';
import { collectionConfig, resolveConfig } from './config.js';
import { PBOError, toPBOError } from './errors.js';
import { generateId } from './ids.js';
import { createEmitter } from './store/emitter.js';
import { derived, fromLiveQuery, writable } from './store/readable.js';
import { createDbManager, sweepStaleOwners, type OpenHandle } from './db/open.js';
import { checkDexiePeer } from './db/peer.js';
import {
  INITIAL_STATE,
  liveState,
  clearSignOutPending,
  markSignOutPending,
  readSignOutPending,
  sameSyncState,
  writeState,
} from './db/state.js';
import { defineCollection } from './collections/define.js';
import { createSyncRunner } from './sync/engine.js';
import { createNudgeChannel, electDrainer, type DrainerHandle } from './sync/locks.js';
import { createRealtime, type Realtime } from './sync/realtime.js';
import { createSession, ownerKeyOf } from './auth/session.js';
import { discard as discardDeadLetterEntry, retry as retryDeadLetterEntry } from './sync/deadletter.js';
import type { PBODexie } from './db/schema.js';

/** SPEC §15 — `signOut()`'s drain wait, and the floor of the retry's. */
const DEFAULT_FLUSH_TIMEOUT_MS = 5_000;

/**
 * Create an engine bound to a PocketBase SDK instance.
 *
 * Synchronous, and performs no I/O (SPEC §2.1): every store is emitting an
 * initial value on return, and database opening and network work start at
 * `start()`.
 */
export function createPBOffline(pb: PocketBase, config: PBOfflineConfig): PBOffline {
  const cfg = resolveConfig(config);
  // SPEC §17.4. Two copies of Dexie in one page is a silent failure — writes
  // land, nothing re-renders — and it is what a `file:`/`link` install produces
  // by default. One stamp on `globalThis`, one warning, no I/O.
  checkDexiePeer(cfg.logger);
  const emitter = createEmitter();
  const tabId = generateId();
  const dbs = createDbManager(cfg);

  let destroyed = false;
  let startChain: Promise<void> | null = null;
  /**
   * SPEC §2.2 `SyncState.started`. Per-instance lifecycle, never persisted and
   * never read back from `_state` — `start()` in effect on THIS handle, which
   * stays true across an auth change and across a signed-out engine that opens
   * no database at all.
   */
  let started = false;
  /** Bound to the OPEN database; dropped on every auth switch. */
  const teardown: Unsubscriber[] = [];
  /** Bound to the instance; dropped only by `destroy()`. */
  const instanceTeardown: Unsubscriber[] = [];
  /** Owner changes serialise: two switches must not interleave their steps. */
  let switchChain: Promise<void> = Promise.resolve();
  let channel: ReturnType<typeof createNudgeChannel> | null = null;
  /** The drainer election for the OPEN database (SPEC §13). */
  let drainer: DrainerHandle | null = null;
  /** SSE for the open database, driven only while this tab is the drainer. */
  let realtime: Realtime | null = null;

  // ── composed state ───────────────────────────────────────────────────────
  // Three sources feed one store: the `_state` row the drainer writes, this
  // tab's own view of the local queue, and the session identity. Composing
  // rather than mirroring keeps queue depth honest in a tab that never drains.
  let fromRow: SyncState = { ...INITIAL_STATE };
  let pendingCount = 0;
  let deadLetterCount = 0;

  // SPEC §14.1: the owner identity is the auth store's, and the session watches
  // it for changes that are changes — a token refresh is not one.
  const session = createSession(pb);
  const ownerId = (): string | null => session.ownerId;
  const ownerKey = (): string => ownerKeyOf(ownerId());

  // ── the sync engine ──────────────────────────────────────────────────────
  // Everything that touches the network lives behind this seam (SPEC §1.1);
  // the handle below owns the database, the stores and the events.
  const nudgeListeners = new Set<() => void>();
  const runner = createSyncRunner({
    pb,
    cfg,
    getDb: () => dbs.current()?.db,
    getOwnerId: ownerId,
    emit: emitter.emit,
    tabId,
    onNudge: (cb) => {
      nudgeListeners.add(cb);
      return () => nudgeListeners.delete(cb);
    },
    isDrainer: () => drainer?.isDrainer ?? false,
    // SPEC §3.1 step 3 — and the ordering is the point: the cursor pull runs
    // first, so the gap between the last cursor and the first live event closes.
    resubscribe: async () => {
      await realtime?.subscribe();
    },
  });

  const stateStore = writable<SyncState>({ ...INITIAL_STATE, ownerId: ownerId() });
  const deadLetterStore = writable<DeadLetter[]>([]);

  const recompute = (): void => {
    const prev = stateStore.get();
    const next: SyncState = { ...fromRow, pendingCount, deadLetterCount, ownerId: ownerId(), started };
    // SPEC §2.5.4 — identity changes only when the value does. A fresh object per
    // recompute would defeat the store's `Object.is` guard and re-render every
    // consumer on every sync step.
    if (sameSyncState(prev, next)) return;
    stateStore.set(next);
    if (prev.status !== next.status) emitter.emit('status', { from: prev.status, to: next.status });
  };

  const status = derived(stateStore, (s) => s.status);
  const pendingCountStore = derived(stateStore, (s) => s.pendingCount);
  const lastSyncedAt = derived(stateStore, (s) => s.lastSyncedAt);
  const online = derived(stateStore, (s) => s.online);

  // ── collection handles (constructed eagerly, resolve their table lazily) ──
  const handles = new Map<string, Collection<PBRecord>>();
  const nudge = (): void => {
    // SPEC §13.3: an optimisation only. Correctness never depends on it, so a
    // missing BroadcastChannel (Node, older Safari) is not an error.
    channel?.nudge();
    // A BroadcastChannel never delivers to the posting context, so the drainer
    // tab's own writes have to reach it directly.
    runner.nudge();
  };
  for (const c of cfg.collections) {
    handles.set(
      c.name,
      defineCollection({
        getDb: () => dbs.current()?.db,
        // SPEC §2.3: reactive reads built before `start()`, or kept across an
        // account switch, rebuild on this rather than staying dead on the
        // observability set Dexie recorded when there was no mirror to read.
        revision: dbs.revision,
        cfg,
        collectionCfg: c,
        getOwnerId: ownerId,
        nudge,
        onError: (err) => emitter.emit('error', { error: toPBOError(err, 'db/open-failed').toShape() }),
      }),
    );
  }

  const assertUsable = (): void => {
    if (destroyed) {
      throw new PBOError('internal/invariant', 'this instance has been destroyed');
    }
  };

  const requireDb = (): PBODexie => {
    const handle = dbs.current();
    if (!handle) throw new PBOError('internal/invariant', 'the engine has not been started');
    return handle.db;
  };

  /** Anonymous data is disabled and there is no session: nothing is opened. */
  const storageDisabled = (): boolean => cfg.anonymousMode === 'disabled' && ownerId() === null;

  const detach = (): void => {
    while (teardown.length > 0) teardown.pop()?.();
    channel?.close();
    channel = null;
  };

  /**
   * Release everything bound to the CURRENT database, in SPEC §14.2's order:
   * stop the drain, tear down SSE, drop the drainer lock, unbind the stores.
   * The database itself is not closed here and never wiped — the callers differ
   * on both counts.
   */
  async function release(): Promise<void> {
    await runner.stop();
    await realtime?.unsubscribe();
    realtime = null;
    await drainer?.release();
    drainer = null;
    detach();
  }

  /**
   * Bind the open database: stores, nudge channel, drainer election, SSE.
   *
   * Election happens BEFORE the runner starts, so the very first pass already
   * knows whether this tab may touch the network (SPEC §13.1, §3.6).
   */
  async function attach(handle: OpenHandle): Promise<void> {
    const db = handle.db;

    teardown.push(
      liveState(db, tabId).subscribe((s) => {
        fromRow = s;
        recompute();
        // SPEC §3.6: a non-drainer tab does not ping — it takes the drainer's
        // verdict out of `_state` and hands it to its own watcher.
        runner.adoptOnline(s.online);
      }),
      fromLiveQuery(() => db.pbo.pending.count(), 0).subscribe((n) => {
        pendingCount = n;
        recompute();
      }),
      fromLiveQuery<DeadLetter[]>(() => db.pbo.deadletter.toArray(), []).subscribe((rows) => {
        deadLetterCount = rows.length;
        deadLetterStore.set(rows);
        recompute();
      }),
    );

    channel = createNudgeChannel(handle.dbName);
    teardown.push(
      channel.onNudge(() => {
        for (const cb of [...nudgeListeners]) cb();
      }),
    );

    realtime = createRealtime({
      pb,
      db,
      cfg,
      getOwnerId: ownerId,
      emit: emitter.emit,
      onError: (err) => emitter.emit('error', { error: toPBOError(err).toShape() }),
      // SPEC §9.7 / §13.6: an event that arrives after the role is gone is dropped.
      isActive: () => drainer?.isDrainer === true,
      onConnectionChange: (open) => runner.observeSse(open),
    });

    drainer = await electDrainer({
      db,
      dbName: handle.dbName,
      tabId,
      onFallback: () => {
        emitter.emit('error', {
          error: new PBOError(
            'lock/unavailable',
            'the Web Locks API is unavailable; falling back to the `_state` lease (SPEC §13.5)',
            { context: { dbName: handle.dbName, tabId } },
          ).toShape(),
        });
      },
    });
    teardown.push(drainer.onChange((isDrainer) => void onRoleChange(isDrainer)));

    if (drainer.isDrainer) await stampDrainer(db);
    await runner.start();
  }

  /** The `_state` drainer stamp (SPEC §13.4). Only ever written by the holder. */
  async function stampDrainer(db: PBODexie): Promise<void> {
    await writeState(db, { ownerId: ownerId(), status: 'offline' }, { tabId, isDrainer: true });
  }

  /**
   * SPEC §13.6 — the role changed under this tab.
   *
   * Gaining it stamps `_state` and lets the runner reconnect. Losing it tears
   * down SSE and stops any NEW drain; the request already on the wire completes
   * and records its result, because its Dexie transaction is the serialisation
   * point.
   */
  async function onRoleChange(isDrainer: boolean): Promise<void> {
    try {
      if (!isDrainer) {
        runner.roleChanged(false);
        await realtime?.unsubscribe();
        return;
      }
      const db = dbs.current()?.db;
      if (!db) return;
      await stampDrainer(db);
      runner.roleChanged(true);
    } catch (err) {
      emitter.emit('error', { error: toPBOError(err).toShape() });
    }
  }

  /**
   * Push what is queued, waiting at most `flushTimeoutMs` for it.
   *
   * BOUNDED, not abandoned: a request already on the wire records its result
   * either way, and what the timeout decides is only whether to keep WAITING.
   * Shared by `signOut()` and by the sign-out retry `start()` owes (SPEC §15).
   */
  async function drainBounded(flushTimeoutMs: number): Promise<void> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        runner.push().catch(() => undefined),
        new Promise<void>((resolve) => {
          timer = setTimeout(resolve, flushTimeoutMs);
        }),
      ]);
    } finally {
      if (timer !== undefined) clearTimeout(timer);
    }
  }

  /**
   * SPEC §15 — a sign-out this mirror still owes.
   *
   * `signOut()` without `force` leaves the database in place when writes are
   * still queued, and the application clears its token anyway; the mirror is
   * then a complete copy of the account sitting on the device with nothing left
   * running that remembers it should be gone. The marker in `_state` is that
   * memory, and this is where it is honoured: drain again, now that there is a
   * session and probably a link, and wipe once the queue is empty.
   *
   * A queue that STILL will not drain keeps the marker rather than forcing the
   * wipe — destroying unpushed work is the caller's decision (`force`), never
   * a retry's. Nor does a write the server REFUSED during this drain: it is
   * parked in `_deadletter`, the queue counts as empty, and a wipe would
   * destroy the only copy of it. So the wipe needs both tables empty; with a
   * dead letter the mirror stays open for the owner who is signed in again,
   * who sees the parked write as on any other day, and the marker is cleared —
   * that sign-out is over, and a later `start()` must not wipe a mirror its
   * owner is using. Returns whether the database was wiped, in which case the
   * caller must open a fresh one: the handle it holds is dead.
   */
  async function settleSignOutDebt(handle: OpenHandle): Promise<boolean> {
    const debt = await readSignOutPending(handle.db).catch(() => null);
    if (debt === null) return false;

    // The retry waits at least the default: a sign-out that could not wait at
    // all (no link, `flushTimeoutMs: 0`) says nothing about this session,
    // which has a token and probably a link.
    await drainBounded(Math.max(debt.flushTimeoutMs, DEFAULT_FLUSH_TIMEOUT_MS));
    // The owner can change under a drain; a wipe aimed at the mirror that is no
    // longer the open one would delete the wrong account's data.
    if (dbs.current() !== handle || ownerKey() !== handle.ownerKey) return false;

    const pending = await handle.db.pbo.pending.count();
    if (pending > 0) {
      cfg.logger?.({
        level: 'warn',
        msg: `${BRAND.name}: an unfinished sign-out could not drain; the mirror stays on this device (SPEC §15)`,
        data: { ownerKey: handle.ownerKey, pending, since: debt.at },
        at: Date.now(),
      });
      return false;
    }

    const deadLetters = await handle.db.pbo.deadletter.count();
    if (deadLetters > 0) {
      await clearSignOutPending(handle.db).catch(() => undefined);
      cfg.logger?.({
        level: 'warn',
        msg: `${BRAND.name}: an unfinished sign-out left refused writes parked; the mirror stays on this device (SPEC §15)`,
        data: { ownerKey: handle.ownerKey, deadLetters, since: debt.at },
        at: Date.now(),
      });
      return false;
    }

    await release();
    await dbs.wipe();
    cfg.logger?.({
      level: 'info',
      msg: `${BRAND.name}: finished a sign-out that could not complete earlier; the mirror is wiped (SPEC §15)`,
      data: { ownerKey: handle.ownerKey, since: debt.at },
      at: Date.now(),
    });
    return true;
  }

  /** SPEC §14.2 — evict other owners' mirrors, if the application opted in. */
  async function sweepOtherOwners(): Promise<void> {
    if (cfg.staleOwnerDays === null) return;
    try {
      const swept = await sweepStaleOwners(cfg, ownerKey());
      if (swept.deleted.length === 0) return;
      cfg.logger?.({
        level: 'info',
        msg: `${BRAND.name}: deleted ${swept.deleted.length} stale owner mirror(s) (staleOwnerDays)`,
        data: { deleted: swept.deleted, kept: swept.kept, staleOwnerDays: cfg.staleOwnerDays },
        at: Date.now(),
      });
    } catch (err) {
      // Housekeeping. A mirror that survives one more boot is not a reason for
      // `start()` to fail.
      cfg.logger?.({
        level: 'warn',
        msg: `${BRAND.name}: the stale-owner sweep failed`,
        data: { error: toPBOError(err).toShape() },
        at: Date.now(),
      });
    }
  }

  async function doStart(): Promise<void> {
    if (storageDisabled()) {
      fromRow = { ...INITIAL_STATE, status: 'offline' };
      recompute();
      return;
    }
    const handle = await dbs.open(ownerKey());

    // SPEC §5.4: the SDK cancels concurrent requests to the same collection by
    // default, which would silently kill a drain overlapping an app read.
    if (typeof (pb as { autoCancellation?: (v: boolean) => void }).autoCancellation === 'function') {
      (pb as { autoCancellation: (v: boolean) => void }).autoCancellation(false);
    }

    await attach(handle);

    // Both of these need a bound runner (the drain is the runner's), so they
    // run after `attach` rather than before it.
    if (await settleSignOutDebt(handle)) {
      // The mirror the previous session left behind is gone. Open a fresh one
      // for the same owner and bind to that instead of returning a started
      // engine with no database.
      await attach(await dbs.open(ownerKey()));
    }
    await sweepOtherOwners();
  }

  /**
   * SPEC §14.2 — an owner change, in this exact order:
   *
   *   stop the drain → tear down SSE → close the current database → open the new
   *   owner's → emit `auth-change` → reconnect if authenticated.
   *
   * The outgoing database is CLOSED, never wiped. Wiping is `signOut()`'s job and
   * only `signOut()`'s: switching accounts preserves both, queues included.
   */
  async function onOwnerChange(next: string | null, previous: string | null): Promise<void> {
    if (destroyed || !startChain) return;
    switchChain = switchChain.then(async () => {
      try {
        await release();
        await dbs.close();

        fromRow = { ...INITIAL_STATE };
        pendingCount = 0;
        deadLetterCount = 0;
        deadLetterStore.set([]);

        const handle = storageDisabled() ? undefined : await dbs.open(ownerKeyOf(next));
        // Published only now: `ownerId` names the owner whose mirror the
        // collection handles are actually reading, and until this line that was
        // still the outgoing one.
        recompute();
        // §14.2's order is normative, and `auth-change` comes BEFORE the
        // reconnect — `attach` ends in `runner.start()`, which pings, which runs
        // the whole sequence. Emitting afterwards handed applications a "new
        // owner" event with that owner's push, pull and database writes already
        // in flight, so anything gated on the event (promoteAnonymousData,
        // seeding, deciding whether to sync at all) raced the engine.
        emitter.emit('auth-change', { ownerId: next, previous });
        if (handle) await attach(handle);
        if (next !== null && drainer?.isDrainer === true) await runner.sync();
      } catch (err) {
        const e = toPBOError(err, 'db/open-failed');
        fromRow = { ...fromRow, status: 'error', lastError: e.toShape() };
        recompute();
        emitter.emit('error', { error: e.toShape() });
      }
    });
    return switchChain;
  }

  instanceTeardown.push(
    session.onChange((next, previous) => {
      // With no started engine there is no database to move, so the new identity
      // is publishable at once. With one, `onOwnerChange` publishes it after the
      // new owner's database is open — until then `ownerId` would name an owner
      // whose data is not the data a `live()` query is showing.
      if (!startChain) recompute();
      void onOwnerChange(next, previous);
    }),
  );

  const engine: PBOffline = {
    collection<T extends PBRecord = PBRecord>(name: string): Collection<T> {
      assertUsable();
      collectionConfig(cfg, name); // throws config/unknown-collection
      return handles.get(name) as unknown as Collection<T>;
    },

    status,
    pendingCount: pendingCountStore,
    lastSyncedAt,
    deadLetters: { subscribe: deadLetterStore.subscribe },
    online,
    state: { subscribe: stateStore.subscribe },

    on: emitter.on,
    once: emitter.once,

    async start(): Promise<void> {
      assertUsable();
      if (!startChain) {
        // Published BEFORE the chain runs: `started` reports that a start is in
        // effect, not that it has finished. A consumer gating a first render on
        // it would otherwise miss the whole open.
        started = true;
        recompute();
        startChain = doStart().catch((err) => {
          startChain = null;
          started = false;
          const e = toPBOError(err, 'db/open-failed');
          fromRow = { ...fromRow, status: 'error', lastError: e.toShape() };
          recompute();
          emitter.emit('error', { error: e.toShape() });
          throw e;
        });
      }
      return startChain;
    },

    async stop(): Promise<void> {
      if (!startChain) return;
      await startChain.catch(() => undefined);
      await switchChain.catch(() => undefined);
      startChain = null;
      await release();
      started = false;
      recompute();
    },

    async push(): Promise<PushResult> {
      assertUsable();
      return runner.push();
    },
    async pull(collections?: string[]): Promise<PullResult> {
      assertUsable();
      return runner.pull(collections);
    },
    async sync(): Promise<void> {
      assertUsable();
      return runner.sync();
    },
    async fullResync(reason?: string): Promise<void> {
      assertUsable();
      return runner.fullResync(reason);
    },

    /**
     * SPEC §15. Never calls `pb.authStore.clear()`: token lifetime is the
     * application's decision, and this must be safe on either side of it.
     *
     * The drain is bounded, not abandoned: a request already on the wire records
     * its result, and what the timeout decides is only whether to WAIT longer.
     *
     * THE ESCAPE HATCH IS `force`. A queue that will not drain leaves the mirror
     * on the device — the right default, because the alternative is destroying
     * work the user cannot get back — and `{ force: true }` is how an
     * application says the data matters less than the device does (a shared or
     * borrowed machine). Whichever way it goes, an unwiped mirror is MARKED, so
     * the next `start()` for that owner retries the drain and the wipe instead
     * of leaving the copy there for ever.
     */
    async signOut(opts?: { flushTimeoutMs?: number; force?: boolean }): Promise<SignOutResult> {
      assertUsable();
      const flushTimeoutMs = opts?.flushTimeoutMs ?? DEFAULT_FLUSH_TIMEOUT_MS;
      const force = opts?.force ?? false;
      const previous = ownerId();

      const handle = dbs.current();
      if (!handle) return { drained: true, wiped: false, pending: 0 };

      await drainBounded(flushTimeoutMs);

      const pending = await handle.db.pbo.pending.count();
      if (pending > 0 && !force) {
        // The application is about to clear its token and forget this ever
        // happened; the mirror must not. Written before returning so a marker
        // exists even if the process dies on the next line.
        await markSignOutPending(handle.db, { ownerId: previous, flushTimeoutMs, at: Date.now() }).catch(() => undefined);
        return { drained: false, wiped: false, pending };
      }

      await release();
      startChain = null;
      // The mirror this engine was started on is about to stop existing, and
      // `startChain` is cleared, so the next `start()` is a real start — say so.
      started = false;
      // BOUNDED. `indexedDB.deleteDatabase` blocks for as long as another tab
      // holds this database open, and §13 makes a second tab the expected state,
      // not an edge case — unbounded, this promise would never settle and the
      // application's sign-out flow would hang with no error and no result. A
      // blocked wipe throws `db/version-conflict` instead: the engine is stopped
      // and released, the data is intact, and the caller can say which
      // cooperation is missing.
      await dbs.wipe();
      fromRow = { ...INITIAL_STATE };
      pendingCount = 0;
      deadLetterCount = 0;
      deadLetterStore.set([]);
      recompute();
      emitter.emit('auth-change', { ownerId: null, previous });
      return { drained: pending === 0, wiped: true, pending };
    },

    async promoteAnonymousData(opts?: { strategy?: 'copy' | 'move' }): Promise<PromotionResult> {
      assertUsable();
      const owner = ownerId();
      if (owner === null) {
        throw new PBOError('auth/unauthenticated', 'promotion requires a signed-in owner (SPEC §14.4)');
      }
      const { promoteAnonymousData } = await import('./auth/promotion.js');
      try {
        const current = dbs.current();
        // The engine's own handle, so the promoted rows land in the instance the
        // application is already reading from rather than in a second connection.
        const target = current && current.ownerKey === owner ? { targetDb: current.db } : {};
        const result = await promoteAnonymousData(cfg, owner, { ...opts, ...target });
        if (result.promoted > 0) nudge();
        return result;
      } catch (err) {
        throw toPBOError(err);
      }
    },

    /** Re-enqueue a dead letter at the TAIL: it is newer intent than the queue. */
    async retryDeadLetter(id: number): Promise<void> {
      assertUsable();
      await retryDeadLetterEntry(requireDb(), id);
      nudge();
    },

    async discardDeadLetter(id: number): Promise<void> {
      assertUsable();
      await discardDeadLetterEntry(requireDb(), id);
    },

    /**
     * Release every resource: the drain, the SSE subscriptions, the drainer lock
     * (so another tab is promoted at once rather than after a lease expiry), the
     * nudge channel, the auth binding and the database handle.
     */
    async destroy(): Promise<void> {
      if (destroyed) return;
      await engine.stop();
      destroyed = true;
      started = false;
      recompute();
      await release();
      session.stop();
      while (instanceTeardown.length > 0) instanceTeardown.pop()?.();
      await dbs.close();
      emitter.clear();
    },
  };

  return engine;
}

export { PBOError, isRetryable } from './errors.js';
export { BRAND } from './brand.js';
export { DEFAULTS, DEFAULT_ID_FORMAT } from './config.js';
export { generateId, isValidId } from './ids.js';

export type {
  PBOffline,
  PBOfflineConfig,
  CollectionConfig,
  Collection,
  PBRecord,
  Readable,
  Unsubscriber,
  SyncStatus,
  SyncState,
  RecordSyncState,
  RecordMeta,
  PendingOp,
  PendingEntry,
  DeadLetter,
  Cursor,
  LocalQuery,
  WriteOpts,
  BatchOp,
  ConflictResolver,
  ConflictContext,
  PBOErrorCode,
  PBOErrorShape,
  PBOLogEvent,
  PBOEvents,
  PushResult,
  PullResult,
  SignOutResult,
  PromotionResult,
  IdFormat,
} from './types.js';
