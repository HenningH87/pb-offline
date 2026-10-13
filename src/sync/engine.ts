/**
 * Orchestration: the status state machine and the reconnect sequence (SPEC §3.1).
 *
 * The engine is the only module that owns timers besides the connectivity
 * watcher (SPEC §18.5), and the only one that sequences push → pull →
 * tombstones → subscribe. That order is normative: pushing first makes the queue
 * the authority on local intent; pulling before resubscribing closes the gap
 * between the last cursor and the first live event.
 */
import type PocketBase from 'pocketbase';
import type {
  PBOErrorShape,
  PBOEvents,
  PromotionResult,
  PullResult,
  PushResult,
  ResolvedConfig,
  SignOutResult,
  SyncStatus,
  Unsubscriber,
} from '../types.js';
import type { PBODexie } from '../db/schema.js';
import { toPBOError } from '../errors.js';
import { isAuthValid } from '../auth/session.js';
import { writeState } from '../db/state.js';
import { createConnectivityWatcher, type ConnectivityWatcher, type OfflineReason } from '../net/connectivity.js';
import { drain, type PushDeps } from './push.js';
import { pull, type PullDeps } from './pull.js';
import { fullResync as runFullResync, shouldResync } from './resync.js';

/**
 * The seam the local layers call through.
 *
 * `index.ts` owns the handle, the stores and the database; everything that
 * touches the network lives behind this interface.
 */
export interface SyncRunner {
  /** Called at the end of `PBOffline.start()`, after the database is open. */
  start(): Promise<void>;
  /** Called at the start of `PBOffline.stop()`, before subscriptions are torn down. */
  stop(): Promise<void>;
  push(): Promise<PushResult>;
  pull(collections?: string[]): Promise<PullResult>;
  sync(): Promise<void>;
  fullResync(reason?: string): Promise<void>;
  /**
   * Wake the drainer after a local write in THIS tab (SPEC §13.3).
   *
   * A BroadcastChannel never delivers to the posting context, so the drainer's
   * own writes need this direct call; the channel covers the other tabs. Both
   * are optimisations — the poll timer is what makes correctness independent
   * of them.
   */
  nudge(): void;
  /**
   * The drainer role changed under this tab (SPEC §13.6).
   *
   * Gained: publish and run the reconnect sequence. Lost: abort at the entry
   * boundary, so no NEW drain starts — a request already on the wire is allowed
   * to complete and record its result, because its Dexie transaction is the
   * serialisation point.
   */
  roleChanged(isDrainer: boolean): void;
  /**
   * Adopt the drainer's `online` verdict in a non-drainer tab (SPEC §3.6).
   *
   * Those tabs never ping — they read the drainer's answer out of `_state` and
   * hand it to the watcher, which is the only module allowed to hold that
   * boolean (SPEC §18.1).
   */
  adoptOnline(online: boolean): void;
  /**
   * Feed the SSE connection state to the watcher (SPEC §3.1c) — an open
   * subscription implies up, a disconnect implies SUSPECT and is probed. The
   * watcher decides; realtime.ts only reports.
   */
  observeSse(open: boolean): void;
}

export interface SyncRunnerDeps {
  pb: PocketBase;
  cfg: ResolvedConfig;
  /** The open database, or undefined before start()/between auth changes. */
  getDb: () => PBODexie | undefined;
  getOwnerId: () => string | null;
  emit: <E extends keyof PBOEvents>(event: E, payload: PBOEvents[E]) => void;
  /** This tab's id, for the `_state` drainer stamp (SPEC §13.4). */
  tabId: string;
  /** Cross-tab nudges from other tabs' writes. */
  onNudge?: (cb: () => void) => Unsubscriber;
  /**
   * Does this tab hold the drainer role (SPEC §13)? Default: yes.
   *
   * A non-drainer tab still runs a watcher — so `online` is observable and
   * `adoptOnline` has somewhere to land — but it never pings, never drains,
   * never pulls and never writes `_state`.
   */
  isDrainer?: () => boolean;
  /** SSE resubscribe (SPEC §3.1 step 3, §9). Idempotent by contract. */
  resubscribe?: () => Promise<void>;
  /** Drainer poll interval. 0 disables the timer (tests). */
  pollMs?: number;
  now?: () => number;
}

const DEFAULT_POLL_MS = 2_000;

/**
 * Build the runner.
 *
 * The runner does not elect anything — `sync/locks.ts` does, and `index.ts` feeds
 * the answer in through `deps.isDrainer`. A runner with no `isDrainer` is the
 * single-tab case and behaves as the drainer, which is what a headless consumer
 * (a Node script, a test) wants.
 */
export function createSyncRunner(deps: SyncRunnerDeps): SyncRunner {
  const now = (): number => (deps.now ?? Date.now)();
  const halted = new Map<string, PBOErrorShape>();

  let started = false;
  let watcher: ConnectivityWatcher | null = null;
  let controller = new AbortController();
  let offNudge: Unsubscriber | null = null;
  let timer: ReturnType<typeof setInterval> | null = null;

  let busy = 0;
  let lastSyncedAt: number | null = null;
  let lastError: PBOErrorShape | null = null;
  /** A drain stopped by the link with work left: SPEC §2.2's `connecting`. */
  let retryPending = false;
  let drainPromise: Promise<PushResult> | null = null;
  let chain: Promise<unknown> = Promise.resolve();
  let publishing: Promise<void> = Promise.resolve();

  /** SPEC §14.3: with no session nothing is ever pushed, and status is offline. */
  const syncDisabled = (): boolean => deps.getOwnerId() === null;
  /**
   * SPEC §13.1 and §3.6 — the single gate on every network act. It is passed to
   * the watcher (so a non-drainer tab never pings), to push (so a drain aborts at
   * the entry boundary when the role is lost) and checked before every pass.
   */
  const isDrainer = (): boolean => started && holdsRole();
  /**
   * The role itself, independent of `started` — `stop()` still writes the tab's
   * final `offline` into `_state`, and a tab that LOST the role writes nothing.
   */
  const holdsRole = (): boolean => deps.isDrainer?.() ?? true;

  const onError = (err: unknown): void => {
    const shape = toPBOError(err).toShape();
    lastError = shape;
    deps.emit('error', { error: shape });
  };

  /** A database that closed under a background pass is not an application fault. */
  const isTornDown = (err: unknown): boolean => {
    const name = (err as { name?: string } | null)?.name ?? '';
    return !started || name === 'DatabaseClosedError' || name === 'AbortError';
  };

  // ── status (SPEC §2.2) ─────────────────────────────────────────────────────

  function computeStatus(deadLetters: number): SyncStatus {
    if (!started || syncDisabled()) return 'offline';
    // `error` is never transient: 5xx and network failures are `connecting`.
    if (deadLetters > 0 || halted.size > 0) return 'error';
    if (busy > 0) return 'syncing';
    // `connecting` covers the whole re-establishment attempt — the backoff wait
    // as much as the ping — so a UI has a stable state to render.
    if (!watcher?.isOnline()) return watcher?.isSeeking() ? 'connecting' : 'offline';
    if (retryPending) return 'connecting';
    return 'online';
  }

  /** Write the drainer's view of the world into `_state` (SPEC §4.2, §13.4). */
  function publish(): Promise<void> {
    publishing = publishing.then(async () => {
      const db = deps.getDb();
      // SPEC §13.4: one writer. A non-drainer tab READS this row; writing it
      // would be two tabs racing on the state every tab renders from.
      if (!db || !holdsRole()) return;
      try {
        const deadLetters = await db.pbo.deadletter.count();
        await writeState(
          db,
          {
            status: computeStatus(deadLetters),
            online: watcher?.isOnline() ?? false,
            lastSyncedAt,
            lastError,
            ownerId: deps.getOwnerId(),
          },
          { tabId: deps.tabId, isDrainer: true },
        );
      } catch (err) {
        if (!isTornDown(err)) onError(err);
      }
    });
    return publishing;
  }

  // ── dependency bundles ─────────────────────────────────────────────────────

  function pushDeps(db: PBODexie): PushDeps {
    const d: PushDeps = {
      pb: deps.pb,
      db,
      cfg: deps.cfg,
      isDrainer,
      isOnline: () => watcher?.isOnline() ?? false,
      abort: controller.signal,
      emit: deps.emit,
      observeRequest: (ok, netFail) => watcher?.observeRequest(ok, netFail),
      isAuthValid: () => isAuthValid(deps.pb),
      onAuthLost: () => {
        deps.emit('offline', { at: now(), reason: 'auth' });
        watcher?.observeAuthLoss();
      },
      now,
    };
    if (watcher) d.backoff = watcher.backoff;
    return d;
  }

  function pullDeps(db: PBODexie): PullDeps {
    return {
      pb: deps.pb,
      db,
      cfg: deps.cfg,
      getOwnerId: deps.getOwnerId,
      abort: controller.signal,
      emit: deps.emit,
      halted,
      isOnline: () => watcher?.isOnline() ?? false,
      observeRequest: (ok, netFail) => watcher?.observeRequest(ok, netFail),
      isAuthValid: () => isAuthValid(deps.pb),
      onAuthLost: () => {
        deps.emit('offline', { at: now(), reason: 'auth' });
        watcher?.observeAuthLoss();
      },
      onError,
      drain: () => runDrain(),
      now,
    };
  }

  // ── the three primitives ───────────────────────────────────────────────────

  /** SPEC §5.11 — at most one drain per engine; concurrent callers share it. */
  function runDrain(): Promise<PushResult> {
    if (drainPromise) return drainPromise;
    const db = deps.getDb();
    if (!db || syncDisabled() || !isDrainer()) {
      return Promise.resolve({ pushed: 0, failed: 0, deadLettered: 0, remaining: 0, stoppedBy: 'stopped' });
    }
    busy += 1;
    void publish();
    drainPromise = (async () => {
      try {
        const result = await drain(pushDeps(db));
        retryPending = result.stoppedBy === 'link' && result.remaining > 0;
        return result;
      } finally {
        drainPromise = null;
        busy -= 1;
        void publish();
      }
    })();
    return drainPromise;
  }

  /** Put `fn` on the serialisation chain. Pulls and resyncs share it. */
  function serialise<R>(fn: () => Promise<R>): Promise<R> {
    const next = chain.then(fn, fn);
    chain = next.then(
      () => undefined,
      () => undefined,
    );
    return next;
  }

  function runPull(collections?: string[]): Promise<PullResult> {
    const db = deps.getDb();
    if (!db || syncDisabled() || !isDrainer()) {
      return Promise.resolve({ byCollection: {}, fullResyncTriggered: false });
    }
    const run = async (): Promise<PullResult> => {
      // SPEC §11: the automatic triggers MUST be checked "before every pull
      // pass" — not only inside sequence(). An application driving sync itself
      // with `engine.pull()` on a timer is a documented use of the API, and it
      // would otherwise pull for ever with a tombstone cursor years past the
      // server's retention window, silently missing every delete in it.
      //
      // `doFull` is called directly rather than through `runFull`: this closure
      // is ALREADY on the chain, and re-entering it would deadlock.
      const reason = await shouldResync(pullDeps(db));
      if (reason !== null) {
        await doFull(reason);
        // `fullResyncTriggered` means "a resync is DUE" — `sequence()` acts on it
        // by running one. This resync has already HAPPENED, and reporting it as
        // pending would run a second, needlessly wiping and refetching the whole
        // mirror. The `full-resync` events are what say it occurred.
        return { byCollection: {}, fullResyncTriggered: false };
      }
      busy += 1;
      void publish();
      try {
        return await pull(pullDeps(db), collections);
      } finally {
        busy -= 1;
        void publish();
      }
    };
    // Pulls serialise: two overlapping passes would race on the same cursor row.
    return serialise(run);
  }

  /** The resync body. MUST be called from inside the chain, never outside it. */
  async function doFull(reason: string): Promise<void> {
    const db = deps.getDb();
    if (!db || syncDisabled() || !isDrainer()) return;
    busy += 1;
    void publish();
    try {
      // Only a resync that reached {phase:'done'} may claim a successful sync.
      if (await runFullResync(pullDeps(db), reason)) lastSyncedAt = now();
    } finally {
      busy -= 1;
      void publish();
    }
  }

  /**
   * A resync runs on the SAME chain as a pull — so `stop()` and `destroy()` await
   * it, and so an owner switch cannot close Dexie out from under step 3's wipe,
   * leaving the outgoing owner with an empty mirror and null cursors.
   */
  function runFull(reason: string): Promise<void> {
    return serialise(() => doFull(reason));
  }

  /**
   * SPEC §3.1 — the reconnect sequence. The steps MUST NOT interleave.
   *
   *   1 drain `_pending`   2 pull every collection, tombstones LAST
   *   3 resubscribe SSE    4 lastSyncedAt, status `online`
   */
  async function sequence(): Promise<void> {
    const db = deps.getDb();
    if (!db || syncDisabled() || !isDrainer()) return;
    try {
      const reason = await shouldResync(pullDeps(db));
      if (reason !== null) {
        await runFull(reason);
        await deps.resubscribe?.();
        return;
      }

      const pushed = await runDrain();
      if (pushed.stoppedBy === 'link' || pushed.stoppedBy === 'auth') return;

      const pulled = await runPull();
      if (pulled.fullResyncTriggered) await runFull('malformed-cursor');

      await deps.resubscribe?.();
      lastSyncedAt = now();
    } catch (err) {
      if (!isTornDown(err)) onError(err);
    } finally {
      await publish();
    }
  }

  // ── link transitions ───────────────────────────────────────────────────────

  function onUp(): void {
    retryPending = false;
    // SPEC §6.5 halts a collection so a doomed request is not retried in a LOOP.
    // It does not make the halt permanent for the life of the page, and it must
    // not: a halted collection is skipped before every pass, so nothing else can
    // ever clear it. A link re-establishment — which is also what a recovered
    // session looks like from here (§5.10 puts the watcher down with reason
    // 'auth' and up again on the next successful probe) — is a discrete,
    // rate-limited event, so re-probe once. A genuine configuration fault simply
    // halts again on the next pass, loudly, exactly as §6.5 asks.
    halted.clear();
    deps.emit('online', { at: now() });
    void publish();
    void sequence();
  }

  function onDown(reason: OfflineReason): void {
    // SPEC §3.4: cancel in-flight pushes at the entry boundary. The abort is not
    // a request cancel — a request already on the wire still records its result.
    controller.abort();
    controller = new AbortController();
    if (reason !== 'auth') deps.emit('offline', { at: now(), reason });
    void publish();
  }

  // ── the drainer's timer and the cross-tab nudge (SPEC §13.3) ───────────────

  async function poll(): Promise<void> {
    if (!isDrainer() || syncDisabled() || drainPromise) return;
    const db = deps.getDb();
    if (!db) return;
    if (!(watcher?.isOnline() ?? false)) return;
    try {
      const pending = await db.pbo.pending.count();
      if (pending > 0) await runDrain();
    } catch (err) {
      if (!isTornDown(err)) onError(err);
    }
  }

  return {
    async start(): Promise<void> {
      if (started) return;
      started = true;
      controller = new AbortController();
      halted.clear();
      retryPending = false;

      if (syncDisabled()) {
        // Anonymous: local only. No watcher, no timer, no request — ever.
        await publish();
        return;
      }

      watcher = createConnectivityWatcher({
        pb: deps.pb,
        backoff: deps.cfg.backoff,
        isDrainer,
        onUp,
        onDown,
      });
      if (deps.onNudge) offNudge = deps.onNudge(() => void poll());

      const pollMs = deps.pollMs ?? DEFAULT_POLL_MS;
      if (pollMs > 0) {
        timer = setInterval(() => void poll(), pollMs);
        (timer as unknown as { unref?: () => void }).unref?.();
      }

      // `start()` must not await the network (SPEC §2.1): the first ping and the
      // reconnect sequence it triggers run in the background.
      watcher.start();
      // Published AFTER the watcher starts, so the first observable status is
      // `connecting` rather than a stale `offline` (SPEC §2.2).
      await publish();
    },

    async stop(): Promise<void> {
      if (!started) return;
      started = false;
      controller.abort();
      if (timer !== null) {
        clearInterval(timer);
        timer = null;
      }
      offNudge?.();
      offNudge = null;
      watcher?.stop();
      watcher = null;
      await drainPromise?.catch(() => undefined);
      await chain.catch(() => undefined);
      await publishing.catch(() => undefined);
    },

    async push(): Promise<PushResult> {
      // SPEC §3.3: an explicit call pings immediately, ignoring the backoff.
      if (watcher && !watcher.isOnline()) await watcher.pingNow();
      else void watcher?.pingNow();
      return runDrain();
    },

    async pull(collections?: string[]): Promise<PullResult> {
      if (watcher && !watcher.isOnline()) await watcher.pingNow();
      else void watcher?.pingNow();
      return runPull(collections);
    },

    async sync(): Promise<void> {
      if (watcher && !watcher.isOnline()) await watcher.pingNow();
      else void watcher?.pingNow();
      await sequence();
    },

    async fullResync(reason?: string): Promise<void> {
      await runFull(reason ?? 'manual');
    },

    nudge(): void {
      void poll();
    },

    roleChanged(nowDrainer: boolean): void {
      if (!started) return;
      if (!nowDrainer) {
        // SPEC §13.6 — the entry boundary, not a request cancel.
        controller.abort();
        controller = new AbortController();
        return;
      }
      void publish();
      // A promoted tab has not pinged yet; the ping is what promotes the link,
      // and the link coming up is what runs the reconnect sequence.
      if (watcher && !watcher.isOnline()) void watcher.pingNow();
      else void sequence();
    },

    adoptOnline(next: boolean): void {
      if (!started || isDrainer()) return;
      watcher?.adopt(next);
    },

    observeSse(open: boolean): void {
      watcher?.observeSse(open);
    },
  };
}

export type { PushResult, PullResult, SignOutResult, PromotionResult };
