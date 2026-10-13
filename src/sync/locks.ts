/**
 * Drainer election (SPEC §13).
 *
 * Exactly one tab per open database performs push, pull and SSE. Every tab is
 * fully reactive regardless, because Dexie's liveQuery observes IndexedDB across
 * tabs — a record written by the drainer's pull re-emits everywhere.
 *
 * Election is the Web Locks API: hold `${lockPrefix}:drain:${dbName}`
 * indefinitely; other tabs request the same lock and are promoted when the holder
 * goes away. No polling, no heartbeat, no split-brain — the browser owns the
 * liveness question.
 *
 * Where Web Locks is unavailable (older Safari, an insecure context, a non-browser
 * host) the fallback is a lease row in `_state`, claimed by compare-and-set inside
 * a Dexie transaction. It is best-effort by construction and SPEC §13.5 says so:
 * a duplicate drain is possible and survivable, because every push is either
 * idempotent (client-supplied ids, §4.4) or resolved by §5.7.
 */
import type { PBODexie } from '../db/schema.js';
import { channelNameFor, drainLockNameFor } from '../brand.js';
import { claimLease, heartbeat, readState, releaseLease } from '../db/state.js';

/** Lease renewal interval (SPEC §13.5). */
export const LEASE_RENEW_MS = 2_000;
/** A lease unrenewed for this long may be stolen (SPEC §13.5). */
export const LEASE_STEAL_MS = 6_000;

/**
 * The slice of `LockManager` this module uses.
 *
 * Declared structurally rather than imported from lib.dom so the package builds
 * in a DOM-free `tsconfig` and so a test can inject a double.
 */
export interface LockManagerLike {
  request(
    name: string,
    options: { mode?: 'exclusive' | 'shared'; ifAvailable?: boolean },
    callback: (lock: unknown) => unknown,
  ): Promise<unknown>;
}

export interface DrainerHandle {
  readonly isDrainer: boolean;
  readonly tabId: string;
  /** How the role was decided — reported so a caller can log the fallback. */
  readonly mode: 'web-locks' | 'lease';
  /** Fires when this tab gains or loses the role. */
  onChange(cb: (isDrainer: boolean) => void): () => void;
  release(): Promise<void>;
}

export interface ElectionDeps {
  db: PBODexie;
  /** The Dexie database name; the lock name is derived from it. */
  dbName: string;
  tabId: string;
  /**
   * Injectable lock manager. `undefined` ⇒ `navigator.locks`; `null` ⇒ force the
   * lease fallback (which is how a test exercises SPEC §13.5).
   */
  locks?: LockManagerLike | null;
  /** Called exactly once, before the first lease claim, when falling back. */
  onFallback?: () => void;
  renewMs?: number;
  staleMs?: number;
  setTimeoutFn?: (fn: () => void, ms: number) => unknown;
  clearTimeoutFn?: (handle: unknown) => void;
}

/** The platform lock manager, or null when the context cannot provide one. */
export function webLockManager(): LockManagerLike | null {
  const nav = (globalThis as { navigator?: { locks?: unknown } }).navigator;
  const locks = nav?.locks as LockManagerLike | undefined;
  return locks && typeof locks.request === 'function' ? locks : null;
}

/** True when the Web Locks API is usable in this context. */
export function hasWebLocks(): boolean {
  return webLockManager() !== null;
}

/**
 * Contest the drainer lock for a database.
 *
 * Resolves once the FIRST election attempt has settled — either this tab holds
 * the role, or it is queued behind the tab that does. It never blocks waiting for
 * promotion, because `start()` must return in every tab, not only the winner's.
 */
export async function electDrainer(deps: ElectionDeps): Promise<DrainerHandle> {
  const listeners = new Set<(v: boolean) => void>();
  let isDrainer = false;
  let released = false;

  const setRole = (next: boolean): void => {
    if (next === isDrainer || (released && next)) return;
    isDrainer = next;
    for (const cb of [...listeners]) cb(next);
  };

  const manager = deps.locks === undefined ? webLockManager() : deps.locks;

  /**
   * Built rather than spread: `isDrainer` has to stay a live getter over the
   * closure, and a spread would freeze whatever it read at construction time.
   */
  const handleFor = (mode: DrainerHandle['mode'], release: () => Promise<void>): DrainerHandle => ({
    get isDrainer() {
      return isDrainer;
    },
    tabId: deps.tabId,
    mode,
    onChange(cb: (v: boolean) => void) {
      listeners.add(cb);
      return () => listeners.delete(cb);
    },
    release,
  });

  if (manager) {
    const name = drainLockNameFor(deps.dbName);
    let releaseHold!: () => void;
    const held = new Promise<void>((resolve) => (releaseHold = resolve));

    /**
     * `ifAvailable` first, so the outcome of the first attempt is a fact rather
     * than a race with a timer: the callback is handed `null` when another tab
     * holds the lock, and the grant otherwise. A blocking request would tell us
     * the same thing only by never resolving.
     */
    const outcome = await new Promise<'granted' | 'busy'>((resolve) => {
      void manager
        .request(name, { mode: 'exclusive', ifAvailable: true }, (lock) => {
          if (lock === null || lock === undefined) {
            resolve('busy');
            return undefined;
          }
          resolve('granted');
          return held; // hold it until release()
        })
        .catch(() => resolve('busy'));
    });

    if (outcome === 'granted') {
      setRole(true);
    } else {
      // Queue for promotion. The holder going away — released, tab closed,
      // crashed — is what resolves this, and the browser owns that question.
      void manager
        .request(name, { mode: 'exclusive' }, () => {
          if (released) return undefined;
          setRole(true);
          return held;
        })
        .catch(() => undefined);
    }

    return handleFor('web-locks', async () => {
      if (released) return;
      released = true;
      setRole(false);
      releaseHold();
    });
  }

  // ── the lease fallback (SPEC §13.5) ────────────────────────────────────────
  deps.onFallback?.();

  const setTimer = deps.setTimeoutFn ?? ((fn, ms) => setTimeout(fn, ms));
  const clearTimer = deps.clearTimeoutFn ?? ((h) => clearTimeout(h as ReturnType<typeof setTimeout>));
  const renewMs = deps.renewMs ?? LEASE_RENEW_MS;
  const staleMs = deps.staleMs ?? LEASE_STEAL_MS;
  let timer: unknown = null;

  const schedule = (): void => {
    if (released || timer !== null) return;
    timer = setTimer(() => {
      timer = null;
      void tick();
    }, renewMs);
    (timer as { unref?: () => void })?.unref?.();
  };

  /**
   * One lease turn: renew what we hold, notice a steal, or try to take a lease
   * nobody has renewed inside the stale window.
   *
   * A recursive `setTimeout` rather than `setInterval` (SPEC §18.5 reserves that
   * for the engine and the watcher) and, more importantly, so a slow Dexie turn
   * can never queue two overlapping claims.
   */
  async function tick(): Promise<void> {
    if (released) return;
    try {
      if (isDrainer) {
        const row = await readState(deps.db);
        if (row?.drainerTabId !== deps.tabId) setRole(false);
        else await heartbeat(deps.db, deps.tabId);
      }
      if (!isDrainer && !released) {
        if (await claimLease(deps.db, deps.tabId, staleMs)) setRole(true);
      }
    } catch (err) {
      // A closed database ends the lease loop; anything else is retried on the
      // next turn rather than surfaced — the lease is best-effort by design.
      if ((err as { name?: string } | null)?.name === 'DatabaseClosedError') {
        released = true;
        setRole(false);
        return;
      }
    }
    schedule();
  }

  await tick();

  return handleFor('lease', async () => {
    if (released) return;
    released = true;
    if (timer !== null) {
      clearTimer(timer);
      timer = null;
    }
    const wasDrainer = isDrainer;
    setRole(false);
    // Hand the lease back rather than making the next tab wait out the stale
    // window for a departure we know about.
    if (wasDrainer) {
      try {
        await releaseLease(deps.db, deps.tabId);
      } catch {
        /* a closed database has no lease to hand back */
      }
    }
  });
}

/**
 * Cross-tab nudge so the drainer wakes on another tab's write immediately
 * (SPEC §13.3). An OPTIMISATION — correctness MUST NOT depend on it, because the
 * drainer also polls `_pending` on a timer.
 */
export function createNudgeChannel(dbName: string): {
  nudge: () => void;
  onNudge: (cb: () => void) => () => void;
  close: () => void;
} {
  const listeners = new Set<() => void>();
  let channel: BroadcastChannel | null = null;

  if (typeof BroadcastChannel === 'function') {
    channel = new BroadcastChannel(channelNameFor(dbName));
    channel.onmessage = (ev: MessageEvent): void => {
      if ((ev.data as { t?: string } | null)?.t !== 'nudge') return;
      for (const cb of [...listeners]) cb();
    };
  }

  return {
    nudge() {
      try {
        channel?.postMessage({ t: 'nudge' });
      } catch {
        /* a closed channel is not a write failure */
      }
    },
    onNudge(cb) {
      listeners.add(cb);
      return () => listeners.delete(cb);
    },
    close() {
      listeners.clear();
      try {
        channel?.close();
      } catch {
        /* already closed */
      }
      channel = null;
    },
  };
}
