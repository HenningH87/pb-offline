/**
 * The link watcher (SPEC §3) — self-healing connectivity.
 *
 * This module owns ONE boolean and no other module may decide the link is up or
 * down (SPEC §18.1). It combines three inputs in a strict authority order:
 *
 *   1. a completed HTTP round trip to PB — authoritative UP, even for a 4xx
 *      (a 4xx proves the link works)
 *   2. a network-level failure of such a request — authoritative DOWN
 *   3. the SSE connection state — open implies up; a disconnect implies SUSPECT,
 *      not down
 *   4. navigator.onLine — `false` is authoritative DOWN; `true` is a HINT only.
 *      It means "the OS has an interface", not "PocketBase is reachable".
 */
import type PocketBase from 'pocketbase';
import type { Readable } from '../types.js';
import { writable } from '../store/readable.js';
import { createBackoff, type Backoff } from './backoff.js';

export type OfflineReason = 'browser' | 'sse' | 'ping' | 'auth';

export interface ConnectivityDeps {
  pb: PocketBase;
  backoff: { min: number; max: number };
  onUp: () => void;
  onDown: (reason: OfflineReason) => void;
  /**
   * SPEC §3.6: a non-drainer tab MUST NOT ping. It observes `online` from
   * `_state` instead, so N tabs do not become N pingers. Defaults to true, which
   * is what a single-tab engine wants.
   */
  isDrainer?: () => boolean;
  /** Injectable for tests. */
  random?: () => number;
  setTimeoutFn?: (fn: () => void, ms: number) => unknown;
  clearTimeoutFn?: (handle: unknown) => void;
}

export interface ConnectivityWatcher {
  readonly online: Readable<boolean>;
  /** The current value, without subscribing. */
  isOnline(): boolean;
  /** True while a health ping is in flight. */
  isPinging(): boolean;
  /**
   * True while the watcher is actively trying to re-establish the link: the
   * `connecting` half of SPEC §2.2, which counts the backoff wait as well as
   * the ping itself. False when the OS says there is no interface at all, or
   * when nothing is retrying — that is honestly `offline`.
   */
  isSeeking(): boolean;
  /** Feed an observed request outcome. Authority 1 and 2. */
  observeRequest(ok: boolean, networkFailure: boolean): void;
  /** Feed the SSE connection state. Authority 3 — a hint. */
  observeSse(open: boolean): void;
  /** The session died under us (SPEC §5.10). Down, with reason 'auth'. */
  observeAuthLoss(): void;
  /** Adopt the drainer's verdict in a non-drainer tab (SPEC §3.6). */
  adopt(online: boolean): void;
  /** Ping now, ignoring the backoff timer. */
  pingNow(): Promise<boolean>;
  /** The backoff, exposed so push retries share one schedule. */
  readonly backoff: Backoff;
  start(): void;
  stop(): void;
}

/** `false` from `navigator.onLine` is authoritative; absence of the API is not. */
function browserSaysOffline(): boolean {
  const nav = (globalThis as { navigator?: { onLine?: boolean } }).navigator;
  return nav !== undefined && nav.onLine === false;
}

/**
 * The link watcher.
 *
 * Starts DOWN. `navigator.onLine === true` is explicitly not enough to start up
 * (SPEC §3.1d): the first completed round trip is what proves the link, and
 * `start()` schedules one immediately.
 */
export function createConnectivityWatcher(deps: ConnectivityDeps): ConnectivityWatcher {
  const setTimer = deps.setTimeoutFn ?? ((fn, ms) => setTimeout(fn, ms));
  const clearTimer = deps.clearTimeoutFn ?? ((h) => clearTimeout(h as ReturnType<typeof setTimeout>));
  const isDrainer = deps.isDrainer ?? ((): boolean => true);
  const backoff = createBackoff(
    deps.random === undefined
      ? { min: deps.backoff.min, max: deps.backoff.max }
      : { min: deps.backoff.min, max: deps.backoff.max, random: deps.random },
  );

  const online = writable(false);
  let started = false;
  let timer: unknown = null;
  let inFlight: Promise<boolean> | null = null;
  /** The reason a down transition will carry if the next ping fails. */
  let suspectReason: OfflineReason = 'ping';
  const listeners: Array<() => void> = [];

  const clearPending = (): void => {
    if (timer !== null) {
      clearTimer(timer);
      timer = null;
    }
  };

  const setUp = (): void => {
    suspectReason = 'ping';
    // SPEC §3.5 — the backoff resets on ANY transition to up, and staying up
    // must not leave a stale ladder behind either.
    backoff.reset();
    clearPending();
    if (online.get()) return;
    online.set(true);
    deps.onUp();
  };

  const setDown = (reason: OfflineReason): void => {
    if (online.get()) {
      online.set(false);
      deps.onDown(reason);
    }
    schedule();
  };

  /** While down, ping on the jittered ladder. Never in a non-drainer tab. */
  function schedule(): void {
    if (!started || online.get() || timer !== null || !isDrainer()) return;
    timer = setTimer(() => {
      timer = null;
      void ping();
    }, backoff.next());
  }

  async function ping(): Promise<boolean> {
    if (inFlight) return inFlight;
    if (!isDrainer()) return online.get();
    inFlight = (async () => {
      try {
        await deps.pb.health.check();
        setUp();
        return true;
      } catch (err) {
        // A completed round trip is authority 1a even when the status is a
        // failure: the link demonstrably works. Only status 0 (a fetch-level
        // failure, a severed socket, DNS) is authoritative down.
        const status = (err as { status?: number } | null)?.status;
        const aborted = (err as { isAbort?: boolean } | null)?.isAbort === true;
        if (typeof status === 'number' && status > 0 && !aborted) {
          setUp();
          return true;
        }
        setDown(suspectReason);
        return false;
      } finally {
        inFlight = null;
      }
    })();
    return inFlight;
  }

  const onBrowserOffline = (): void => setDown('browser');
  const onBrowserOnline = (): void => {
    // SPEC §3.3: an immediate ping, ignoring the timer. `true` is only a hint,
    // so the ping — not the event — is what may promote the link.
    clearPending();
    void ping();
  };
  const onVisible = (): void => {
    const doc = (globalThis as { document?: { visibilityState?: string } }).document;
    if (doc?.visibilityState !== 'visible') return;
    clearPending();
    void ping();
  };

  function bind(): void {
    const target = globalThis as unknown as {
      addEventListener?: (t: string, f: () => void) => void;
      removeEventListener?: (t: string, f: () => void) => void;
      document?: {
        addEventListener?: (t: string, f: () => void) => void;
        removeEventListener?: (t: string, f: () => void) => void;
      };
    };
    if (typeof target.addEventListener === 'function') {
      target.addEventListener('offline', onBrowserOffline);
      target.addEventListener('online', onBrowserOnline);
      listeners.push(() => target.removeEventListener?.('offline', onBrowserOffline));
      listeners.push(() => target.removeEventListener?.('online', onBrowserOnline));
    }
    const doc = target.document;
    if (doc && typeof doc.addEventListener === 'function') {
      doc.addEventListener('visibilitychange', onVisible);
      listeners.push(() => doc.removeEventListener?.('visibilitychange', onVisible));
    }
  }

  return {
    online: { subscribe: online.subscribe },
    backoff,
    isOnline: () => online.get(),
    isPinging: () => inFlight !== null,
    isSeeking: () => started && !online.get() && isDrainer() && !browserSaysOffline(),

    observeRequest(ok, networkFailure) {
      if (networkFailure) {
        setDown('ping');
        return;
      }
      // `ok` false with no network failure is an HTTP status — authority 1a.
      void ok;
      setUp();
    },

    observeSse(open) {
      if (open) {
        setUp();
        return;
      }
      // SPEC §3.1c: a disconnect implies SUSPECT, not down. Probe, and let the
      // probe decide — but remember what to blame if it fails.
      if (!started) return;
      suspectReason = 'sse';
      clearPending();
      void ping();
    },

    observeAuthLoss() {
      setDown('auth');
    },

    adopt(next) {
      // A non-drainer tab takes the drainer's verdict verbatim; it never pings.
      if (next === online.get()) return;
      online.set(next);
      if (next) deps.onUp();
      else deps.onDown('ping');
    },

    pingNow: () => ping(),

    start() {
      if (started) return;
      started = true;
      bind();
      if (browserSaysOffline()) {
        setDown('browser');
        return;
      }
      // SPEC §3.3: an explicit start is an explicit reconnection attempt.
      void ping();
    },

    stop() {
      started = false;
      clearPending();
      while (listeners.length > 0) listeners.pop()?.();
      online.set(false);
    },
  };
}
