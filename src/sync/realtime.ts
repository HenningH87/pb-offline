/**
 * Realtime / SSE (SPEC §9).
 *
 * Freshness while online, and never the only path. Two rules carry the section:
 *
 *  - Subscribe NARROWLY — one subscription per collection, '*', with the owner
 *    filter. PocketBase evaluates every connected client's subscriptions on every
 *    mutation, so an unfiltered subscription is a server cost multiplied by the
 *    number of connected clients.
 *
 *  - An SSE event MUST NOT advance the pull cursor. SSE delivery is unordered
 *    with respect to the cursor stream and can drop events across a reconnect;
 *    advancing the cursor from an event would skip every record between the
 *    cursor and that event. This is the most consequential rule in the module.
 *
 * The second rule is kept structurally rather than by discipline: this module
 * calls `conflict.applyRemote` and `tombstones.applyTombstone`, and neither of
 * them can write `_cursors` (SPEC §18.2 puts those writes in `pull.ts` and
 * `tombstones.ts` alone). There is no SSE-specific apply path at all.
 */
import type PocketBase from 'pocketbase';
import type { PBOEvents, PBRecord, ResolvedConfig } from '../types.js';
import type { PBODexie } from '../db/schema.js';
import { defaultFilter, expandFilter } from '../config.js';
import { applyRemote } from './conflict.js';
import { applyTombstone, type Tombstone } from './tombstones.js';
import type { PullDeps } from './pull.js';

export interface RealtimeDeps {
  pb: PocketBase;
  db: PBODexie;
  cfg: ResolvedConfig;
  getOwnerId: () => string | null;
  /** SSE connection state feeds the link watcher as a hint, never as authority. */
  onConnectionChange: (open: boolean) => void;
  emit: <E extends keyof PBOEvents>(event: E, payload: PBOEvents[E]) => void;
  onError?: (err: unknown) => void;
  /**
   * False once this tab has stopped or lost the drainer lock. An event already in
   * the SDK's queue when the role changed MUST NOT be applied (SPEC §13.6).
   */
  isActive?: () => boolean;
}

export interface Realtime {
  /**
   * Subscribe every configured collection plus the tombstone collection.
   *
   * Idempotent: this is what SPEC §3.1 step 3 calls on every reconnect, and a
   * live subscription set is left alone rather than cycled — a teardown and
   * re-subscribe per sync would churn the server's subscription table for
   * nothing.
   */
  subscribe(): Promise<void>;
  /** Tear down every subscription. Called on stop, auth change and lock loss. */
  unsubscribe(): Promise<void>;
  isSubscribed(): boolean;
  /** Awaits the applies already queued. Test seam; not part of the engine's path. */
  settled(): Promise<void>;
}

/** The realtime client surface this module touches, structurally typed. */
interface RealtimeClient {
  isConnected?: boolean;
  onDisconnect?: ((activeSubscriptions: string[]) => void) | undefined;
  subscribe?: (topic: string, cb: (data: unknown) => void) => Promise<() => Promise<void>>;
}

type Unsub = () => Promise<void> | void;

interface SsePayload {
  action?: string;
  record?: PBRecord;
}

/**
 * Build the realtime binding for one open database.
 *
 * Only the drainer tab constructs and drives this (SPEC §9.2) — a per-tab
 * connection multiplies the server's connection count by the user's tab count for
 * no benefit, since Dexie's liveQuery already carries the applied changes across
 * tabs.
 */
export function createRealtime(deps: RealtimeDeps): Realtime {
  const unsubs: Unsub[] = [];
  let subscribed = false;
  /**
   * True from the moment the FIRST subscription is created until teardown.
   *
   * Distinct from `subscribed`, which only flips once the last subscription has
   * resolved. Gating applies on `subscribed` dropped every event delivered for
   * collection 1 while collections 2..N were still subscribing — and §3.1 runs
   * the cursor pull BEFORE subscribing, so such a change is already past the
   * cursor and not yet on the stream: invisible until the next reconnect or a
   * manual `sync()`, which on a long-lived connection can be days.
   *
   * Applying is safe this early: `applyRemote`/`applyTombstone` are the same path
   * a pulled record takes, they are idempotent, and neither can touch `_cursors`
   * (§9.4). Teardown still stops delivery, and `isActive()` still covers §13.6.
   */
  let accepting = false;
  let inFlight: Promise<void> | null = null;
  let previousOnDisconnect: ((active: string[]) => void) | undefined;
  let connectUnsub: (() => Promise<void>) | null = null;

  /** Applies serialise: two events for one record would race read-then-write. */
  let chain: Promise<void> = Promise.resolve();
  const run = (fn: () => Promise<void>): void => {
    chain = chain.then(fn, fn).then(
      () => undefined,
      (err) => {
        deps.onError?.(err);
      },
    );
  };

  const active = (): boolean => accepting && (deps.isActive?.() ?? true);

  /**
   * The dependency bundle `applyTombstone` takes. It reads `db`, `cfg` and `emit`
   * and nothing else; the rest is present because the type is the pull's.
   */
  const tombstoneDeps = (): PullDeps => ({
    pb: deps.pb,
    db: deps.db,
    cfg: deps.cfg,
    getOwnerId: deps.getOwnerId,
    abort: new AbortController().signal,
    emit: deps.emit,
    halted: new Map(),
    isOnline: () => true,
  });

  /** SPEC §9.3 and §9.6 — the SAME path a pulled record takes. */
  function onRecordEvent(collection: string, payload: SsePayload): void {
    if (!active()) return;
    const record = payload?.record;
    const id = typeof record?.id === 'string' ? record.id : '';
    if (id === '') return;

    run(async () => {
      if (!active()) return;
      if (payload.action === 'delete') {
        // §9.6: the event carries the same authority as a tombstone, so it takes
        // the tombstone's apply — queued-create preservation and the loud
        // dirty-record conflict included (§8.3–8.4).
        await applyTombstone(tombstoneDeps(), {
          id,
          collection,
          record_id: id,
          updated: typeof record?.updated === 'string' ? record.updated : '',
        });
        return;
      }
      const emit = deps.emit;
      const applyDeps = deps.onError === undefined ? { emit } : { emit, onError: deps.onError };
      await applyRemote(deps.db, deps.cfg, collection, record as PBRecord, applyDeps);
    });
  }

  /** The tombstone collection's own stream: a new row IS a delete to apply. */
  function onTombstoneEvent(payload: SsePayload): void {
    if (!active()) return;
    const row = payload?.record as unknown as Tombstone | undefined;
    if (!row || payload.action !== 'create') return;
    run(async () => {
      if (!active()) return;
      await applyTombstone(tombstoneDeps(), row);
    });
  }

  function realtimeClient(): RealtimeClient | null {
    const rt = (deps.pb as unknown as { realtime?: RealtimeClient }).realtime;
    return rt && typeof rt === 'object' ? rt : null;
  }

  /**
   * Bind the connection hints (SPEC §3.1c). A disconnect is SUSPECT, never
   * authoritative down — the watcher probes and decides.
   */
  function bindConnection(): void {
    const rt = realtimeClient();
    if (!rt) return;
    previousOnDisconnect = rt.onDisconnect;
    rt.onDisconnect = (activeSubscriptions: string[]): void => {
      previousOnDisconnect?.(activeSubscriptions);
      if (!subscribed) return;
      // No active subscriptions means WE unsubscribed; that is not a link event.
      if (Array.isArray(activeSubscriptions) && activeSubscriptions.length === 0) return;
      deps.onConnectionChange(false);
    };
    if (typeof rt.subscribe === 'function') {
      void rt
        .subscribe('PB_CONNECT', () => {
          if (subscribed) deps.onConnectionChange(true);
        })
        .then((fn) => {
          connectUnsub = fn;
        })
        .catch(() => undefined);
    }
  }

  function unbindConnection(): void {
    const rt = realtimeClient();
    if (rt) rt.onDisconnect = previousOnDisconnect;
    previousOnDisconnect = undefined;
    const off = connectUnsub;
    connectUnsub = null;
    if (off) void Promise.resolve(off()).catch(() => undefined);
  }

  async function tearDown(): Promise<void> {
    subscribed = false;
    accepting = false;
    unbindConnection();
    const fns = unsubs.splice(0, unsubs.length);
    for (const fn of fns) {
      try {
        await fn();
      } catch {
        /* a socket already gone needs no closing */
      }
    }
  }

  async function subscribeAll(): Promise<void> {
    const owner = deps.getOwnerId();
    // SPEC §14.3: with no session nothing is pushed and nothing is subscribed —
    // an owner filter with no owner is unsatisfiable, and an unfiltered
    // subscription is exactly what §9.1 forbids.
    if (owner === null) return;
    accepting = true;

    const sub = async (collection: string, filter: string | null, cb: (data: unknown) => void): Promise<void> => {
      const options = filter === null ? {} : { filter };
      const off = (await deps.pb.collection(collection).subscribe('*', cb as never, options)) as unknown as Unsub;
      unsubs.push(off);
    };

    try {
      for (const c of deps.cfg.collections) {
        const filter = expandFilter(c.filter, owner);
        await sub(c.name, filter, (data) => onRecordEvent(c.name, data as SsePayload));
      }
      await sub(deps.cfg.deletionsCollection, expandFilter(defaultFilter(deps.cfg.ownerField), owner), (data) =>
        onTombstoneEvent(data as SsePayload),
      );
    } catch (err) {
      // A half-subscribed set is worse than none: unwind, report, and let the
      // next reconnect try again. SSE is never the only path (§9.5).
      await tearDown();
      deps.onError?.(err);
      deps.onConnectionChange(false);
      return;
    }

    subscribed = true;
    bindConnection();
    deps.onConnectionChange(true);
  }

  return {
    async subscribe(): Promise<void> {
      if (!deps.cfg.realtime.enabled) return;
      if (inFlight) return inFlight;
      if (subscribed) return;
      inFlight = subscribeAll().finally(() => {
        inFlight = null;
      });
      return inFlight;
    },

    async unsubscribe(): Promise<void> {
      if (inFlight) await inFlight.catch(() => undefined);
      if (!subscribed && unsubs.length === 0) {
        unbindConnection();
        return;
      }
      await tearDown();
      await chain.catch(() => undefined);
    },

    isSubscribed: () => subscribed,
    settled: () => chain.catch(() => undefined),
  };
}
