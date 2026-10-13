/**
 * The Svelte store contract (SPEC §2.5).
 *
 * `subscribe(fn) => unsubscribe`, emitting synchronously on subscribe. That is
 * the whole framework story: native in Svelte, `useSyncExternalStore` in React,
 * a callback in vanilla. No framework package ships in v0 (SPEC §16.4).
 */
import { liveQuery, type Subscription } from 'dexie';
import type { Readable, Unsubscriber } from '../types.js';

export interface Writable<T> {
  subscribe: (run: (v: T) => void) => Unsubscriber;
  set: (v: T) => void;
  update: (fn: (v: T) => T) => void;
  get: () => T;
}

interface Sub<T> {
  run: (v: T) => void;
  live: boolean;
}

/**
 * A writable source the engine pushes into (status, pendingCount, …).
 *
 * The four normative properties of SPEC §2.5 are all enforced here: synchronous
 * first emission, an unsubscriber that stops delivery, no delivery after it, and
 * no reference-equal re-emission.
 */
export function writable<T>(initial: T): Writable<T> {
  let value = initial;
  const subs = new Set<Sub<T>>();

  const set = (next: T): void => {
    if (Object.is(next, value)) return;
    value = next;
    // Snapshot: a handler may subscribe or unsubscribe during delivery, and a
    // subscriber added mid-emission has already had its synchronous first value.
    for (const s of [...subs]) {
      if (s.live) s.run(value);
    }
  };

  return {
    subscribe(run) {
      const sub: Sub<T> = { run, live: true };
      subs.add(sub);
      run(value);
      return () => {
        sub.live = false;
        subs.delete(sub);
      };
    },
    set,
    update: (fn) => set(fn(value)),
    get: () => value,
  };
}

/** Map a store. Emits only when the mapped value changes by reference. */
export function derived<T, U>(src: Readable<T>, fn: (v: T) => U): Readable<U> {
  return {
    subscribe(run) {
      let has = false;
      let last: U;
      let live = true;
      const off = src.subscribe((v) => {
        if (!live) return;
        const next = fn(v);
        if (has && Object.is(next, last)) return;
        has = true;
        last = next;
        run(next);
      });
      return () => {
        live = false;
        off();
      };
    },
  };
}

/** A store that never changes. Used where a layer is not wired up yet. */
export function constant<T>(value: T): Readable<T> {
  return {
    subscribe(run) {
      run(value);
      return () => {};
    },
  };
}

/**
 * A monotonic "the thing underneath changed identity" signal (added v0.1 after
 * first-consumer feedback — SPEC §2.3, "Reactive reads outlive the mirror").
 *
 * Why it exists: Dexie's `liveQuery` re-runs a querier only when a write touches
 * a table that querier READ on its previous run, and the observability keys it
 * records name the DATABASE, not the Dexie instance. So a querier that ran while
 * no database was open recorded an EMPTY set and can never be woken, and one
 * that ran against the outgoing owner's database observes keys that the incoming
 * owner's writes never touch. Both are permanently-dead subscriptions that look
 * exactly like a filter bug.
 *
 * A revision is not a value channel: it carries no data and nobody renders it.
 * It exists so `fromLiveQuery` can tear down and rebuild its `liveQuery` at the
 * few moments the mirror underneath changed identity.
 */
export interface Revision {
  /** The current revision. Steps by one on each identity change. */
  get(): number;
  /** Called AFTER each step, never on subscribe. Returns an unsubscriber. */
  onChange(fn: (revision: number) => void): Unsubscriber;
}

/** A `Revision` with the write end attached. Held by whoever owns the identity. */
export interface RevisionSource extends Revision {
  /** Announce an identity change. Listeners run synchronously. */
  step(): number;
}

export function createRevision(): RevisionSource {
  let revision = 0;
  const listeners = new Set<{ fn: (n: number) => void; live: boolean }>();
  return {
    get: () => revision,
    onChange(fn) {
      const l = { fn, live: true };
      listeners.add(l);
      return () => {
        l.live = false;
        listeners.delete(l);
      };
    },
    step() {
      revision += 1;
      // Snapshot: a listener rebuilding its subscription may add or drop one.
      for (const l of [...listeners]) {
        if (l.live) l.fn(revision);
      }
      return revision;
    },
  };
}

/**
 * Wrap a Dexie `liveQuery` Observable in the store contract.
 *
 * The subtlety: `liveQuery` emits ASYNCHRONOUSLY on its first read, but the
 * contract demands a synchronous first emission — so emit `emptyValue`
 * immediately and let the real value follow. A consumer must be able to render
 * before IndexedDB has answered.
 *
 * The underlying subscription is opened on the first subscriber and released
 * when the last one leaves, so an unused store costs nothing.
 *
 * `revision`, when given, is the mirror's identity signal (SPEC §2.3). Each step
 * DISCARDS the current `liveQuery` and builds a new one, which re-runs the
 * querier against whatever database is open now and records a fresh observability
 * set. That, and not a second notification path, is what keeps a store subscribed
 * before `start()` — or across an auth switch — alive.
 */
export function fromLiveQuery<T>(
  query: () => Promise<T> | T,
  emptyValue: T,
  onError?: (err: unknown) => void,
  revision?: Revision,
): Readable<T> {
  let value = emptyValue;
  let observed: Subscription | null = null;
  let offRevision: Unsubscriber | null = null;
  const subs = new Set<Sub<T>>();

  const push = (next: T): void => {
    if (Object.is(next, value)) return;
    value = next;
    for (const s of [...subs]) {
      if (s.live) s.run(value);
    }
  };

  const observe = (): void => {
    observed = liveQuery(query).subscribe({
      next: (v) => push(v as T),
      error: (err) => onError?.(err),
    });
  };

  /**
   * The mirror changed identity. Drop the old observation — its observability
   * set names a database that is closed, deleted or was never open — and take a
   * new one. Nothing is emitted here: the rebuilt querier reads the CURRENT
   * database (or none, and returns the declared empty value) and the result
   * arrives through `push` like any other frame.
   */
  const rebuild = (): void => {
    if (!observed) return;
    observed.unsubscribe();
    observed = null;
    observe();
  };

  const open = (): void => {
    if (observed) return;
    observe();
    if (revision && !offRevision) offRevision = revision.onChange(rebuild);
  };

  const closeIfIdle = (): void => {
    if (subs.size > 0 || !observed) return;
    observed.unsubscribe();
    observed = null;
    // The revision listener is part of the subscription, not of the store: an
    // idle store must cost nothing and must not be woken by an auth switch.
    offRevision?.();
    offRevision = null;
    // The next subscriber must not be handed a value the store stopped
    // maintaining; reset to the declared empty value.
    value = emptyValue;
  };

  return {
    subscribe(run) {
      const sub: Sub<T> = { run, live: true };
      subs.add(sub);
      run(value);
      open();
      return () => {
        if (!sub.live) return;
        sub.live = false;
        subs.delete(sub);
        closeIfIdle();
      };
    },
  };
}

/** One-shot read of a store's current value. Test and internal use. */
export function get<T>(store: Readable<T>): T {
  let out!: T;
  const off = store.subscribe((v) => {
    out = v;
  });
  off();
  return out;
}
