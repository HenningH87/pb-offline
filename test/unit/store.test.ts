/** SPEC §2.4, §2.5 — the store contract and the event emitter. TESTPLAN §1.4. */
import { describe, expect, it, vi } from 'vitest';
import Dexie from 'dexie';
import { constant, createRevision, derived, fromLiveQuery, get, writable } from '../../src/store/readable.js';
import { createEmitter } from '../../src/store/emitter.js';
import { tick, uniqueAppId } from './_helpers.js';

describe('writable — the four normative properties (SPEC §2.5)', () => {
  it('1. calls run synchronously with the current value before returning', () => {
    const store = writable(7);
    const seen: number[] = [];
    let returned = false;
    const off = store.subscribe((v) => {
      expect(returned).toBe(false); // ran before subscribe returned
      seen.push(v);
    });
    returned = true;
    expect(seen).toEqual([7]);
    off();
  });

  it('2 & 3. the unsubscriber stops delivery, and nothing arrives after it', () => {
    const store = writable(0);
    const seen: number[] = [];
    const off = store.subscribe((v) => seen.push(v));
    store.set(1);
    off();
    store.set(2);
    store.set(3);
    expect(seen).toEqual([0, 1]);
  });

  it('3b. a value set DURING delivery does not reach an unsubscribed handler', () => {
    const store = writable(0);
    const seen: number[] = [];
    const off = store.subscribe((v) => {
      seen.push(v);
      if (v === 1) off();
    });
    store.set(1);
    store.set(2);
    expect(seen).toEqual([0, 1]);
  });

  it('4. never emits a value equal by reference to the previous one', () => {
    const a = { n: 1 };
    const store = writable(a);
    const seen: unknown[] = [];
    store.subscribe((v) => seen.push(v));
    store.set(a);
    store.set({ n: 1 }); // deep-equal but a new reference: MUST emit
    expect(seen).toHaveLength(2);
  });

  it('supports several independent subscribers and update()', () => {
    const store = writable(1);
    const a: number[] = [];
    const b: number[] = [];
    const offA = store.subscribe((v) => a.push(v));
    store.subscribe((v) => b.push(v));
    store.update((v) => v + 1);
    offA();
    store.update((v) => v + 1);
    expect(a).toEqual([1, 2]);
    expect(b).toEqual([1, 2, 3]);
    expect(get(store)).toBe(3);
  });

  it('double-unsubscribe is safe', () => {
    const store = writable(1);
    const off = store.subscribe(() => {});
    off();
    expect(() => off()).not.toThrow();
  });
});

describe('derived / constant', () => {
  it('emits synchronously and suppresses reference-equal mappings', () => {
    const src = writable({ status: 'offline', n: 0 });
    const status = derived(src, (s) => s.status);
    const seen: string[] = [];
    status.subscribe((v) => seen.push(v));
    src.set({ status: 'offline', n: 1 }); // status unchanged ⇒ no emission
    src.set({ status: 'online', n: 2 });
    expect(seen).toEqual(['offline', 'online']);
  });

  it('stops mapping after unsubscribe', () => {
    const src = writable(1);
    const seen: number[] = [];
    const off = derived(src, (v) => v * 2).subscribe((v) => seen.push(v));
    off();
    src.set(2);
    expect(seen).toEqual([2]);
  });

  it('constant emits once and never again', () => {
    const seen: number[] = [];
    const off = constant(5).subscribe((v) => seen.push(v));
    off();
    expect(seen).toEqual([5]);
  });
});

describe('fromLiveQuery — the Dexie bridge (SPEC §2.5.1)', () => {
  async function openDb(): Promise<Dexie & { items: Dexie.Table<{ id: string; n: number }, string> }> {
    const db = new Dexie(`store-test-${uniqueAppId()}`);
    db.version(1).stores({ items: 'id' });
    await db.open();
    return db as never;
  }

  it('emits the declared empty value synchronously, then the real value', async () => {
    const db = await openDb();
    await db.table('items').put({ id: 'a', n: 1 });

    const store = fromLiveQuery(() => db.table('items').toArray(), [] as unknown[]);
    const seen: unknown[][] = [];
    const off = store.subscribe((v) => seen.push(v as unknown[]));
    expect(seen).toEqual([[]]); // synchronous, before IndexedDB answered

    await tick();
    expect(seen).toHaveLength(2);
    expect(seen[1]).toEqual([{ id: 'a', n: 1 }]);
    off();
    await db.delete();
  });

  it('re-emits on a write and never emits after unsubscribe', async () => {
    const db = await openDb();
    const store = fromLiveQuery(() => db.table('items').toArray(), [] as unknown[]);
    const seen: unknown[][] = [];
    const off = store.subscribe((v) => seen.push(v as unknown[]));
    await tick();
    await db.table('items').put({ id: 'a', n: 1 });
    await tick();
    const countAtUnsubscribe = seen.length;
    expect(countAtUnsubscribe).toBeGreaterThanOrEqual(2);

    off();
    await db.table('items').put({ id: 'b', n: 2 });
    await tick(20);
    expect(seen).toHaveLength(countAtUnsubscribe);
    await db.delete();
  });

  it('releases the liveQuery only when the LAST subscriber leaves', async () => {
    const db = await openDb();
    let reads = 0;
    const store = fromLiveQuery(async () => {
      reads += 1;
      return db.table('items').toArray();
    }, [] as unknown[]);

    const offA = store.subscribe(() => {});
    const offB = store.subscribe(() => {});
    await tick();
    const afterOpen = reads;

    offA();
    await db.table('items').put({ id: 'a', n: 1 });
    await tick();
    expect(reads).toBeGreaterThan(afterOpen); // B still observing

    const afterB = reads;
    offB();
    await db.table('items').put({ id: 'b', n: 2 });
    await tick(20);
    expect(reads).toBe(afterB);
    await db.delete();
  });

  it('routes a query error to the caller instead of throwing into the app', async () => {
    const errors: unknown[] = [];
    const store = fromLiveQuery(
      async () => {
        throw new Error('query blew up');
      },
      'empty',
      (e) => errors.push(e),
    );
    const seen: string[] = [];
    const off = store.subscribe((v) => seen.push(v));
    await tick();
    expect(seen).toEqual(['empty']);
    expect(errors).toHaveLength(1);
    off();
  });
});

/**
 * SPEC §2.3, "Reactive reads outlive the mirror" (added v0.1 after
 * first-consumer feedback).
 *
 * The defect these pin down: Dexie's `liveQuery` re-runs a querier only when a
 * write touches a table that querier READ on its previous run, and it keys that
 * observability on the database NAME. A querier that ran with no database open
 * read nothing, so nothing can ever overlap it — the subscription is dead for
 * good, silently, while writes keep working. The same hole opens across an
 * account switch: the observed keys name the outgoing owner's database.
 */
describe('fromLiveQuery — a revision keeps a store alive (SPEC §2.3)', () => {
  /** A mirror that starts absent and can be swapped, announcing each change. */
  function mirror(): {
    revision: ReturnType<typeof createRevision>;
    get(): (Dexie & { table: Dexie['table'] }) | undefined;
    set(db: Dexie | undefined): void;
  } {
    const revision = createRevision();
    let current: Dexie | undefined;
    return {
      revision,
      get: () => current as never,
      set(db) {
        current = db;
        revision.step();
      },
    };
  }

  async function openDb(name = `rev-${uniqueAppId()}`): Promise<Dexie> {
    const db = new Dexie(name);
    db.version(1).stores({ items: 'id' });
    await db.open();
    return db;
  }

  it('WITHOUT a revision the store is dead once it has run with no database', async () => {
    const m = mirror();
    const store = fromLiveQuery(async () => {
      const db = m.get();
      return db ? db.table('items').toArray() : [];
    }, [] as unknown[]);

    const seen: unknown[][] = [];
    const off = store.subscribe((v) => seen.push(v as unknown[]));
    await tick();

    const db = await openDb();
    m.set(db);
    await db.table('items').put({ id: 'a', n: 1 });
    await tick(20);

    // This is the bug, asserted so the fix cannot be mistaken for a coincidence.
    expect(seen.flat()).toEqual([]);
    off();
    await db.delete();
  });

  it('WITH a revision it comes alive when the mirror opens', async () => {
    const m = mirror();
    const store = fromLiveQuery(
      async () => {
        const db = m.get();
        return db ? db.table('items').toArray() : [];
      },
      [] as unknown[],
      undefined,
      m.revision,
    );

    const seen: unknown[][] = [];
    const off = store.subscribe((v) => seen.push(v as unknown[]));
    expect(seen).toEqual([[]]); // synchronous first emission, unchanged
    await tick();

    const db = await openDb();
    m.set(db);
    await tick();
    await db.table('items').put({ id: 'a', n: 1 });
    await tick(20);

    expect(seen[seen.length - 1]).toEqual([{ id: 'a', n: 1 }]);
    off();
    await db.delete();
  });

  it('survives the mirror being swapped for another one — the auth-switch shape', async () => {
    const m = mirror();
    const a = await openDb();
    const b = await openDb();
    await a.table('items').put({ id: 'a', n: 1 });
    await b.table('items').put({ id: 'b', n: 2 });

    const store = fromLiveQuery(
      async () => {
        const db = m.get();
        return db ? db.table('items').toArray() : [];
      },
      [] as unknown[],
      undefined,
      m.revision,
    );
    const seen: unknown[][] = [];
    const off = store.subscribe((v) => seen.push(v as unknown[]));

    m.set(a);
    await tick(20);
    expect(seen[seen.length - 1]).toEqual([{ id: 'a', n: 1 }]);

    // The manager's two steps: the outgoing database closes, the incoming opens.
    m.set(undefined);
    m.set(b);
    await tick(20);
    expect(seen[seen.length - 1]).toEqual([{ id: 'b', n: 2 }]);

    // …and a write under the NEW owner re-emits, which is the whole point.
    await b.table('items').put({ id: 'c', n: 3 });
    await tick(20);
    expect(seen[seen.length - 1]).toEqual([
      { id: 'b', n: 2 },
      { id: 'c', n: 3 },
    ]);

    off();
    await a.delete();
    await b.delete();
  });

  it('rebuilds nothing for a store nobody is subscribed to, and leaks no listener', async () => {
    const m = mirror();
    let reads = 0;
    const store = fromLiveQuery(
      async () => {
        reads += 1;
        const db = m.get();
        return db ? db.table('items').toArray() : [];
      },
      [] as unknown[],
      undefined,
      m.revision,
    );

    // Never subscribed: a revision step must not run the querier.
    m.set(undefined);
    await tick();
    expect(reads).toBe(0);

    const seen: unknown[][] = [];
    const off = store.subscribe((v) => seen.push(v as unknown[]));
    await tick();
    const afterOpen = reads;
    expect(afterOpen).toBeGreaterThan(0);

    // Unsubscribed BEFORE the mirror ever opened: the revision listener goes
    // with the subscription, so later steps cost nothing and emit nothing.
    off();
    const framesAtUnsubscribe = seen.length;
    const db = await openDb();
    m.set(db);
    await tick(20);
    expect(reads).toBe(afterOpen);
    expect(seen).toHaveLength(framesAtUnsubscribe);

    // A later subscriber still gets a live store over the current mirror.
    const seenLater: unknown[][] = [];
    const offLater = store.subscribe((v) => seenLater.push(v as unknown[]));
    await db.table('items').put({ id: 'a', n: 1 });
    await tick(20);
    expect(seenLater[seenLater.length - 1]).toEqual([{ id: 'a', n: 1 }]);
    offLater();
    await db.delete();
  });

  it('a rebuild delivers nothing to a handler that unsubscribed mid-flight', async () => {
    const m = mirror();
    const db = await openDb();
    await db.table('items').put({ id: 'a', n: 1 });
    const store = fromLiveQuery(
      async () => {
        const d = m.get();
        return d ? d.table('items').toArray() : [];
      },
      [] as unknown[],
      undefined,
      m.revision,
    );
    const seen: unknown[][] = [];
    const off = store.subscribe((v) => seen.push(v as unknown[]));
    await tick();
    const before = seen.length;

    m.set(db); // steps the revision, rebuild in flight…
    off(); // …and the handler leaves in the same task
    await tick(20);
    expect(seen).toHaveLength(before);
    await db.delete();
  });

  it('createRevision counts up and stops calling a listener that unsubscribed', () => {
    const rev = createRevision();
    const seen: number[] = [];
    expect(rev.get()).toBe(0);
    const off = rev.onChange((n) => seen.push(n));
    expect(seen).toEqual([]); // never fires on subscribe
    rev.step();
    rev.step();
    off();
    rev.step();
    expect(seen).toEqual([1, 2]);
    expect(rev.get()).toBe(3);
  });
});

describe('emitter (SPEC §2.4)', () => {
  it('delivers typed payloads and unsubscribes', () => {
    const em = createEmitter();
    const seen: number[] = [];
    const off = em.on('online', (p) => seen.push(p.at));
    em.emit('online', { at: 1 });
    off();
    em.emit('online', { at: 2 });
    expect(seen).toEqual([1]);
  });

  it('once fires exactly once', () => {
    const em = createEmitter();
    let n = 0;
    em.once('online', () => (n += 1));
    em.emit('online', { at: 1 });
    em.emit('online', { at: 2 });
    expect(n).toBe(1);
  });

  it('a throwing handler is routed to error and does not unwind the engine', () => {
    const em = createEmitter();
    const errors: string[] = [];
    em.on('error', (p) => errors.push(p.error.code));
    em.on('online', () => {
      throw new Error('handler blew up');
    });
    let reachedSecondHandler = false;
    em.on('online', () => {
      reachedSecondHandler = true;
    });

    expect(() => em.emit('online', { at: 1 })).not.toThrow();
    expect(errors).toEqual(['internal/invariant']);
    expect(reachedSecondHandler).toBe(true);
  });

  it('a throwing error handler is swallowed and logged, not re-entered', () => {
    const logged: unknown[] = [];
    const em = createEmitter((e) => logged.push(e));
    em.on('error', () => {
      throw new Error('error handler blew up');
    });
    expect(() => em.emit('error', { error: { code: 'db/quota', message: 'x', retryable: true, at: 0 } })).not.toThrow();
    expect(logged).toHaveLength(1);
  });

  it('clear() drops every handler', () => {
    const em = createEmitter();
    const cb = vi.fn();
    em.on('online', cb);
    em.clear();
    em.emit('online', { at: 1 });
    expect(cb).not.toHaveBeenCalled();
  });
});
