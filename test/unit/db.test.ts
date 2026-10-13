/** SPEC §4.1, §4.2, §13.4 — per-owner databases, schema, `_meta`, `_state`. */
import { afterEach, describe, expect, it, vi } from 'vitest';
import Dexie from 'dexie';
import { BRAND, dbNameFor } from '../../src/brand.js';
import { buildStores, ENGINE_TABLES } from '../../src/db/schema.js';
import {
  close,
  createDbManager,
  deleteDatabase,
  listOwnerDatabases,
  listOwnerKeys,
  openForOwner,
  sweepStaleOwners,
  wipe,
} from '../../src/db/open.js';
import {
  claimLease,
  heartbeat,
  INITIAL_STATE,
  liveState,
  markSignOutPending,
  readOpenedAt,
  readSignOutPending,
  readState,
  stampOpenedAt,
  STATE_KEY,
  writeState,
} from '../../src/db/state.js';
import { clearMeta, deriveState, readMeta, refreshState, toPublicMeta, writeMeta } from '../../src/db/meta.js';
import { PBOError } from '../../src/errors.js';
import { testConfig, tick } from './_helpers.js';
import type { PendingEntry, SyncState } from '../../src/types.js';

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  while (cleanups.length > 0) await cleanups.pop()?.();
});

describe('schema (SPEC §4.2)', () => {
  it('declares the five engine tables plus one table per collection, keyed on id', () => {
    const stores = buildStores(testConfig());
    for (const t of ENGINE_TABLES) expect(stores[t]).toBeDefined();
    expect(stores['projects']).toBe('id');
    expect(stores['tasks']).toBe('id, project');
  });

  it('creates NO table for the tombstone collection — tombstones are applied and discarded', () => {
    expect(buildStores(testConfig())['deletions']).toBeUndefined();
  });
});

describe('open / close / wipe (SPEC §4.1)', () => {
  it('names the database per owner and creates every table', async () => {
    const cfg = testConfig();
    const handle = await openForOwner(cfg, 'user1');
    cleanups.push(() => wipe(handle));
    expect(handle.dbName).toBe(dbNameFor(cfg.appId, 'user1'));
    const tables = handle.db.tables.map((t) => t.name).sort();
    expect(tables).toEqual([...ENGINE_TABLES, 'projects', 'tasks'].sort());
  });

  it('keeps two owners in separate databases — no query can leak', async () => {
    const cfg = testConfig();
    const a = await openForOwner(cfg, 'userA');
    await a.db.table('projects').put({ id: 'p1', title: 'A' });
    await close(a);
    const b = await openForOwner(cfg, 'userB');
    cleanups.push(() => wipe(b));
    expect(await b.db.table('projects').toArray()).toEqual([]);
    await close(b);
    const a2 = await openForOwner(cfg, 'userA');
    cleanups.push(() => wipe(a2));
    expect(await a2.db.table('projects').count()).toBe(1);
  });

  it('wipe is atomic and total: the database itself is gone', async () => {
    const cfg = testConfig();
    const handle = await openForOwner(cfg, 'user1');
    await handle.db.table('projects').put({ id: 'p1' });
    await handle.db.pbo.pending.add({ collection: 'projects', recordId: 'p1', op: 'create', payload: {}, clientUpdatedAt: 1, state: 'queued', attempts: 0, nextAttemptAt: 0, lastError: null } as unknown as PendingEntry);
    await wipe(handle);
    expect(await Dexie.exists(handle.dbName)).toBe(false);

    // Reopening gives an empty mirror AND an empty queue — nothing survives.
    const reopened = await openForOwner(cfg, 'user1');
    cleanups.push(() => wipe(reopened));
    expect(await reopened.db.table('projects').count()).toBe(0);
    expect(await reopened.db.pbo.pending.count()).toBe(0);
  });

  /**
   * REGRESSION (review finding 10) — `indexedDB.deleteDatabase` BLOCKS, with no
   * timeout and no `blocked` handler, while any other connection to that
   * database is open. Multi-tab is a first-class feature (§13), so a second tab
   * is the expected state; unbounded, `signOut()`'s promise simply never settled
   * and the application's sign-out flow hung with no error and no result. The
   * same hazard sits under promotion's `'move'`.
   */
  it('a wipe another connection is blocking fails as db/version-conflict instead of hanging', async () => {
    const cfg = testConfig();
    const handle = await openForOwner(cfg, 'user1');
    // Exactly what a second tab holding the database looks like from here.
    const blocked = vi.spyOn(Dexie, 'delete').mockReturnValue(new Promise<void>(() => {}) as never);

    const started = Date.now();
    await expect(wipe(handle, 25)).rejects.toMatchObject({ code: 'db/version-conflict' });
    expect(Date.now() - started).toBeLessThan(2_000);

    blocked.mockRestore();
    await Dexie.delete(handle.dbName);
  });

  it('the manager drops a handle whose wipe was blocked — it is closed either way', async () => {
    const cfg = testConfig();
    const manager = createDbManager(cfg);
    const handle = await manager.open('user1');
    const blocked = vi.spyOn(Dexie, 'delete').mockReturnValue(new Promise<void>(() => {}) as never);

    await expect(manager.wipe(25)).rejects.toMatchObject({ code: 'db/version-conflict' });
    expect(manager.current()).toBeUndefined();

    blocked.mockRestore();
    await Dexie.delete(handle.dbName);
  });

  it('an account switch closes the outgoing database before opening the next, and wipes neither', async () => {
    const cfg = testConfig();
    const mgr = createDbManager(cfg);
    const a = await mgr.open('userA');
    await a.db.table('projects').put({ id: 'p1' });
    const closedNames: string[] = [];
    const originalClose = a.db.close.bind(a.db);
    a.db.close = ((...args: unknown[]) => {
      closedNames.push(a.dbName);
      return (originalClose as (...a: unknown[]) => unknown)(...args);
    }) as typeof a.db.close;

    const b = await mgr.open('userB');
    cleanups.push(async () => {
      await wipe(b);
      const reopened = await openForOwner(cfg, 'userA');
      await wipe(reopened);
    });

    expect(closedNames).toEqual([a.dbName]); // closed FIRST
    expect(mgr.current()?.ownerKey).toBe('userB');
    expect(await Dexie.exists(a.dbName)).toBe(true); // outgoing db survives
  });

  /**
   * SPEC §14.2 retention — the audit's M5. An account switch keeps the
   * outgoing mirror (the test above), which is right for switching and means a
   * shared device accumulates one unencrypted copy of every account that ever
   * signed in on it. `staleOwnerDays` is the opt-in eviction.
   */
  describe('staleOwnerDays (SPEC §14.2)', () => {
    const day = 24 * 60 * 60 * 1000;

    it('does nothing at all when the option is off', async () => {
      const cfg = testConfig();
      const mgr = createDbManager(cfg);
      await mgr.open('userA');
      await mgr.open('userB');
      cleanups.push(async () => {
        await mgr.close();
        for (const k of ['userA', 'userB']) await Dexie.delete(dbNameFor(cfg.appId, k));
      });

      expect(await sweepStaleOwners(cfg, 'userB', Date.now() + 400 * day)).toEqual({ deleted: [], kept: [] });
      expect(await Dexie.exists(dbNameFor(cfg.appId, 'userA'))).toBe(true);
    });

    it('deletes another owner`s mirror once it is older than the window, and keeps a fresh one', async () => {
      const cfg = testConfig({ staleOwnerDays: 30 });
      const mgr = createDbManager(cfg);
      const old = await mgr.open('userOld');
      await old.db.table('projects').put({ id: 'p1', title: 'birth data' });
      // The manager stamps `opened` on every real open; back-date this one.
      await stampOpenedAt(old.db, Date.now() - 90 * day);
      const recent = await mgr.open('userRecent');
      await stampOpenedAt(recent.db, Date.now() - 2 * day);
      await mgr.open('userNow');
      cleanups.push(async () => {
        await mgr.close();
        for (const k of ['userRecent', 'userNow']) await Dexie.delete(dbNameFor(cfg.appId, k));
      });

      const swept = await sweepStaleOwners(cfg, 'userNow');
      expect(swept.deleted).toEqual(['userOld']);
      expect(swept.kept).toEqual([{ ownerKey: 'userRecent', reason: 'fresh' }]);
      expect(await Dexie.exists(dbNameFor(cfg.appId, 'userOld'))).toBe(false);
      expect(await Dexie.exists(dbNameFor(cfg.appId, 'userRecent'))).toBe(true);
    });

    it('never touches the current owner or the anonymous mirror, whatever their age', async () => {
      // `0` is "every other owner is stale by definition" — the setting a
      // one-account-per-visitor product uses. Even then these two survive:
      // promotion is the only thing entitled to consume the anonymous mirror
      // and the engine cannot know whether the app still intends to (§14.4).
      const cfg = testConfig({ staleOwnerDays: 0 });
      const mgr = createDbManager(cfg);
      await mgr.open(BRAND.anonOwner);
      const mine = await mgr.open('userMine');
      await stampOpenedAt(mine.db, 0);
      cleanups.push(async () => {
        await mgr.close();
        for (const k of [BRAND.anonOwner, 'userMine']) await Dexie.delete(dbNameFor(cfg.appId, k));
      });

      expect((await sweepStaleOwners(cfg, 'userMine')).deleted).toEqual([]);
      expect(await Dexie.exists(dbNameFor(cfg.appId, BRAND.anonOwner))).toBe(true);
      expect(await Dexie.exists(dbNameFor(cfg.appId, 'userMine'))).toBe(true);
    });

    it('stamps an unstamped mirror instead of deleting it — unknown age is not old', async () => {
      const cfg = testConfig({ staleOwnerDays: 0 });
      // Opened the way a pre-`staleOwnerDays` release did: no stamp.
      const legacy = await openForOwner(cfg, 'userLegacy');
      legacy.db.close();
      const mgr = createDbManager(cfg);
      await mgr.open('userNow');
      cleanups.push(async () => {
        await mgr.close();
        for (const k of ['userLegacy', 'userNow']) await Dexie.delete(dbNameFor(cfg.appId, k));
      });

      const first = await sweepStaleOwners(cfg, 'userNow');
      expect(first).toEqual({ deleted: [], kept: [{ ownerKey: 'userLegacy', reason: 'unstamped' }] });
      expect(await Dexie.exists(dbNameFor(cfg.appId, 'userLegacy'))).toBe(true);

      // Stamped by that pass, so the clock now runs from it: a later boot evicts.
      const later = await sweepStaleOwners(cfg, 'userNow', Date.now() + day);
      expect(later.deleted).toEqual(['userLegacy']);
    });
  });

  it('steps its revision on every identity change, and only on one (SPEC §2.3)', async () => {
    const cfg = testConfig();
    const mgr = createDbManager(cfg);
    const steps: number[] = [];
    const off = mgr.revision.onChange((n) => steps.push(n));
    expect(mgr.revision.get()).toBe(0); // no I/O, no step

    await mgr.open('userA');
    expect(steps).toEqual([1]);

    await mgr.open('userA'); // same owner: not an identity change
    expect(steps).toEqual([1]);

    // A switch is TWO steps: the outgoing database is gone before the incoming
    // one exists, and a reactive read must not keep showing owner A's rows in
    // between.
    await mgr.open('userB');
    expect(steps).toEqual([1, 2, 3]);

    await mgr.close();
    expect(steps).toEqual([1, 2, 3, 4]);
    await mgr.close(); // already closed: nothing changed
    expect(steps).toEqual([1, 2, 3, 4]);

    const b = await mgr.open('userB');
    expect(steps).toEqual([1, 2, 3, 4, 5]);
    await mgr.wipe();
    expect(steps).toEqual([1, 2, 3, 4, 5, 6]);

    off();
    await mgr.open('userA');
    expect(steps).toHaveLength(6); // the unsubscriber stops delivery

    cleanups.push(async () => {
      await mgr.close();
      await deleteDatabase(b.dbName).catch(() => undefined);
      const a = await openForOwner(cfg, 'userA');
      await wipe(a);
    });
  });

  it('holds at most one open instance, even under concurrent opens', async () => {
    const cfg = testConfig();
    const mgr = createDbManager(cfg);
    const [x, y] = await Promise.all([mgr.open('userA'), mgr.open('userB')]);
    cleanups.push(async () => {
      await mgr.close();
      for (const k of ['userA', 'userB']) {
        const h = await openForOwner(cfg, k);
        await wipe(h);
      }
    });
    expect(mgr.current()?.ownerKey).toBe('userB');
    expect(x.db.isOpen()).toBe(false); // A was closed when B opened
    expect(y.db.isOpen()).toBe(true);
  });

  it('lists this app’s databases and their owner keys, and nobody else’s', async () => {
    const cfg = testConfig();
    const a = await openForOwner(cfg, 'userA');
    await close(a);
    const anon = await openForOwner(cfg, BRAND.anonOwner);
    cleanups.push(async () => {
      await wipe(anon);
      const h = await openForOwner(cfg, 'userA');
      await wipe(h);
    });
    const names = await listOwnerDatabases(cfg);
    expect(names).toContain(dbNameFor(cfg.appId, 'userA'));
    expect((await listOwnerKeys(cfg)).sort()).toEqual([BRAND.anonOwner, 'userA'].sort());
    expect(names.every((n) => n.startsWith(`${BRAND.dbPrefix}_${cfg.appId}_`))).toBe(true);
  });

  it('bumps the schema version when a collection is added to config', async () => {
    const cfg = testConfig();
    const first = await openForOwner(cfg, 'user1');
    const v1 = first.db.verno;
    await close(first);

    const grown = testConfig();
    (grown as { appId: string }).appId = cfg.appId;
    grown.collections.push({ name: 'notes', indexes: [], idFormat: cfg.collections[0]!.idFormat, filter: null });
    const second = await openForOwner(grown, 'user1');
    cleanups.push(() => wipe(second));
    expect(second.db.verno).toBeGreaterThan(v1);
    expect(second.db.tables.map((t) => t.name)).toContain('notes');
  });

  it('maps an IndexedDB failure to db/open-failed rather than leaking Dexie’s error', async () => {
    const cfg = testConfig();
    const original = globalThis.indexedDB.open;
    globalThis.indexedDB.open = (() => {
      throw new Error('private mode');
    }) as typeof globalThis.indexedDB.open;
    try {
      await expect(openForOwner(cfg, 'user1')).rejects.toMatchObject({ code: 'db/open-failed' });
    } finally {
      globalThis.indexedDB.open = original;
    }
  });
});

describe('_meta (SPEC §4.2, §7)', () => {
  it('upserts, derives state from the queue, and clears', async () => {
    const cfg = testConfig();
    const h = await openForOwner(cfg, 'user1');
    cleanups.push(() => wipe(h));
    const db = h.db;

    expect(await deriveState(db, 'projects', 'p1')).toBe('synced');

    await writeMeta(db, 'projects', 'p1', { clientUpdatedAt: 5, serverUpdated: '2026-01-01 00:00:00.000Z' });
    expect(await readMeta(db, 'projects', 'p1')).toMatchObject({
      collection: 'projects',
      recordId: 'p1',
      clientUpdatedAt: 5,
      lastPushedUpdated: null,
      state: 'synced',
    });

    const seq = await db.pbo.pending.add({ collection: 'projects', recordId: 'p1', op: 'update', payload: { a: 1 }, clientUpdatedAt: 5, state: 'queued', attempts: 0, nextAttemptAt: 0, lastError: null } as unknown as PendingEntry);
    expect(await deriveState(db, 'projects', 'p1')).toBe('dirty');

    await db.pbo.pending.update(seq, { state: 'inflight' });
    expect(await deriveState(db, 'projects', 'p1')).toBe('pending');

    await db.pbo.deadletter.add({ collection: 'projects', recordId: 'p1', op: 'update', payload: {}, error: { code: 'push/rejected', message: 'no', retryable: false, at: 1 }, at: 1 } as never);
    expect(await deriveState(db, 'projects', 'p1')).toBe('dead');

    expect(await refreshState(db, 'projects', 'p1')).toBe('dead');
    expect((await readMeta(db, 'projects', 'p1'))!.state).toBe('dead');

    await clearMeta(db, 'projects', 'p1');
    expect(await readMeta(db, 'projects', 'p1')).toBeUndefined();
  });

  it('projects a public meta shape', () => {
    const pub = toPublicMeta(
      { collection: 'projects', recordId: 'p1', serverUpdated: 'T1', clientUpdatedAt: 9, lastPushedUpdated: 'T1', state: 'dirty', lastError: null },
      2,
    );
    expect(pub).toEqual({ id: 'p1', state: 'dirty', serverUpdated: 'T1', clientUpdatedAt: 9, pendingOps: 2, lastError: null });
    expect('lastPushedUpdated' in pub).toBe(false); // echo bookkeeping stays internal
  });
});

describe('_state (SPEC §4.2, §13.4)', () => {
  it('holds exactly one row and merges patches into it', async () => {
    const cfg = testConfig();
    const h = await openForOwner(cfg, 'user1');
    cleanups.push(() => wipe(h));

    await writeState(h.db, { status: 'connecting', ownerId: 'user1' });
    await writeState(h.db, { pendingCount: 3 });
    expect(await h.db.pbo.state.count()).toBe(1);
    const row = await readState(h.db);
    expect(row!.key).toBe(STATE_KEY);
    expect(row!.state).toMatchObject({ status: 'connecting', ownerId: 'user1', pendingCount: 3 });
  });

  it('refuses a write from a tab that is not the drainer', async () => {
    const cfg = testConfig();
    const h = await openForOwner(cfg, 'user1');
    cleanups.push(() => wipe(h));
    await expect(writeState(h.db, { status: 'online' }, { tabId: 'tab2', isDrainer: false })).rejects.toBeInstanceOf(PBOError);
    expect(await h.db.pbo.state.count()).toBe(0);
  });

  it('keeps the sign-out marker and the opened stamp out of the sync row (SPEC §15, §14.2)', async () => {
    const cfg = testConfig();
    const h = await openForOwner(cfg, 'user1');
    cleanups.push(() => wipe(h));

    expect(await readSignOutPending(h.db)).toBeNull();
    expect(await readOpenedAt(h.db)).toBeNull();

    await writeState(h.db, { status: 'online' });
    await markSignOutPending(h.db, { ownerId: 'user1', flushTimeoutMs: 5_000, at: 1_700 });
    await stampOpenedAt(h.db, 42);

    expect(await readSignOutPending(h.db)).toEqual({ ownerId: 'user1', flushTimeoutMs: 5_000, at: 1_700 });
    expect(await readOpenedAt(h.db)).toBe(42);
    // Separate keys: the observable state every tab reads is untouched by both.
    expect((await readState(h.db))!.state).toMatchObject({ status: 'online' });
  });

  it('is observable in every tab, with isDrainer overlaid per reader', async () => {
    const cfg = testConfig();
    const h = await openForOwner(cfg, 'user1');
    cleanups.push(() => wipe(h));

    const drainerSeen: boolean[] = [];
    const otherSeen: boolean[] = [];
    const statuses: string[] = [];
    const off1 = liveState(h.db, 'tab1').subscribe((s) => {
      drainerSeen.push(s.isDrainer);
      statuses.push(s.status);
    });
    const off2 = liveState(h.db, 'tab2').subscribe((s) => otherSeen.push(s.isDrainer));
    expect(statuses[0]).toBe(INITIAL_STATE.status); // synchronous initial frame

    await writeState(h.db, { status: 'online', pendingCount: 1 }, { tabId: 'tab1' });
    await tick(20);

    expect(statuses).toContain('online');
    expect(drainerSeen.at(-1)).toBe(true);
    expect(otherSeen.at(-1)).toBe(false);
    off1();
    off2();
  });

  /**
   * REGRESSION (review finding 11) — `_state` write amplification.
   *
   * `writeState` stamped `heartbeatAt: Date.now()` unconditionally, so the row
   * changed on EVERY `publish()` — drain start, drain end, pull start, pull end,
   * onUp, onDown, every sequence's `finally` — even when the projected state was
   * byte-identical. `liveQuery` then re-emitted in every tab, `projectState`
   * built a fresh object so the `Object.is` guard could never fire, and every
   * `$state`/`useSyncExternalStore` consumer re-rendered continuously. Under the
   * §13.5 lease fallback the heartbeat alone did it every 2 s, for ever.
   */
  it('a publish that changes nothing writes nothing (SPEC §2.5.4)', async () => {
    const cfg = testConfig();
    const h = await openForOwner(cfg, 'user1');
    cleanups.push(() => wipe(h));

    await writeState(h.db, { status: 'online', online: true, pendingCount: 0 }, { tabId: 'tab1' });
    const before = await readState(h.db);
    await tick(5);
    await writeState(h.db, { status: 'online', online: true, pendingCount: 0 }, { tabId: 'tab1' });
    const after = await readState(h.db);

    expect(after).toEqual(before);
    expect(after!.heartbeatAt).toBe(before!.heartbeatAt);

    // A real change still lands.
    await writeState(h.db, { pendingCount: 1 }, { tabId: 'tab1' });
    expect(((await readState(h.db))!.state as SyncState).pendingCount).toBe(1);
  });

  it('the lease heartbeat does not re-emit the state store in any tab', async () => {
    const cfg = testConfig();
    const h = await openForOwner(cfg, 'user1');
    cleanups.push(() => wipe(h));
    await writeState(h.db, { status: 'online', online: true }, { tabId: 'tab1' });

    const seen: SyncState[] = [];
    const off = liveState(h.db, 'tab1').subscribe((s) => seen.push(s));
    await tick(20);
    const settled = seen.length;

    // §13.5 renews the lease every 2 s for the life of the tab. That is a
    // liveQuery change, and it MUST NOT become a re-render.
    await heartbeat(h.db, 'tab1');
    await heartbeat(h.db, 'tab1');
    await tick(30);

    expect(seen.length).toBe(settled);
    off();
  });

  it('elects one lease holder, renews it, and allows a steal only after expiry', async () => {
    const cfg = testConfig();
    const h = await openForOwner(cfg, 'user1');
    cleanups.push(() => wipe(h));

    expect(await claimLease(h.db, 'tab1', 6_000)).toBe(true);
    expect(await claimLease(h.db, 'tab2', 6_000)).toBe(false);
    expect(await claimLease(h.db, 'tab1', 6_000)).toBe(true); // the holder re-claims

    await heartbeat(h.db, 'tab2'); // not the holder ⇒ no effect
    const before = (await readState(h.db))!.heartbeatAt;
    await tick(2);
    await heartbeat(h.db, 'tab1');
    expect((await readState(h.db))!.heartbeatAt).toBeGreaterThanOrEqual(before);

    await h.db.pbo.state.update(STATE_KEY, { heartbeatAt: Date.now() - 10_000 });
    expect(await claimLease(h.db, 'tab2', 6_000)).toBe(true); // stale ⇒ stealable
  });
});
