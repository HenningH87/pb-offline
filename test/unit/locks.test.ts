/** SPEC §13 — drainer election, the lease fallback, and the nudge channel. */
import { afterEach, describe, expect, it } from 'vitest';
import {
  createNudgeChannel,
  electDrainer,
  hasWebLocks,
  webLockManager,
  LEASE_RENEW_MS,
  LEASE_STEAL_MS,
  type DrainerHandle,
} from '../../src/sync/locks.js';
import { claimLease, readState, STATE_KEY } from '../../src/db/state.js';
import { drainLockNameFor } from '../../src/brand.js';
import { createLockManager } from '../fakes/locks.js';
import { openTestDb, testConfig, tick, type TestDb } from './_helpers.js';

const dbs: TestDb[] = [];
const handles: DrainerHandle[] = [];

afterEach(async () => {
  while (handles.length > 0) await handles.pop()?.release();
  while (dbs.length > 0) await dbs.pop()?.destroy();
});

async function open(): Promise<TestDb> {
  const t = await openTestDb(testConfig());
  dbs.push(t);
  return t;
}

function track(h: DrainerHandle): DrainerHandle {
  handles.push(h);
  return h;
}

describe('Web Locks election (SPEC §13.1)', () => {
  it('elects exactly one of N tabs, and releasing promotes the next', async () => {
    const t = await open();
    const locks = createLockManager();
    const roles: string[] = [];

    const a = track(await electDrainer({ db: t.handle.db, dbName: t.handle.dbName, tabId: 'a', locks }));
    const b = track(await electDrainer({ db: t.handle.db, dbName: t.handle.dbName, tabId: 'b', locks }));
    const c = track(await electDrainer({ db: t.handle.db, dbName: t.handle.dbName, tabId: 'c', locks }));
    for (const h of [a, b, c]) h.onChange((v) => roles.push(`${h.tabId}:${v}`));

    expect([a, b, c].filter((h) => h.isDrainer)).toEqual([a]);
    expect(a.mode).toBe('web-locks');
    expect(locks.held()).toEqual([drainLockNameFor(t.handle.dbName)]);

    // The holder goes away — a closed tab, a crash, or this.
    await a.release();
    await tick(1);
    expect(a.isDrainer).toBe(false);
    expect(b.isDrainer).toBe(true);
    expect(c.isDrainer).toBe(false);

    await b.release();
    await tick(1);
    expect(c.isDrainer).toBe(true);
    expect(roles).toEqual(['a:false', 'b:true', 'b:false', 'c:true']);
  });

  it('never promotes a tab that released before its turn came', async () => {
    const t = await open();
    const locks = createLockManager();
    const a = track(await electDrainer({ db: t.handle.db, dbName: t.handle.dbName, tabId: 'a', locks }));
    const b = track(await electDrainer({ db: t.handle.db, dbName: t.handle.dbName, tabId: 'b', locks }));

    await b.release(); // the tab closed while waiting
    await a.release();
    await tick(1);
    expect(b.isDrainer).toBe(false);
  });

  it('reports the platform manager honestly', () => {
    // Node ≥24 ships `navigator.locks`; older ones do not. Either answer is fine,
    // but the two functions MUST agree — a disagreement is what silently sends a
    // real browser down the fallback path.
    expect(hasWebLocks()).toBe(webLockManager() !== null);
  });
});

describe('the lease fallback (SPEC §13.5)', () => {
  const lease = (t: TestDb, tabId: string, over?: Record<string, unknown>) =>
    electDrainer({
      db: t.handle.db,
      dbName: t.handle.dbName,
      tabId,
      locks: null, // force the fallback
      renewMs: 10,
      staleMs: 40,
      ...over,
    });

  it('claims, stamps `_state`, and reports the fallback exactly once', async () => {
    const t = await open();
    let falls = 0;
    const a = track(await lease(t, 'a', { onFallback: () => (falls += 1) }));

    expect(a.mode).toBe('lease');
    expect(a.isDrainer).toBe(true);
    expect((await readState(t.handle.db))?.drainerTabId).toBe('a');
    await tick(40);
    expect(falls).toBe(1);
  });

  it('a second tab loses, and cannot steal a lease that is being renewed', async () => {
    const t = await open();
    const a = track(await lease(t, 'a'));
    const b = track(await lease(t, 'b'));

    expect(a.isDrainer).toBe(true);
    expect(b.isDrainer).toBe(false);

    // Well past the steal window, but `a` is heartbeating every 10 ms.
    await tick(120);
    expect(b.isDrainer).toBe(false);
    expect(a.isDrainer).toBe(true);
    expect((await readState(t.handle.db))?.drainerTabId).toBe('a');
  });

  it('steals a lease nobody renewed', async () => {
    const t = await open();
    // `a`'s tab froze: it holds the lease and will never heartbeat again.
    const a = track(await lease(t, 'a', { renewMs: 60_000 }));
    const b = track(await lease(t, 'b'));
    expect(a.isDrainer).toBe(true);
    expect(b.isDrainer).toBe(false);

    await tick(120); // past the 40 ms stale window
    expect(b.isDrainer).toBe(true);
    expect((await readState(t.handle.db))?.drainerTabId).toBe('b');
  });

  it('the abandoned holder notices it was stolen from and stands down', async () => {
    const t = await open();
    // A long steal window, so standing down is not immediately undone by `a`
    // taking the lease straight back.
    const a = track(await lease(t, 'a', { staleMs: 60_000 }));
    const roles: boolean[] = [];
    a.onChange((v) => roles.push(v));
    expect(a.isDrainer).toBe(true);

    // Another tab took the lease — the row is the only evidence there is.
    await t.handle.db.pbo.state.update(STATE_KEY, { drainerTabId: 'z', heartbeatAt: Date.now() });
    await tick(60);
    expect(a.isDrainer).toBe(false);
    expect(roles).toEqual([false]);
  });

  it('hands the lease back on release rather than making the next tab wait it out', async () => {
    const t = await open();
    const a = track(await lease(t, 'a', { renewMs: 60_000, staleMs: 60_000 }));
    await a.release();
    expect((await readState(t.handle.db))?.drainerTabId).toBeNull();
    // Immediately claimable, without waiting out a 60 s window.
    expect(await claimLease(t.handle.db, 'b', 60_000)).toBe(true);
  });

  it('a duplicate drain under the fallback is possible — and survivable', async () => {
    // SPEC §13.5 says so out loud. The mechanism that makes it survivable is the
    // compare-and-set: two tabs racing cannot both observe a free lease, so the
    // window is a STALE lease, not a concurrent claim.
    const t = await open();
    const winners = await Promise.all([
      claimLease(t.handle.db, 'a', 40),
      claimLease(t.handle.db, 'b', 40),
      claimLease(t.handle.db, 'c', 40),
    ]);
    expect(winners.filter(Boolean)).toHaveLength(1);

    // And after the window elapses with no renewal, exactly one tab takes over.
    await t.handle.db.pbo.state.update(STATE_KEY, { heartbeatAt: Date.now() - 10_000 });
    const stealers = await Promise.all([claimLease(t.handle.db, 'd', 40), claimLease(t.handle.db, 'e', 40)]);
    expect(stealers.filter(Boolean)).toHaveLength(1);
  });

  it('stops its loop when the database closes under it', async () => {
    const t = await open();
    const a = track(await lease(t, 'a'));
    expect(a.isDrainer).toBe(true);
    t.handle.db.close();
    await tick(60);
    expect(a.isDrainer).toBe(false);
    // The database is gone; the afterEach cleanup would otherwise reopen it.
    dbs.length = 0;
  });

  it('documents the spec intervals', () => {
    expect(LEASE_RENEW_MS).toBe(2_000);
    expect(LEASE_STEAL_MS).toBe(6_000);
  });
});

describe('the nudge channel (SPEC §13.3)', () => {
  it('delivers a nudge from one context to another, and never to the sender', async () => {
    const name = `nudge-test-${Math.random().toString(36).slice(2)}`;
    const a = createNudgeChannel(name);
    const b = createNudgeChannel(name);
    let toA = 0;
    let toB = 0;
    a.onNudge(() => (toA += 1));
    b.onNudge(() => (toB += 1));

    a.nudge();
    await tick(20);
    expect(toA).toBe(0); // a BroadcastChannel never delivers to the poster
    expect(toB).toBe(1);

    a.close();
    b.close();
    a.nudge(); // closed is not an error: it is an optimisation, not a write
    await tick(5);
    expect(toB).toBe(1);
  });
});
