/**
 * SPEC §13 through the public API — TESTPLAN §1.12 and as much of S6 as one
 * process can honestly carry.
 *
 * Two engine instances against ONE database name are two tabs in every respect
 * that matters here: they share IndexedDB, they contest one lock, and they see
 * each other only through Dexie. What they do NOT share is a browser's tab
 * lifecycle, so "closing tab 1 promotes tab 2" is exercised as `destroy()`.
 * The real thing is S6's Playwright scenario.
 */
import { afterEach, describe, expect, it } from 'vitest';
import Dexie from 'dexie';
import { createPBOffline } from '../../src/index.js';
import { dbNameFor } from '../../src/brand.js';
import { hasWebLocks } from '../../src/sync/locks.js';
import { createSyncRunner } from '../../src/sync/engine.js';
import { readState } from '../../src/db/state.js';
import { asPB, createFakePB, type FakePB } from '../fakes/pb.js';
import { openTestDb, testConfig, tick, uniqueAppId } from './_helpers.js';
import type { PBOffline, PBOfflineConfig, PBRecord, SyncState } from '../../src/types.js';

interface Project extends PBRecord {
  title?: string;
}

const engines: PBOffline[] = [];
const dbNames: string[] = [];
let hiddenNavigator = false;

afterEach(async () => {
  while (engines.length > 0) await engines.pop()?.destroy();
  while (dbNames.length > 0) await Dexie.delete(dbNames.pop()!);
  if (hiddenNavigator) restoreNavigator();
});

const savedNavigator = Object.getOwnPropertyDescriptor(globalThis, 'navigator');

/** Take Web Locks away, which is what an insecure context or old Safari does. */
function hideWebLocks(): void {
  hiddenNavigator = true;
  Object.defineProperty(globalThis, 'navigator', {
    value: { userAgent: 'test' },
    configurable: true,
    writable: true,
  });
}

function restoreNavigator(): void {
  hiddenNavigator = false;
  if (savedNavigator) Object.defineProperty(globalThis, 'navigator', savedNavigator);
  else delete (globalThis as { navigator?: unknown }).navigator;
}

/** One "tab": its own engine and its own SDK instance, on a shared database. */
function tab(appId: string, pb: FakePB, overrides?: Partial<PBOfflineConfig>): PBOffline {
  const engine = createPBOffline(asPB(pb), {
    appId,
    collections: [{ name: 'projects' }],
    writeCoalesceMs: 0,
    backoff: { min: 5, max: 20 },
    ...overrides,
  } as PBOfflineConfig);
  engines.push(engine);
  return engine;
}

const stateOf = (engine: PBOffline): SyncState => {
  let s!: SyncState;
  engine.state.subscribe((v) => (s = v))();
  return s;
};

async function until(what: string, fn: () => boolean | Promise<boolean>, timeoutMs = 3_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await fn()) return;
    if (Date.now() > deadline) throw new Error(`timed out waiting for: ${what}`);
    await tick(10);
  }
}

describe.skipIf(!hasWebLocks())('one drainer per database (SPEC §13.1, §13.2)', () => {
  it('elects one tab; the others push, pull and subscribe to nothing', async () => {
    const appId = uniqueAppId();
    dbNames.push(dbNameFor(appId, 'user1'));
    const pb1 = createFakePB({ ownerId: 'user1' });
    const pb2 = createFakePB({ ownerId: 'user1' });
    const pb3 = createFakePB({ ownerId: 'user1' });

    const a = tab(appId, pb1);
    await a.start();
    const b = tab(appId, pb2);
    const c = tab(appId, pb3);
    await Promise.all([b.start(), c.start()]);
    await tick(40);

    expect([a, b, c].filter((e) => stateOf(e).isDrainer)).toHaveLength(1);
    expect(stateOf(a).isDrainer).toBe(true);
    // The whole point of §9.2 and §5: N tabs are not N pushers, N pullers or N
    // SSE connections.
    expect(pb2.requests).toEqual([]);
    expect(pb3.requests).toEqual([]);
    expect(pb1.subscriptions.length).toBeGreaterThan(0);
  });

  it('a non-drainer tab still writes, and the drainer pushes what it wrote', async () => {
    const appId = uniqueAppId();
    dbNames.push(dbNameFor(appId, 'user1'));
    const pb1 = createFakePB({ ownerId: 'user1' });
    const pb2 = createFakePB({ ownerId: 'user1' });

    const a = tab(appId, pb1);
    await a.start();
    const b = tab(appId, pb2);
    await b.start();
    await tick(30);
    expect(stateOf(b).isDrainer).toBe(false);

    const written = await b.collection<Project>('projects').create({ title: 'written in tab 2' });
    await until('the drainer pushed it', () => pb1.get('projects', written.id) !== undefined);

    expect(pb2.recordRequests()).toEqual([]);
    expect(pb1.get('projects', written.id)).toMatchObject({ title: 'written in tab 2' });
    // Every tab is reactive regardless: Dexie carries the drainer's apply back.
    await until('tab 2 sees the row', async () => (await b.collection<Project>('projects').get(written.id)) !== undefined);
  });

  it('every tab observes the drainer’s sync state, and only one writes it', async () => {
    const appId = uniqueAppId();
    dbNames.push(dbNameFor(appId, 'user1'));
    const a = tab(appId, createFakePB({ ownerId: 'user1' }));
    await a.start();
    const b = tab(appId, createFakePB({ ownerId: 'user1' }));
    await b.start();

    await until('the drainer reported online', () => stateOf(a).online);
    // SPEC §3.6 / §13.4: the non-drainer never pinged; it adopted the verdict.
    await until('the non-drainer adopted it', () => stateOf(b).online);
    let mirrored = false;
    const off = b.online.subscribe((v) => (mirrored = v));
    expect(mirrored).toBe(true);
    off();
  });

  it('promotes another tab when the drainer goes away, and the drain resumes', async () => {
    const appId = uniqueAppId();
    dbNames.push(dbNameFor(appId, 'user1'));
    const pb1 = createFakePB({ ownerId: 'user1' });
    const pb2 = createFakePB({ ownerId: 'user1' });

    const a = tab(appId, pb1);
    await a.start();
    const b = tab(appId, pb2);
    await b.start();
    await until('tab 1 is the drainer', () => stateOf(a).isDrainer);
    expect(stateOf(b).isDrainer).toBe(false);

    // Tab 1 closes.
    await a.destroy();
    engines.splice(engines.indexOf(a), 1);
    await until('tab 2 was promoted', () => stateOf(b).isDrainer);

    const written = await b.collection<Project>('projects').create({ title: 'after promotion' });
    await until('the new drainer pushed it', () => pb2.get('projects', written.id) !== undefined);
    expect(pb2.subscriptions.length).toBeGreaterThan(0);
  });
});

describe('losing the role mid-drain (SPEC §13.6)', () => {
  it('records the in-flight result and starts no new drain', async () => {
    const t = await openTestDb(testConfig({ writeCoalesceMs: 0 }));
    const pb = createFakePB({ ownerId: 'user1' });
    let role = true;

    for (const title of ['first', 'second', 'third']) {
      await t.handle.db.pbo.pending.add({
        collection: 'projects',
        recordId: `${title.padEnd(15, 'x')}`,
        op: 'create',
        payload: { id: `${title.padEnd(15, 'x')}`, title },
        clientUpdatedAt: Date.now(),
        state: 'queued',
        attempts: 0,
        nextAttemptAt: 0,
        lastError: null,
      } as never);
    }

    const runner = createSyncRunner({
      pb: asPB(pb),
      cfg: t.cfg,
      getDb: () => t.handle.db,
      getOwnerId: () => 'user1',
      emit: () => {},
      tabId: 'tab-1',
      pollMs: 0,
      isDrainer: () => role,
      resubscribe: async () => {},
    });

    // The lock goes to another tab while the FIRST request is on the wire.
    pb.onRequest = (req) => {
      if (req.method === 'create') role = false;
    };

    await runner.start();
    await runner.push();

    // The request that was already on the wire recorded its result…
    expect(pb.recordRequests().filter((r) => r.method === 'create')).toHaveLength(1);
    expect(pb.get('projects', 'firstxxxxxxxxxx')).toBeDefined();
    // …and the entries behind it were left for whoever holds the lock now.
    const left = await t.handle.db.pbo.pending.orderBy('seq').toArray();
    expect(left.map((e) => e.payload?.['title'])).toEqual(['second', 'third']);
    // And a later push from the demoted tab starts no new drain at all.
    const stamp = await readState(t.handle.db);
    await runner.push();
    expect(pb.recordRequests().filter((r) => r.method === 'create')).toHaveLength(1);
    // Nor does it write `_state` any more: that row is the holder's (SPEC §13.4).
    expect(await readState(t.handle.db)).toEqual(stamp);

    await runner.stop();
    await t.destroy();
  });
});

describe('the lease fallback through the public API (SPEC §13.5)', () => {
  it('elects one drainer, reports `lock/unavailable` once per tab, and still drains', async () => {
    hideWebLocks();
    const appId = uniqueAppId();
    dbNames.push(dbNameFor(appId, 'user1'));
    const pb1 = createFakePB({ ownerId: 'user1' });
    const pb2 = createFakePB({ ownerId: 'user1' });

    const codesA: string[] = [];
    const codesB: string[] = [];
    const a = tab(appId, pb1);
    a.on('error', (e) => codesA.push(e.error.code));
    await a.start();
    const b = tab(appId, pb2);
    b.on('error', (e) => codesB.push(e.error.code));
    await b.start();
    await tick(30);

    expect(codesA.filter((c) => c === 'lock/unavailable')).toHaveLength(1);
    expect(codesB.filter((c) => c === 'lock/unavailable')).toHaveLength(1);
    expect([a, b].filter((e) => stateOf(e).isDrainer)).toHaveLength(1);
    expect(pb2.requests).toEqual([]);

    const written = await b.collection<Project>('projects').create({ title: 'lease-drained' });
    await until('the lease holder pushed it', () => pb1.get('projects', written.id) !== undefined);
  });

  it('survives a duplicate drain: the same queue drained twice loses nothing', async () => {
    // SPEC §13.5 admits the fallback can double-drain and says why that is
    // survivable — client-supplied ids make a create idempotent through §5.7.
    hideWebLocks();
    const appId = uniqueAppId();
    dbNames.push(dbNameFor(appId, 'user1'));
    // ONE server, two tabs: exactly the situation a duplicate drain creates.
    const pb = createFakePB({ ownerId: 'user1' });

    const a = tab(appId, pb);
    await a.start();
    const b = tab(appId, pb);
    await b.start();
    await tick(20);

    const dead: unknown[] = [];
    a.on('dead-letter', (e) => dead.push(e));
    b.on('dead-letter', (e) => dead.push(e));

    const rec = await a.collection<Project>('projects').create({ title: 'once' });
    // Both tabs drain the same queue, concurrently.
    await Promise.all([a.push(), b.push(), a.push(), b.push()]);
    await tick(20);

    expect(dead).toEqual([]);
    expect(pb.get('projects', rec.id)).toMatchObject({ title: 'once' });
    expect(pb.records.get('projects')?.size).toBe(1);
    expect(await a.collection<Project>('projects').count()).toBe(1);
  });
});
