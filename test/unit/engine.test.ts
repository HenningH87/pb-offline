/** SPEC §2.2, §3.1 — the status machine and the reconnect sequence. */
import { afterEach, describe, expect, it } from 'vitest';
import Dexie from 'dexie';
import { createPBOffline } from '../../src/index.js';
import { createSyncRunner } from '../../src/sync/engine.js';
import { dbNameFor } from '../../src/brand.js';
import { writeCursor } from '../../src/sync/pull.js';
import { DEFAULT_RETENTION_MS } from '../../src/sync/tombstones.js';
import { readState } from '../../src/db/state.js';
import { openTestDb, testConfig, tick, uniqueAppId, type TestDb } from './_helpers.js';
import { asPB, createFakePB, sdkError, type FakePB } from '../fakes/pb.js';
import type { PBOEvents, PBOffline, PBOfflineConfig, PBRecord, SyncState, SyncStatus } from '../../src/types.js';

interface Project extends PBRecord {
  title?: string;
}

const engines: PBOffline[] = [];
const dbNames: string[] = [];
let t: TestDb | undefined;

afterEach(async () => {
  while (engines.length > 0) await engines.pop()?.destroy();
  while (dbNames.length > 0) await Dexie.delete(dbNames.pop()!);
  await t?.destroy();
  t = undefined;
});

function makeEngine(pb: FakePB, overrides?: Partial<PBOfflineConfig>): PBOffline {
  const appId = uniqueAppId();
  const engine = createPBOffline(asPB(pb), {
    appId,
    collections: [{ name: 'projects' }, { name: 'tasks' }],
    writeCoalesceMs: 0,
    backoff: { min: 5, max: 20 },
    ...overrides,
  } as PBOfflineConfig);
  engines.push(engine);
  dbNames.push(dbNameFor(appId, 'user1'));
  return engine;
}

const statuses = (engine: PBOffline): SyncStatus[] => {
  const seen: SyncStatus[] = [];
  engine.on('status', (e) => seen.push(e.to));
  return seen;
};

describe('the reconnect sequence (SPEC §3.1)', () => {
  it('runs push → pull → tombstones → resubscribe, in that order and without interleaving', async () => {
    t = await openTestDb(testConfig({ writeCoalesceMs: 0 }));
    const pb = createFakePB({ ownerId: 'user1' });
    const order: string[] = [];
    const enqueue = async (): Promise<void> => {
      await t!.handle.db.pbo.pending.add({
      collection: 'projects',
      recordId: 'aaaaaaaaaaaaaaa',
      op: 'create',
      payload: { id: 'aaaaaaaaaaaaaaa', title: 'queued before the link came up' },
      clientUpdatedAt: Date.now(),
      state: 'queued',
      attempts: 0,
      nextAttemptAt: 0,
      lastError: null,
      } as never);
    };
    pb.onRequest = (req) => {
      order.push(`${req.method}:${req.collection}`);
    };

    const runner = createSyncRunner({
      pb: asPB(pb),
      cfg: t.cfg,
      getDb: () => t!.handle.db,
      getOwnerId: () => 'user1',
      emit: () => {},
      tabId: 'tab-1',
      pollMs: 0,
      resubscribe: async () => {
        order.push('resubscribe');
      },
    });

    await runner.start();
    // start() kicks a pass of its own in the background; let it settle, then
    // measure ONE explicit pass.
    await tick(40);
    order.length = 0;
    await enqueue();
    await runner.sync();

    expect(order).toEqual([
      'create:projects', // 1 · drain
      'getList:projects', // 2 · pull every collection…
      'getList:tasks',
      'getList:deletions', // …tombstones LAST
      'resubscribe', // 3
    ]);
    await runner.stop();
  });

  /**
   * REGRESSION (review finding 4) — SPEC §11: the automatic triggers "MUST be
   * checked before every pull pass".
   *
   * `shouldResync` was called from `sequence()` alone, so an application driving
   * sync itself with `engine.pull()` on a timer — a documented use of the API —
   * pulled for ever with a tombstone cursor years past the server's retention
   * window, silently missing every delete in it, with no `full-resync` ever
   * firing.
   */
  it('checks the §11 triggers before a bare pull(), not only inside sync()', async () => {
    t = await openTestDb(testConfig({ writeCoalesceMs: 0 }));
    const pb = createFakePB({ ownerId: 'user1' });
    const events: Array<{ type: string; payload: unknown }> = [];
    const runner = createSyncRunner({
      pb: asPB(pb),
      cfg: t.cfg,
      getDb: () => t!.handle.db,
      getOwnerId: () => 'user1',
      emit: (type, payload) => events.push({ type, payload }),
      tabId: 'tab-1',
      pollMs: 0,
    });
    await runner.start();
    await tick(40); // let start()'s own pass finish and freshen the cursors

    // §11.1: the tombstone cursor is now older than the retention window.
    await writeCursor(t.handle.db, 'deletions', { updated: null, id: null }, Date.now() - DEFAULT_RETENTION_MS - 1_000);
    events.length = 0;

    await runner.pull(['projects']);

    expect(events.filter((e) => e.type === 'full-resync').map((e) => (e.payload as { phase: string }).phase)).toEqual([
      'start',
      'done',
    ]);
    await runner.stop();
  });

  /**
   * REGRESSION (review finding 7) — `runFull` was tracked in neither
   * `drainPromise` nor `chain`, so `stop()`/`destroy()` returned while step 3's
   * wipe was still running and `dbs.close()` could pull Dexie out from under it,
   * leaving the outgoing owner's mirror empty with null cursors and no meta.
   */
  it('stop() waits for an in-flight full resync instead of returning under it', async () => {
    t = await openTestDb(testConfig({ writeCoalesceMs: 0 }));
    const pb = createFakePB({ ownerId: 'user1' });
    const runner = createSyncRunner({
      pb: asPB(pb),
      cfg: t.cfg,
      getDb: () => t!.handle.db,
      getOwnerId: () => 'user1',
      emit: () => {},
      tabId: 'tab-1',
      pollMs: 0,
    });
    await runner.start();
    await tick(30); // start()'s own pass finishes and leaves the chain idle

    // Only the RESYNC's pull is held open, so nothing else can be what `stop()`
    // is waiting on.
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    pb.onRequest = async (req) => {
      if (req.method === 'getList' && req.collection === 'projects') await gate;
    };

    const full = runner.fullResync('manual');
    await tick(20);
    let stopped = false;
    const stopping = runner.stop().then(() => {
      stopped = true;
    });
    await tick(30);
    expect(stopped).toBe(false); // still holding the resync's transaction open

    release();
    await full;
    await stopping;
    expect(stopped).toBe(true);
  });

  /**
   * REGRESSION (review finding 13) — `fullResync` never throws, so `runFull`
   * always reached `lastSyncedAt = now()`. A UI then rendered "last synced 2
   * seconds ago" over a mirror that had been wiped and never refilled.
   */
  it('a FAILED resync does not stamp lastSyncedAt', async () => {
    t = await openTestDb(testConfig({ writeCoalesceMs: 0 }));
    const pb = createFakePB({ ownerId: 'user1', offline: true });
    // A collection the open database has no table for: step 3's `db.table()`
    // throws, which is "a failure at any step" per §11.
    const brokenCfg = testConfig({
      collections: [{ name: 'projects' }, { name: 'tasks' }, { name: 'ghost' }],
    });
    const phases: string[] = [];
    const runner = createSyncRunner({
      pb: asPB(pb),
      cfg: brokenCfg,
      getDb: () => t!.handle.db,
      getOwnerId: () => 'user1',
      emit: (type, payload) => {
        if (type === 'full-resync') phases.push((payload as { phase: string }).phase);
      },
      tabId: 'tab-1',
      pollMs: 0,
    });
    await runner.start();
    await runner.fullResync('manual');
    await tick(20);

    expect(phases).toEqual(['start', 'failed']);
    expect(((await readState(t.handle.db))?.state as SyncState | undefined)?.lastSyncedAt ?? null).toBeNull();
    await runner.stop();
  });

  it('does not pull when the drain stopped on the link — the queue is the authority', async () => {
    t = await openTestDb(testConfig({ writeCoalesceMs: 0 }));
    const pb = createFakePB({ ownerId: 'user1' });
    await t.handle.db.pbo.pending.add({
      collection: 'projects',
      recordId: 'aaaaaaaaaaaaaaa',
      op: 'create',
      payload: { id: 'aaaaaaaaaaaaaaa' },
      clientUpdatedAt: Date.now(),
      state: 'queued',
      attempts: 0,
      nextAttemptAt: 0,
      lastError: null,
    } as never);
    pb.failNext('projects', 'create', sdkError(503));

    const runner = createSyncRunner({
      pb: asPB(pb),
      cfg: t.cfg,
      getDb: () => t!.handle.db,
      getOwnerId: () => 'user1',
      emit: () => {},
      tabId: 'tab-1',
      pollMs: 0,
    });
    await runner.start();
    await runner.sync();
    expect(pb.requests.some((r) => r.method === 'getList')).toBe(false);
    await runner.stop();
  });
});

describe('the status machine (SPEC §2.2)', () => {
  it('walks offline → connecting → syncing → online on a reconnect', async () => {
    const pb = createFakePB({ ownerId: 'user1', offline: true });
    const engine = makeEngine(pb);
    const seen = statuses(engine);
    const now = (): SyncStatus => {
      let v: SyncStatus = 'offline';
      engine.status.subscribe((s) => (v = s))();
      return v;
    };
    expect(now()).toBe('offline');

    await engine.start();
    await tick(40);
    expect(now()).toBe('connecting');

    pb.offline = false;
    // A slow server, so `syncing` is observable rather than instantaneous.
    pb.onRequest = async () => {
      await tick(15);
    };
    const pass = engine.sync();
    await tick(40);
    expect(now()).toBe('syncing');

    await pass;
    await tick(40);
    expect(now()).toBe('online');
    expect(seen).toContain('connecting');
    expect(seen).toContain('syncing');
  }, 15_000);

  it('a 5xx is `connecting`, NEVER `error` (SPEC §2.2)', async () => {
    const pb = createFakePB({ ownerId: 'user1' });
    const engine = makeEngine(pb);
    await engine.start();
    await tick(30);

    pb.failNext('projects', 'create', sdkError(500));
    await engine.collection<Project>('projects').create({ title: 'A' });
    const result = await engine.push();
    expect(result.stoppedBy).toBe('link');
    await tick(30);

    let status: SyncStatus = 'online';
    engine.status.subscribe((v) => (status = v))();
    expect(status).toBe('connecting');
    expect(status).not.toBe('error');
  });

  it('a dead letter is `error`, and the engine keeps syncing everything else', async () => {
    const pb = createFakePB({ ownerId: 'user1' });
    const engine = makeEngine(pb);
    await engine.start();
    await tick(30);

    pb.failNext('projects', 'create', sdkError(400));
    await engine.collection<Project>('projects').create({ title: 'rejected' });
    const good = await engine.collection<Project>('tasks').create({ title: 'fine' });
    await engine.push();
    await tick(30);

    let status: SyncStatus = 'online';
    engine.status.subscribe((v) => (status = v))();
    expect(status).toBe('error');
    expect(pb.get('tasks', good.id)).toBeDefined();

    let dead: unknown[] = [];
    engine.deadLetters.subscribe((v) => (dead = v))();
    expect(dead).toHaveLength(1);
  });

  it('a halted collection is `error` while the others keep going (SPEC §6.5)', async () => {
    const pb = createFakePB({ ownerId: 'user1' });
    const engine = makeEngine(pb);
    const codes: string[] = [];
    engine.on('error', (e: PBOEvents['error']) => codes.push(e.error.code));
    await engine.start();
    await tick(30);

    pb.failNext('projects', 'getList', sdkError(403));
    await engine.pull();
    await tick(30);

    let status: SyncStatus = 'online';
    engine.status.subscribe((v) => (status = v))();
    expect(status).toBe('error');
    expect(codes).toContain('pull/denied');
  });

  /**
   * REGRESSION (review finding 2) — `halted` was cleared in exactly one place,
   * `runner.start()`, and a halted collection is skipped before every pass, so
   * nothing else could ever clear it. §6.5 halts so a doomed request is not
   * retried in a LOOP; it does not make the halt survive until a page reload.
   */
  it('re-probes a halted collection once the link comes back (SPEC §6.5)', async () => {
    const pb = createFakePB({ ownerId: 'user1' });
    const engine = makeEngine(pb);
    await engine.start();
    await tick(30);

    pb.failNext('projects', 'getList', sdkError(403));
    await engine.pull();
    await tick(20);
    const whileHalted = pb.requests.filter((r) => r.method === 'getList' && r.collection === 'projects').length;

    // Halted really means halted: no request while the link stays up.
    await engine.pull();
    await tick(20);
    expect(pb.requests.filter((r) => r.method === 'getList' && r.collection === 'projects').length).toBe(whileHalted);

    // The link drops…
    pb.offline = true;
    await engine.sync();
    await tick(60);
    // …and comes back, which is also what a recovered session looks like here.
    pb.offline = false;
    await engine.sync();
    await tick(60);

    expect(
      pb.requests.filter((r) => r.method === 'getList' && r.collection === 'projects').length,
    ).toBeGreaterThan(whileHalted);
    let status: SyncStatus = 'error';
    engine.status.subscribe((v) => (status = v))();
    expect(status).not.toBe('error');
  }, 15_000);

  it('reports lastSyncedAt and online once a pass completes', async () => {
    const pb = createFakePB({ ownerId: 'user1' });
    const engine = makeEngine(pb);
    await engine.start();
    await engine.sync();
    await tick(30);

    let at: number | null = null;
    engine.lastSyncedAt.subscribe((v) => (at = v))();
    let online = false;
    engine.online.subscribe((v) => (online = v))();
    expect(at).not.toBeNull();
    expect(online).toBe(true);
  });

  it('stays offline, and silent, with no session', async () => {
    const pb = createFakePB({ ownerId: null });
    const engine = makeEngine(pb);
    await engine.start();
    await engine.sync();
    await tick(30);
    let status: SyncStatus = 'online';
    engine.status.subscribe((v) => (status = v))();
    expect(status).toBe('offline');
    expect(pb.requests).toEqual([]);
    dbNames.length = 0;
  });
});

describe('scheduling and re-entrancy (SPEC §5.11, §13.3)', () => {
  it('a concurrent push() returns the in-flight drain, not a second one', async () => {
    const pb = createFakePB({ ownerId: 'user1' });
    const engine = makeEngine(pb);
    await engine.start();
    await tick(30);
    await engine.collection<Project>('projects').create({ title: 'A' });

    const [a, b, c] = await Promise.all([engine.push(), engine.push(), engine.push()]);
    expect(pb.recordRequests().filter((r) => r.method === 'create')).toHaveLength(1);
    expect(a).toEqual(b);
    expect(b).toEqual(c);
    expect(pb.maxInFlight).toBe(1);
  });

  it('a local write wakes the drainer without anyone calling push()', async () => {
    const pb = createFakePB({ ownerId: 'user1' });
    const engine = makeEngine(pb);
    await engine.start();
    await tick(30);

    const rec = await engine.collection<Project>('projects').create({ title: 'nudged' });
    await tick(50);
    expect(pb.get('projects', rec.id)).toBeDefined();
    let pending = -1;
    engine.pendingCount.subscribe((v) => (pending = v))();
    expect(pending).toBe(0);
  });

  it('the poll timer drains a queue nothing nudged about', async () => {
    const pb = createFakePB({ ownerId: 'user1', offline: true });
    const engine = makeEngine(pb, { pollMs: 10 } as never);
    await engine.start();
    const rec = await engine.collection<Project>('projects').create({ title: 'stranded' });
    await tick(30);
    expect(pb.get('projects', rec.id)).toBeUndefined();

    pb.offline = false;
    // No nudge, no explicit push: only the watcher's ladder and the poll timer.
    await tick(200);
    expect(pb.get('projects', rec.id)).toBeDefined();
  }, 15_000);

  it('stop() ends the timers and the pinging', async () => {
    const pb = createFakePB({ ownerId: 'user1', offline: true });
    const engine = makeEngine(pb);
    await engine.start();
    await tick(40);
    await engine.stop();
    const after = pb.requests.length;
    await tick(60);
    expect(pb.requests.length).toBe(after);
  });
});
