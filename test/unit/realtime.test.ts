/** SPEC §9 — realtime, all seven rules. */
import { afterEach, describe, expect, it } from 'vitest';
import Dexie from 'dexie';
import { createPBOffline } from '../../src/index.js';
import { dbNameFor } from '../../src/brand.js';
import { createRealtime, type Realtime } from '../../src/sync/realtime.js';
import { readCursor, writeCursor } from '../../src/sync/pull.js';
import { writeMeta } from '../../src/db/meta.js';
import { asPB, createFakePB, sdkError, type FakePB } from '../fakes/pb.js';
import { openTestDb, queue, testConfig, tick, uniqueAppId, type TestDb } from './_helpers.js';
import type { PBOEvents, PBOfflineConfig, PBRecord, ResolvedConfig } from '../../src/types.js';

interface Rig {
  t: TestDb;
  pb: FakePB;
  rt: Realtime;
  events: Array<{ type: keyof PBOEvents; payload: unknown }>;
  connection: boolean[];
  errors: unknown[];
  active: boolean;
}

const rigs: Rig[] = [];

afterEach(async () => {
  while (rigs.length > 0) {
    const r = rigs.pop()!;
    await r.rt.unsubscribe();
    await r.t.destroy();
  }
});

async function rig(cfg: ResolvedConfig = testConfig(), ownerId: string | null = 'user1'): Promise<Rig> {
  const t = await openTestDb(cfg);
  const pb = createFakePB({ ownerId });
  const r: Rig = {
    t,
    pb,
    events: [],
    connection: [],
    errors: [],
    active: true,
    rt: undefined as unknown as Realtime,
  };
  r.rt = createRealtime({
    pb: asPB(pb),
    db: t.handle.db,
    cfg,
    getOwnerId: () => ownerId,
    emit: (type, payload) => r.events.push({ type, payload }),
    onError: (err) => r.errors.push(err),
    onConnectionChange: (open) => r.connection.push(open),
    isActive: () => r.active,
  });
  rigs.push(r);
  return r;
}

/** Let the module's serialising apply chain settle. */
async function settled(r: Rig): Promise<void> {
  await r.rt.settled();
  await tick(5);
}

describe('subscription shape (SPEC §9.1, §9.2)', () => {
  it('subscribes once per collection plus the tombstones, each filtered by owner', async () => {
    const r = await rig();
    await r.rt.subscribe();

    expect(r.pb.subscriptions.map((s) => s.collection)).toEqual(['projects', 'tasks', 'deletions']);
    expect(r.pb.subscriptions.every((s) => s.topic === '*')).toBe(true);
    // Never a collection-wide unfiltered subscription: PB evaluates every
    // connected client's subscriptions on every mutation.
    expect(r.pb.subscriptions.map((s) => s.filter)).toEqual([
      'owner = "user1"',
      'owner = "user1"',
      'owner = "user1"',
    ]);
  });

  it('omits the filter for a collection configured with filter: null', async () => {
    const r = await rig(testConfig({ collections: [{ name: 'projects', filter: null }] }));
    await r.rt.subscribe();
    expect(r.pb.subscriptions.find((s) => s.collection === 'projects')?.filter).toBeNull();
    // The tombstone stream is owner-scoped regardless: deletes are never shared.
    expect(r.pb.subscriptions.find((s) => s.collection === 'deletions')?.filter).toBe('owner = "user1"');
  });

  it('is idempotent — a second subscribe does not double the connections', async () => {
    const r = await rig();
    await r.rt.subscribe();
    await r.rt.subscribe();
    await Promise.all([r.rt.subscribe(), r.rt.subscribe()]);
    expect(r.pb.subscriptions).toHaveLength(3);
  });

  it('subscribes to nothing with no session (SPEC §14.3) or with realtime disabled', async () => {
    const anon = await rig(testConfig(), null);
    await anon.rt.subscribe();
    expect(anon.pb.subscriptions).toEqual([]);
    expect(anon.pb.requests).toEqual([]);

    const off = await rig(testConfig({ realtime: { enabled: false } }));
    await off.rt.subscribe();
    expect(off.pb.subscriptions).toEqual([]);
  });

  it('unwinds a half-subscribed set rather than leaving one collection live', async () => {
    const r = await rig();
    r.pb.failNext('tasks', 'subscribe', sdkError(500));
    await r.rt.subscribe();

    expect(r.rt.isSubscribed()).toBe(false);
    expect(r.pb.subscriptions).toEqual([]);
    expect(r.errors).toHaveLength(1);
    expect(r.connection).toEqual([false]);
  });
});

describe('applying an event (SPEC §9.3, §9.4)', () => {
  it('applies a create through the pull path and does NOT advance the cursor', async () => {
    const r = await rig();
    await writeCursor(r.t.handle.db, 'projects', { updated: '2026-01-01 00:00:00.000Z', id: 'aaa' }, 1);
    const before = await readCursor(r.t.handle.db, 'projects');

    await r.rt.subscribe();
    r.pb.emitEvent('projects', 'create', {
      id: 'bbbbbbbbbbbbbbb',
      title: 'from SSE',
      updated: '2026-02-02 00:00:00.000Z',
      created: '2026-02-02 00:00:00.000Z',
    });
    await settled(r);

    expect(await r.t.handle.db.table('projects').get('bbbbbbbbbbbbbbb')).toMatchObject({ title: 'from SSE' });
    // THE rule of §9: an event is unordered with respect to the cursor stream,
    // so advancing from it would skip every record in between.
    expect(await readCursor(r.t.handle.db, 'projects')).toEqual(before);
    expect(r.events.map((e) => e.type)).toEqual(['record-synced']);
  });

  /**
   * REGRESSION (review finding 8) — the subscribe window.
   *
   * `subscribed` only flipped once the LAST subscription resolved, and the gate
   * at `onRecordEvent`'s entry read it, so every event delivered for collection 1
   * while collections 2..N and the tombstone stream were still subscribing was
   * silently discarded. §3.1 runs the cursor pull BEFORE subscribing (step 2,
   * then step 3), so a change made in that window is already past the cursor and
   * not yet on the stream — invisible until the next reconnect or a manual
   * `sync()`, and the drainer's poll only drains. On a long-lived connection that
   * gap persists for days.
   */
  it('applies an event that arrives while the LATER subscriptions are still opening', async () => {
    const r = await rig();
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });

    // Hold the last subscription (the tombstone stream) open mid-handshake.
    const base = r.pb.collection.bind(r.pb);
    (r.pb as unknown as { collection: unknown }).collection = (name: string) => {
      const c = base(name);
      if (name !== 'deletions') return c;
      return {
        ...c,
        subscribe: async (topic: string, cb: unknown, o?: unknown) => {
          await gate;
          return c.subscribe(topic, cb as never, o);
        },
      };
    };

    const subscribing = r.rt.subscribe();
    await tick(5);
    expect(r.pb.subscriptions.map((s) => s.collection)).toEqual(['projects', 'tasks']);
    expect(r.rt.isSubscribed()).toBe(false); // still mid-handshake

    r.pb.emitEvent('projects', 'create', {
      id: 'eeeeeeeeeeeeeee',
      title: 'landed mid-handshake',
      updated: '2026-02-02 00:00:00.000Z',
      created: '2026-02-02 00:00:00.000Z',
    });

    release();
    await subscribing;
    await settled(r);

    expect(await r.t.handle.db.table('projects').get('eeeeeeeeeeeeeee')).toMatchObject({
      title: 'landed mid-handshake',
    });
  });

  it('still drops an event once the subscriptions are torn down', async () => {
    const r = await rig();
    await r.rt.subscribe();
    const live = r.pb.subscriptions[0]!;
    await r.rt.unsubscribe();

    // The SDK can still hand over an event already in its queue.
    live.cb({
      action: 'create',
      record: { id: 'fffffffffffffff', title: 'too late', updated: '2026-02-02 00:00:00.000Z' },
    });
    await settled(r);
    expect(await r.t.handle.db.table('projects').get('fffffffffffffff')).toBeUndefined();
  });

  it('drops this device’s own write coming back, exactly as a pull would', async () => {
    const r = await rig();
    const db = r.t.handle.db;
    await db.table('projects').put({ id: 'ccccccccccccccc', title: 'mine' });
    await db.transaction('rw', db.pbo.meta, async () => {
      await writeMeta(db, 'projects', 'ccccccccccccccc', {
        serverUpdated: '2026-03-03 00:00:00.000Z',
        lastPushedUpdated: '2026-03-03 00:00:00.000Z',
      });
    });

    await r.rt.subscribe();
    r.pb.emitEvent('projects', 'update', {
      id: 'ccccccccccccccc',
      title: 'mine',
      updated: '2026-03-03 00:00:00.000Z',
    });
    await settled(r);

    // Echo suppression is the SAME guard, because it is the same apply path.
    expect(r.events).toEqual([]);
  });

  it('protects a queued local mutation from an incoming remote version', async () => {
    const r = await rig();
    const db = r.t.handle.db;
    await db.table('projects').put({ id: 'ddddddddddddddd', title: 'local edit' });
    await db.pbo.pending.add({
      collection: 'projects',
      recordId: 'ddddddddddddddd',
      op: 'update',
      payload: { title: 'local edit' },
      clientUpdatedAt: Date.now(),
      state: 'queued',
      attempts: 0,
      nextAttemptAt: 0,
      lastError: null,
    } as never);

    await r.rt.subscribe();
    r.pb.emitEvent('projects', 'update', {
      id: 'ddddddddddddddd',
      title: 'remote edit',
      updated: '2026-04-04 00:00:00.000Z',
    });
    await settled(r);

    expect(await db.table('projects').get('ddddddddddddddd')).toMatchObject({ title: 'local edit' });
    expect(r.events.map((e) => e.type)).toEqual(['conflict']);
  });

  it('applies events one at a time, in arrival order', async () => {
    const r = await rig();
    await r.rt.subscribe();
    for (let i = 1; i <= 5; i++) {
      r.pb.emitEvent('projects', 'update', {
        id: 'eeeeeeeeeeeeeee',
        title: `v${i}`,
        updated: `2026-05-05 00:00:00.00${i}Z`,
      });
    }
    await settled(r);
    expect(await r.t.handle.db.table('projects').get('eeeeeeeeeeeeeee')).toMatchObject({ title: 'v5' });
  });
});

describe('deletes over SSE (SPEC §9.6, §8.3–8.4)', () => {
  it('applies a delete action directly, as a tombstone would', async () => {
    const r = await rig();
    const db = r.t.handle.db;
    await db.table('projects').put({ id: 'fffffffffffffff', title: 'doomed' });

    await r.rt.subscribe();
    r.pb.emitEvent('projects', 'delete', { id: 'fffffffffffffff' });
    await settled(r);

    expect(await db.table('projects').get('fffffffffffffff')).toBeUndefined();
  });

  it('preserves a queued create: id reuse after a delete is legal (SPEC §8.3)', async () => {
    const r = await rig();
    const db = r.t.handle.db;
    await db.table('projects').put({ id: 'ggggggggggggggg', title: 'recreated' });
    await db.pbo.pending.add({
      collection: 'projects',
      recordId: 'ggggggggggggggg',
      op: 'create',
      payload: { id: 'ggggggggggggggg', title: 'recreated' },
      clientUpdatedAt: Date.now(),
      state: 'queued',
      attempts: 0,
      nextAttemptAt: 0,
      lastError: null,
    } as never);
    await db.pbo.pending.add({
      collection: 'projects',
      recordId: 'ggggggggggggggg',
      op: 'update',
      payload: { title: 'edited' },
      clientUpdatedAt: Date.now(),
      state: 'queued',
      attempts: 0,
      nextAttemptAt: 0,
      lastError: null,
    } as never);

    await r.rt.subscribe();
    r.pb.emitEvent('projects', 'delete', { id: 'ggggggggggggggg' });
    await settled(r);

    const remaining = await queue(db);
    expect(remaining.map((e) => e.op)).toEqual(['create']);
    // A delete is not mergeable, so the lost edit is reported loudly (§8.4).
    expect(r.events.map((e) => e.type)).toEqual(['conflict']);
  });

  it('applies a tombstone row arriving on the tombstone stream', async () => {
    const r = await rig();
    const db = r.t.handle.db;
    await db.table('tasks').put({ id: 'hhhhhhhhhhhhhhh', title: 'deleted elsewhere' });

    await r.rt.subscribe();
    r.pb.emitEvent('deletions', 'create', {
      id: 'tombstone000001',
      collection: 'tasks',
      record_id: 'hhhhhhhhhhhhhhh',
      updated: '2026-06-06 00:00:00.000Z',
    });
    await settled(r);
    expect(await db.table('tasks').get('hhhhhhhhhhhhhhh')).toBeUndefined();

    // A PRUNE of that tombstone must not resurrect anything.
    await db.table('tasks').put({ id: 'iiiiiiiiiiiiiii', title: 'still here' });
    r.pb.emitEvent('deletions', 'delete', {
      id: 'tombstone000001',
      collection: 'tasks',
      record_id: 'iiiiiiiiiiiiiii',
    });
    await settled(r);
    expect(await db.table('tasks').get('iiiiiiiiiiiiiii')).toBeDefined();
  });
});

describe('teardown (SPEC §9.7, §13.6)', () => {
  it('unsubscribes every stream and ignores events that arrive afterwards', async () => {
    const r = await rig();
    await r.rt.subscribe();
    const streams = [...r.pb.subscriptions];
    await r.rt.unsubscribe();

    expect(r.rt.isSubscribed()).toBe(false);
    expect(r.pb.subscriptions).toEqual([]);
    expect(r.pb.requests.filter((q) => q.method === 'unsubscribe')).toHaveLength(3);

    // A callback the SDK still holds must not write anything.
    for (const s of streams) s.cb({ action: 'create', record: { id: 'jjjjjjjjjjjjjjj' } });
    await settled(r);
    expect(await r.t.handle.db.table('projects').get('jjjjjjjjjjjjjjj')).toBeUndefined();
  });

  it('drops an event delivered after the drainer lock was lost', async () => {
    const r = await rig();
    await r.rt.subscribe();
    r.active = false; // the lock went to another tab between event and apply
    r.pb.emitEvent('projects', 'create', { id: 'kkkkkkkkkkkkkkk', title: 'not ours to apply' });
    await settled(r);
    expect(await r.t.handle.db.table('projects').get('kkkkkkkkkkkkkkk')).toBeUndefined();
  });

  it('is safe to unsubscribe without ever having subscribed', async () => {
    const r = await rig();
    await expect(r.rt.unsubscribe()).resolves.toBeUndefined();
  });
});

describe('connection state feeds the watcher (SPEC §3.1c)', () => {
  it('reports open on subscribe, suspect on a drop, and open again on reconnect', async () => {
    const r = await rig();
    await r.rt.subscribe();
    expect(r.connection).toEqual([true]);

    r.pb.dropConnection();
    expect(r.connection).toEqual([true, false]);

    r.pb.reconnect();
    expect(r.connection).toEqual([true, false, true]);
  });

  it('does not report our own teardown as a connection loss', async () => {
    const r = await rig();
    await r.rt.subscribe();
    await r.rt.unsubscribe();
    // The SDK fires onDisconnect with an empty active list when WE unsubscribe.
    r.pb.realtime.onDisconnect?.([]);
    expect(r.connection).toEqual([true]);
  });

  it('restores whatever onDisconnect hook the application had installed', async () => {
    const r = await rig();
    const theirs = (): void => {};
    r.pb.realtime.onDisconnect = theirs;
    await r.rt.subscribe();
    expect(r.pb.realtime.onDisconnect).not.toBe(theirs);
    await r.rt.unsubscribe();
    expect(r.pb.realtime.onDisconnect).toBe(theirs);
  });
});

describe('the engine wires SSE state into the link watcher (SPEC §3.1c)', () => {
  it('probes the link when the SSE connection drops — suspect, not down', async () => {
    const appId = uniqueAppId();
    const pb = createFakePB({ ownerId: 'user1' });
    const engine = createPBOffline(asPB(pb), {
      appId,
      collections: [{ name: 'projects' }],
      backoff: { min: 5, max: 20 },
    } as PBOfflineConfig);

    try {
      await engine.start();
      await tick(30);
      expect(pb.subscriptions.length).toBeGreaterThan(0);

      const pingsBefore = pb.requests.filter((r) => r.method === 'health').length;
      pb.dropConnection();
      await tick(20);

      // A disconnect is a HINT: the watcher pings and lets the ping decide.
      expect(pb.requests.filter((r) => r.method === 'health').length).toBeGreaterThan(pingsBefore);
      let online = false;
      engine.online.subscribe((v) => (online = v))();
      expect(online).toBe(true); // the probe succeeded, so the link is up
    } finally {
      await engine.destroy();
      await Dexie.delete(dbNameFor(appId, 'user1'));
    }
  });
});

describe('the cursor is never touched (SPEC §9.4, §18.2)', () => {
  it('holds across a create, an update and a delete', async () => {
    const r = await rig();
    const db = r.t.handle.db;
    await writeCursor(db, 'projects', { updated: '2026-01-01 00:00:00.000Z', id: 'zzz' }, 1);
    await writeCursor(db, 'deletions', { updated: '2026-01-01 00:00:00.000Z', id: 'yyy' }, 1);
    const before = [await readCursor(db, 'projects'), await readCursor(db, 'deletions')];

    await r.rt.subscribe();
    const rec: PBRecord = { id: 'lllllllllllllll', title: 'x', updated: '2026-09-09 00:00:00.000Z' };
    r.pb.emitEvent('projects', 'create', rec);
    r.pb.emitEvent('projects', 'update', { ...rec, title: 'y', updated: '2026-09-09 00:00:01.000Z' });
    r.pb.emitEvent('projects', 'delete', rec);
    r.pb.emitEvent('deletions', 'create', {
      id: 'tombstone000002',
      collection: 'projects',
      record_id: 'lllllllllllllll',
      updated: '2026-09-09 00:00:02.000Z',
    });
    await settled(r);

    expect([await readCursor(db, 'projects'), await readCursor(db, 'deletions')]).toEqual(before);
  });
});
