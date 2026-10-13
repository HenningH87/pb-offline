/** SPEC §7 — echo suppression, the ONLY guard between the two data directions. */
import { afterEach, describe, expect, it } from 'vitest';
import { clearEcho, isEcho, isEchoOf, recordPush } from '../../src/sync/echo.js';
import { applyRemote } from '../../src/sync/conflict.js';
import { pullCollection, readCursorRow } from '../../src/sync/pull.js';
import { drain } from '../../src/sync/push.js';
import { create as crudCreate, update as crudUpdate } from '../../src/collections/crud.js';
import { readMeta } from '../../src/db/meta.js';
import { openTestDb, syncHarness, testConfig, type TestDb } from './_helpers.js';
import { createFakePB, type FakePB, type FakeRecord } from '../fakes/pb.js';

let t: TestDb;
let pb: FakePB;

afterEach(async () => {
  await t?.destroy();
});

async function open(): Promise<ReturnType<typeof syncHarness>> {
  t = await openTestDb(testConfig({ writeCoalesceMs: 0, pullPageSize: 10 }));
  pb = createFakePB({ ownerId: 'user1' });
  return syncHarness(t, pb);
}

describe('the predicate', () => {
  it('is exactly `remote.updated === meta.lastPushedUpdated`', () => {
    const T = '2026-01-01 00:00:00.000Z';
    expect(isEchoOf({ lastPushedUpdated: T }, T)).toBe(true);
    expect(isEchoOf({ lastPushedUpdated: T }, '2026-01-01 00:00:00.001Z')).toBe(false);
    expect(isEchoOf({ lastPushedUpdated: null }, T)).toBe(false);
    expect(isEchoOf(undefined, T)).toBe(false);
    // Never a window, never a payload comparison, never a request id.
    expect(isEchoOf({ lastPushedUpdated: T }, undefined)).toBe(false);
  });

  it('reads and clears through `_meta`', async () => {
    await open();
    const T = '2026-01-01 00:00:00.000Z';
    await recordPush(t.handle.db, 'projects', 'aaaaaaaaaaaaaaa', T);
    expect(await isEcho(t.handle.db, 'projects', { id: 'aaaaaaaaaaaaaaa', updated: T })).toBe(true);
    await clearEcho(t.handle.db, 'projects', 'aaaaaaaaaaaaaaa');
    expect(await isEcho(t.handle.db, 'projects', { id: 'aaaaaaaaaaaaaaa', updated: T })).toBe(false);
    // Clearing the echo must not lose what the server last said.
    expect((await readMeta(t.handle.db, 'projects', 'aaaaaaaaaaaaaaa'))?.serverUpdated).toBe(T);
  });
});

describe('an echo through the pull path (SPEC §7.2, §7.3)', () => {
  it('is dropped before the mirror write, still advances the cursor, and emits nothing', async () => {
    const h = await open();
    const rec = await crudCreate(t.deps('projects'), { title: 'mine' });
    await drain(h.push());
    const server = pb.get('projects', rec.id)!;
    const syncedAfterPush = h.of('record-synced').length;
    expect(syncedAfterPush).toBe(1);

    // The server hands this device its own write back.
    pb.setPages('projects', [[{ ...server } as FakeRecord]]);
    const localBefore = await t.handle.db.table('projects').get(rec.id);

    const stats = await pullCollection(h.pull(), 'projects');
    expect(stats).toMatchObject({ applied: 0, skipped: 1 });
    expect(await t.handle.db.table('projects').get(rec.id)).toEqual(localBefore);
    // 3 · the cursor moved past it: it WAS consumed.
    expect(await readCursorRow(t.handle.db, 'projects')).toMatchObject({
      updated: server.updated,
      id: rec.id,
    });
    // 3 · and no second record-synced.
    expect(h.of('record-synced')).toHaveLength(syncedAfterPush);
  });

  it('clears lastPushedUpdated once a strictly newer remote value is applied (§7.4)', async () => {
    const h = await open();
    const rec = await crudCreate(t.deps('projects'), { title: 'mine' });
    await drain(h.push());
    const pushed = (await readMeta(t.handle.db, 'projects', rec.id))!.lastPushedUpdated;
    expect(pushed).not.toBeNull();

    await applyRemote(t.handle.db, t.cfg, 'projects', {
      id: rec.id,
      title: 'from another device',
      updated: '2099-01-01 00:00:00.000Z',
    }, { emit: h.emit });

    const meta = (await readMeta(t.handle.db, 'projects', rec.id))!;
    expect(meta.lastPushedUpdated).toBeNull();
    expect(meta.serverUpdated).toBe('2099-01-01 00:00:00.000Z');
  });
});

describe('an echo through the event path (SPEC §7.2)', () => {
  it('takes exactly the same path as a pulled record and is dropped identically', async () => {
    const h = await open();
    const rec = await crudCreate(t.deps('projects'), { title: 'mine' });
    await drain(h.push());
    const server = pb.get('projects', rec.id)!;

    // An SSE event carries the same body a page would.
    const outcome = await applyRemote(t.handle.db, t.cfg, 'projects', { ...server }, { emit: h.emit });
    expect(outcome).toBe('echo-dropped');
    expect(h.of('record-synced')).toHaveLength(1);
  });
});

describe('the round trip never oscillates (TESTPLAN §1.7, S4)', () => {
  it('50 rounds of write → push → event echo → pull echo terminate with an empty queue', async () => {
    const h = await open();
    const rec = await crudCreate(t.deps('projects'), { title: 'round 0' });
    await drain(h.push());

    for (let i = 1; i <= 50; i++) {
      await crudUpdate(t.deps('projects'), rec.id, { title: `round ${i}` });
      await drain(h.push());
      const server = pb.get('projects', rec.id)!;

      // The event echo…
      expect(await applyRemote(t.handle.db, t.cfg, 'projects', { ...server }, { emit: h.emit })).toBe('echo-dropped');
      // …and the pull echo.
      pb.setPages('projects', [[{ ...server } as FakeRecord]]);
      await pullCollection(h.pull(), 'projects');
    }

    expect(await t.handle.db.pbo.pending.count()).toBe(0);
    // One push request per write: 1 create + 50 updates. No retries, no loop.
    expect(pb.recordRequests().filter((r) => r.method === 'create')).toHaveLength(1);
    expect(pb.recordRequests().filter((r) => r.method === 'update')).toHaveLength(50);
    // One record-synced per write, and not one more.
    expect(h.of('record-synced')).toHaveLength(51);
    expect((await t.handle.db.table('projects').get(rec.id)) as { title: string }).toMatchObject({
      title: 'round 50',
    });
  }, 30_000);
});
