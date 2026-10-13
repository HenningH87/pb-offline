/** SPEC §8 — tombstones: the only thing that turns a remote delete into a local one. */
import { afterEach, describe, expect, it } from 'vitest';
import { applyTombstone, isCursorStale, pullTombstones, DEFAULT_RETENTION_MS } from '../../src/sync/tombstones.js';
import { pull, readCursorRow } from '../../src/sync/pull.js';
import { drain } from '../../src/sync/push.js';
import { create as crudCreate, remove as crudRemove, update as crudUpdate } from '../../src/collections/crud.js';
import { readMeta } from '../../src/db/meta.js';
import { openTestDb, queue, syncHarness, testConfig, type TestDb } from './_helpers.js';
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

const tombstone = (collection: string, recordId: string, updated = '2026-01-01 00:00:01.000Z'): FakeRecord =>
  ({ id: `ts-${recordId}`, collection, record_id: recordId, owner: 'user1', created: updated, updated }) as FakeRecord;

describe('applying one tombstone (SPEC §8.3, §8.4)', () => {
  it('deletes the mirror row and its meta', async () => {
    const h = await open();
    const rec = await crudCreate(t.deps('projects'), { title: 'doomed' });
    await drain(h.push());

    expect(await applyTombstone(h.pull(), tombstone('projects', rec.id) as never)).toBe(true);
    expect(await t.handle.db.table('projects').get(rec.id)).toBeUndefined();
    expect(await readMeta(t.handle.db, 'projects', rec.id)).toBeUndefined();
  });

  it('cancels queued update and delete entries for that record', async () => {
    const h = await open();
    const rec = await crudCreate(t.deps('projects'), { title: 'x' });
    await drain(h.push());
    await crudUpdate(t.deps('projects'), rec.id, { title: 'edited' });
    const other = await crudCreate(t.deps('tasks'), { title: 'unrelated' });

    await applyTombstone(h.pull(), tombstone('projects', rec.id) as never);
    const rest = await queue(t.handle.db);
    expect(rest).toHaveLength(1);
    expect(rest[0]).toMatchObject({ collection: 'tasks', recordId: other.id });
  });

  it('PRESERVES a queued create — id reuse after a delete is legal (SPEC §8.3)', async () => {
    const h = await open();
    const rec = await crudCreate(t.deps('projects'), { title: 'new incarnation' });

    await applyTombstone(h.pull(), tombstone('projects', rec.id) as never);
    const rest = await queue(t.handle.db);
    expect(rest).toHaveLength(1);
    expect(rest[0]).toMatchObject({ op: 'create', recordId: rec.id });
    // The record's SERVER incarnation is gone, so nothing is confirmed any more.
    const meta = await readMeta(t.handle.db, 'projects', rec.id);
    expect(meta?.serverUpdated).toBeNull();
    expect(meta?.lastPushedUpdated).toBeNull();
  });

  it('IGNORES a tombstone older than the incarnation the mirror holds (SPEC §8.7)', async () => {
    const h = await open();
    const rec = await crudCreate(t.deps('projects'), { title: 'first' });
    await drain(h.push());
    await crudRemove(t.deps('projects'), rec.id);
    await drain(h.push());
    // Made again under the same id and confirmed AFTER the delete (stamped past
    // its tombstone's 00:00:01), then edited, before the tombstone is pulled.
    pb.clock = 2_000;
    await crudCreate(t.deps('projects'), { id: rec.id, title: 'again' });
    await drain(h.push());
    await crudUpdate(t.deps('projects'), rec.id, { title: 'edited again' });

    expect(await applyTombstone(h.pull(), tombstone('projects', rec.id) as never)).toBe(false);
    expect(await t.handle.db.table('projects').get(rec.id)).toMatchObject({ title: 'edited again' });
    expect(await queue(t.handle.db)).toEqual([expect.objectContaining({ op: 'update', recordId: rec.id })]);
    expect(h.of('conflict')).toEqual([]);

    // The current incarnation's own delete still applies.
    await drain(h.push());
    const later = tombstone('projects', rec.id, '2026-01-01 00:00:09.000Z');
    expect(await applyTombstone(h.pull(), later as never)).toBe(true);
    expect(await t.handle.db.table('projects').get(rec.id)).toBeUndefined();
  });

  it('a tombstone for a dirty record emits conflict{remote} and then deletes (SPEC §8.4)', async () => {
    const h = await open();
    const rec = await crudCreate(t.deps('projects'), { title: 'x' });
    await drain(h.push());
    await crudUpdate(t.deps('projects'), rec.id, { title: 'my unsent edit' });

    await applyTombstone(h.pull(), tombstone('projects', rec.id) as never);
    expect(h.of('conflict')).toEqual([
      expect.objectContaining({ collection: 'projects', id: rec.id, resolution: 'remote' }),
    ]);
    expect(await t.handle.db.table('projects').get(rec.id)).toBeUndefined();
  });

  it('a tombstone for a clean record emits no conflict', async () => {
    const h = await open();
    const rec = await crudCreate(t.deps('projects'), { title: 'x' });
    await drain(h.push());
    await applyTombstone(h.pull(), tombstone('projects', rec.id) as never);
    expect(h.of('conflict')).toEqual([]);
  });

  it('a tombstone for a collection this client does not mirror is inert', async () => {
    const h = await open();
    expect(await applyTombstone(h.pull(), tombstone('somewhere-else', 'aaaaaaaaaaaaaaa') as never)).toBe(false);
    expect(h.events).toEqual([]);
  });

  it('a tombstone for a record this client never had is a harmless no-op', async () => {
    const h = await open();
    expect(await applyTombstone(h.pull(), tombstone('projects', 'zzzzzzzzzzzzzzz') as never)).toBe(false);
  });
});

describe('pulling them (SPEC §8.2, §8.5)', () => {
  it('uses its own cursor row and the same (updated,id) mechanism', async () => {
    const h = await open();
    const rec = await crudCreate(t.deps('projects'), { title: 'x' });
    await drain(h.push());
    pb.setPages('deletions', [[tombstone('projects', rec.id, '2026-02-01 00:00:00.000Z')]]);

    const stats = await pullTombstones(h.pull());
    expect(stats).toMatchObject({ deleted: 1, pages: 1 });
    expect(await readCursorRow(t.handle.db, 'deletions')).toMatchObject({
      updated: '2026-02-01 00:00:00.000Z',
      id: `ts-${rec.id}`,
    });
    expect(h.of('pull')).toEqual([
      expect.objectContaining({ collection: 'deletions', deleted: 1 }),
    ]);
  });

  it('is pulled AFTER every record collection in a pass', async () => {
    const h = await open();
    await pull(h.pull());
    expect(pb.requests.filter((r) => r.method === 'getList').map((r) => r.collection)).toEqual([
      'projects',
      'tasks',
      'deletions',
    ]);
  });

  it('makes no request at all with no session', async () => {
    t = await openTestDb(testConfig(), 'anon');
    pb = createFakePB({ ownerId: null });
    const h = syncHarness(t, pb, null);
    const stats = await pullTombstones(h.pull());
    expect(stats).toMatchObject({ deleted: 0, pages: 0 });
    expect(pb.requests).toEqual([]);
  });

  it('records lastPullAt even on an empty page, so a healthy client is never called stale', async () => {
    const h = await open();
    await pullTombstones(h.pull());
    const row = await readCursorRow(t.handle.db, 'deletions');
    expect(row?.lastPullAt).toBeGreaterThan(0);
    expect(row?.updated).toBeNull();
  });
});

describe('retention (SPEC §8.6)', () => {
  it('is stale only once the cursor predates the window', () => {
    const now = Date.UTC(2026, 5, 1);
    expect(isCursorStale(now - 1_000, DEFAULT_RETENTION_MS, now)).toBe(false);
    expect(isCursorStale(now - DEFAULT_RETENTION_MS - 1, DEFAULT_RETENTION_MS, now)).toBe(true);
    // Never pulled ⇒ a null cursor asks for the whole window; not stale.
    expect(isCursorStale(null, DEFAULT_RETENTION_MS, now)).toBe(false);
    expect(isCursorStale(now - 10 ** 12, 0, now)).toBe(false);
  });
});
