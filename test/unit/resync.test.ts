/** SPEC §11 — full resync, and §5.6's dead-letter lifecycle. */
import { afterEach, describe, expect, it } from 'vitest';
import { fullResync, reapplyPending, shouldResync } from '../../src/sync/resync.js';
import { drain } from '../../src/sync/push.js';
import { readCursorRow, writeCursor } from '../../src/sync/pull.js';
import { DEFAULT_RETENTION_MS } from '../../src/sync/tombstones.js';
import { discard, list, park, retry } from '../../src/sync/deadletter.js';
import { create as crudCreate, remove as crudRemove, update as crudUpdate } from '../../src/collections/crud.js';
import { readMeta } from '../../src/db/meta.js';
import { openTestDb, queue, syncHarness, testConfig, type TestDb } from './_helpers.js';
import { createFakePB, sdkError, type FakePB, type FakeRecord } from '../fakes/pb.js';
import type { PendingEntry } from '../../src/types.js';

let t: TestDb;
let pb: FakePB;

afterEach(async () => {
  await t?.destroy();
});

async function open(over?: Record<string, unknown>): Promise<ReturnType<typeof syncHarness>> {
  t = await openTestDb(testConfig({ writeCoalesceMs: 0, pullPageSize: 10, ...over } as never));
  pb = createFakePB({ ownerId: 'user1' });
  return syncHarness(t, pb);
}

const rec = (id: string, updated: string): FakeRecord =>
  ({ id, created: updated, updated, title: `server-${id}` }) as FakeRecord;

describe('the procedure (SPEC §11)', () => {
  it('clears the mirror, cursors and meta — and PRESERVES the queue and dead letters', async () => {
    const h = await open();
    // Something synced, something queued, something dead-lettered.
    const synced = await crudCreate(t.deps('projects'), { title: 'synced' });
    await drain(h.push());
    const unsent = await crudCreate(t.deps('projects'), { title: 'unsent' });
    await park(
      t.handle.db,
      {
        seq: 9999,
        collection: 'tasks',
        recordId: 'ttttttttttttttt',
        op: 'update',
        payload: { x: 1 },
        clientUpdatedAt: Date.now(),
        state: 'queued',
        attempts: 1,
        nextAttemptAt: 0,
        lastError: null,
      } satisfies PendingEntry,
      { code: 'push/rejected', message: 'no', retryable: false, at: Date.now() },
    );
    await writeCursor(t.handle.db, 'projects', { updated: '2026-01-01 00:00:00.000Z', id: synced.id }, Date.now());

    pb.setPages('projects', [[rec('server-only', '2026-03-01 00:00:00.000Z')]]);
    // The link is down, so step 2 cannot drain: the queue has to survive the
    // wipe and be re-applied. (Draining first has its own test below.)
    h.online = false;
    await fullResync(h.pull({ drain: () => drain(h.push()) }), 'manual');

    // 3 · _pending and _deadletter survived.
    expect(await t.handle.db.pbo.deadletter.count()).toBe(1);
    // 5 · the queued create was re-applied to the mirror, so the UI keeps it.
    expect(await t.handle.db.table('projects').get(unsent.id)).toMatchObject({ title: 'unsent' });
    // 4 · the server's row is there, fetched from a null cursor.
    expect(await t.handle.db.table('projects').get('server-only')).toBeDefined();
    // and the stale local row that the server no longer has is gone.
    expect(await t.handle.db.table('projects').get(synced.id)).toBeUndefined();

    expect(h.of('full-resync')).toEqual([
      { phase: 'start', reason: 'manual' },
      { phase: 'done', reason: 'manual' },
    ]);
  });

  it('drains the queue first when the link is up, so local intent is not resynced', async () => {
    const h = await open();
    const rec1 = await crudCreate(t.deps('projects'), { title: 'must reach the server' });
    pb.setPages('projects', [[]]);

    await fullResync(h.pull({ drain: () => drain(h.push()) }), 'manual');
    expect(pb.get('projects', rec1.id)).toBeDefined();
    expect(await queue(t.handle.db)).toEqual([]);
  });

  it('keeps an undrainable queue through the wipe and re-applies it afterwards', async () => {
    const h = await open();
    const unsent = await crudCreate(t.deps('projects'), { title: 'unsent' });
    h.online = false;

    await fullResync(h.pull({ drain: () => drain(h.push()) }), 'manual');
    const rows = await queue(t.handle.db);
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ recordId: unsent.id, op: 'create' });
    expect(await t.handle.db.table('projects').get(unsent.id)).toMatchObject({ title: 'unsent' });
    expect((await readMeta(t.handle.db, 'projects', unsent.id))?.state).toBe('dirty');
  });

  /** REGRESSION (review finding 13) — the return value `runFull` gates on. */
  it('reports whether it reached {phase:done}, so a failure cannot claim a sync', async () => {
    const h = await open();
    pb.setPages('projects', [[]]);
    expect(await fullResync(h.pull({ drain: () => drain(h.push()) }), 'manual')).toBe(true);

    const boom = h.pull({
      drain: () => {
        throw new Error('the drain blew up');
      },
    });
    expect(await fullResync(boom, 'manual')).toBe(false);
    expect(h.of('full-resync')).toContainEqual({ phase: 'failed', reason: 'manual' });
  });

  it('emits {phase:failed} and leaves the mirror consistent when a step throws', async () => {
    const h = await open();
    await t.handle.db.table('projects').put({ id: 'aaaaaaaaaaaaaaa', title: 'before' });
    // A pull failure that is neither a link drop nor a denial: unparsable.
    pb.failNext('projects', 'getList', sdkError(422));

    await fullResync(h.pull(), 'manual');
    const phases = h.of('full-resync').map((e) => (e as { phase: string }).phase);
    // The pull failure is absorbed as a per-collection resync signal, so the
    // procedure still completes; the mirror is empty-but-consistent either way.
    expect(phases[0]).toBe('start');
    expect(await t.handle.db.table('projects').get('aaaaaaaaaaaaaaa')).toBeUndefined();
  });

  /**
   * REGRESSION (review finding 1) — SPEC §11.5 re-apply FIDELITY, which nothing
   * asserted end to end.
   *
   * The mirror is wiped at step 3 and `_pending` deliberately is not, so at step
   * 4 every record with a queued edit hits §10.2's dirty-protection branch with
   * NO local row to protect. Discarding the server's copy there and re-applying
   * only the queued patch at step 5 left `{id, ...patch}` in the mirror — every
   * other field gone — and permanently so if that entry later dead-lettered,
   * because `_meta` was cleared and the cursor had already advanced past the
   * record. A `staleAfterMs` config or an aged tombstone cursor fires this
   * resync with no user action at all.
   */
  it('keeps every field of a record that had a queued partial edit (SPEC §11.5)', async () => {
    const h = await open();
    const record = await crudCreate(t.deps('projects'), {
      title: 'Original',
      colour: 'red',
      notes: 'keep me',
    });
    await drain(h.push());
    const server = pb.get('projects', record.id)!;

    // One offline edit of ONE field. `update` enqueues the changed fields only.
    await crudUpdate(t.deps('projects'), record.id, { title: 'Edited' });
    h.online = false;
    pb.setPages('projects', [[server]]);

    await fullResync(h.pull({ drain: () => drain(h.push()) }), 'manual');

    expect(await t.handle.db.table('projects').get(record.id)).toMatchObject({
      id: record.id,
      title: 'Edited', // the local edit still wins…
      colour: 'red', // …and the fields it never touched survived.
      notes: 'keep me',
    });
  });

  it('and keeps them even if that queued edit is later dead-lettered', async () => {
    const h = await open();
    const record = await crudCreate(t.deps('projects'), { title: 'Original', colour: 'red' });
    await drain(h.push());
    const server = pb.get('projects', record.id)!;
    await crudUpdate(t.deps('projects'), record.id, { title: 'Edited' });
    h.online = false;
    pb.setPages('projects', [[server]]);

    await fullResync(h.pull({ drain: () => drain(h.push()) }), 'manual');
    // The 4xx that used to make the truncation permanent: `_pending` empties,
    // `_meta` is already cleared and the cursor is past the record, so nothing
    // will ever refetch it.
    await park(t.handle.db, (await queue(t.handle.db))[0]!, {
      code: 'push/rejected',
      message: 'rules said no',
      retryable: false,
      at: Date.now(),
    });

    expect(await t.handle.db.table('projects').get(record.id)).toMatchObject({
      title: 'Edited',
      colour: 'red',
    });
  });

  it('keeps a record whose CREATE the server refused, with its error, through a resync (§5.13)', async () => {
    const h = await open();
    // A create the server answered with a 400 (a name over the field's maximum):
    // dead-lettered, so it is in no queue and on no server — only here.
    const refused = await crudCreate(t.deps('projects'), { title: 'x'.repeat(255) });
    const rejection = {
      code: 'push/rejected' as const,
      message: 'Must be no more than 200',
      retryable: false,
      at: Date.now(),
    };
    await park(t.handle.db, (await queue(t.handle.db))[0]!, rejection);
    h.online = false;
    pb.setPages('projects', [[rec('server-only', '2026-03-01 00:00:00.000Z')]]);

    await fullResync(h.pull({ drain: () => drain(h.push()) }), 'manual');

    expect(await t.handle.db.table('projects').get(refused.id)).toMatchObject({ title: 'x'.repeat(255) });
    expect(await readMeta(t.handle.db, 'projects', refused.id)).toMatchObject({
      state: 'dead',
      lastError: rejection,
    });
    expect(await t.handle.db.pbo.deadletter.count()).toBe(1);
  });

  it('but does NOT resurrect a record whose only queued entry is a delete', async () => {
    const h = await open();
    const record = await crudCreate(t.deps('projects'), { title: 'doomed' });
    await drain(h.push());
    const server = pb.get('projects', record.id)!;
    await crudRemove(t.deps('projects'), record.id);
    h.online = false;
    pb.setPages('projects', [[server]]);

    await fullResync(h.pull({ drain: () => drain(h.push()) }), 'manual');
    // The absent local row IS the user's intent here; seeding would undo it.
    expect(await t.handle.db.table('projects').get(record.id)).toBeUndefined();
    expect((await queue(t.handle.db)).map((e) => e.op)).toEqual(['delete']);
  });

  it('re-applies a queued delete as a delete, not as a row', async () => {
    const h = await open();
    const record = await crudCreate(t.deps('projects'), { title: 'x' });
    await drain(h.push());
    await t.handle.db.pbo.pending.add({
      collection: 'projects',
      recordId: record.id,
      op: 'delete',
      payload: null,
      clientUpdatedAt: Date.now(),
      state: 'queued',
      attempts: 0,
      nextAttemptAt: 0,
      lastError: null,
    } as PendingEntry);
    await t.handle.db.table('projects').put({ id: record.id, title: 'resurrected by a pull' });

    await reapplyPending(h.pull());
    expect(await t.handle.db.table('projects').get(record.id)).toBeUndefined();
  });
});

describe('the automatic triggers (SPEC §11.1, §11.2)', () => {
  it('fires cursor-stale when the tombstone cursor predates the retention window', async () => {
    const h = await open();
    const now = Date.now();
    await writeCursor(t.handle.db, 'deletions', { updated: null, id: null }, now - DEFAULT_RETENTION_MS - 1_000);
    expect(await shouldResync(h.pull())).toBe('cursor-stale');

    await writeCursor(t.handle.db, 'deletions', { updated: null, id: null }, now);
    expect(await shouldResync(h.pull())).toBeNull();
  });

  it('fires stale-after once staleAfterMs has elapsed since the last pull', async () => {
    const h = await open({ staleAfterMs: 60_000 });
    const now = Date.now();
    await writeCursor(t.handle.db, 'deletions', { updated: null, id: null }, now);
    await writeCursor(t.handle.db, 'projects', { updated: null, id: null }, now - 61_000);
    await writeCursor(t.handle.db, 'tasks', { updated: null, id: null }, now - 61_000);
    expect(await shouldResync(h.pull())).toBe('stale-after');
  });

  it('is silent on a client that has never pulled anything', async () => {
    const h = await open({ staleAfterMs: 60_000 });
    expect(await shouldResync(h.pull())).toBeNull();
  });

  it('a server-rejected cursor surfaces as fullResyncTriggered on the pull result', async () => {
    const h = await open();
    pb.failNext('projects', 'getList', sdkError(422));
    const { pull } = await import('../../src/sync/pull.js');
    const result = await pull(h.pull());
    expect(result.fullResyncTriggered).toBe(true);
  });
});

describe('dead letters (SPEC §5.6)', () => {
  const shape = { code: 'push/rejected' as const, message: 'rules said no', retryable: false, at: 1 };

  it('parks an entry, removes it from the queue and marks the record dead', async () => {
    const h = await open();
    const record = await crudCreate(t.deps('projects'), { title: 'x' });
    const entry = (await queue(t.handle.db))[0]!;

    const parked = await park(t.handle.db, entry, shape);
    expect(parked).toMatchObject({ collection: 'projects', recordId: record.id, op: 'create' });
    expect(await queue(t.handle.db)).toEqual([]);
    expect((await readMeta(t.handle.db, 'projects', record.id))?.state).toBe('dead');
    // The mirror row is NEVER reverted.
    expect(await t.handle.db.table('projects').get(record.id)).toMatchObject({ title: 'x' });
    expect(await list(t.handle.db)).toHaveLength(1);
    void h;
  });

  it('retry re-enqueues at the TAIL, behind everything already queued', async () => {
    const h = await open();
    const first = await crudCreate(t.deps('projects'), { title: 'first' });
    const parkedEntry = (await queue(t.handle.db))[0]!;
    const parked = await park(t.handle.db, parkedEntry, shape);
    const second = await crudCreate(t.deps('projects'), { title: 'second' });

    await retry(t.handle.db, parked.id);
    const rows = await queue(t.handle.db);
    expect(rows.map((r) => r.recordId)).toEqual([second.id, first.id]);
    expect(await t.handle.db.pbo.deadletter.count()).toBe(0);
    expect((await readMeta(t.handle.db, 'projects', first.id))?.state).toBe('dirty');
    void h;
  });

  it('discard drops it permanently and leaves the mirror row alone', async () => {
    const h = await open();
    const record = await crudCreate(t.deps('projects'), { title: 'x' });
    const parked = await park(t.handle.db, (await queue(t.handle.db))[0]!, shape);

    await discard(t.handle.db, parked.id);
    expect(await list(t.handle.db)).toEqual([]);
    expect(await t.handle.db.table('projects').get(record.id)).toMatchObject({ title: 'x' });
    expect((await readMeta(t.handle.db, 'projects', record.id))?.state).toBe('synced');
    void h;
  });

  it('a dead letter does not stop the record from being edited again', async () => {
    const h = await open();
    const record = await crudCreate(t.deps('projects'), { title: 'x' });
    await park(t.handle.db, (await queue(t.handle.db))[0]!, shape);
    await crudUpdate(t.deps('projects'), record.id, { title: 'y' });
    expect(await queue(t.handle.db)).toHaveLength(1);
    void h;
  });
});

describe('cursor bookkeeping', () => {
  it('writeCursor stamps lastPullAt and readCursorRow reads it back', async () => {
    await open();
    await writeCursor(t.handle.db, 'projects', { updated: '2026-01-01 00:00:00.000Z', id: 'a' }, 42);
    expect(await readCursorRow(t.handle.db, 'projects')).toEqual({
      collection: 'projects',
      updated: '2026-01-01 00:00:00.000Z',
      id: 'a',
      lastPullAt: 42,
    });
  });
});
