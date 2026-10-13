/** SPEC §6 — the `(updated, id)` cursor pull, and §6.5's hazards. */
import { afterEach, describe, expect, it } from 'vitest';
import { pull, pullCollection, readCursorRow } from '../../src/sync/pull.js';
import { drain } from '../../src/sync/push.js';
import { create as crudCreate } from '../../src/collections/crud.js';
import { readMeta } from '../../src/db/meta.js';
import { openTestDb, syncHarness, testConfig, type TestDb } from './_helpers.js';
import { createFakePB, networkError, sdkError, type FakePB, type FakeRecord } from '../fakes/pb.js';

let t: TestDb;
let pb: FakePB;

afterEach(async () => {
  await t?.destroy();
});

async function open(pageSize = 3): Promise<ReturnType<typeof syncHarness>> {
  t = await openTestDb(testConfig({ pullPageSize: pageSize, writeCoalesceMs: 0 }));
  pb = createFakePB({ ownerId: 'user1' });
  return syncHarness(t, pb);
}

const rec = (id: string, updated: string, extra?: Record<string, unknown>): FakeRecord =>
  ({ id, created: updated, updated, title: id, ...extra }) as FakeRecord;

const filtersFor = (collection: string): Array<string | undefined> =>
  pb.requests
    .filter((r) => r.method === 'getList' && r.collection === collection)
    .map((r) => (r.args[2] as { filter?: string }).filter);

describe('paging and the cursor (SPEC §6.1, §6.3)', () => {
  it('pages until a short page and advances the cursor per page, in one transaction', async () => {
    const h = await open(3);
    pb.setPages('projects', [
      [rec('a', '2026-01-01 00:00:01.000Z'), rec('b', '2026-01-01 00:00:02.000Z'), rec('c', '2026-01-01 00:00:03.000Z')],
      [rec('d', '2026-01-01 00:00:04.000Z')],
    ]);

    const stats = await pullCollection(h.pull(), 'projects');
    expect(stats).toMatchObject({ pages: 2, applied: 4, skipped: 0 });
    expect(await t.handle.db.table('projects').count()).toBe(4);

    const cursor = await readCursorRow(t.handle.db, 'projects');
    expect(cursor).toMatchObject({ updated: '2026-01-01 00:00:04.000Z', id: 'd' });
    expect(cursor?.lastPullAt).toBeGreaterThan(0);

    // Request 1 has no cursor clause; request 2 carries the END of page 1.
    const filters = filtersFor('projects');
    expect(filters[0]).toBe('owner = "user1"');
    expect(filters[1]).toBe(
      '(owner = "user1") && (updated > "2026-01-01 00:00:03.000Z" || ' +
        '(updated = "2026-01-01 00:00:03.000Z" && id > "c"))',
    );
  });

  it('leaves the cursor untouched when the very first page never arrives', async () => {
    const h = await open(2);
    pb.setPages('projects', [
      [rec('a', '2026-01-01 00:00:01.000Z'), rec('b', '2026-01-01 00:00:02.000Z')],
      [rec('c', '2026-01-01 00:00:03.000Z'), rec('d', '2026-01-01 00:00:04.000Z')],
    ]);
    // Page 3 never arrives.
    pb.failNext('projects', 'getList', networkError());
    pb.failNext('projects', 'getList', networkError());
    pb.failNext('projects', 'getList', networkError());

    const stats = await pullCollection(h.pull(), 'projects');
    expect(stats.linkDown).toBe(true);
    expect(await t.handle.db.table('projects').count()).toBe(0);
    expect(await readCursorRow(t.handle.db, 'projects')).toBeUndefined();
  });

  it('crash on page 3 leaves the cursor at the end of page 2, not at the start', async () => {
    const h = await open(2);
    pb.setPages('projects', [
      [rec('a', '2026-01-01 00:00:01.000Z'), rec('b', '2026-01-01 00:00:02.000Z')],
      [rec('c', '2026-01-01 00:00:03.000Z'), rec('d', '2026-01-01 00:00:04.000Z')],
    ]);
    let calls = 0;
    pb.onRequest = (req) => {
      if (req.method !== 'getList') return;
      calls += 1;
      if (calls === 3) throw networkError();
    };

    const stats = await pullCollection(h.pull(), 'projects');
    expect(stats).toMatchObject({ pages: 2, linkDown: true });
    expect(await t.handle.db.table('projects').count()).toBe(4);
    expect(await readCursorRow(t.handle.db, 'projects')).toMatchObject({
      updated: '2026-01-01 00:00:04.000Z',
      id: 'd',
    });
  });

  it('tie-breaks on id when a whole page shares one `updated` (SPEC §6.2)', async () => {
    const h = await open(2);
    const same = '2026-01-01 00:00:00.000Z';
    pb.setPages('projects', [
      [rec('a1', same), rec('a2', same)],
      [rec('a3', same), rec('a4', same)],
      [rec('a5', same)],
    ]);

    const stats = await pullCollection(h.pull(), 'projects');
    expect(stats).toMatchObject({ pages: 3, applied: 5 });
    expect(await t.handle.db.table('projects').count()).toBe(5);

    const filters = filtersFor('projects');
    // Neither the naive loop (an unchanged filter, forever) nor the naive skip
    // (a bare `updated >` that drops the tail of the tie) happened.
    expect(filters[1]).toContain('(updated = "2026-01-01 00:00:00.000Z" && id > "a2")');
    expect(filters[2]).toContain('(updated = "2026-01-01 00:00:00.000Z" && id > "a4")');
    expect(filters[1]).not.toBe(filters[2]);
    expect(await readCursorRow(t.handle.db, 'projects')).toMatchObject({ updated: same, id: 'a5' });
  });

  it('sends sort=updated,id and skipTotal on every page', async () => {
    const h = await open(2);
    pb.setPages('projects', [[rec('a', '2026-01-01 00:00:01.000Z')]]);
    await pullCollection(h.pull(), 'projects');
    const opts = pb.requests.find((r) => r.method === 'getList')!.args[2] as Record<string, unknown>;
    expect(opts['sort']).toBe('updated,id');
    expect(opts['skipTotal']).toBe(true);
  });
});

describe('what a pull must never do (SPEC §6.5)', () => {
  it('absence from a page NEVER deletes a local row', async () => {
    const h = await open(10);
    await t.handle.db.table('projects').put({ id: 'kept', title: 'still here' });
    pb.setPages('projects', [[rec('other', '2026-01-01 00:00:01.000Z')]]);

    await pullCollection(h.pull(), 'projects');
    expect(await t.handle.db.table('projects').get('kept')).toMatchObject({ title: 'still here' });
    expect(await t.handle.db.table('projects').count()).toBe(2);
  });

  it('a 403 halts THAT collection only; the others keep syncing', async () => {
    const h = await open(10);
    pb.failNext('projects', 'getList', sdkError(403));
    pb.setPages('tasks', [[rec('t1', '2026-01-01 00:00:01.000Z')]]);

    const result = await pull(h.pull());
    expect(h.halted.has('projects')).toBe(true);
    expect(h.halted.get('projects')?.code).toBe('pull/denied');
    expect(result.byCollection['tasks']).toMatchObject({ applied: 1 });
    expect(await t.handle.db.table('tasks').get('t1')).toBeDefined();
    expect(h.of('error')).toEqual([expect.objectContaining({ error: expect.objectContaining({ code: 'pull/denied' }) })]);
  });

  it('a halted collection is skipped on the next pass, and no request is made for it', async () => {
    const h = await open(10);
    pb.failNext('projects', 'getList', sdkError(403));
    await pull(h.pull());
    const before = pb.requests.filter((r) => r.collection === 'projects').length;
    await pull(h.pull());
    expect(pb.requests.filter((r) => r.collection === 'projects').length).toBe(before);
  });

  it('a pushed record the user cannot read back does not loop (SPEC §6.5)', async () => {
    const h = await open(10);
    const record = await crudCreate(t.deps('projects'), { title: 'write-only' });
    await drain(h.push());
    // The server accepted the write but never returns the row.
    pb.setPages('projects', [[]]);

    await pull(h.pull());
    await pull(h.pull());

    expect(await t.handle.db.table('projects').get(record.id)).toBeDefined();
    const meta = await readMeta(t.handle.db, 'projects', record.id);
    expect(meta?.serverUpdated).toBe(pb.get('projects', record.id)!.updated);
    // Two passes, two page requests. No per-record refetch, no loop.
    expect(pb.requests.filter((r) => r.method === 'getList' && r.collection === 'projects')).toHaveLength(2);
  });
});

/**
 * REGRESSION (review finding 3) — SPEC §6.2/§6.3 TERMINATION.
 *
 * `advanceCursor` used to hand back `{updated: null, id}` for a row without an
 * `updated`; `isEmptyCursor` reads such a row as "no cursor at all", so the next
 * request omitted the cursor clause and re-fetched page 1. The loop only exits on
 * a short page, so against a server that keeps returning the same full page
 * `pullCollection` NEVER RETURNS — an unbounded request loop inside `runPull`'s
 * chain, behind which every later `pull()`/`sync()` queues for ever. A green
 * suite cannot notice a function that does not return, which is why this asserts
 * the request COUNT and not just the outcome.
 */
describe('a page that cannot advance the cursor (SPEC §2.6.1)', () => {
  /** A record from a collection whose `updated` autodate is missing (§2.6.1). */
  const noUpdated = (id: string): FakeRecord =>
    ({ id, created: '2026-01-01 00:00:00.000Z', title: id }) as unknown as FakeRecord;

  it('stops after ONE request instead of refetching page 1 for ever', async () => {
    const h = await open(2);
    // Twenty identical FULL pages: the shape a real server presents when the
    // cursor never moves. Pre-fix this made twenty requests and then kept going.
    pb.setPages(
      'projects',
      Array.from({ length: 20 }, () => [rec('a1', '2026-01-01 00:00:01.000Z'), noUpdated('a2')]),
    );

    const stats = await pullCollection(h.pull(), 'projects');

    expect(stats.resync).toBe(true);
    expect(stats.pages).toBe(0);
    expect(pb.requests.filter((r) => r.method === 'getList' && r.collection === 'projects')).toHaveLength(1);
  });

  it('writes no cursor row at all rather than a null one, and says pull/malformed', async () => {
    const h = await open(2);
    pb.setPages('projects', [[rec('a1', '2026-01-01 00:00:01.000Z'), noUpdated('a2')]]);

    await pullCollection(h.pull(), 'projects');

    // A `{updated: null}` row is indistinguishable from "never pulled".
    expect(await readCursorRow(t.handle.db, 'projects')).toBeUndefined();
    // Nothing from the malformed page was applied either.
    expect(await t.handle.db.table('projects').count()).toBe(0);
    expect(h.of('error')).toEqual([
      expect.objectContaining({ error: expect.objectContaining({ code: 'pull/malformed', retryable: true }) }),
    ]);
  });

  it('a partial LAST page is fine: only the cursor-bearing row has to be well formed', async () => {
    const h = await open(3);
    // A short page still terminates the loop, so a bad row in it cannot loop —
    // but the cursor must not advance to it either.
    pb.setPages('projects', [[rec('a1', '2026-01-01 00:00:01.000Z')]]);
    const stats = await pullCollection(h.pull(), 'projects');
    expect(stats).toMatchObject({ pages: 1, applied: 1 });
    expect(await readCursorRow(t.handle.db, 'projects')).toMatchObject({ id: 'a1' });
  });
});

/**
 * REGRESSION (review finding 2) — SPEC §5.10 on the pull side.
 *
 * A 401 is an expired access token, not the configuration fault §6.5 halts for.
 * `halted` was consulted before every pass and cleared in exactly one place —
 * `runner.start()` — so a single token expiry (which 401s EVERY collection in the
 * same pass) wedged the whole engine for the life of the page: pulls stopped, no
 * further HTTP request was made, and `computeStatus` returned `error` for ever.
 */
describe('401 is a session condition, not a §6.5 halt', () => {
  it('does not halt, and the NEXT pull actually reaches the server again', async () => {
    const h = await open(10);
    let authLost = 0;
    const deps = h.pull({ isAuthValid: () => false, onAuthLost: () => (authLost += 1) });

    pb.failNext('projects', 'getList', sdkError(401));
    const first = await pullCollection(deps, 'projects');
    expect(first.linkDown).toBe(true);
    expect(first.halted).toBeUndefined();
    expect(h.halted.has('projects')).toBe(false);
    expect(authLost).toBe(1);

    // The token refreshed (same user, so no owner change and no restart).
    pb.setPages('projects', [[rec('a', '2026-01-01 00:00:01.000Z')]]);
    const second = await pullCollection(h.pull(), 'projects');
    expect(second.applied).toBe(1);
    expect(pb.requests.filter((r) => r.method === 'getList' && r.collection === 'projects')).toHaveLength(2);
  });

  it('a 403 with a DEAD auth store is the same condition wearing a different status', async () => {
    const h = await open(10);
    pb.failNext('projects', 'getList', sdkError(403));
    const stats = await pullCollection(h.pull({ isAuthValid: () => false }), 'projects');
    expect(stats.linkDown).toBe(true);
    expect(h.halted.has('projects')).toBe(false);
  });

  it('a 403 with a LIVE token is still a configuration fault and still halts', async () => {
    const h = await open(10);
    pb.failNext('projects', 'getList', sdkError(403));
    const stats = await pullCollection(h.pull({ isAuthValid: () => true }), 'projects');
    expect(stats.halted).toBe(true);
    expect(h.halted.get('projects')?.code).toBe('pull/denied');
  });

  it('the tombstone stream follows the same rule', async () => {
    const h = await open(10);
    const { pullTombstones } = await import('../../src/sync/tombstones.js');
    pb.failNext('deletions', 'getList', sdkError(401));
    const stats = await pullTombstones(h.pull({ isAuthValid: () => false }));
    expect(stats.linkDown).toBe(true);
    expect(h.halted.has('deletions')).toBe(false);
  });
});

describe('the pass (SPEC §3.1 step 2)', () => {
  it('pulls tombstones LAST, and only when the whole set was asked for', async () => {
    const h = await open(10);
    await pull(h.pull());
    const order = pb.requests.filter((r) => r.method === 'getList').map((r) => r.collection);
    expect(order).toEqual(['projects', 'tasks', 'deletions']);

    pb.requests.length = 0;
    await pull(h.pull(), ['projects']);
    expect(pb.requests.map((r) => r.collection)).toEqual(['projects']);
  });

  it('stops the whole pass when the link drops mid-collection', async () => {
    const h = await open(10);
    pb.failNext('projects', 'getList', networkError());
    const result = await pull(h.pull());
    expect(Object.keys(result.byCollection)).toEqual(['projects']);
    expect(pb.requests.some((r) => r.collection === 'tasks')).toBe(false);
  });

  it('emits pull with the collection, the applied count and the cursor', async () => {
    const h = await open(10);
    pb.setPages('projects', [[rec('a', '2026-01-01 00:00:01.000Z')]]);
    await pullCollection(h.pull(), 'projects');
    expect(h.of('pull')).toEqual([
      {
        collection: 'projects',
        applied: 1,
        deleted: 0,
        cursor: { updated: '2026-01-01 00:00:01.000Z', id: 'a' },
      },
    ]);
  });
});
