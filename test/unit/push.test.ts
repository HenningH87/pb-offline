/** SPEC §5 — draining `_pending`. One test per numbered invariant. */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { drain } from '../../src/sync/push.js';
import { create as crudCreate, remove as crudRemove, update as crudUpdate } from '../../src/collections/crud.js';
import { readMeta } from '../../src/db/meta.js';
import { createBackoff } from '../../src/net/backoff.js';
import { openTestDb, queue, syncHarness, testConfig, tick, type TestDb } from './_helpers.js';
import { createFakePB, idTakenError, networkError, sdkError, type FakePB } from '../fakes/pb.js';
import type { PBRecord } from '../../src/types.js';

let t: TestDb;
let pb: FakePB;

afterEach(async () => {
  await t?.destroy();
  vi.restoreAllMocks();
});

async function open(overrides?: Parameters<typeof testConfig>[0]): Promise<ReturnType<typeof syncHarness>> {
  t = await openTestDb(testConfig({ writeCoalesceMs: 0, ...overrides }));
  pb = createFakePB({ ownerId: 'user1' });
  return syncHarness(t, pb);
}

describe('ordering and re-entry (invariants 1, 2)', () => {
  it('processes entries in strictly ascending seq, one request at a time', async () => {
    const h = await open();
    const a = await crudCreate(t.deps('projects'), { title: 'A' });
    const b = await crudCreate(t.deps('projects'), { title: 'B' });
    await crudUpdate(t.deps('projects'), a.id, { title: 'A2' });
    const c = await crudCreate(t.deps('tasks'), { title: 'C' });

    const result = await drain(h.push());
    expect(result).toMatchObject({ pushed: 4, failed: 0, deadLettered: 0, remaining: 0, stoppedBy: 'empty' });
    expect(pb.maxInFlight).toBe(1);
    expect(pb.recordRequests().map((r) => `${r.method}:${r.collection}`)).toEqual([
      'create:projects',
      'create:projects',
      'update:projects',
      'create:tasks',
    ]);
    expect((pb.recordRequests()[0]!.args[0] as { id: string }).id).toBe(a.id);
    expect((pb.recordRequests()[1]!.args[0] as { id: string }).id).toBe(b.id);
    expect(pb.get('tasks', c.id)).toBeDefined();
  });

  it('re-reads the entry immediately before the request (another tab coalesced)', async () => {
    const h = await open();
    const a = await crudCreate(t.deps('projects'), { title: 'A' });
    await crudCreate(t.deps('projects'), { title: 'B' });
    const entries = await queue(t.handle.db);
    const second = entries[1]!;

    // While request #1 is on the wire, "another tab" merges into entry #2.
    pb.onRequest = async (req) => {
      if (req.method !== 'create') return;
      if ((req.args[0] as { title?: string }).title !== 'A') return;
      const row = await t.handle.db.pbo.pending.get(second.seq);
      await t.handle.db.pbo.pending.put({ ...row!, payload: { ...row!.payload, title: 'B-coalesced' } });
    };

    await drain(h.push());
    const sent = pb.recordRequests().filter((r) => r.method === 'create');
    expect((sent[1]!.args[0] as { title: string }).title).toBe('B-coalesced');
    expect(a.id).toBeTruthy();
  });

  it('marks the entry inflight while it is on the wire and clears it on every exit', async () => {
    const h = await open();
    await crudCreate(t.deps('projects'), { title: 'A' });
    const states: string[] = [];
    pb.onRequest = async () => {
      const row = (await queue(t.handle.db))[0];
      if (row) states.push(row.state);
    };
    pb.failNext('projects', 'create', sdkError(500));
    await drain(h.push());
    expect(states).toEqual(['inflight']);
    expect((await queue(t.handle.db))[0]!.state).toBe('queued');
  });

  it('never leaves an entry inflight when the request throws something unexpected', async () => {
    const h = await open();
    await crudCreate(t.deps('projects'), { title: 'A' });
    pb.onRequest = () => {
      throw new Error('not an SDK error at all');
    };
    // A thrown non-SDK fault is classified like any other request failure; the
    // entry must never be left inflight.
    await drain(h.push());
    expect((await queue(t.handle.db))[0]!.state).toBe('queued');
  });
});

describe('success (invariant 5)', () => {
  it('removes the entry, mirrors the response and records BOTH updated fields', async () => {
    const h = await open();
    const rec = await crudCreate(t.deps('projects'), { title: 'A' });

    const result = await drain(h.push());
    expect(result.pushed).toBe(1);
    expect(await queue(t.handle.db)).toEqual([]);

    const server = pb.get('projects', rec.id)!;
    const local = await t.handle.db.table('projects').get(rec.id);
    expect(local).toMatchObject({ id: rec.id, title: 'A', updated: server.updated });

    const meta = await readMeta(t.handle.db, 'projects', rec.id);
    expect(meta?.serverUpdated).toBe(server.updated);
    expect(meta?.lastPushedUpdated).toBe(server.updated);
    expect(meta?.state).toBe('synced');

    expect(h.of('record-synced')).toEqual([{ collection: 'projects', id: rec.id, updated: server.updated }]);
    expect(h.of('push')).toEqual([{ collection: 'projects', op: 'create', id: rec.id, ok: true, attempt: 1 }]);
  });

  it('merges server-authored fields but keeps fields a LATER queue entry holds', async () => {
    const h = await open();
    const rec = await crudCreate(t.deps('projects'), { title: 'A', note: 'first' });
    // The user moved on before the create was answered.
    await crudUpdate(t.deps('projects'), rec.id, { title: 'A-newer' });

    pb.onRequest = async (req) => {
      void req;
    };
    const result = await drain(h.push({ isOnline: () => pb.recordRequests().length < 1 }));
    // Only the create went out (the second pass stops at the entry boundary).
    expect(result.pushed).toBe(1);

    const local = (await t.handle.db.table('projects').get(rec.id)) as PBRecord;
    expect(local.title).toBe('A-newer'); // the queued local field survives
    expect(local.note).toBe('first');
    expect(typeof local.updated).toBe('string'); // the server-authored field merged
  });

  it('a delete success drops the mirror row and the meta row', async () => {
    const h = await open();
    const rec = await crudCreate(t.deps('projects'), { title: 'A' });
    await drain(h.push());
    await crudRemove(t.deps('projects'), rec.id);

    await drain(h.push());
    expect(await t.handle.db.table('projects').get(rec.id)).toBeUndefined();
    expect(await readMeta(t.handle.db, 'projects', rec.id)).toBeUndefined();
    expect(pb.get('projects', rec.id)).toBeUndefined();
    // No record-synced for a delete: there is no `updated` to report.
    expect(h.of('record-synced')).toHaveLength(1);
  });

  /**
   * REGRESSION (review finding 6) — §5.5's "only if the record is not dirty from
   * a LATER queue entry", which the delete branch never honoured.
   *
   * Id reuse after a delete is explicitly legal: §8.3 requires a queued create
   * for a tombstoned id to be PRESERVED, for exactly this reason. The delete
   * branch nevertheless removed the mirror row and the meta row unconditionally,
   * so a record recreated while its delete was on the wire vanished from every
   * `live()` query — and stayed vanished if the link dropped before the create
   * drained, with the only copy of its data sitting in `_pending`. Online and
   * healthy it self-heals on the next iteration, which is why it hid: the bug
   * needs the interruption that is the normal offline case.
   */
  it('a delete success does NOT wipe a row a LATER queued create re-made (SPEC §5.5, §8.3)', async () => {
    const h = await open();
    const rec = await crudCreate(t.deps('projects'), { title: 'first' });
    await drain(h.push());
    await crudRemove(t.deps('projects'), rec.id);

    // Mid-request the user recreates the id — this tab or another — and the link
    // then drops before the create can be drained.
    pb.onRequest = async (req) => {
      if (req.method !== 'delete') return;
      pb.onRequest = null;
      await t.handle.db.table('projects').put({ id: rec.id, title: 'recreated' });
      await h.enqueue({
        collection: 'projects',
        recordId: rec.id,
        op: 'create',
        payload: { id: rec.id, title: 'recreated' },
      });
      h.online = false;
    };

    const result = await drain(h.push());
    expect(result.stoppedBy).toBe('link');
    expect(pb.get('projects', rec.id)).toBeUndefined(); // the delete DID succeed
    // …but the user's record is still on screen, and its create is still queued.
    expect(await t.handle.db.table('projects').get(rec.id)).toMatchObject({ title: 'recreated' });
    expect((await queue(t.handle.db)).map((e) => e.op)).toEqual(['create']);
    // The server incarnation is gone, so no stale echo guard survives it.
    expect(await readMeta(t.handle.db, 'projects', rec.id)).toMatchObject({
      serverUpdated: null,
      lastPushedUpdated: null,
      state: 'dirty',
    });
  });
});

describe('4xx (invariants 6, 7, 8, 10)', () => {
  it('dead-letters and CONTINUES with the next entry', async () => {
    const h = await open();
    const bad = await crudCreate(t.deps('projects'), { title: 'rejected' });
    const good = await crudCreate(t.deps('projects'), { title: 'fine' });
    pb.failNext('projects', 'create', sdkError(400, { response: { message: 'rules said no' } }));

    const result = await drain(h.push());
    expect(result).toMatchObject({ pushed: 1, failed: 1, deadLettered: 1, stoppedBy: 'empty' });
    expect(pb.get('projects', good.id)).toBeDefined();

    const dead = await t.handle.db.pbo.deadletter.toArray();
    expect(dead).toHaveLength(1);
    expect(dead[0]).toMatchObject({ recordId: bad.id, op: 'create' });
    expect(dead[0]!.error.code).toBe('push/rejected');
    expect(h.of('dead-letter')).toHaveLength(1);
    // 6 · the local mirror row is left exactly as the user left it.
    expect(await t.handle.db.table('projects').get(bad.id)).toMatchObject({ title: 'rejected' });
    expect((await readMeta(t.handle.db, 'projects', bad.id))?.state).toBe('dead');
  });

  it('converts a taken id to an update and retries ONCE (§5.7)', async () => {
    const h = await open();
    const rec = await crudCreate(t.deps('projects'), { title: 'A' });
    // The server already has it: the response to the first create was lost.
    pb.seed('projects', [{ id: rec.id, title: 'A', owner: 'user1' }]);

    const result = await drain(h.push());
    expect(result).toMatchObject({ pushed: 1, deadLettered: 0 });
    const methods = pb.recordRequests().map((r) => r.method);
    expect(methods).toEqual(['create', 'update']);
    // The id moved to the URL and left the body.
    expect(pb.recordRequests()[1]!.args[0]).toBe(rec.id);
    expect(pb.recordRequests()[1]!.args[1]).not.toHaveProperty('id');
    expect(await queue(t.handle.db)).toEqual([]);
    expect(h.of('push').at(-1)).toMatchObject({ op: 'update', ok: true });
  });

  it('an ambiguous create that already landed is sent as an update, never re-created (§5.12)', async () => {
    const h = await open();
    const rec = await crudCreate(t.deps('projects'), { title: 'A' });
    // The tab that sent it navigated away mid-request: the server made the row,
    // the entry was left inflight, and a create rule that counts rows of this
    // shape would now refuse a re-sent create with a 403, not §5.7's 400.
    pb.seed('projects', [{ id: rec.id, title: 'A', owner: 'user1' }]);
    const [left] = await queue(t.handle.db);
    await t.handle.db.pbo.pending.put({ ...left!, state: 'inflight' });

    const result = await drain(h.push({ isAuthValid: () => true }));
    expect(result).toMatchObject({ pushed: 1, deadLettered: 0, remaining: 0 });
    expect(pb.recordRequests().map((r) => r.method)).toEqual(['getList', 'update']);
    expect(pb.recordRequests()[1]!.args[0]).toBe(rec.id);
    expect(pb.recordRequests()[1]!.args[1]).not.toHaveProperty('id');

    // One that never landed is created as it is, after the same check.
    pb.failNext('projects', 'create', sdkError(500));
    const other = await crudCreate(t.deps('projects'), { title: 'B' });
    await drain(h.push());
    await drain(h.push({ now: () => Date.now() + 3_600_000 }));
    expect(pb.get('projects', other.id)).toMatchObject({ title: 'B' });
    expect(pb.recordRequests().slice(2).map((r) => r.method)).toEqual(['create', 'getList', 'create']);
  });

  it('dead-letters with push/conflict when the converted update also fails', async () => {
    const h = await open();
    const rec = await crudCreate(t.deps('projects'), { title: 'A' });
    pb.failNext('projects', 'create', idTakenError());
    pb.failNext('projects', 'update', sdkError(400));

    const result = await drain(h.push());
    expect(result).toMatchObject({ pushed: 0, deadLettered: 1 });
    const dead = await t.handle.db.pbo.deadletter.toArray();
    expect(dead[0]!.error.code).toBe('push/conflict');
    expect(dead[0]!.recordId).toBe(rec.id);
  });

  it('404 on delete is success; 404 on update is push/vanished', async () => {
    const h = await open();
    await h.enqueue({ collection: 'projects', recordId: 'aaaaaaaaaaaaaaa', op: 'delete' });
    await h.enqueue({ collection: 'projects', recordId: 'bbbbbbbbbbbbbbb', op: 'update', payload: { title: 'x' } });

    const result = await drain(h.push());
    expect(result).toMatchObject({ pushed: 1, deadLettered: 1, remaining: 0 });
    const dead = await t.handle.db.pbo.deadletter.toArray();
    expect(dead).toHaveLength(1);
    expect(dead[0]).toMatchObject({ recordId: 'bbbbbbbbbbbbbbb' });
    expect(dead[0]!.error.code).toBe('push/vanished');
  });

  it('401 with a dead session is a LINK condition, not a dead letter (§5.10)', async () => {
    const h = await open();
    await crudCreate(t.deps('projects'), { title: 'A' });
    pb.failNext('projects', 'create', sdkError(401));
    let authLost = 0;

    const result = await drain(h.push({ isAuthValid: () => false, onAuthLost: () => (authLost += 1) }));
    expect(result.stoppedBy).toBe('auth');
    expect(authLost).toBe(1);
    expect(await t.handle.db.pbo.deadletter.count()).toBe(0);
    expect((await queue(t.handle.db))[0]).toMatchObject({ state: 'queued', attempts: 1 });
  });

  it('403 with a live session is a rules rejection ⇒ dead letter', async () => {
    const h = await open();
    await crudCreate(t.deps('projects'), { title: 'A' });
    pb.failNext('projects', 'create', sdkError(403));

    const result = await drain(h.push({ isAuthValid: () => true }));
    expect(result).toMatchObject({ deadLettered: 1, stoppedBy: 'empty' });
    expect((await t.handle.db.pbo.deadletter.toArray())[0]!.error.code).toBe('push/rejected');
  });
});

describe('transient failure (invariant 9)', () => {
  it('a 5xx stops the drain and leaves every later entry untouched', async () => {
    const h = await open();
    await crudCreate(t.deps('projects'), { title: 'A' });
    await crudCreate(t.deps('projects'), { title: 'B' });
    await crudCreate(t.deps('projects'), { title: 'C' });
    pb.failNext('projects', 'create', sdkError(503));

    const result = await drain(h.push());
    expect(result).toMatchObject({ pushed: 0, failed: 1, deadLettered: 0, remaining: 3, stoppedBy: 'link' });
    expect(pb.recordRequests()).toHaveLength(1);

    const rows = await queue(t.handle.db);
    expect(rows[0]).toMatchObject({ state: 'queued', attempts: 1 });
    expect(rows[0]!.nextAttemptAt).toBeGreaterThan(Date.now());
    expect(rows[1]).toMatchObject({ state: 'queued', attempts: 0, nextAttemptAt: 0 });
    expect(rows[2]).toMatchObject({ state: 'queued', attempts: 0, nextAttemptAt: 0 });
    expect(await t.handle.db.pbo.deadletter.count()).toBe(0);
  });

  it('a network failure stops the drain and reports it to the link watcher', async () => {
    const h = await open();
    await crudCreate(t.deps('projects'), { title: 'A' });
    pb.failNext('projects', 'create', networkError());
    const observed: Array<[boolean, boolean]> = [];

    const result = await drain(h.push({ observeRequest: (ok, net) => observed.push([ok, net]) }));
    expect(result.stoppedBy).toBe('link');
    expect(observed).toEqual([[false, true]]);
  });

  /**
   * DEVIATION from SPEC §5.9, asserted rather than hidden (review finding 5).
   *
   * `Retry-After` is a HEADER, and `ClientResponseError` carries `{url, status,
   * response, isAbort, originalError}` where `response` is the parsed JSON body —
   * the SDK never exposes a header, so the clause cannot be honoured. A 429 is
   * therefore a plain stop-the-drain condition on the backoff ladder, exactly
   * like a 5xx. The old test passed only because the FAKE invented the field.
   */
  it('429 stops the drain and waits out the backoff ladder (no Retry-After exists)', async () => {
    const h = await open();
    await crudCreate(t.deps('projects'), { title: 'A' });
    const err = sdkError(429, { response: { code: 429, message: 'Too Many Requests.' } });
    // The failure the engine is handed carries no header and no retry hint.
    expect((err.response as Record<string, unknown>)['retryAfter']).toBeUndefined();
    pb.failNext('projects', 'create', err);
    const before = Date.now();

    const result = await drain(h.push({ backoff: createBackoff({ min: 4_000, max: 4_000 }) }));
    expect(result.stoppedBy).toBe('link');
    const row = (await queue(t.handle.db))[0]!;
    expect(row).toMatchObject({ state: 'queued', attempts: 1 });
    expect(row.nextAttemptAt).toBeGreaterThanOrEqual(before + 4_000);
  });

  it('a head entry still waiting out its backoff stops the drain (later entries may depend on it)', async () => {
    const h = await open();
    await h.enqueue({
      collection: 'projects',
      recordId: 'aaaaaaaaaaaaaaa',
      op: 'update',
      payload: { title: 'x' },
      nextAttemptAt: Date.now() + 60_000,
    });
    await h.enqueue({ collection: 'projects', recordId: 'bbbbbbbbbbbbbbb', op: 'update', payload: { title: 'y' } });

    const result = await drain(h.push());
    expect(result).toMatchObject({ stoppedBy: 'link', pushed: 0, remaining: 2 });
    expect(pb.recordRequests()).toEqual([]);
  });

  it('an offline link stops at the entry boundary without a request (SPEC §3.4)', async () => {
    const h = await open();
    await crudCreate(t.deps('projects'), { title: 'A' });
    const result = await drain(h.push({ isOnline: () => false }));
    expect(result).toMatchObject({ stoppedBy: 'link', pushed: 0, remaining: 1 });
    expect(pb.requests).toEqual([]);
  });

  it('an aborted drain stops as "stopped", not as a failure', async () => {
    const h = await open();
    await crudCreate(t.deps('projects'), { title: 'A' });
    h.controller.abort();
    const result = await drain(h.push());
    expect(result).toMatchObject({ stoppedBy: 'stopped', pushed: 0 });
    expect(pb.requests).toEqual([]);
  });
});

describe('a long queue', () => {
  it('drains 200 mixed mutations in strict order with no dead letters', async () => {
    const h = await open();
    const ids: string[] = [];
    for (let i = 0; i < 100; i++) {
      const rec = await crudCreate(t.deps('projects'), { title: `p${i}` });
      ids.push(rec.id);
    }
    for (const id of ids) await crudUpdate(t.deps('projects'), id, { title: 'done' });

    const result = await drain(h.push());
    expect(result).toMatchObject({ pushed: 200, deadLettered: 0, remaining: 0, stoppedBy: 'empty' });
    expect(pb.maxInFlight).toBe(1);
    const methods = pb.recordRequests().map((r) => r.method);
    expect(methods.slice(0, 100).every((m) => m === 'create')).toBe(true);
    expect(methods.slice(100).every((m) => m === 'update')).toBe(true);
    expect(pb.records.get('projects')!.size).toBe(100);
    await tick(0);
  }, 30_000);
});
