/** SPEC §4.3, §4.4 — local-first writes, coalescing, transient, batch. TESTPLAN §1.3. */
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { batch, canCoalesce, create, remove, update } from '../../src/collections/crud.js';
import { defineCollection } from '../../src/collections/define.js';
import { readMeta } from '../../src/db/meta.js';
import { PBOError } from '../../src/errors.js';
import { createFakePB } from '../fakes/pb.js';
import { openTestDb, testConfig, type TestDb } from './_helpers.js';
import type { PBRecord, PendingEntry } from '../../src/types.js';
import type { CollectionDeps } from '../../src/collections/define.js';

interface Project extends PBRecord {
  title?: string;
  colour?: string;
  count?: number;
}

let t: TestDb;
let deps: CollectionDeps;

beforeEach(async () => {
  t = await openTestDb();
  deps = t.deps('projects');
});
afterEach(async () => {
  await t.destroy();
});

const queue = (): Promise<PendingEntry[]> => t.handle.db.pbo.pending.orderBy('seq').toArray();
const rows = (): Promise<Project[]> => t.handle.db.table('projects').toArray() as Promise<Project[]>;

describe('create (SPEC §4.3, §4.4)', () => {
  it('writes the mirror, the meta and one queue entry, with no network call', async () => {
    const pb = createFakePB({ ownerId: 'user1' });
    const rec = await create<Project>(deps, { title: 'One' });

    expect(rec.id).toMatch(/^[a-z0-9]{15}$/);
    expect(await rows()).toEqual([{ id: rec.id, title: 'One', owner: 'user1' }]);
    const q = await queue();
    expect(q).toHaveLength(1);
    expect(q[0]).toMatchObject({ collection: 'projects', recordId: rec.id, op: 'create', state: 'queued', attempts: 0 });
    expect(q[0]!.payload).toMatchObject({ id: rec.id, title: 'One' });
    expect(await readMeta(t.handle.db, 'projects', rec.id)).toMatchObject({ state: 'dirty', serverUpdated: null, lastPushedUpdated: null });
    expect(pb.requests).toEqual([]);
    expect(t.nudges).toBe(1);
  });

  it('never writes the server-owned clock locally', async () => {
    const rec = await create<Project>(deps, { title: 'One', created: 'X', updated: 'Y' } as never);
    expect(rec.created).toBeUndefined();
    expect(rec.updated).toBeUndefined();
    expect((await queue())[0]!.payload).not.toHaveProperty('updated');
  });

  it('accepts a caller-supplied id and stamps the owner field', async () => {
    const id = 'abcdefghij12345';
    const rec = await create<Project>(deps, { id, title: 'One' });
    expect(rec.id).toBe(id);
    expect(rec['owner']).toBe('user1');
  });

  it('rejects a bad caller-supplied id with config/bad-id BEFORE any Dexie write', async () => {
    await expect(create<Project>(deps, { id: 'TOO-SHORT', title: 'x' })).rejects.toMatchObject({ code: 'config/bad-id' });
    expect(await rows()).toEqual([]);
    expect(await queue()).toEqual([]);
  });

  it('honours a per-collection idFormat', async () => {
    const cfg = testConfig({ collections: [{ name: 'projects', idFormat: { length: 4, alphabet: 'abcd' } }] });
    const db2 = await openTestDb(cfg);
    try {
      const rec = await create<Project>(db2.deps('projects'), { title: 'x' });
      expect(rec.id).toMatch(/^[abcd]{4}$/);
      await expect(create<Project>(db2.deps('projects'), { id: 'zzzz' })).rejects.toMatchObject({ code: 'config/bad-id' });
    } finally {
      await db2.destroy();
    }
  });

  it('rolls the whole write back when any part of the transaction fails', async () => {
    const db = t.handle.db;
    const original = db.pbo.meta.put.bind(db.pbo.meta);
    db.pbo.meta.put = (() => Promise.reject(new Error('meta write failed'))) as unknown as typeof db.pbo.meta.put;
    try {
      await expect(create<Project>(deps, { id: 'aaaaaaaaaaaaaaa', title: 'x' })).rejects.toThrow();
    } finally {
      db.pbo.meta.put = original;
    }
    expect(await rows()).toEqual([]);
    expect(await queue()).toEqual([]);
  });
});

describe('update + coalescing (SPEC §4.3)', () => {
  it('merges two updates inside the window into ONE entry, keeping the original seq', async () => {
    const rec = await create<Project>(deps, { title: 'One' });
    await update<Project>(deps, rec.id, { title: 'Two' });
    const first = await queue();
    const seq = first.at(-1)!.seq;

    await update<Project>(deps, rec.id, { colour: 'red' });
    const q = await queue();
    expect(q).toHaveLength(1); // still just the create, merged into
    expect(q[0]!.seq).toBe(seq);
    expect(q[0]!.op).toBe('create');
    expect(q[0]!.payload).toMatchObject({ title: 'Two', colour: 'red' });
    expect((await rows())[0]).toMatchObject({ title: 'Two', colour: 'red' });
  });

  /**
   * REGRESSION (review finding 12) — API drift.
   *
   * `push/vanished` is defined by §12 as a PUSH outcome — a 404 on an update,
   * which the engine dead-letters — so raising it for a synchronous local
   * rejection left an application unable to tell "you gave me an id I do not
   * have" from "the server lost your record", with a `retryable` flag that means
   * nothing locally.
   */
  it('update() on an id absent from the mirror is a local misuse, not push/vanished', async () => {
    const err = await update<Project>(deps, 'nosuchrecord01', { title: 'x' }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(PBOError);
    expect((err as PBOError).code).not.toBe('push/vanished');
    expect((err as PBOError).code).toBe('internal/invariant');
    expect((err as PBOError).retryable).toBe(false);
    expect((err as PBOError).context).toMatchObject({ collection: 'projects', id: 'nosuchrecord01' });
    // And it wrote nothing on the way out.
    expect(await queue()).toEqual([]);
  });

  it('does not merge an update into an entry older than the window', async () => {
    const cfg = testConfig({ writeCoalesceMs: 5 });
    const d2 = await openTestDb(cfg);
    try {
      const dep = d2.deps('projects');
      const rec = await create<Project>(dep, { title: 'One' });
      await new Promise((r) => setTimeout(r, 20));
      await update<Project>(dep, rec.id, { title: 'Two' });
      const q = await d2.handle.db.pbo.pending.orderBy('seq').toArray();
      expect(q).toHaveLength(2);
      expect(q[1]).toMatchObject({ op: 'update', payload: { title: 'Two' } });
    } finally {
      await d2.destroy();
    }
  });

  it('writeCoalesceMs: 0 disables coalescing entirely', async () => {
    const cfg = testConfig({ writeCoalesceMs: 0 });
    const d2 = await openTestDb(cfg);
    try {
      const dep = d2.deps('projects');
      const rec = await create<Project>(dep, { title: 'One' });
      await update<Project>(dep, rec.id, { title: 'Two' });
      await update<Project>(dep, rec.id, { title: 'Three' });
      expect(await d2.handle.db.pbo.pending.count()).toBe(3);
    } finally {
      await d2.destroy();
    }
  });

  it('noCoalesce forces its own entry', async () => {
    const rec = await create<Project>(deps, { title: 'One' });
    await update<Project>(deps, rec.id, { title: 'Two' }, { noCoalesce: true });
    const q = await queue();
    expect(q).toHaveLength(2);
    expect(q[1]).toMatchObject({ op: 'update', payload: { title: 'Two' } });
  });

  it('an update-into-update merges payloads without losing an earlier field', async () => {
    await create<Project>(deps, { id: 'aaaaaaaaaaaaaaa', title: 'One' });
    await t.handle.db.pbo.pending.clear(); // simulate a pushed create
    await update<Project>(deps, 'aaaaaaaaaaaaaaa', { title: 'Two' });
    await update<Project>(deps, 'aaaaaaaaaaaaaaa', { colour: 'red' });
    const q = await queue();
    expect(q).toHaveLength(1);
    expect(q[0]).toMatchObject({ op: 'update', payload: { title: 'Two', colour: 'red' } });
  });

  it('never coalesces into an inflight entry — that payload is on the wire', async () => {
    const rec = await create<Project>(deps, { title: 'One' });
    await t.handle.db.pbo.pending.update((await queue())[0]!.seq, { state: 'inflight' });
    await update<Project>(deps, rec.id, { title: 'Two' });
    const q = await queue();
    expect(q).toHaveLength(2);
    expect(q[0]!.payload).toMatchObject({ title: 'One' });
  });

  it('throws when the record is not in the mirror, without enqueuing anything', async () => {
    await expect(update<Project>(deps, 'aaaaaaaaaaaaaaa', { title: 'x' })).rejects.toBeInstanceOf(PBOError);
    expect(await queue()).toEqual([]);
  });

  it('canCoalesce encodes the eligibility rules literally', () => {
    const now = 1_000;
    expect(canCoalesce({ state: 'queued', op: 'update', clientUpdatedAt: 900 }, 250, now)).toBe(true);
    expect(canCoalesce({ state: 'queued', op: 'create', clientUpdatedAt: 900 }, 250, now)).toBe(true);
    expect(canCoalesce({ state: 'queued', op: 'delete', clientUpdatedAt: 900 }, 250, now)).toBe(false);
    expect(canCoalesce({ state: 'inflight', op: 'update', clientUpdatedAt: 900 }, 250, now)).toBe(false);
    expect(canCoalesce({ state: 'queued', op: 'update', clientUpdatedAt: 700 }, 250, now)).toBe(false);
    expect(canCoalesce({ state: 'queued', op: 'update', clientUpdatedAt: 1_000 }, 0, now)).toBe(false);
  });
});

describe('transient writes (SPEC §4.3)', () => {
  it('update the mirror, enqueue nothing, and do not make the record dirty', async () => {
    await create<Project>(deps, { id: 'aaaaaaaaaaaaaaa', title: 'One' });
    await t.handle.db.pbo.pending.clear();
    await t.handle.db.pbo.meta.update(['projects', 'aaaaaaaaaaaaaaa'], { state: 'synced', serverUpdated: 'T1' });
    const before = t.nudges;

    await update<Project>(deps, 'aaaaaaaaaaaaaaa', { count: 42 }, { transient: true });

    expect((await rows())[0]).toMatchObject({ count: 42 });
    expect(await queue()).toEqual([]);
    const meta = await readMeta(t.handle.db, 'projects', 'aaaaaaaaaaaaaaa');
    expect(meta!.state).toBe('synced');
    expect(meta!.serverUpdated).toBe('T1');
    expect(meta!.clientUpdatedAt).toBeGreaterThan(0);
    expect(t.nudges).toBe(before); // nothing to drain, so no nudge
  });
});

describe('delete (SPEC §4.3)', () => {
  it('an offline create-then-delete collapses to nothing — no request is ever made', async () => {
    const pb = createFakePB({ ownerId: 'user1' });
    const rec = await create<Project>(deps, { title: 'One' });
    await update<Project>(deps, rec.id, { title: 'Two' }, { noCoalesce: true });
    await remove(deps, rec.id);

    expect(await queue()).toEqual([]);
    expect(await rows()).toEqual([]);
    expect(await readMeta(t.handle.db, 'projects', rec.id)).toBeUndefined();
    expect(pb.requests).toEqual([]);
  });

  it('removes queued updates for a pushed record and appends its own entry', async () => {
    await create<Project>(deps, { id: 'aaaaaaaaaaaaaaa', title: 'One' });
    await t.handle.db.pbo.pending.clear(); // the create has been pushed
    await update<Project>(deps, 'aaaaaaaaaaaaaaa', { title: 'Two' }, { noCoalesce: true });
    await update<Project>(deps, 'aaaaaaaaaaaaaaa', { colour: 'red' }, { noCoalesce: true });
    expect(await t.handle.db.pbo.pending.count()).toBe(2);

    await remove(deps, 'aaaaaaaaaaaaaaa');
    const q = await queue();
    expect(q).toHaveLength(1);
    expect(q[0]).toMatchObject({ op: 'delete', payload: null, recordId: 'aaaaaaaaaaaaaaa' });
    expect(await rows()).toEqual([]);
  });

  it('leaves an INFLIGHT create alone and still enqueues the delete', async () => {
    const rec = await create<Project>(deps, { title: 'One' });
    await t.handle.db.pbo.pending.update((await queue())[0]!.seq, { state: 'inflight' });
    await remove(deps, rec.id);
    const q = await queue();
    expect(q.map((e) => e.op)).toEqual(['create', 'delete']);
  });

  it('does not touch another record’s queue entries', async () => {
    const a = await create<Project>(deps, { title: 'A' });
    const b = await create<Project>(deps, { title: 'B' });
    await remove(deps, a.id);
    const q = await queue();
    expect(q).toHaveLength(1);
    expect(q[0]!.recordId).toBe(b.id);
  });

  it('is idempotent for a record that is not there', async () => {
    await expect(remove(deps, 'aaaaaaaaaaaaaaa')).resolves.toBeUndefined();
    expect(await queue()).toEqual([]);
  });
});

describe('batch (SPEC §4.3)', () => {
  it('writes one transaction with contiguous seq values', async () => {
    await create<Project>(t.deps('tasks'), { title: 'other collection' });
    await batch<Project>(deps, [
      { op: 'create', data: { id: 'aaaaaaaaaaaaaaa', title: 'A' } },
      { op: 'create', data: { id: 'bbbbbbbbbbbbbbb', title: 'B' } },
      { op: 'update', id: 'aaaaaaaaaaaaaaa', patch: { colour: 'red' } },
      { op: 'create', data: { id: 'ccccccccccccccc', title: 'C' } },
    ]);

    const q = await queue();
    const batchEntries = q.filter((e) => e.collection === 'projects');
    const seqs = batchEntries.map((e) => e.seq);
    for (let i = 1; i < seqs.length; i++) expect(seqs[i]).toBe(seqs[i - 1]! + 1);
    expect(batchEntries).toHaveLength(3); // the update coalesced into A's create
    expect(batchEntries[0]!.payload).toMatchObject({ title: 'A', colour: 'red' });
    expect((await rows()).map((r) => r.id).sort()).toEqual(['aaaaaaaaaaaaaaa', 'bbbbbbbbbbbbbbb', 'ccccccccccccccc']);
  });

  it('rolls every op back when one of them fails', async () => {
    await expect(
      batch<Project>(deps, [
        { op: 'create', data: { id: 'aaaaaaaaaaaaaaa', title: 'A' } },
        { op: 'update', id: 'missingmissing1', patch: { title: 'nope' } },
      ]),
    ).rejects.toBeInstanceOf(PBOError);
    expect(await rows()).toEqual([]);
    expect(await queue()).toEqual([]);
  });

  it('is a no-op for an empty op list', async () => {
    await expect(batch<Project>(deps, [])).resolves.toBeUndefined();
    expect(await queue()).toEqual([]);
  });
});

/**
 * SPEC §4.3 — ownership is not a writable field.
 *
 * The audit's M3: `create()` used to DEFAULT the owner ("set it if the caller
 * left it blank"), which makes it caller-supplied, and the ordinary way a
 * "duplicate this record" feature gets written is to spread a record that
 * already carries one. What stood between that and the server was the
 * consuming app's `createRule` — a boundary this library prescribes and does
 * not install.
 */
describe('ownership is forced, never accepted (SPEC §4.3)', () => {
  it('forces the session owner over one the caller supplied for somebody else', async () => {
    await expect(create<Project>(deps, { owner: 'user2', title: 'planted' } as never)).rejects.toMatchObject({
      code: 'internal/invariant',
    });
    // Rejected BEFORE any Dexie write: no mirror row, no queue entry.
    expect(await rows()).toEqual([]);
    expect(await queue()).toEqual([]);
  });

  it('accepts a redundant owner that names the session owner itself', async () => {
    const rec = await create<Project>(deps, { owner: 'user1', title: 'mine' } as never);
    expect(rec['owner']).toBe('user1');
    expect((await queue())[0]!.payload).toMatchObject({ owner: 'user1' });
  });

  it('refuses an update that reassigns the owner — locally, not on the wire', async () => {
    const rec = await create<Project>(deps, { title: 'mine' });
    await expect(update<Project>(deps, rec.id, { owner: 'user2' } as never)).rejects.toMatchObject({
      code: 'internal/invariant',
    });
    // The mirror still says user1, and nothing was queued for the hand-off.
    expect((await rows())[0]).toMatchObject({ owner: 'user1' });
    expect(await queue()).toHaveLength(1);
  });

  it('refuses the hand-off inside a batch too, and the batch takes nothing with it', async () => {
    const rec = await create<Project>(deps, { title: 'mine' });
    await expect(
      batch<Project>(deps, [
        { op: 'update', id: rec.id, patch: { title: 'edited' } },
        { op: 'update', id: rec.id, patch: { owner: 'user2' } as never },
      ]),
    ).rejects.toMatchObject({ code: 'internal/invariant' });
    expect((await rows())[0]).toMatchObject({ owner: 'user1', title: 'mine' });
  });

  it('leaves the field alone with no session owner (anonymous, unowned data)', async () => {
    const dep = { ...t.deps('projects'), getOwnerId: () => null };
    const rec = await create<Project>(dep, { title: 'anon' });
    expect(rec['owner']).toBeUndefined();
  });
});

describe('anonymousMode: disabled (SPEC §14.3)', () => {
  it('writes throw auth/unauthenticated and reads return empty', async () => {
    const cfg = testConfig({ anonymousMode: 'disabled' });
    const d2 = await openTestDb(cfg, 'anon');
    try {
      const dep = { ...d2.deps('projects'), getOwnerId: () => null };
      await expect(create<Project>(dep, { title: 'x' })).rejects.toMatchObject({ code: 'auth/unauthenticated' });
      await expect(update<Project>(dep, 'aaaaaaaaaaaaaaa', {})).rejects.toMatchObject({ code: 'auth/unauthenticated' });
      await expect(remove(dep, 'aaaaaaaaaaaaaaa')).rejects.toMatchObject({ code: 'auth/unauthenticated' });

      const handle = defineCollection<Project>(dep);
      expect(await handle.getAll()).toEqual([]);
      expect(await handle.get('aaaaaaaaaaaaaaa')).toBeUndefined();
      expect(await handle.count()).toBe(0);
      expect(await handle.find()).toEqual([]);
    } finally {
      await d2.destroy();
    }
  });
});
