/** SPEC §10 — applying a remote record, table-driven over the five cases. */
import { afterEach, describe, expect, it } from 'vitest';
import { applyRemote, compareUpdated, resolverFor } from '../../src/sync/conflict.js';
import { create as crudCreate, update as crudUpdate } from '../../src/collections/crud.js';
import { readMeta, writeMeta } from '../../src/db/meta.js';
import { openTestDb, queue, syncHarness, testConfig, type TestDb } from './_helpers.js';
import { createFakePB, type FakePB } from '../fakes/pb.js';
import type { ConflictResolver, PBRecord, ResolvedConfig } from '../../src/types.js';

let t: TestDb;
let pb: FakePB;

afterEach(async () => {
  await t?.destroy();
});

const T0 = '2026-01-01 00:00:00.000Z';
const T1 = '2026-01-02 00:00:00.000Z';
const T2 = '2026-01-03 00:00:00.000Z';
const ID = 'aaaaaaaaaaaaaaa';

async function open(cfgOver?: Partial<Parameters<typeof testConfig>[0]>): Promise<ReturnType<typeof syncHarness>> {
  t = await openTestDb(testConfig({ writeCoalesceMs: 0, ...cfgOver }));
  pb = createFakePB({ ownerId: 'user1' });
  return syncHarness(t, pb);
}

const remote = (over?: Partial<PBRecord>): PBRecord => ({ id: ID, title: 'remote', updated: T1, ...over });
const row = async (): Promise<PBRecord | undefined> => t.handle.db.table('projects').get(ID) as Promise<PBRecord | undefined>;

describe('the five cases (SPEC §10.1–§10.4)', () => {
  it('absent ⇒ insert, serverUpdated recorded, record-synced emitted', async () => {
    const h = await open();
    expect(await applyRemote(t.handle.db, t.cfg, 'projects', remote(), { emit: h.emit })).toBe('inserted');
    expect(await row()).toMatchObject({ id: ID, title: 'remote', updated: T1 });
    expect((await readMeta(t.handle.db, 'projects', ID))?.serverUpdated).toBe(T1);
    expect(h.of('record-synced')).toEqual([{ collection: 'projects', id: ID, updated: T1 }]);
  });

  it('present + pending ⇒ keep local, conflict{local}, serverUpdated recorded', async () => {
    const h = await open();
    await crudCreate(t.deps('projects'), { id: ID, title: 'local' });

    expect(await applyRemote(t.handle.db, t.cfg, 'projects', remote(), { emit: h.emit })).toBe('kept-local');
    expect(await row()).toMatchObject({ title: 'local' });
    // The client now knows what it is about to overwrite.
    expect((await readMeta(t.handle.db, 'projects', ID))?.serverUpdated).toBe(T1);
    expect(h.of('conflict')).toEqual([
      { collection: 'projects', id: ID, local: expect.objectContaining({ title: 'local' }), remote: remote(), resolution: 'local' },
    ]);
    expect(h.of('record-synced')).toEqual([]);
    // And the queued mutation is untouched — it is what eventually wins.
    expect(await queue(t.handle.db)).toHaveLength(1);
  });

  it('present, clean, remote NEWER ⇒ apply wholesale', async () => {
    const h = await open();
    await t.handle.db.table('projects').put({ id: ID, title: 'stale', updated: T0 });
    await writeMeta(t.handle.db, 'projects', ID, { serverUpdated: T0 });

    expect(await applyRemote(t.handle.db, t.cfg, 'projects', remote({ updated: T1 }), { emit: h.emit })).toBe('applied');
    expect(await row()).toMatchObject({ title: 'remote', updated: T1 });
    expect(h.of('record-synced')).toHaveLength(1);
  });

  it('present, clean, remote EQUAL ⇒ no-op', async () => {
    const h = await open();
    await t.handle.db.table('projects').put({ id: ID, title: 'local', updated: T1 });
    await writeMeta(t.handle.db, 'projects', ID, { serverUpdated: T1 });

    expect(await applyRemote(t.handle.db, t.cfg, 'projects', remote({ updated: T1 }), { emit: h.emit })).toBe('no-op');
    expect(await row()).toMatchObject({ title: 'local' });
    expect(h.events).toEqual([]);
  });

  it('present, clean, remote OLDER ⇒ no-op (an SSE event racing a pull page)', async () => {
    const h = await open();
    await t.handle.db.table('projects').put({ id: ID, title: 'newer', updated: T2 });
    await writeMeta(t.handle.db, 'projects', ID, { serverUpdated: T2 });

    expect(await applyRemote(t.handle.db, t.cfg, 'projects', remote({ updated: T1 }), { emit: h.emit })).toBe('no-op');
    expect(await row()).toMatchObject({ title: 'newer' });
  });

  it('present, clean, serverUpdated null ⇒ apply (server data beats unconfirmed local)', async () => {
    const h = await open();
    // A transient write: the mirror holds it, nothing is queued, nothing confirmed.
    await crudCreate(t.deps('projects'), { id: ID, title: 'seed' });
    await t.handle.db.pbo.pending.clear();
    await crudUpdate(t.deps('projects'), ID, { title: 'ticking' }, { transient: true });
    await t.handle.db.pbo.pending.clear();
    expect((await readMeta(t.handle.db, 'projects', ID))?.serverUpdated).toBeNull();

    expect(await applyRemote(t.handle.db, t.cfg, 'projects', remote(), { emit: h.emit })).toBe('applied');
    expect(await row()).toMatchObject({ title: 'remote' });
  });

  it('compares as strings and nothing else', () => {
    expect(compareUpdated(T1, T0)).toBe(1);
    expect(compareUpdated(T0, T1)).toBe(-1);
    expect(compareUpdated(T1, T1)).toBe(0);
    expect(compareUpdated(null, T0)).toBe(-1);
  });
});

describe('the conflict hook (SPEC §10.5)', () => {
  const withHook = async (hook: ConflictResolver, perCollection = false): Promise<ReturnType<typeof syncHarness>> =>
    open(perCollection
      ? ({ collections: [{ name: 'projects', onConflict: hook }, { name: 'tasks' }] } as never)
      : ({ onConflict: hook } as never));

  it('runs ONLY in the pending case', async () => {
    const calls: string[] = [];
    const h = await withHook(() => {
      calls.push('called');
      return 'local';
    });

    // absent ⇒ insert, no hook
    await applyRemote(t.handle.db, t.cfg, 'projects', remote(), { emit: h.emit });
    expect(calls).toEqual([]);
    // clean + newer ⇒ apply, no hook
    await applyRemote(t.handle.db, t.cfg, 'projects', remote({ updated: T2 }), { emit: h.emit });
    expect(calls).toEqual([]);
    // pending ⇒ hook
    await crudCreate(t.deps('projects'), { id: 'bbbbbbbbbbbbbbb', title: 'local' });
    await applyRemote(t.handle.db, t.cfg, 'projects', remote({ id: 'bbbbbbbbbbbbbbb' }), { emit: h.emit });
    expect(calls).toEqual(['called']);
  });

  it("'remote' discards that record's queued mutations and applies the server row", async () => {
    const h = await withHook(() => 'remote');
    await crudCreate(t.deps('projects'), { id: ID, title: 'local' });
    await crudCreate(t.deps('tasks'), { title: 'unrelated' });

    expect(await applyRemote(t.handle.db, t.cfg, 'projects', remote(), { emit: h.emit })).toBe('applied');
    expect(await row()).toMatchObject({ title: 'remote' });
    const rest = await queue(t.handle.db);
    expect(rest).toHaveLength(1);
    expect(rest[0]!.collection).toBe('tasks'); // only THAT record's entries went
    expect(h.of('conflict')).toEqual([expect.objectContaining({ resolution: 'remote' })]);
    expect(h.of('record-synced')).toHaveLength(1);
  });

  it('a returned record field-merges into the queued payload and into the mirror', async () => {
    const h = await withHook(({ local, remote: r }) => ({ ...local, title: `${String(r['title'])}+${String(local['title'])}` }));
    await crudCreate(t.deps('projects'), { id: ID, title: 'local', note: 'kept' });

    expect(await applyRemote(t.handle.db, t.cfg, 'projects', remote(), { emit: h.emit })).toBe('merged');
    expect(await row()).toMatchObject({ title: 'remote+local', note: 'kept' });
    const entry = (await queue(t.handle.db))[0]!;
    expect(entry.payload).toMatchObject({ title: 'remote+local', note: 'kept' });
    // The client never authors the sync clock, not even through a hook.
    expect(entry.payload).not.toHaveProperty('updated');
    expect(h.of('conflict')).toEqual([expect.objectContaining({ resolution: 'merged' })]);
  });

  it("'local' is the default, and an absent hook behaves identically", async () => {
    const explicit = await withHook(() => 'local');
    await crudCreate(t.deps('projects'), { id: ID, title: 'local' });
    expect(await applyRemote(t.handle.db, t.cfg, 'projects', remote(), { emit: explicit.emit })).toBe('kept-local');
    await t.destroy();

    const plain = await open();
    await crudCreate(t.deps('projects'), { id: ID, title: 'local' });
    expect(await applyRemote(t.handle.db, t.cfg, 'projects', remote(), { emit: plain.emit })).toBe('kept-local');
  });

  it('a hook that throws routes to the error sink and falls back to local', async () => {
    const thrown: unknown[] = [];
    const h = await withHook(() => {
      throw new Error('hook exploded');
    });
    await crudCreate(t.deps('projects'), { id: ID, title: 'local' });

    const outcome = await applyRemote(t.handle.db, t.cfg, 'projects', remote(), {
      emit: h.emit,
      onError: (e) => thrown.push(e),
    });
    expect(outcome).toBe('kept-local');
    expect(thrown).toHaveLength(1);
    expect(await row()).toMatchObject({ title: 'local' });
  });

  it('an async hook is awaited (and the Dexie transaction survives it)', async () => {
    const h = await withHook(async ({ local }) => {
      await new Promise((r) => setTimeout(r, 5));
      return { ...local, title: 'async-merged' };
    });
    await crudCreate(t.deps('projects'), { id: ID, title: 'local' });
    expect(await applyRemote(t.handle.db, t.cfg, 'projects', remote(), { emit: h.emit })).toBe('merged');
    expect(await row()).toMatchObject({ title: 'async-merged' });
  });

  it('a per-collection hook overrides the global one', async () => {
    const global: ConflictResolver = () => 'local';
    const perCollection: ConflictResolver = () => 'remote';
    const cfg = testConfig({
      onConflict: global,
      collections: [{ name: 'projects', onConflict: perCollection }, { name: 'tasks' }],
    } as never) as ResolvedConfig;
    expect(resolverFor(cfg, 'projects')).toBe(perCollection);
    expect(resolverFor(cfg, 'tasks')).toBe(global);
  });
});

describe('property: the greatest server `updated` wins', () => {
  it('over 200 random interleavings of local writes and remote applies', async () => {
    const h = await open();
    let seed = 12345;
    const rnd = (): number => {
      seed = (seed * 1103515245 + 12345) & 0x7fffffff;
      return seed / 0x7fffffff;
    };

    for (let round = 0; round < 200; round++) {
      const id = `r${String(round).padStart(14, '0')}`;
      await t.handle.db.table('projects').delete(id);
      await t.handle.db.pbo.meta.delete(['projects', id]);

      // A clean local row confirmed at some server time, then a remote apply.
      const known = `2026-01-0${1 + Math.floor(rnd() * 8)} 00:00:00.000Z`;
      const incoming = `2026-01-0${1 + Math.floor(rnd() * 8)} 00:00:00.000Z`;
      await t.handle.db.table('projects').put({ id, title: 'known', updated: known });
      await writeMeta(t.handle.db, 'projects', id, { serverUpdated: known });

      await applyRemote(t.handle.db, t.cfg, 'projects', { id, title: 'incoming', updated: incoming }, { emit: h.emit });

      const final = (await t.handle.db.table('projects').get(id)) as PBRecord;
      const winner = incoming > known ? 'incoming' : 'known';
      expect(final.title).toBe(winner);
      expect(final.updated).toBe(incoming > known ? incoming : known);
    }
  }, 60_000);
});
