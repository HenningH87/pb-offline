/** SPEC §2.3, §2.5 — reactive reads over the mirror. TESTPLAN §1.4. */
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { defineCollection } from '../../src/collections/define.js';
import { runQuery } from '../../src/collections/query.js';
import { create, remove, update } from '../../src/collections/crud.js';
import { openTestDb, testConfig, tick, type TestDb } from './_helpers.js';
import { createRevision } from '../../src/store/readable.js';
import { openForOwner, type OpenHandle } from '../../src/db/open.js';
import type { CollectionDeps } from '../../src/collections/define.js';
import type { PBODexie } from '../../src/db/schema.js';
import type {
  Collection,
  PBOfflineConfig,
  PBRecord,
  PendingEntry,
  Readable,
  ResolvedConfig,
} from '../../src/types.js';
import Dexie, { type Table } from 'dexie';

interface Project extends PBRecord {
  title?: string;
  colour?: string;
  rank?: number;
}

let t: TestDb;
let projects: Collection<Project>;

beforeEach(async () => {
  t = await openTestDb();
  projects = defineCollection<Project>(t.deps('projects'));
});
afterEach(async () => {
  await t.destroy();
});

const ID = 'aaaaaaaaaaaaaaa';

/** Collect every frame a store emits, and stop cleanly. */
function collect<T>(store: { subscribe: (r: (v: T) => void) => () => void }): { frames: T[]; off: () => void } {
  const frames: T[] = [];
  const off = store.subscribe((v) => frames.push(v));
  return { frames, off };
}

describe('runQuery (SPEC §2.3)', () => {
  beforeEach(async () => {
    await t.handle.db.table('projects').bulkPut([
      { id: 'a', title: 'Alpha', colour: 'red', rank: 2 },
      { id: 'b', title: 'Beta', colour: 'blue', rank: 1 },
      { id: 'c', title: 'Gamma', colour: 'red', rank: 3 },
    ]);
  });

  it('filters by equality, sorts, offsets and limits', async () => {
    const table = t.handle.db.table('projects') as unknown as Table<Project, string>;
    expect((await runQuery<Project>(table, { where: { colour: 'red' } })).map((r) => r.id).sort()).toEqual(['a', 'c']);
    expect((await runQuery<Project>(table, { sort: { field: 'rank' } })).map((r) => r.id)).toEqual(['b', 'a', 'c']);
    expect((await runQuery<Project>(table, { sort: { field: 'rank', dir: 'desc' } })).map((r) => r.id)).toEqual(['c', 'a', 'b']);
    expect((await runQuery<Project>(table, { sort: { field: 'rank' }, offset: 1, limit: 1 })).map((r) => r.id)).toEqual(['a']);
    expect(await runQuery<Project>(table)).toHaveLength(3);
  });

  it('combines several equality fields', async () => {
    const table = t.handle.db.table('projects') as unknown as Table<Project, string>;
    expect(await runQuery<Project>(table, { where: { colour: 'red', rank: 3 } })).toHaveLength(1);
    expect(await runQuery<Project>(table, { where: { colour: 'red', rank: 99 } })).toHaveLength(0);
  });

  it('uses an index when one covers the field, with the same result', async () => {
    const tasks = t.handle.db.table('tasks') as unknown as Table<PBRecord, string>;
    await tasks.bulkPut([
      { id: 'a', project: 'p1' },
      { id: 'b', project: 'p2' },
    ]);
    expect((await runQuery(tasks, { where: { project: 'p1' } })).map((r) => (r as PBRecord).id)).toEqual(['a']);
  });
});

/**
 * SPEC §2.3 `sort.localFallback` (added v0.1 after first-consumer feedback).
 *
 * The friction it answers: `created`/`updated` are SERVER stamps, so a record
 * made offline has neither, and a list sorted on one scatters the app's own
 * newest writes to whichever end `undefined` happens to land on.
 */
describe('sort.localFallback (SPEC §2.3)', () => {
  /** Stamp a record's `_meta.clientUpdatedAt`, the way a local write would. */
  async function stamp(id: string, iso: string): Promise<void> {
    await t.handle.db.pbo.meta.put({
      collection: 'projects',
      recordId: id,
      serverUpdated: null,
      clientUpdatedAt: Date.parse(iso),
      lastPushedUpdated: null,
      state: 'dirty',
      lastError: null,
    });
  }

  /** Two synced rows around one offline row, in PocketBase's own date shape. */
  async function mixed(): Promise<void> {
    await t.handle.db.table('projects').bulkPut([
      { id: 'synced-old', title: 'Alpha', created: '2026-01-01 09:00:00.000Z' },
      { id: 'unsynced-1', title: 'Beta' },
      { id: 'synced-new', title: 'Gamma', created: '2026-01-03 09:00:00.000Z' },
    ]);
    await stamp('unsynced-1', '2026-01-02T09:00:00.000Z');
  }

  const ids = (rows: Project[]): string[] => rows.map((r) => r.id);
  const table = (): Table<Project, string> => t.handle.db.table('projects') as unknown as Table<Project, string>;

  it('INTERLEAVES an unsynced row into a date-shaped sort, by its clientUpdatedAt', async () => {
    await mixed();
    expect(ids(await runQuery<Project>(table(), { sort: { field: 'created', localFallback: 'clientUpdatedAt' } })))
      .toEqual(['synced-old', 'unsynced-1', 'synced-new']);
    expect(
      ids(
        await runQuery<Project>(table(), {
          sort: { field: 'created', dir: 'desc', localFallback: 'clientUpdatedAt' },
        }),
      ),
    ).toEqual(['synced-new', 'unsynced-1', 'synced-old']);
  });

  it('reads PocketBase’s space separator and ISO’s `T` as the same instant', async () => {
    await t.handle.db.table('projects').bulkPut([
      { id: 'pb-shape', created: '2026-01-02 09:00:00.000Z' },
      { id: 'iso-shape', created: '2026-01-02T08:00:00.000Z' },
      { id: 'offline', created: '' },
    ]);
    await stamp('offline', '2026-01-02T08:30:00.000Z');
    // Plain string comparison would put every `T` row after every space row.
    expect(ids(await runQuery<Project>(table(), { sort: { field: 'created', localFallback: 'clientUpdatedAt' } })))
      .toEqual(['iso-shape', 'offline', 'pb-shape']);
  });

  it('leaves the default sort exactly as it was — the fallback is opt-in', async () => {
    await mixed();
    // No `localFallback`: `undefined` sorts first, whatever the timestamps say.
    expect(ids(await runQuery<Project>(table(), { sort: { field: 'created' } }))).toEqual([
      'unsynced-1',
      'synced-old',
      'synced-new',
    ]);
  });

  it('keeps a non-date field and the clock apart: unsynced rows trail in asc, lead in desc', async () => {
    await t.handle.db.table('projects').bulkPut([
      { id: 'has-b', title: 'Beta' },
      { id: 'no-title-late', title: '' },
      { id: 'has-a', title: 'Alpha' },
      { id: 'no-title-early' },
    ]);
    await stamp('no-title-early', '2026-01-01T00:00:00.000Z');
    await stamp('no-title-late', '2026-01-05T00:00:00.000Z');

    const q = { field: 'title' as const, localFallback: 'clientUpdatedAt' as const };
    expect(ids(await runQuery<Project>(table(), { sort: q }))).toEqual([
      'has-a',
      'has-b',
      'no-title-early',
      'no-title-late',
    ]);
    expect(ids(await runQuery<Project>(table(), { sort: { ...q, dir: 'desc' } }))).toEqual([
      'no-title-late',
      'no-title-early',
      'has-b',
      'has-a',
    ]);
  });

  it('treats `` and null as absent, and 0 as a value', async () => {
    await t.handle.db.table('projects').bulkPut([
      { id: 'zero', rank: 0 },
      { id: 'empty', rank: '' },
      { id: 'nulled', rank: null },
      { id: 'two', rank: 2 },
    ]);
    await stamp('empty', '2026-01-01T00:00:00.000Z');
    await stamp('nulled', '2026-01-02T00:00:00.000Z');
    expect(ids(await runQuery<Project>(table(), { sort: { field: 'rank', localFallback: 'clientUpdatedAt' } })))
      .toEqual(['zero', 'two', 'empty', 'nulled']);
  });

  it('orders an all-unsynced list by clientUpdatedAt, and breaks ties by id', async () => {
    await t.handle.db.table('projects').bulkPut([
      { id: 'c' },
      { id: 'a' },
      { id: 'b' },
      { id: 'later' },
    ]);
    for (const id of ['a', 'b', 'c']) await stamp(id, '2026-01-01T00:00:00.000Z');
    await stamp('later', '2026-01-09T00:00:00.000Z');
    expect(ids(await runQuery<Project>(table(), { sort: { field: 'created', localFallback: 'clientUpdatedAt' } })))
      .toEqual(['a', 'b', 'c', 'later']);
  });

  it('gives a record with no _meta row at all a clientUpdatedAt of 0', async () => {
    await t.handle.db.table('projects').bulkPut([
      { id: 'known', created: '2026-01-01 09:00:00.000Z' },
      { id: 'ghost' },
    ]);
    expect(ids(await runQuery<Project>(table(), { sort: { field: 'created', localFallback: 'clientUpdatedAt' } })))
      .toEqual(['ghost', 'known']);
  });

  it('applies before limit/offset, so find() and a limited count() agree with live()', async () => {
    await mixed();
    const q = { sort: { field: 'created' as const, localFallback: 'clientUpdatedAt' as const }, limit: 2 };
    expect(ids(await projects.find(q))).toEqual(['synced-old', 'unsynced-1']);
    expect(await projects.count(q)).toBe(2);

    const { frames, off } = collect(projects.live(q));
    await tick();
    expect(ids(frames.at(-1) as Project[])).toEqual(['synced-old', 'unsynced-1']);
    off();

    const counted = collect(projects.liveCount(q));
    await tick();
    expect(counted.frames.at(-1)).toBe(2);
    counted.off();
  });

  it('re-emits a correctly reordered list when a local write moves a record', async () => {
    await mixed();
    const { frames, off } = collect(projects.live({ sort: { field: 'created', localFallback: 'clientUpdatedAt' } }));
    await tick();
    expect(ids(frames.at(-1) as Project[])).toEqual(['synced-old', 'unsynced-1', 'synced-new']);

    // A real local write: the mirror row changes AND `_meta.clientUpdatedAt`
    // advances to now, which is past both server stamps.
    await projects.update('unsynced-1', { title: 'Beta, edited' });
    await tick();
    expect(ids(frames.at(-1) as Project[])).toEqual(['synced-old', 'synced-new', 'unsynced-1']);

    // A create with no server stamp lands at the end of an ascending list too.
    // (Stamped explicitly: `update` above and `create` here can land in the same
    // millisecond, and this test is about ordering, not about clock resolution.)
    await projects.create({ id: ID, title: 'Brand new' });
    await stamp(ID, '2027-01-01T00:00:00.000Z');
    await tick();
    expect(ids(frames.at(-1) as Project[])).toEqual(['synced-old', 'synced-new', 'unsynced-1', ID]);
    off();
  });

  it('re-emits when ONLY _meta moves, because _meta is part of the observed set', async () => {
    await mixed();
    const { frames, off } = collect(projects.live({ sort: { field: 'created', localFallback: 'clientUpdatedAt' } }));
    await tick();
    const before = frames.length;

    await stamp('unsynced-1', '2026-01-04T09:00:00.000Z');
    await tick();
    expect(frames.length).toBeGreaterThan(before);
    expect(ids(frames.at(-1) as Project[])).toEqual(['synced-old', 'synced-new', 'unsynced-1']);
    off();
  });
});

describe('live() re-emits on every source of change (SPEC §2.5, TESTPLAN §1.4)', () => {
  it('1. a local create', async () => {
    const { frames, off } = collect(projects.live());
    await tick();
    await projects.create({ id: ID, title: 'One' });
    await tick();
    expect(frames.at(-1)).toEqual([{ id: ID, title: 'One', owner: 'user1' }]);
    off();
  });

  it('2. a local update', async () => {
    await projects.create({ id: ID, title: 'One' });
    const { frames, off } = collect(projects.live());
    await tick();
    await projects.update(ID, { title: 'Two' });
    await tick();
    expect((frames.at(-1) as Project[])[0]!.title).toBe('Two');
    off();
  });

  it('3. a local delete', async () => {
    await projects.create({ id: ID, title: 'One' });
    const { frames, off } = collect(projects.live());
    await tick();
    await projects.delete(ID);
    await tick();
    expect(frames.at(-1)).toEqual([]);
    off();
  });

  it('4. a queue-drain reconciliation (the mirror row rewritten from a push response)', async () => {
    await projects.create({ id: ID, title: 'One' });
    const { frames, off } = collect(projects.live());
    await tick();
    // What push.ts does on success: server record into the mirror, entry gone.
    await t.handle.db.transaction('rw', t.handle.db.table('projects'), t.handle.db.pbo.pending, async () => {
      await t.handle.db.table('projects').put({ id: ID, title: 'One', updated: '2026-01-01 00:00:00.000Z' });
      await t.handle.db.pbo.pending.clear();
    });
    await tick();
    expect((frames.at(-1) as Project[])[0]!['updated']).toBe('2026-01-01 00:00:00.000Z');
    off();
  });

  it('5. a cursor-pull apply', async () => {
    const { frames, off } = collect(projects.live());
    await tick();
    await t.handle.db.table('projects').put({ id: 'remote1', title: 'From the server' });
    await tick();
    expect((frames.at(-1) as Project[]).map((r) => r.id)).toEqual(['remote1']);
    off();
  });

  it('6. an SSE apply', async () => {
    await t.handle.db.table('projects').put({ id: 'remote1', title: 'v1' });
    const { frames, off } = collect(projects.live());
    await tick();
    await t.handle.db.table('projects').put({ id: 'remote1', title: 'v2' });
    await tick();
    expect((frames.at(-1) as Project[])[0]!.title).toBe('v2');
    off();
  });

  it('7. a tombstone delete', async () => {
    await t.handle.db.table('projects').put({ id: 'remote1', title: 'v1' });
    const { frames, off } = collect(projects.live());
    await tick();
    await t.handle.db.transaction('rw', t.handle.db.table('projects'), t.handle.db.pbo.meta, async () => {
      await t.handle.db.table('projects').delete('remote1');
      await t.handle.db.pbo.meta.delete(['projects', 'remote1']);
    });
    await tick();
    expect(frames.at(-1)).toEqual([]);
    off();
  });

  it('emits the declared empty value synchronously and never a reference-equal frame', async () => {
    const { frames, off } = collect(projects.live());
    expect(frames).toEqual([[]]); // before IndexedDB has answered
    await tick();
    await projects.create({ id: ID, title: 'One' });
    await tick();
    const seen = new Set(frames);
    expect(seen.size).toBe(frames.length);
    off();
  });

  it('stops emitting after unsubscribe', async () => {
    const { frames, off } = collect(projects.live());
    await tick();
    const before = frames.length;
    off();
    await projects.create({ id: ID, title: 'One' });
    await tick(20);
    expect(frames).toHaveLength(before);
  });

  it('a filtered live() ignores a change that does not affect its result', async () => {
    await projects.create({ id: ID, title: 'One', colour: 'red' });
    await projects.create({ id: 'bbbbbbbbbbbbbbb', title: 'Two', colour: 'blue' });
    const { frames, off } = collect(projects.live({ where: { colour: 'red' } }));
    await tick();
    expect((frames.at(-1) as Project[]).map((r) => r.id)).toEqual([ID]);
    await projects.update('bbbbbbbbbbbbbbb', { title: 'Two!' });
    await tick();
    expect((frames.at(-1) as Project[]).map((r) => r.id)).toEqual([ID]);
    off();
  });
});

describe('liveOne / liveCount / liveMeta / liveDirty', () => {
  it('liveOne re-emits for its record and stays quiet for another', async () => {
    await projects.create({ id: ID, title: 'One' });
    await projects.create({ id: 'bbbbbbbbbbbbbbb', title: 'Two' });
    const { frames, off } = collect(projects.liveOne(ID));
    expect(frames).toEqual([undefined]);
    await tick();

    await projects.update('bbbbbbbbbbbbbbb', { title: 'other' });
    await tick();
    const afterOther = frames.length;

    await projects.update(ID, { title: 'mine' });
    await tick();
    expect(frames.length).toBeGreaterThan(afterOther);
    expect((frames.at(-1) as Project).title).toBe('mine');
    off();
  });

  it('liveCount tracks the table and starts at 0', async () => {
    const { frames, off } = collect(projects.liveCount());
    expect(frames).toEqual([0]);
    await projects.create({ id: ID });
    await tick();
    expect(frames.at(-1)).toBe(1);
    await projects.delete(ID);
    await tick();
    expect(frames.at(-1)).toBe(0);
    off();
  });

  it('liveMeta reports the record’s sync state as the queue changes', async () => {
    const { frames, off } = collect(projects.liveMeta(ID));
    expect(frames).toEqual([undefined]);
    await projects.create({ id: ID, title: 'One' });
    await tick();
    expect(frames.at(-1)).toMatchObject({ id: ID, state: 'dirty', serverUpdated: null, pendingOps: 1 });

    await t.handle.db.pbo.pending.clear(); // pushed
    await tick();
    expect(frames.at(-1)).toMatchObject({ state: 'synced', pendingOps: 0 });
    off();
  });

  it('liveDirty lists pending and dead-lettered records for this collection only', async () => {
    const { frames, off } = collect(projects.liveDirty());
    expect(frames).toEqual([[]]);
    await projects.create({ id: ID });
    await create(t.deps('tasks'), { id: 'bbbbbbbbbbbbbbb' });
    await tick();
    expect(frames.at(-1)).toEqual([{ id: ID, state: 'dirty' }]);

    await t.handle.db.pbo.deadletter.add({
      collection: 'projects',
      recordId: 'ccccccccccccccc',
      op: 'update',
      payload: {},
      error: { code: 'push/rejected', message: 'no', retryable: false, at: 1 },
      at: 1,
    } as never);
    await tick();
    expect(frames.at(-1)).toContainEqual({ id: 'ccccccccccccccc', state: 'dead' });
    off();
  });

  it('liveRaw runs arbitrary Dexie work and re-runs on a change', async () => {
    const { frames, off } = collect(projects.liveRaw(async (table) => (await table.toArray()).map((r) => r.id)));
    expect(frames).toEqual([undefined]);
    await projects.create({ id: ID });
    await tick();
    expect(frames.at(-1)).toEqual([ID]);
    off();
  });
});

describe('one-shot reads never touch the network', () => {
  it('get / getAll / find / count / meta read the mirror', async () => {
    await projects.create({ id: ID, title: 'One', colour: 'red' });
    await create(t.deps('tasks'), { id: 'bbbbbbbbbbbbbbb', project: ID });

    expect((await projects.get(ID))!.title).toBe('One');
    expect(await projects.getAll()).toHaveLength(1);
    expect(await projects.find({ where: { colour: 'red' } })).toHaveLength(1);
    expect(await projects.find({ where: { colour: 'blue' } })).toHaveLength(0);
    expect(await projects.count()).toBe(1);
    expect(await projects.count({ where: { colour: 'blue' } })).toBe(0);
    expect(await projects.meta(ID)).toMatchObject({ id: ID, state: 'dirty', pendingOps: 1 });
    expect(await projects.meta('nope')).toBeUndefined();
  });

  it('meta counts every queued op for the record', async () => {
    await projects.create({ id: ID });
    await projects.update(ID, { title: 'x' }, { noCoalesce: true });
    expect((await projects.meta(ID))!.pendingOps).toBe(2);
    const entries: PendingEntry[] = await t.handle.db.pbo.pending.toArray();
    expect(entries.map((e) => e.op)).toEqual(['create', 'update']);
  });
});

/**
 * SPEC §2.3, "Reactive reads outlive the mirror" (added v0.1 after
 * first-consumer feedback).
 *
 * The first consumer's boot order — a `start()` that is not awaited, components
 * building their signals a turn later — is the ordinary one, and it lost this
 * race every time. Dexie re-runs a querier only for tables the querier read LAST
 * time, so a reactive read built before the mirror opened observed nothing and
 * stayed empty for ever while writes went through perfectly. Every one of the
 * six reactive reads on the handle is exercised here, before the mirror opens
 * and across an account switch.
 */
describe('reactive reads built before the mirror opens (SPEC §2.3)', () => {
  interface Mirror {
    deps(collection?: string): CollectionDeps;
    /** What the db manager does: swap the handle and announce the change. */
    set(db: PBODexie | undefined): void;
  }

  function mirror(cfg: ResolvedConfig, ownerId: () => string | null = () => 'user1'): Mirror {
    const revision = createRevision();
    let current: PBODexie | undefined;
    return {
      deps(collection = 'projects'): CollectionDeps {
        return {
          getDb: () => current,
          revision,
          cfg,
          collectionCfg: cfg.collections.find((c) => c.name === collection)!,
          getOwnerId: ownerId,
          nudge: () => {},
        };
      },
      set(db) {
        current = db;
        revision.step();
      },
    };
  }

  /** Everything a record needs to be visible to all six reactive reads. */
  async function seed(db: PBODexie, id: string, title: string): Promise<void> {
    await db.table('projects').put({ id, title });
    await db.pbo.meta.put({
      collection: 'projects',
      recordId: id,
      serverUpdated: null,
      clientUpdatedAt: Date.now(),
      lastPushedUpdated: null,
      state: 'dirty',
      lastError: null,
    });
    await db.pbo.pending.add({
      collection: 'projects',
      recordId: id,
      op: 'create',
      payload: { title },
      clientUpdatedAt: Date.now(),
      state: 'queued',
      attempts: 0,
      nextAttemptAt: 0,
      lastError: null,
    } as unknown as PendingEntry);
  }

  let cfg: ResolvedConfig;
  let a: OpenHandle;
  let b: OpenHandle;

  beforeEach(async () => {
    cfg = testConfig();
    a = await openForOwner(cfg, 'ownerA');
    b = await openForOwner(cfg, 'ownerB');
  });
  afterEach(async () => {
    a.db.close();
    b.db.close();
    await Dexie.delete(a.dbName);
    await Dexie.delete(b.dbName);
  });

  /** Subscribe to all six reactive reads and report the latest frame of each. */
  function subscribeAll(handle: Collection<Project>): {
    latest(): Record<string, unknown>;
    frameCounts(): Record<string, number>;
    off(): void;
  } {
    const stores: Record<string, Readable<unknown>> = {
      live: handle.live(),
      liveOne: handle.liveOne(ID),
      liveCount: handle.liveCount(),
      liveRaw: handle.liveRaw(async (table) => (await table.toArray()).map((r) => r.id)),
      liveMeta: handle.liveMeta(ID),
      liveDirty: handle.liveDirty(),
    };
    const latest: Record<string, unknown> = {};
    const counts: Record<string, number> = {};
    const offs = Object.entries(stores).map(([name, store]) =>
      store.subscribe((v) => {
        latest[name] = v;
        counts[name] = (counts[name] ?? 0) + 1;
      }),
    );
    return {
      latest: () => latest,
      frameCounts: () => counts,
      off: () => offs.forEach((f) => f()),
    };
  }

  it('all six come alive when the mirror opens, and every one of them was empty first', async () => {
    const m = mirror(cfg);
    const handle = defineCollection<Project>(m.deps());
    const seen = subscribeAll(handle);

    // Property 1 of the store contract still holds: a synchronous empty frame.
    expect(seen.latest()).toEqual({
      live: [],
      liveOne: undefined,
      liveCount: 0,
      liveRaw: undefined,
      liveMeta: undefined,
      liveDirty: [],
    });
    await tick(20);

    m.set(a.db);
    await seed(a.db, ID, 'One');
    await tick(20);

    expect(seen.latest()['live']).toEqual([{ id: ID, title: 'One' }]);
    expect(seen.latest()['liveOne']).toEqual({ id: ID, title: 'One' });
    expect(seen.latest()['liveCount']).toBe(1);
    expect(seen.latest()['liveRaw']).toEqual([ID]);
    expect(seen.latest()['liveMeta']).toMatchObject({ id: ID, state: 'dirty', pendingOps: 1 });
    expect(seen.latest()['liveDirty']).toEqual([{ id: ID, state: 'dirty' }]);
    seen.off();
  });

  it('all six follow an account switch — writes under the new owner emit (SPEC §14.2)', async () => {
    const m = mirror(cfg);
    const handle = defineCollection<Project>(m.deps());
    const seen = subscribeAll(handle);

    m.set(a.db);
    await seed(a.db, ID, 'A');
    await tick(20);
    expect(seen.latest()['live']).toEqual([{ id: ID, title: 'A' }]);

    // §14.2's two steps: the outgoing owner's database closes, the incoming
    // owner's opens. Owner A's rows must not survive into owner B's session.
    m.set(undefined);
    m.set(b.db);
    await tick(20);
    expect(seen.latest()['live']).toEqual([]);
    expect(seen.latest()['liveOne']).toBeUndefined();
    expect(seen.latest()['liveCount']).toBe(0);
    expect(seen.latest()['liveDirty']).toEqual([]);

    // A write under B, which touches a database A's query never observed.
    await seed(b.db, ID, 'B');
    await tick(20);
    expect(seen.latest()['live']).toEqual([{ id: ID, title: 'B' }]);
    expect(seen.latest()['liveOne']).toEqual({ id: ID, title: 'B' });
    expect(seen.latest()['liveCount']).toBe(1);
    expect(seen.latest()['liveRaw']).toEqual([ID]);
    expect(seen.latest()['liveMeta']).toMatchObject({ id: ID, state: 'dirty' });
    expect(seen.latest()['liveDirty']).toEqual([{ id: ID, state: 'dirty' }]);
    seen.off();
  });

  it('unsubscribing before the mirror opens leaks nothing and emits nothing after', async () => {
    const m = mirror(cfg);
    const handle = defineCollection<Project>(m.deps());
    const seen = subscribeAll(handle);
    await tick(20);
    const counts = { ...seen.frameCounts() };

    seen.off();
    m.set(a.db);
    await seed(a.db, ID, 'One');
    await tick(30);

    expect(seen.frameCounts()).toEqual(counts);
  });

  it("liveDirty is not frozen by the session it was built in (SPEC §14.3)", async () => {
    let owner: string | null = null;
    const disabled = testConfig({ anonymousMode: 'disabled' } as Partial<PBOfflineConfig>);
    const m = mirror(disabled, () => owner);
    const handle = defineCollection<Project>(m.deps());

    // Built with reads blocked: it used to become a `constant([])` here, which
    // signing in could never wake.
    const frames: Array<Array<{ id: string; state: string }>> = [];
    const off = handle.liveDirty().subscribe((v) => frames.push(v));
    expect(frames).toEqual([[]]);
    await tick(20);

    const signedIn = await openForOwner(disabled, 'ownerC');
    try {
      owner = 'ownerC';
      m.set(signedIn.db);
      await seed(signedIn.db, ID, 'One');
      await tick(20);
      expect(frames.at(-1)).toEqual([{ id: ID, state: 'dirty' }]);
    } finally {
      off();
      signedIn.db.close();
      await Dexie.delete(signedIn.dbName);
    }
  });
});
