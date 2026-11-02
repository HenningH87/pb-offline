/** SPEC §2.1, §2.2, §14, §15 — the engine handle and its lifecycle. */
import { afterEach, describe, expect, it, vi } from 'vitest';
import Dexie from 'dexie';
import { createPBOffline } from '../../src/index.js';
import { BRAND, dbNameFor } from '../../src/brand.js';
import { PBOError } from '../../src/errors.js';
import { asPB, createFakePB, sdkError, type FakePB } from '../fakes/pb.js';
import { resolveConfig } from '../../src/config.js';
import { close, openForOwner } from '../../src/db/open.js';
import { readSignOutPending } from '../../src/db/state.js';
import { tick, uniqueAppId } from './_helpers.js';
import type { PBOffline, PBOfflineConfig, PBRecord, SyncState } from '../../src/types.js';

interface Project extends PBRecord {
  title?: string;
}

const engines: PBOffline[] = [];
const dbNames: string[] = [];

afterEach(async () => {
  while (engines.length > 0) await engines.pop()?.destroy();
  while (dbNames.length > 0) await Dexie.delete(dbNames.pop()!);
  vi.restoreAllMocks();
});

function makeEngine(overrides?: Partial<PBOfflineConfig>, pb: FakePB = createFakePB({ ownerId: 'user1' })): {
  engine: PBOffline;
  pb: FakePB;
  appId: string;
} {
  const appId = uniqueAppId();
  const engine = createPBOffline(asPB(pb), {
    appId,
    collections: [{ name: 'projects' }, { name: 'tasks', indexes: ['project'] }],
    ...overrides,
  } as PBOfflineConfig);
  engines.push(engine);
  dbNames.push(dbNameFor(appId, 'user1'), dbNameFor(appId, BRAND.anonOwner));
  return { engine, pb, appId };
}

describe('createPBOffline (SPEC §2.1)', () => {
  it('is synchronous, performs no I/O, and returns with every store emitting', () => {
    const open = vi.spyOn(globalThis.indexedDB, 'open');
    const { engine, pb } = makeEngine();

    const seen: Record<string, unknown> = {};
    engine.status.subscribe((v) => (seen['status'] = v))();
    engine.pendingCount.subscribe((v) => (seen['pendingCount'] = v))();
    engine.lastSyncedAt.subscribe((v) => (seen['lastSyncedAt'] = v))();
    engine.online.subscribe((v) => (seen['online'] = v))();
    engine.deadLetters.subscribe((v) => (seen['deadLetters'] = v))();
    let state!: SyncState;
    engine.state.subscribe((v) => (state = v))();

    expect(seen).toEqual({ status: 'offline', pendingCount: 0, lastSyncedAt: null, online: false, deadLetters: [] });
    expect(state).toMatchObject({ status: 'offline', ownerId: 'user1', isDrainer: false, deadLetterCount: 0 });
    expect(open).not.toHaveBeenCalled();
    expect(pb.requests).toEqual([]);
  });

  it('throws config/invalid synchronously for bad config', () => {
    const pb = createFakePB();
    expect(() => createPBOffline(asPB(pb), { appId: '', collections: [] } as PBOfflineConfig)).toThrowError(PBOError);
    try {
      createPBOffline(asPB(pb), { appId: 'ok', collections: [] } as PBOfflineConfig);
    } catch (e) {
      expect((e as PBOError).code).toBe('config/invalid');
    }
  });

  it('throws config/unknown-collection for a name that is not configured', () => {
    const { engine } = makeEngine();
    expect(engine.collection('projects').name).toBe('projects');
    try {
      engine.collection('nope');
      throw new Error('should have thrown');
    } catch (e) {
      expect((e as PBOError).code).toBe('config/unknown-collection');
    }
  });
});

describe('lifecycle', () => {
  it('start() opens the owner database and disables SDK auto-cancellation (SPEC §5.4)', async () => {
    const { engine, pb, appId } = makeEngine();
    await engine.start();
    expect(await Dexie.exists(dbNameFor(appId, 'user1'))).toBe(true);
    expect(pb.autoCancellationCalls).toEqual([false]);
  });

  it('start() is safe to call concurrently with itself', async () => {
    const { engine } = makeEngine();
    await Promise.all([engine.start(), engine.start(), engine.start()]);
    await expect(engine.start()).resolves.toBeUndefined();
  });

  it('surfaces a database failure as db/open-failed through status, not as an async throw', async () => {
    const { engine } = makeEngine();
    const errors: string[] = [];
    engine.on('error', (e) => errors.push(e.error.code));
    const original = globalThis.indexedDB.open;
    globalThis.indexedDB.open = (() => {
      throw new Error('private mode');
    }) as typeof globalThis.indexedDB.open;
    try {
      await expect(engine.start()).rejects.toMatchObject({ code: 'db/open-failed' });
    } finally {
      globalThis.indexedDB.open = original;
    }
    let status = '';
    engine.status.subscribe((v) => (status = v))();
    expect(status).toBe('error');
    expect(errors).toEqual(['db/open-failed']);
  });

  it('tracks the local queue depth in this tab, drainer or not', async () => {
    // Offline: the queue depth being observable is the claim, and a drain that
    // succeeded would empty it before the assertion could see it.
    const { engine } = makeEngine(undefined, createFakePB({ ownerId: 'user1', offline: true }));
    await engine.start();
    const counts: number[] = [];
    const off = engine.pendingCount.subscribe((v) => counts.push(v));
    await engine.collection<Project>('projects').create({ title: 'One' });
    await tick(20);
    expect(counts.at(-1)).toBe(1);
    off();
  });

  it('emits a status event when the status changes', async () => {
    const { engine } = makeEngine();
    const events: string[] = [];
    engine.on('status', (e) => events.push(`${e.from}->${e.to}`));
    await engine.start();
    await tick();
    expect(events.every((e) => /^[a-z]+->[a-z]+$/.test(e))).toBe(true);
  });

  it('stop() then start() works, and destroy() makes the instance unusable', async () => {
    const { engine } = makeEngine();
    await engine.start();
    await engine.stop();
    await engine.start();
    await engine.destroy();
    expect(() => engine.collection('projects')).toThrowError(PBOError);
    await expect(engine.push()).rejects.toMatchObject({ code: 'internal/invariant' });
    await expect(engine.destroy()).resolves.toBeUndefined(); // idempotent
    engines.length = 0;
  });

  it('routes a failed reactive read to the error event instead of dropping it', async () => {
    const { engine } = makeEngine();
    await engine.start();
    const codes: string[] = [];
    engine.on('error', (e) => codes.push(e.error.code));
    const off = engine
      .collection<Project>('projects')
      .liveRaw(async () => {
        throw new Error('a caller’s own liveRaw threw');
      })
      .subscribe(() => {});
    await tick(20);
    off();
    expect(codes).toHaveLength(1);
  });

  it('reads and writes work before any network exists — nothing is gated on the link', async () => {
    const { engine, pb } = makeEngine(undefined, createFakePB({ ownerId: 'user1', offline: true }));
    await engine.start();
    const projects = engine.collection<Project>('projects');
    const rec = await projects.create({ title: 'One' });
    await projects.update(rec.id, { title: 'Two' });
    expect((await projects.get(rec.id))!.title).toBe('Two');
    // Health pings are the link watcher doing its job; no RECORD request was
    // made, and neither write waited for one.
    expect(pb.recordRequests()).toEqual([]);
  });
});

/**
 * SPEC §2.2 `SyncState.started` (added v0.1 after first-consumer feedback).
 *
 * A consumer that starts the engine from a framework effect has no way to ask
 * whether it did — `start()` is idempotent and returns the same promise, so
 * calling it again tells you nothing. This is the answer, and it is a property
 * of the INSTANCE: never persisted to `_state`, never inherited from another
 * tab's row.
 */
describe('SyncState.started (SPEC §2.2)', () => {
  const startedOf = (engine: PBOffline): boolean => {
    let s!: SyncState;
    engine.state.subscribe((v) => (s = v))();
    return s.started;
  };

  it('is false before start(), true after, and emits the transition', async () => {
    const { engine } = makeEngine();
    const seen: boolean[] = [];
    const off = engine.state.subscribe((s) => seen.push(s.started));
    expect(seen).toEqual([false]);

    await engine.start();
    expect(startedOf(engine)).toBe(true);
    expect(seen).toContain(true);
    off();
  });

  it('goes back to false on stop(), and true again on a restart', async () => {
    const { engine } = makeEngine();
    await engine.start();
    await engine.stop();
    expect(startedOf(engine)).toBe(false);
    await engine.start();
    expect(startedOf(engine)).toBe(true);
  });

  it('is false after destroy()', async () => {
    const { engine } = makeEngine();
    await engine.start();
    await engine.destroy();
    expect(startedOf(engine)).toBe(false);
    engines.length = 0;
  });

  it('is false again when a start() fails — a rejected start is not a start', async () => {
    const { engine } = makeEngine();
    const original = globalThis.indexedDB.open;
    globalThis.indexedDB.open = (() => {
      throw new Error('private mode');
    }) as typeof globalThis.indexedDB.open;
    try {
      await expect(engine.start()).rejects.toMatchObject({ code: 'db/open-failed' });
    } finally {
      globalThis.indexedDB.open = original;
    }
    expect(startedOf(engine)).toBe(false);
  });

  it('SURVIVES an auth change — the engine is still started, on another mirror', async () => {
    const pb = createFakePB({ ownerId: 'user1' });
    const { engine, appId } = makeEngine(undefined, pb);
    dbNames.push(dbNameFor(appId, 'user2'));
    await engine.start();

    pb.setOwner('user2');
    await tick(20);
    expect(startedOf(engine)).toBe(true);

    pb.setOwner(null);
    await tick(20);
    expect(startedOf(engine)).toBe(true);
  });

  it("is true with anonymousMode 'disabled' and no session, which opens no database", async () => {
    const { engine, appId } = makeEngine({ anonymousMode: 'disabled' }, createFakePB({ ownerId: null }));
    await engine.start();
    expect(await Dexie.exists(dbNameFor(appId, BRAND.anonOwner))).toBe(false);
    // `start()` is in effect — the engine is watching the auth store for the
    // sign-in that will open one. That is not the same as "nothing happened".
    expect(startedOf(engine)).toBe(true);
  });

  it('is false after a signOut() that wiped, because the next start() is a real one', async () => {
    const { engine } = makeEngine();
    await engine.start();
    expect(await engine.signOut()).toMatchObject({ wiped: true });
    expect(startedOf(engine)).toBe(false);
  });

  it('is never persisted to _state, so a second tab cannot inherit it', async () => {
    const { engine, appId } = makeEngine();
    await engine.start();
    await tick(20);
    const handle = await openForOwner(
      resolveConfig({ appId, collections: [{ name: 'projects' }, { name: 'tasks', indexes: ['project'] }] }),
      'user1',
    );
    try {
      const row = await handle.db.pbo.state.get('sync');
      expect((row?.state as SyncState | undefined)?.started ?? false).toBe(false);
    } finally {
      await close(handle);
    }
  });
});

describe('anonymous mode (SPEC §14.3)', () => {
  it("'local' opens the anon database and reports ownerId null", async () => {
    const pb = createFakePB({ ownerId: null });
    const { engine, appId } = makeEngine(undefined, pb);
    await engine.start();
    expect(await Dexie.exists(dbNameFor(appId, BRAND.anonOwner))).toBe(true);
    let state!: SyncState;
    engine.state.subscribe((v) => (state = v))();
    expect(state.ownerId).toBeNull();
    expect(state.status).toBe('offline');
    await engine.collection<Project>('projects').create({ title: 'local only' });
    expect(pb.requests).toEqual([]);
  });

  it("'disabled' opens nothing, reads empty and writes throw auth/unauthenticated", async () => {
    const pb = createFakePB({ ownerId: null });
    const { engine, appId } = makeEngine({ anonymousMode: 'disabled' }, pb);
    await engine.start();
    expect(await Dexie.exists(dbNameFor(appId, BRAND.anonOwner))).toBe(false);
    const projects = engine.collection<Project>('projects');
    expect(await projects.getAll()).toEqual([]);
    await expect(projects.create({ title: 'x' })).rejects.toMatchObject({ code: 'auth/unauthenticated' });
  });
});

describe('dead letters', () => {
  /** Park an entry the way push.ts will (SPEC §5.6), before the engine starts. */
  async function withDeadLetter(offline = false): Promise<{ engine: PBOffline; id: number }> {
    const appId = uniqueAppId();
    const config = { appId, collections: [{ name: 'projects' }] } as PBOfflineConfig;
    const cfg = resolveConfig(config);
    const handle = await openForOwner(cfg, 'user1');
    const id = (await handle.db.pbo.deadletter.add({
      collection: 'projects',
      recordId: 'aaaaaaaaaaaaaaa',
      op: 'create',
      payload: { id: 'aaaaaaaaaaaaaaa', title: 'One' },
      error: { code: 'push/rejected', message: 'rules said no', retryable: false, at: Date.now() },
      at: Date.now(),
    } as never)) as number;
    await close(handle);

    const engine = createPBOffline(asPB(createFakePB({ ownerId: 'user1', offline })), config);
    engines.push(engine);
    dbNames.push(dbNameFor(appId, 'user1'));
    await engine.start();
    return { engine, id };
  }

  it('surfaces them through the store and counts them in state', async () => {
    const { engine } = await withDeadLetter();
    await tick(20);
    let rows: unknown[] = [];
    engine.deadLetters.subscribe((v) => (rows = v))();
    expect(rows).toHaveLength(1);
    let state!: SyncState;
    engine.state.subscribe((v) => (state = v))();
    expect(state.deadLetterCount).toBe(1);
  });

  it('retry re-enqueues at the tail and discard drops it', async () => {
    // Offline, so the re-enqueued entry is still in `_pending` to be counted.
    const { engine, id } = await withDeadLetter(true);
    await tick(20);

    await engine.retryDeadLetter(id);
    await tick(20);
    let rows: unknown[] = [];
    engine.deadLetters.subscribe((v) => (rows = v))();
    expect(rows).toEqual([]);
    let pending = 0;
    engine.pendingCount.subscribe((v) => (pending = v))();
    expect(pending).toBe(1);

    const { engine: e2, id: id2 } = await withDeadLetter(true);
    await e2.discardDeadLetter(id2);
    await tick(20);
    e2.deadLetters.subscribe((v) => (rows = v))();
    expect(rows).toEqual([]);
  });
});

describe('signOut (SPEC §15)', () => {
  it('wipes the database when the queue is empty and never clears the auth store', async () => {
    const { engine, pb, appId } = makeEngine();
    await engine.start();
    const clear = vi.spyOn(pb.authStore, 'clear');
    const changes: Array<string | null> = [];
    engine.on('auth-change', (e) => changes.push(e.previous));

    const result = await engine.signOut();
    expect(result).toEqual({ drained: true, wiped: true, pending: 0 });
    expect(await Dexie.exists(dbNameFor(appId, 'user1'))).toBe(false);
    expect(clear).not.toHaveBeenCalled();
    expect(changes).toEqual(['user1']);
  });

  it('refuses to wipe an undrainable queue, and reports how many changes would be lost', async () => {
    const { engine, appId } = makeEngine(undefined, createFakePB({ ownerId: 'user1', offline: true }));
    await engine.start();
    await engine.collection<Project>('projects').create({ title: 'unsent' });

    const result = await engine.signOut({ flushTimeoutMs: 10 });
    expect(result).toEqual({ drained: false, wiped: false, pending: 1 });
    expect(await Dexie.exists(dbNameFor(appId, 'user1'))).toBe(true);
  });

  it('force wipes anyway', async () => {
    const { engine, appId } = makeEngine(undefined, createFakePB({ ownerId: 'user1', offline: true }));
    await engine.start();
    await engine.collection<Project>('projects').create({ title: 'unsent' });
    const result = await engine.signOut({ flushTimeoutMs: 10, force: true });
    expect(result).toMatchObject({ wiped: true, pending: 1 });
    expect(await Dexie.exists(dbNameFor(appId, 'user1'))).toBe(false);
  });

  /**
   * The audit's M1. An unforced sign-out that cannot drain leaves the whole
   * mirror on the device — every mirrored row, no session needed to read it —
   * while the application clears its token and the UI says "signed out". The
   * marker is what stops that being permanent.
   */
  it('marks an unwiped mirror so the next start() for that owner finishes the sign-out', async () => {
    const appId = uniqueAppId();
    const cfg = { appId, collections: [{ name: 'projects' }, { name: 'tasks', indexes: ['project'] }] };
    dbNames.push(dbNameFor(appId, 'user1'));

    const pb = createFakePB({ ownerId: 'user1', offline: true });
    const first = createPBOffline(asPB(pb), cfg as PBOfflineConfig);
    engines.push(first);
    await first.start();
    await first.collection<Project>('projects').create({ title: 'unsent' });
    expect(await first.signOut({ flushTimeoutMs: 10 })).toMatchObject({ wiped: false, pending: 1 });
    expect(await Dexie.exists(dbNameFor(appId, 'user1'))).toBe(true);
    await first.destroy();

    // Same owner, next visit, and this time the link is up: the debt is paid
    // — the queue drains to the server and the old mirror is destroyed.
    pb.offline = false;
    const second = createPBOffline(asPB(pb), cfg as PBOfflineConfig);
    engines.push(second);
    await second.start();
    await tick(20);

    expect(pb.recordRequests().some((r) => r.method === 'create')).toBe(true);
    // A fresh, empty mirror for the session that is actually running — not the
    // previous owner's rows, and not a started engine with no database at all.
    expect(await second.collection<Project>('projects').getAll()).toEqual([]);
    let state!: SyncState;
    second.state.subscribe((v) => (state = v))();
    expect(state.started).toBe(true);
  });

  it('keeps the marker, and the data, when the retry still cannot drain', async () => {
    const appId = uniqueAppId();
    const cfg = { appId, collections: [{ name: 'projects' }] };
    dbNames.push(dbNameFor(appId, 'user1'));

    const pb = createFakePB({ ownerId: 'user1', offline: true });
    const first = createPBOffline(asPB(pb), cfg as PBOfflineConfig);
    engines.push(first);
    await first.start();
    await first.collection<Project>('projects').create({ title: 'unsent' });
    await first.signOut({ flushTimeoutMs: 10 });
    await first.destroy();

    // Still offline. Destroying unpushed work is `force`'s decision, never a
    // retry's, so the mirror survives — and so does the marker.
    const second = createPBOffline(asPB(pb), cfg as PBOfflineConfig);
    engines.push(second);
    await second.start();
    await tick(20);
    expect(await second.collection<Project>('projects').getAll()).toHaveLength(1);
    expect(await Dexie.exists(dbNameFor(appId, 'user1'))).toBe(true);
  });

  /**
   * SYNC-KEEP-1. The server refuses the retried write: it is parked in
   * `_deadletter`, the queue counts as empty, and a wipe would destroy the only
   * copy of it. The retry keeps the mirror and clears the marker.
   */
  it('keeps the mirror when the retry drain parks a refused write, and clears the marker', async () => {
    const appId = uniqueAppId();
    const cfg = { appId, collections: [{ name: 'projects' }] };
    dbNames.push(dbNameFor(appId, 'user1'));

    const pb = createFakePB({ ownerId: 'user1', offline: true });
    const first = createPBOffline(asPB(pb), cfg as PBOfflineConfig);
    engines.push(first);
    await first.start();
    await first.collection<Project>('projects').create({ title: 'refused' });
    expect(await first.signOut({ flushTimeoutMs: 0 })).toMatchObject({ wiped: false, pending: 1 });
    await first.destroy();

    pb.offline = false;
    pb.failNext('projects', 'create', sdkError(400, { response: { message: 'rules said no' } }));
    const second = createPBOffline(asPB(pb), cfg as PBOfflineConfig);
    engines.push(second);
    await second.start();
    await tick(20);

    expect(pb.recordRequests().some((r) => r.method === 'create')).toBe(true);
    expect(await second.collection<Project>('projects').getAll()).toHaveLength(1);
    let rows: unknown[] = [];
    second.deadLetters.subscribe((v) => (rows = v))();
    expect(rows).toHaveLength(1);
    await second.destroy();

    const handle = await openForOwner(resolveConfig(cfg as PBOfflineConfig), 'user1');
    try {
      expect(await readSignOutPending(handle.db)).toBeNull();
      expect(await handle.db.pbo.deadletter.count()).toBe(1);
    } finally {
      await close(handle);
    }
  });
});

describe('sync surface (SPEC §5–§11)', () => {
  it('push/pull/sync/fullResync reach the server and report what they did', async () => {
    const { engine, pb } = makeEngine();
    await engine.start();
    expect(await engine.push()).toMatchObject({ pushed: 0, remaining: 0, stoppedBy: 'empty' });
    const pulled = await engine.pull();
    expect(Object.keys(pulled.byCollection).sort()).toEqual(['deletions', 'projects', 'tasks']);
    await expect(engine.sync()).resolves.toBeUndefined();
    await expect(engine.fullResync('test')).resolves.toBeUndefined();
    expect(pb.recordRequests().some((r) => r.method === 'getList')).toBe(true);
  });

  it('makes no request at all with no session (SPEC §14.3)', async () => {
    const pb = createFakePB({ ownerId: null });
    const { engine } = makeEngine(undefined, pb);
    await engine.start();
    await engine.sync();
    await engine.push();
    await tick(20);
    expect(pb.requests).toEqual([]);
  });

  it('promotion requires a session and never runs on its own', async () => {
    const pb = createFakePB({ ownerId: null });
    const { engine } = makeEngine(undefined, pb);
    await engine.start();
    await expect(engine.promoteAnonymousData()).rejects.toMatchObject({ code: 'auth/unauthenticated' });
  });
});

/**
 * SPEC §2.3, "Reactive reads outlive the mirror" (added v0.1 after
 * first-consumer feedback).
 *
 * The first consumer's boot order is the ordinary one: an effect calls `start()`
 * without awaiting it and the view builds its live queries a turn later. That
 * used to produce a permanently dead subscription — the list stayed empty
 * through writes, reloads and everything else — because Dexie re-runs a querier
 * only for tables the querier read on its PREVIOUS run, and this one had read
 * none. These are that boot order, through the public API only.
 */
describe('a live query built before start() (SPEC §2.3)', () => {
  /** Collect every frame a store emits. */
  function collect<T>(store: { subscribe: (r: (v: T) => void) => () => void }): { frames: T[]; off: () => void } {
    const frames: T[] = [];
    const off = store.subscribe((v) => frames.push(v));
    return { frames, off };
  }

  it('comes alive when start() opens the mirror, and shows the first write', async () => {
    const { engine } = makeEngine();
    const projects = engine.collection<Project>('projects');

    // Subscribed BEFORE start(), and start() is not awaited first — the race
    // exactly as an unawaited effect loses it.
    const list = collect<Project[]>(projects.live());
    const one = collect<Project | undefined>(projects.liveOne('aaaaaaaaaaaaaaa'));
    const count = collect<number>(projects.liveCount());
    expect(list.frames).toEqual([[]]);

    // The querier's first run has to land BEFORE the mirror opens, which is the
    // whole race: that is the run whose (empty) observability set Dexie keeps.
    await tick(10);

    await engine.start();
    await projects.create({ id: 'aaaaaaaaaaaaaaa', title: 'One' });
    await tick(20);

    expect(list.frames.at(-1)).toMatchObject([{ id: 'aaaaaaaaaaaaaaa', title: 'One' }]);
    expect(one.frames.at(-1)).toMatchObject({ title: 'One' });
    expect(count.frames.at(-1)).toBe(1);
    list.off();
    one.off();
    count.off();
  });

  it('follows the account switch and shows the new owner’s writes (SPEC §14.2)', async () => {
    const pb = createFakePB({ ownerId: 'user1' });
    const { engine, appId } = makeEngine(undefined, pb);
    dbNames.push(dbNameFor(appId, 'user2'));
    const projects = engine.collection<Project>('projects');
    const list = collect<Project[]>(projects.live());

    await engine.start();
    await projects.create({ id: 'aaaaaaaaaaaaaaa', title: 'One' });
    await tick(20);
    expect(list.frames.at(-1)).toHaveLength(1);

    pb.setOwner('user2');
    await tick(30);
    // user1's mirror is closed, not this one — nothing of theirs may show.
    expect(list.frames.at(-1)).toEqual([]);

    await projects.create({ id: 'bbbbbbbbbbbbbbb', title: 'Two' });
    await tick(20);
    expect(list.frames.at(-1)).toMatchObject([{ id: 'bbbbbbbbbbbbbbb', title: 'Two' }]);
    list.off();
  });

  it('empties when signOut() wipes the mirror out from under it (SPEC §15)', async () => {
    const { engine } = makeEngine();
    const projects = engine.collection<Project>('projects');
    await engine.start();
    await projects.create({ id: 'aaaaaaaaaaaaaaa', title: 'One' });
    const list = collect<Project[]>(projects.live());
    await tick(20);
    expect(list.frames.at(-1)).toHaveLength(1);

    expect(await engine.signOut({ force: true })).toMatchObject({ wiped: true });
    await tick(20);
    expect(list.frames.at(-1)).toEqual([]);
    list.off();
  });

  it('a second tab that subscribed before its own start() sees the first tab’s write', async () => {
    const appId = uniqueAppId();
    const config = {
      appId,
      collections: [{ name: 'projects' }],
    } as PBOfflineConfig;
    const tabA = createPBOffline(asPB(createFakePB({ ownerId: 'user1' })), config);
    const tabB = createPBOffline(asPB(createFakePB({ ownerId: 'user1' })), config);
    engines.push(tabA, tabB);
    dbNames.push(dbNameFor(appId, 'user1'));

    // B subscribes first and starts last: both halves of the race at once.
    const list = collect<Project[]>(tabB.collection<Project>('projects').live());
    await tabA.start();
    await tabA.collection<Project>('projects').create({ id: 'aaaaaaaaaaaaaaa', title: 'One' });
    await tick(20);
    expect(list.frames.at(-1)).toEqual([]);

    await tabB.start();
    await tick(30);
    expect(list.frames.at(-1)).toMatchObject([{ id: 'aaaaaaaaaaaaaaa', title: 'One' }]);

    // …and a later write in A still reaches B, cross-tab, with no extra wiring.
    await tabA.collection<Project>('projects').create({ id: 'bbbbbbbbbbbbbbb', title: 'Two' });
    await tick(30);
    expect(list.frames.at(-1)).toHaveLength(2);
    list.off();
  });
});
