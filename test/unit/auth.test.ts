/** SPEC §14, §15 — the session binding, account switching, promotion, sign-out. */
import { afterEach, describe, expect, it, vi } from 'vitest';
import Dexie from 'dexie';
import { createPBOffline } from '../../src/index.js';
import { createSession, isAuthValid, ownerIdOf, ownerKeyOf } from '../../src/auth/session.js';
import { promoteAnonymousData } from '../../src/auth/promotion.js';
import { resolveConfig } from '../../src/config.js';
import { openForOwner } from '../../src/db/open.js';
import { BRAND, dbNameFor } from '../../src/brand.js';
import { asPB, createFakePB, type FakePB } from '../fakes/pb.js';
import { queue, tick, uniqueAppId } from './_helpers.js';
import type { PBOffline, PBOfflineConfig, PBRecord, PendingEntry, SyncState } from '../../src/types.js';

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

function makeEngine(
  pb: FakePB,
  overrides?: Partial<PBOfflineConfig>,
): { engine: PBOffline; appId: string; config: PBOfflineConfig } {
  const appId = uniqueAppId();
  const config = {
    appId,
    collections: [{ name: 'projects' }, { name: 'tasks' }],
    writeCoalesceMs: 0,
    backoff: { min: 5, max: 20 },
    ...overrides,
  } as PBOfflineConfig;
  const engine = createPBOffline(asPB(pb), config);
  engines.push(engine);
  dbNames.push(dbNameFor(appId, 'user1'), dbNameFor(appId, 'user2'), dbNameFor(appId, BRAND.anonOwner));
  return { engine, appId, config };
}

const stateOf = (engine: PBOffline): SyncState => {
  let s!: SyncState;
  engine.state.subscribe((v) => (s = v))();
  return s;
};

describe('createSession (SPEC §14.1)', () => {
  it('reports the owner identity and reacts to a change of identity only', () => {
    const pb = createFakePB({ ownerId: 'user1' });
    const session = createSession(asPB(pb));
    const changes: Array<[string | null, string | null]> = [];
    session.onChange((next, previous) => changes.push([next, previous]));

    expect(session.ownerId).toBe('user1');
    expect(session.ownerKey).toBe('user1');
    expect(session.authenticated).toBe(true);

    // A token refresh fires `onChange` in the real SDK too, and it is NOT an
    // owner change: acting on it would close and reopen the database each time
    // the token rotated.
    pb.setOwner('user1');
    expect(changes).toEqual([]);

    pb.setOwner('user2');
    pb.setOwner(null);
    expect(changes).toEqual([
      ['user2', 'user1'],
      [null, 'user2'],
    ]);
    expect(session.ownerKey).toBe(BRAND.anonOwner);

    session.stop();
    pb.setOwner('user3');
    expect(changes).toHaveLength(2);
  });

  it('maps identities to database keys and reads token validity', () => {
    const pb = createFakePB({ ownerId: null });
    expect(ownerIdOf(asPB(pb))).toBeNull();
    expect(ownerKeyOf(null)).toBe(BRAND.anonOwner);
    expect(ownerKeyOf('user7')).toBe('user7');
    expect(isAuthValid(asPB(pb))).toBe(false);
    pb.setOwner('user7');
    expect(isAuthValid(asPB(pb))).toBe(true);
  });
});

describe('owner change (SPEC §14.2)', () => {
  it('runs the steps in order and never wipes the outgoing database', async () => {
    const pb = createFakePB({ ownerId: 'user1' });
    const { engine, appId } = makeEngine(pb);
    const log: string[] = [];
    pb.onUnsubscribe = () => {
      if (log.at(-1) !== 'sse-teardown') log.push('sse-teardown');
    };
    engine.on('auth-change', () => log.push('auth-change'));

    await engine.start();
    await tick(20);
    expect(pb.subscriptions.length).toBeGreaterThan(0); // the drainer subscribed

    const wipe = vi.spyOn(Dexie, 'delete');
    // Dexie's own lifecycle, wrapped rather than mocked: the steps under test are
    // "close THEN open", and the names are what tell the two databases apart.
    const realClose = Dexie.prototype.close;
    const realOpen = Dexie.prototype.open;
    Dexie.prototype.close = function (this: Dexie, ...args: never[]) {
      log.push(`close:${this.name}`);
      return realClose.apply(this, args);
    } as typeof Dexie.prototype.close;
    Dexie.prototype.open = function (this: Dexie, ...args: never[]) {
      log.push(`open:${this.name}`);
      return realOpen.apply(this, args);
    } as typeof Dexie.prototype.open;

    try {
      pb.setOwner('user2');
      await tick(60);
    } finally {
      Dexie.prototype.close = realClose;
      Dexie.prototype.open = realOpen;
    }

    const nameA = dbNameFor(appId, 'user1');
    const nameB = dbNameFor(appId, 'user2');
    // SPEC §14.2's order: stop the drain, tear down SSE, close, open, announce.
    expect(log.indexOf('sse-teardown')).toBeGreaterThanOrEqual(0);
    expect(log.indexOf('sse-teardown')).toBeLessThan(log.indexOf(`close:${nameA}`));
    expect(log.indexOf(`close:${nameA}`)).toBeLessThan(log.indexOf(`open:${nameB}`));
    expect(log.indexOf(`open:${nameB}`)).toBeLessThan(log.indexOf('auth-change'));

    // Both databases exist. Switching accounts is not signing out.
    expect(await Dexie.exists(nameA)).toBe(true);
    expect(await Dexie.exists(nameB)).toBe(true);
    expect(wipe).not.toHaveBeenCalled();
    expect(stateOf(engine).ownerId).toBe('user2');
  });

  /**
   * REGRESSION (review finding 9) — §14.2's step order is normative, and
   * `auth-change` comes BEFORE the reconnect sequence.
   *
   * The event was emitted after `attach()`, which ends in `runner.start()`, which
   * pings, which runs the whole sequence. So an application's handler ran with
   * the new owner's push, pull and database writes already in flight, and
   * anything gated on the event — `promoteAnonymousData`, seeding the new owner,
   * deciding whether to sync at all — raced the engine.
   */
  it('emits auth-change BEFORE the new owner’s reconnect makes any request', async () => {
    const pb = createFakePB({ ownerId: 'user1' });
    const { engine } = makeEngine(pb);
    await engine.start();
    await tick(30);

    // The reconnect's own acts, as opposed to the OUTGOING owner's teardown
    // (which §14.2 does place before the event).
    const reconnectRequests = (): number =>
      pb.requests.filter((r) => r.method === 'create' || r.method === 'getList' || r.method === 'subscribe').length;

    let requestsAtEvent = -1;
    let subscriptionsAtEvent = -1;
    let ownerAtEvent: string | null | undefined;
    engine.on('auth-change', (e) => {
      requestsAtEvent = reconnectRequests();
      subscriptionsAtEvent = pb.subscriptions.length;
      ownerAtEvent = e.ownerId;
    });

    pb.requests.length = 0;
    pb.setOwner('user2');
    await tick(60);

    expect(ownerAtEvent).toBe('user2');
    // Not one push, pull or subscribe for the new owner had happened yet…
    expect(requestsAtEvent).toBe(0);
    expect(subscriptionsAtEvent).toBe(0);
    // …but the handler could already read the new owner's mirror, which is the
    // other half of §14.2: the database is open by then.
    expect(stateOf(engine).ownerId).toBe('user2');
    // And the reconnect did happen afterwards.
    expect(reconnectRequests()).toBeGreaterThan(0);
  });

  it('A → B → A restores A’s mirror and its unsent queue intact', async () => {
    const pb = createFakePB({ ownerId: 'user1', offline: true });
    const { engine } = makeEngine(pb);
    await engine.start();

    const a = await engine.collection<Project>('projects').create({ title: 'A’s work' });
    await tick(20);
    expect(stateOf(engine).pendingCount).toBe(1);

    pb.setOwner('user2');
    await tick(40);
    expect(await engine.collection<Project>('projects').get(a.id)).toBeUndefined();
    const b = await engine.collection<Project>('projects').create({ title: 'B’s work' });
    await tick(20);

    pb.setOwner('user1');
    await tick(40);
    expect((await engine.collection<Project>('projects').get(a.id))?.title).toBe('A’s work');
    expect(await engine.collection<Project>('projects').get(b.id)).toBeUndefined();
    expect(stateOf(engine).pendingCount).toBe(1); // A's create is still queued
  });

  it('signing out to anonymous keeps the owner’s data and opens the anon database', async () => {
    const pb = createFakePB({ ownerId: 'user1', offline: true });
    const { engine, appId } = makeEngine(pb);
    await engine.start();
    const kept = await engine.collection<Project>('projects').create({ title: 'still mine' });
    await tick(20);

    pb.setOwner(null);
    await tick(40);

    expect(stateOf(engine).ownerId).toBeNull();
    expect(await Dexie.exists(dbNameFor(appId, 'user1'))).toBe(true);
    expect(await Dexie.exists(dbNameFor(appId, BRAND.anonOwner))).toBe(true);
    expect(await engine.collection<Project>('projects').get(kept.id)).toBeUndefined();
  });

  it('opens nothing on a switch when anonymous storage is disabled', async () => {
    const pb = createFakePB({ ownerId: 'user1' });
    const { engine, appId } = makeEngine(pb, { anonymousMode: 'disabled' });
    await engine.start();
    pb.setOwner(null);
    await tick(40);

    expect(await Dexie.exists(dbNameFor(appId, BRAND.anonOwner))).toBe(false);
    const projects = engine.collection<Project>('projects');
    expect(await projects.getAll()).toEqual([]);
    await expect(projects.create({ title: 'x' })).rejects.toMatchObject({ code: 'auth/unauthenticated' });
  });
});

describe('promotion (SPEC §14.4)', () => {
  /** Seed an anonymous database the way an anonymous session would have. */
  async function seedAnon(config: PBOfflineConfig, rows: { projects: number; tasks: number }): Promise<string[]> {
    const cfg = resolveConfig(config);
    const handle = await openForOwner(cfg, BRAND.anonOwner);
    const ids: string[] = [];
    let n = 0;
    const put = async (collection: string, count: number): Promise<void> => {
      for (let i = 0; i < count; i++) {
        const id = `anon${collection.slice(0, 1)}${String(++n).padStart(10, '0')}`;
        ids.push(id);
        await handle.db.table(collection).put({ id, title: `${collection}-${i}` });
      }
    };
    await put('projects', rows.projects);
    await put('tasks', rows.tasks);
    handle.db.close();
    return ids;
  }

  it('is never automatic: signing in promotes nothing on its own', async () => {
    const pb = createFakePB({ ownerId: null, offline: true });
    const { engine, appId } = makeEngine(pb);
    await engine.start();
    await engine.collection<Project>('projects').create({ title: 'anonymous work' });
    await tick(20);

    pb.setOwner('user1');
    await tick(40);
    expect(await engine.collection<Project>('projects').getAll()).toEqual([]);
    expect(await Dexie.exists(dbNameFor(appId, BRAND.anonOwner))).toBe(true);
  });

  it('copies in configured collection order, stamps the owner, and enqueues creates', async () => {
    const pb = createFakePB({ ownerId: 'user1', offline: true });
    const { engine, config, appId } = makeEngine(pb);
    const ids = await seedAnon(config, { projects: 2, tasks: 3 });
    await engine.start();

    const result = await engine.promoteAnonymousData();
    expect(result).toEqual({ promoted: 5, byCollection: { projects: 2, tasks: 3 }, strategy: 'copy' });

    const projects = await engine.collection<Project>('projects').getAll();
    expect(projects.map((p) => p.id).sort()).toEqual(ids.slice(0, 2).sort());
    expect(projects.every((p) => p['owner'] === 'user1')).toBe(true);

    const handle = await openForOwner(resolveConfig(config), 'user1');
    const entries = await queue(handle.db);
    handle.db.close();
    expect(entries).toHaveLength(5);
    expect(entries.every((e: PendingEntry) => e.op === 'create')).toBe(true);
    // Parents precede children, because promotion follows the configured order.
    expect(entries.map((e) => e.collection)).toEqual(['projects', 'projects', 'tasks', 'tasks', 'tasks']);
    expect(entries[0]!.payload).toMatchObject({ id: ids[0], owner: 'user1' });

    // 'copy' leaves the anonymous database intact.
    expect(await Dexie.exists(dbNameFor(appId, BRAND.anonOwner))).toBe(true);
  });

  it("'move' deletes the anonymous database, but only after a successful copy", async () => {
    const pb = createFakePB({ ownerId: 'user1', offline: true });
    const { engine, config, appId } = makeEngine(pb);
    await seedAnon(config, { projects: 1, tasks: 1 });
    await engine.start();

    const result = await engine.promoteAnonymousData({ strategy: 'move' });
    expect(result).toMatchObject({ promoted: 2, strategy: 'move' });
    expect(await Dexie.exists(dbNameFor(appId, BRAND.anonOwner))).toBe(false);
    expect(await engine.collection<Project>('projects').count()).toBe(1);
  });

  it('is a no-op when the anonymous database is absent or empty — including under move', async () => {
    const pb = createFakePB({ ownerId: 'user1', offline: true });
    const { engine, config, appId } = makeEngine(pb);
    await engine.start();

    expect(await engine.promoteAnonymousData()).toEqual({ promoted: 0, byCollection: {}, strategy: 'copy' });

    await seedAnon(config, { projects: 0, tasks: 0 }); // exists, but empty
    expect(await engine.promoteAnonymousData({ strategy: 'move' })).toEqual({
      promoted: 0,
      byCollection: {},
      strategy: 'move',
    });
    // A no-op is a no-op: an empty move deletes nothing.
    expect(await Dexie.exists(dbNameFor(appId, BRAND.anonOwner))).toBe(true);
  });

  it('resolves a promoted id the server already holds through SPEC §5.7', async () => {
    const pb = createFakePB({ ownerId: 'user1' });
    const { engine, config } = makeEngine(pb);
    const ids = await seedAnon(config, { projects: 1, tasks: 0 });
    // That id already exists on the server, owned by someone who created it on
    // another device: the create comes back as a taken id.
    pb.seed('projects', [{ id: ids[0]!, title: 'server copy' }]);

    const dead: unknown[] = [];
    engine.on('dead-letter', (e) => dead.push(e));
    await engine.start();
    await engine.promoteAnonymousData();
    await engine.push();
    await tick(20);

    expect(dead).toEqual([]);
    expect(pb.get('projects', ids[0]!)).toMatchObject({ title: 'projects-0' });
    const attempts = pb
      .recordRequests()
      .filter((r) => r.collection === 'projects' && ['create', 'update', 'delete'].includes(r.method));
    // §5.7: the create becomes an update in place and is retried exactly once.
    expect(attempts.map((r) => r.method)).toEqual(['create', 'update']);

    // And the conversion DROPS the owner (§2.6). The prescribed updateRule
    // refuses an update body that so much as mentions that field — it is the
    // clause that stops a record being handed to another account — so a
    // converted create that kept its owner would 400 against a conformant
    // server and dead-letter the promotion. The record's owner does not change:
    // it is already whoever the server says, and the client can only reach
    // records it owns in the first place.
    const converted = attempts.find((r) => r.method === 'update');
    expect(converted!.args[1]).not.toHaveProperty('owner');
    expect(converted!.args[1]).toMatchObject({ title: 'projects-0' });
  });

  it('requires a session and is callable directly against a config', async () => {
    const pb = createFakePB({ ownerId: null });
    const { engine, config } = makeEngine(pb);
    await engine.start();
    await expect(engine.promoteAnonymousData()).rejects.toMatchObject({ code: 'auth/unauthenticated' });

    await seedAnon(config, { projects: 1, tasks: 0 });
    const direct = await promoteAnonymousData(resolveConfig(config), 'user9');
    expect(direct.promoted).toBe(1);
    dbNames.push(dbNameFor(config.appId, 'user9'));
  });
});

describe('signOut (SPEC §15)', () => {
  it('releases SSE and the drainer role before wiping', async () => {
    const pb = createFakePB({ ownerId: 'user1' });
    const { engine, appId } = makeEngine(pb);
    await engine.start();
    await tick(20);
    expect(pb.subscriptions.length).toBeGreaterThan(0);

    const result = await engine.signOut();
    expect(result).toEqual({ drained: true, wiped: true, pending: 0 });
    expect(pb.subscriptions).toEqual([]);
    expect(await Dexie.exists(dbNameFor(appId, 'user1'))).toBe(false);
    expect(stateOf(engine).isDrainer).toBe(false);
  });

  it('returns inside the flush timeout when the queue cannot drain', async () => {
    const pb = createFakePB({ ownerId: 'user1', offline: true });
    const { engine } = makeEngine(pb);
    await engine.start();
    await engine.collection<Project>('projects').create({ title: 'unsent' });

    const started = Date.now();
    const result = await engine.signOut({ flushTimeoutMs: 50 });
    expect(Date.now() - started).toBeLessThan(5_000);
    expect(result).toEqual({ drained: false, wiped: false, pending: 1 });
  });

  it('never clears the auth store, on either side of the call', async () => {
    const pb = createFakePB({ ownerId: 'user1' });
    const { engine } = makeEngine(pb);
    const clear = vi.spyOn(pb.authStore, 'clear');
    await engine.start();
    await engine.signOut();
    expect(clear).not.toHaveBeenCalled();
    expect(pb.authStore.record).toEqual({ id: 'user1' });
    // And the application clearing it afterwards is safe.
    pb.authStore.clear();
    await tick(30);
    expect(stateOf(engine).ownerId).toBeNull();
  });
});
