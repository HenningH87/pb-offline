/**
 * Realtime, auth switching, promotion and single-drainer election against a REAL
 * PocketBase (TESTPLAN §2.3, §3 — S2's connected half, S7, S8, S10, and as much
 * of S6 as one process can carry).
 *
 * The SSE half is what the unit tier cannot prove: that PocketBase's own event
 * payloads reach the same apply path a pulled record takes, and that the cursor
 * is left alone while they do.
 */
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import Dexie from 'dexie';
import PocketBase from 'pocketbase';

import { hasPocketBase, startHarness, type PBHandle, type PBUser } from '../harness';
import { createPBOffline } from '../../src/index.js';
import { BRAND, dbNameFor } from '../../src/brand.js';
import { resolveConfig } from '../../src/config.js';
import { openForOwner } from '../../src/db/open.js';
import { readCursor } from '../../src/sync/pull.js';
import type { PBOffline, PBOfflineConfig, PBRecord, SyncState } from '../../src/types.js';

const BOOT_TIMEOUT = 180_000;

interface Project extends PBRecord {
  title?: string;
  owner?: string;
}

let pbHandle: PBHandle;
let userA: PBUser;
let userB: PBUser;
let appSeq = 0;

const engines: PBOffline[] = [];
const dbNames: string[] = [];

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

async function until(what: string, fn: () => Promise<boolean> | boolean, timeoutMs = 30_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await fn()) return;
    if (Date.now() > deadline) throw new Error(`timed out waiting for: ${what}`);
    await sleep(50);
  }
}

const stateOf = (engine: PBOffline): SyncState => {
  let s!: SyncState;
  engine.state.subscribe((v) => (s = v))();
  return s;
};

/**
 * A second, independent SDK client — "another device" in every scenario below.
 *
 * `direct` puts it on PocketBase's own port rather than the severable proxy, so
 * it keeps working while the engine's link is down (S8).
 */
async function client(user: PBUser, direct = false): Promise<PocketBase> {
  const pb = new PocketBase(direct ? pbHandle.directUrl : pbHandle.url);
  pb.autoCancellation(false);
  await pb.collection('users').authWithPassword(user.email, user.password);
  return pb;
}

interface Rig {
  engine: PBOffline;
  pb: PocketBase;
  appId: string;
  config: PBOfflineConfig;
  projects: ReturnType<PBOffline['collection']>;
}

async function rig(opts?: { user?: PBUser; overrides?: Partial<PBOfflineConfig>; appId?: string; start?: boolean }): Promise<Rig> {
  const user = opts?.user;
  const pb = new PocketBase(pbHandle.url);
  pb.autoCancellation(false);
  if (user) await pb.collection('users').authWithPassword(user.email, user.password);

  appSeq += 1;
  const appId = opts?.appId ?? `rt${appSeq}-${Date.now()}`;
  const config: PBOfflineConfig = {
    appId,
    collections: [{ name: 'projects' }, { name: 'tasks' }],
    writeCoalesceMs: 0,
    pullPageSize: 50,
    backoff: { min: 100, max: 500 },
    ...opts?.overrides,
  };
  const engine = createPBOffline(pb, config);
  engines.push(engine);
  for (const key of [userA.id, userB.id, BRAND.anonOwner]) dbNames.push(dbNameFor(appId, key));
  if (opts?.start !== false) await engine.start();
  return { engine, pb, appId, config, projects: engine.collection<Project>('projects') as never };
}

describe.skipIf(!hasPocketBase())('realtime, auth and promotion against a real PocketBase', () => {
  beforeAll(async () => {
    pbHandle = await startHarness();
    userA = await pbHandle.createUser();
    userB = await pbHandle.createUser();
  }, BOOT_TIMEOUT);

  afterEach(async () => {
    while (engines.length > 0) await engines.pop()?.destroy();
    while (dbNames.length > 0) await Dexie.delete(dbNames.pop()!);
    await pbHandle.restore();
  }, 60_000);

  afterAll(async () => {
    await pbHandle?.stop();
  }, 60_000);

  it('SPEC §9 · a record created elsewhere arrives over SSE, and the cursor does not move', async () => {
    const r = await rig({ user: userA });
    await r.engine.sync();
    await until('the subscriptions are live', () => r.pb.realtime.isConnected);

    const side = await openForOwner(resolveConfig(r.config), userA.id);
    const cursorBefore = await readCursor(side.db, 'projects');

    const other = await client(userA);
    const created = await other.collection('projects').create({ title: 'from another device', owner: userA.id });

    // No pull is made here: the ONLY way this can appear is the event.
    await until('the event was applied', async () => (await r.projects.get(created.id)) !== undefined, 20_000);
    expect((await r.projects.get(created.id)) as Project).toMatchObject({ title: 'from another device' });

    // SPEC §9.4, the most consequential rule in the section.
    expect(await readCursor(side.db, 'projects')).toEqual(cursorBefore);

    // …and a cursor pull afterwards DOES advance it, over the same record.
    await r.engine.pull(['projects']);
    const cursorAfter = await readCursor(side.db, 'projects');
    side.db.close();
    expect(cursorAfter).not.toEqual(cursorBefore);
    expect(cursorAfter.id).toBe(created.id);
  }, 120_000);

  it('SPEC §9.6 · S2 connected · a delete made elsewhere arrives over SSE and is applied', async () => {
    const r = await rig({ user: userA });
    const doomed = (await r.projects.create({ title: 'doomed' })) as Project;
    await r.engine.sync();
    await until('it reached the server', async () => {
      try {
        await r.pb.collection('projects').getOne(doomed.id);
        return true;
      } catch {
        return false;
      }
    });
    await until('the subscriptions are live', () => r.pb.realtime.isConnected);

    const other = await client(userA);
    await other.collection('projects').delete(doomed.id);

    await until('the local row went with it', async () => (await r.projects.get(doomed.id)) === undefined, 20_000);
  }, 120_000);

  it('S8 · a mutation missed while SSE was down is backfilled by the cursor pull', async () => {
    const r = await rig({ user: userA });
    const seen = (await r.projects.create({ title: 'before the gap' })) as Project;
    await r.engine.sync();
    await until('the subscriptions are live', () => r.pb.realtime.isConnected);

    // A second client on a DIRECT connection, so the severed proxy takes down
    // this engine's link only.
    const other = await client(userA, true);

    await pbHandle.sever();
    await r.engine.pull().catch(() => undefined);
    await until('the engine noticed', () => !stateOf(r.engine).online);

    const missed = await other.collection('projects').create({ title: 'during the gap', owner: userA.id });
    await other.collection('projects').update(seen.id, { title: 'changed during the gap' });

    await pbHandle.restore();
    await r.engine.sync();

    // Neither of those could have arrived over SSE: the socket was gone.
    await until('the missed create was backfilled', async () => (await r.projects.get(missed.id)) !== undefined, 30_000);
    await until(
      'the missed update was backfilled at its final value',
      async () => ((await r.projects.get(seen.id)) as Project | undefined)?.title === 'changed during the gap',
      30_000,
    );

    // Applied exactly once at the final value, and the cursor is contiguous with
    // the server: a second pass finds nothing left to apply.
    const again = await r.engine.pull(['projects']);
    expect(again.byCollection['projects']?.applied ?? 0).toBe(0);
  }, 180_000);

  it('S7 · switching A → B → A preserves both mirrors and wipes neither', async () => {
    const r = await rig({ user: userA });
    const aRecord = (await r.projects.create({ title: 'A owns this' })) as Project;
    await r.engine.sync();
    await until('A’s record reached the server', async () => {
      try {
        await r.pb.collection('projects').getOne(aRecord.id);
        return true;
      } catch {
        return false;
      }
    });

    const changes: Array<string | null> = [];
    r.engine.on('auth-change', (e) => changes.push(e.ownerId));

    await r.pb.collection('users').authWithPassword(userB.email, userB.password);
    await until('the engine switched to B', () => stateOf(r.engine).ownerId === userB.id, 20_000);
    // B sees B's data, and none of A's.
    expect(await r.projects.get(aRecord.id)).toBeUndefined();
    const bRecord = (await r.projects.create({ title: 'B owns this' })) as Project;
    await r.engine.sync();

    await r.pb.collection('users').authWithPassword(userA.email, userA.password);
    await until('the engine switched back to A', () => stateOf(r.engine).ownerId === userA.id, 20_000);
    await until('A’s mirror came back', async () => (await r.projects.get(aRecord.id)) !== undefined, 20_000);
    expect(await r.projects.get(bRecord.id)).toBeUndefined();

    // Neither database was wiped: switching accounts is not signing out.
    expect(await Dexie.exists(dbNameFor(r.appId, userA.id))).toBe(true);
    expect(await Dexie.exists(dbNameFor(r.appId, userB.id))).toBe(true);
    expect(changes).toEqual([userB.id, userA.id]);
  }, 180_000);

  it('S10 · promoted anonymous rows land on the server owned by the new user', async () => {
    // An anonymous session first: no auth on the client, everything local.
    const r = await rig({ overrides: { anonymousMode: 'local' } });
    expect(stateOf(r.engine).ownerId).toBeNull();
    const one = (await r.projects.create({ title: 'made before signing in' })) as Project;
    const two = (await r.projects.create({ title: 'and this one' })) as Project;
    // Nothing is pushed with no session (SPEC §14.3).
    expect(stateOf(r.engine).status).toBe('offline');

    await r.pb.collection('users').authWithPassword(userA.email, userA.password);
    await until('the engine switched to A', () => stateOf(r.engine).ownerId === userA.id, 20_000);
    // Explicit only: signing in promoted nothing.
    expect(await r.projects.get(one.id)).toBeUndefined();

    const result = await r.engine.promoteAnonymousData({ strategy: 'move' });
    expect(result).toMatchObject({ promoted: 2, strategy: 'move' });

    await until(
      'both promoted records reached the server',
      async () => {
        await r.engine.push();
        const remote = await r.pb.collection('projects').getFullList({ filter: `owner = "${userA.id}"` });
        const ids = new Set(remote.map((x) => x.id));
        return ids.has(one.id) && ids.has(two.id);
      },
      60_000,
    );

    const promoted = await r.pb.collection('projects').getOne(one.id);
    expect(promoted['title']).toBe('made before signing in');
    expect(promoted['owner']).toBe(userA.id); // ids preserved, owner stamped
    expect(await Dexie.exists(dbNameFor(r.appId, BRAND.anonOwner))).toBe(false); // 'move'
  }, 180_000);

  it('S6 (single process) · two engines on one database elect one drainer', async () => {
    const appId = `mt${Date.now()}`;
    const a = await rig({ user: userA, appId });
    const b = await rig({ user: userA, appId });
    await until('a drainer emerged', () => stateOf(a.engine).isDrainer || stateOf(b.engine).isDrainer);

    const drainerFirst = stateOf(a.engine).isDrainer;
    expect(stateOf(a.engine).isDrainer).not.toBe(stateOf(b.engine).isDrainer);
    const follower = drainerFirst ? b : a;
    const drainer = drainerFirst ? a : b;

    // The follower writes; the drainer is what puts it on the wire.
    const written = (await follower.projects.create({ title: 'written by the follower' })) as Project;
    await until(
      'the drainer pushed it',
      async () => {
        try {
          await drainer.pb.collection('projects').getOne(written.id);
          return true;
        } catch {
          return false;
        }
      },
      60_000,
    );
    // Only one SSE connection exists for the pair.
    expect(follower.pb.realtime.isConnected).toBe(false);
  }, 180_000);
});
