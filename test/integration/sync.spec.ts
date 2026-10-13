/**
 * The sync engine against a REAL PocketBase (TESTPLAN §2.3, §3).
 *
 * These are the acceptance scenarios that can be run single-tab: the ordered
 * drain after a link drop, the 500-mutation gate (S5), an offline child of an
 * offline parent, delete propagation through tombstones (S2), the stale-cursor
 * resync (S3), echo suppression that terminates (S4), and a denied collection
 * that halts alone (§6.5).
 *
 * The link is dropped with the harness's severable proxy — sockets destroyed,
 * no clean close — because that is what a real link drop looks like to a client.
 */
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import Dexie from 'dexie';
import PocketBase from 'pocketbase';

import { hasPocketBase, startHarness, type PBHandle, type PBUser } from '../harness';
import { createPBOffline } from '../../src/index.js';
import { resolveConfig } from '../../src/config.js';
import { dbNameFor } from '../../src/brand.js';
import { openForOwner } from '../../src/db/open.js';
import { DEFAULT_RETENTION_MS } from '../../src/sync/tombstones.js';
import type { PBOffline, PBOfflineConfig, PBRecord } from '../../src/types.js';

const BOOT_TIMEOUT = 180_000;

interface Project extends PBRecord {
  title?: string;
  owner?: string;
}
interface Task extends PBRecord {
  title?: string;
  project?: string;
  owner?: string;
  done?: boolean;
}

let pbHandle: PBHandle;
let user: PBUser;
let appSeq = 0;

const engines: PBOffline[] = [];
const dbNames: string[] = [];

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/** Wait for a condition, or fail loudly rather than hang. */
async function until(what: string, fn: () => Promise<boolean> | boolean, timeoutMs = 30_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await fn()) return;
    if (Date.now() > deadline) throw new Error(`timed out waiting for: ${what}`);
    await sleep(50);
  }
}

async function client(): Promise<PocketBase> {
  const pb = new PocketBase(pbHandle.url);
  pb.autoCancellation(false);
  await pb.collection('users').authWithPassword(user.email, user.password);
  return pb;
}

interface Rig {
  engine: PBOffline;
  pb: PocketBase;
  appId: string;
  projects: ReturnType<PBOffline['collection']>;
  tasks: ReturnType<PBOffline['collection']>;
  config: PBOfflineConfig;
}

/** The engine only learns the link is down by TRYING; make it try. */
async function goOffline(r: Rig): Promise<void> {
  await pbHandle.sever();
  await r.engine.pull().catch(() => undefined);
  await until('the engine noticed the link is down', () => !isOnline(r));
}

function isOnline(r: Rig): boolean {
  let online = false;
  r.engine.online.subscribe((v) => (online = v))();
  return online;
}

function pendingNow(r: Rig): number {
  let n = -1;
  r.engine.pendingCount.subscribe((v) => (n = v))();
  return n;
}

async function drained(r: Rig, timeoutMs = 60_000): Promise<void> {
  await until(
    'the queue drained',
    async () => {
      if (pendingNow(r) === 0) return true;
      await r.engine.push();
      return pendingNow(r) === 0;
    },
    timeoutMs,
  );
}

async function rig(overrides?: Partial<PBOfflineConfig>): Promise<Rig> {
  const pb = await client();
  appSeq += 1;
  const appId = `it${appSeq}-${Date.now()}`;
  const config: PBOfflineConfig = {
    appId,
    collections: [{ name: 'projects' }, { name: 'tasks' }],
    writeCoalesceMs: 0,
    pullPageSize: 50,
    backoff: { min: 100, max: 500 },
    ...overrides,
  };
  const engine = createPBOffline(pb, config);
  engines.push(engine);
  dbNames.push(dbNameFor(appId, user.id));
  await engine.start();
  return {
    engine,
    pb,
    appId,
    config,
    projects: engine.collection<Project>('projects') as never,
    tasks: engine.collection<Task>('tasks') as never,
  };
}

describe.skipIf(!hasPocketBase())('engine against a real PocketBase', () => {
  beforeAll(async () => {
    pbHandle = await startHarness();
    user = await pbHandle.createUser();
  }, BOOT_TIMEOUT);

  afterEach(async () => {
    while (engines.length > 0) await engines.pop()?.destroy();
    while (dbNames.length > 0) await Dexie.delete(dbNames.pop()!);
    await pbHandle.restore();
  }, 60_000);

  afterAll(async () => {
    await pbHandle?.stop();
  }, 60_000);

  it('round-trips a create, an update and a delete', async () => {
    const r = await rig();
    const project = (await r.projects.create({ title: 'first' })) as Project;
    await until('the create reached the server', async () => {
      try {
        await r.pb.collection('projects').getOne(project.id);
        return true;
      } catch {
        return false;
      }
    });

    await r.projects.update(project.id, { title: 'renamed' });
    await until('the update reached the server', async () => {
      const remote = await r.pb.collection('projects').getOne(project.id);
      return remote['title'] === 'renamed';
    });

    // The `updated` the server minted is exactly what the mirror holds — this is
    // what echo suppression compares, so a serialisation difference would break it.
    const remote = await r.pb.collection('projects').getOne(project.id);
    const local = (await r.projects.get(project.id)) as Project;
    expect(local.updated).toBe(remote['updated']);

    await r.projects.delete(project.id);
    await until('the delete reached the server', async () => {
      try {
        await r.pb.collection('projects').getOne(project.id);
        return false;
      } catch {
        return true;
      }
    });
    expect(await r.projects.get(project.id)).toBeUndefined();
  }, 120_000);

  /**
   * SPEC §2.3 `sort.localFallback` against server-minted stamps.
   *
   * The unit tier fixes the semantics; this fixes the one thing it cannot: that
   * PocketBase's actual `created` serialisation interleaves with a client
   * millisecond clock. A newest-first list is the shape every consumer builds,
   * and an offline create belongs at the top of it, not at whichever end
   * `undefined` lands on.
   */
  it('orders unsynced records among synced ones by their local clock (SPEC §2.3)', async () => {
    const r = await rig();
    const synced: string[] = [];
    for (const title of ['server one', 'server two']) {
      synced.push(((await r.projects.create({ title, owner: user.id })) as Project).id);
    }
    await drained(r);
    await until('both creates came back stamped', async () =>
      (await Promise.all(synced.map((id) => r.projects.get(id)))).every(
        (rec) => typeof (rec as Project | undefined)?.created === 'string',
      ),
    );

    await goOffline(r);
    const offline: string[] = [];
    for (const title of ['offline one', 'offline two']) {
      offline.push(((await r.projects.create({ title, owner: user.id })) as Project).id);
      await sleep(5); // distinct clientUpdatedAt values
    }

    // No fallback: the unstamped rows sort as `undefined`, all together, at one
    // end — which is the friction, reproduced.
    const plain = (await r.projects.find({ sort: { field: 'created', dir: 'desc' } })) as Project[];
    expect(plain.slice(-2).map((p) => p.id).sort()).toEqual([...offline].sort());

    // With it: newest first means the two offline creates, newest of them first.
    const ordered = (await r.projects.find({
      sort: { field: 'created', dir: 'desc', localFallback: 'clientUpdatedAt' },
    })) as Project[];
    expect(ordered.map((p) => p.id)).toEqual([offline[1], offline[0], synced[1], synced[0]]);

    // And the order survives the drain, now that the server has stamped them.
    await pbHandle.restore();
    await drained(r);
    await until('the offline creates came back stamped', async () =>
      (await Promise.all(offline.map((id) => r.projects.get(id)))).every(
        (rec) => typeof (rec as Project | undefined)?.created === 'string',
      ),
    );
    const after = (await r.projects.find({
      sort: { field: 'created', dir: 'desc', localFallback: 'clientUpdatedAt' },
    })) as Project[];
    expect(after.map((p) => p.id)).toEqual([offline[1], offline[0], synced[1], synced[0]]);
  }, 120_000);

  it('S1 · writes made offline drain in seq order when the link returns', async () => {
    const r = await rig();
    const pushes: Array<{ op: string; id: string }> = [];
    r.engine.on('push', (e) => {
      if (e.ok) pushes.push({ op: e.op, id: e.id });
    });

    await goOffline(r);

    // 12 interleaved writes across two collections, parents before children.
    const parent = (await r.projects.create({ title: 'parent' })) as Project;
    const children: string[] = [];
    for (let i = 0; i < 5; i++) {
      const task = (await r.tasks.create({ title: `t${i}`, project: parent.id })) as Task;
      children.push(task.id);
    }
    await r.projects.update(parent.id, { title: 'parent renamed' });
    for (let i = 0; i < 5; i++) await r.tasks.update(children[i]!, { done: true });

    // `pendingCount` is a liveQuery store: it settles a tick behind the write.
    await until('all 12 writes are queued', () => pendingNow(r) === 12);
    // Nothing reached the server: it was not reachable.
    expect(await r.pb.collection('projects').getList(1, 50).catch(() => null)).toBeNull();

    await pbHandle.restore();
    await r.engine.sync();
    await drained(r);

    // The parent create preceded every child create.
    const parentAt = pushes.findIndex((p) => p.id === parent.id);
    expect(parentAt).toBe(0);
    for (const child of children) {
      expect(pushes.findIndex((p) => p.id === child)).toBeGreaterThan(parentAt);
    }
    // The final server state equals the final local state.
    const remoteParent = await r.pb.collection('projects').getOne(parent.id);
    expect(remoteParent['title']).toBe('parent renamed');
    const remoteTasks = await r.pb.collection('tasks').getFullList({ filter: `project = "${parent.id}"` });
    expect(remoteTasks).toHaveLength(5);
    expect(remoteTasks.every((t) => t['done'] === true)).toBe(true);
  }, 180_000);

  it('S5 · 500 queued mutations drain in strict order with no dead letters', async () => {
    const r = await rig();
    const ok: string[] = [];
    const failures: unknown[] = [];
    r.engine.on('push', (e) => {
      if (e.ok) ok.push(`${e.op}:${e.id}`);
    });
    r.engine.on('dead-letter', (e) => failures.push(e));

    // Records the offline batch will DELETE have to exist on the server first:
    // deleting a record whose create is still queued collapses to nothing and
    // never becomes a request at all (SPEC §4.3).
    const doomed: string[] = [];
    for (let i = 0; i < 50; i++) {
      const p = (await r.projects.create({ title: `doomed${i}` })) as Project;
      doomed.push(p.id);
    }
    await drained(r, 120_000);
    ok.length = 0;

    await goOffline(r);

    const expected: string[] = [];
    const projectIds: string[] = [];
    for (let i = 0; i < 100; i++) {
      const p = (await r.projects.create({ title: `p${i}` })) as Project;
      projectIds.push(p.id);
      expected.push(`create:${p.id}`);
    }
    const taskIds: string[] = [];
    for (let i = 0; i < 200; i++) {
      const t = (await r.tasks.create({ title: `t${i}`, project: projectIds[i % 100] })) as Task;
      taskIds.push(t.id);
      expected.push(`create:${t.id}`);
    }
    // Updates to records created in the same batch (coalescing is off).
    for (let i = 0; i < 100; i++) {
      await r.projects.update(projectIds[i]!, { title: `p${i}-v2` });
      expected.push(`update:${projectIds[i]!}`);
    }
    for (let i = 0; i < 50; i++) {
      await r.tasks.update(taskIds[i]!, { done: true });
      expected.push(`update:${taskIds[i]!}`);
    }
    for (const id of doomed) {
      await r.projects.delete(id);
      expected.push(`delete:${id}`);
    }
    expect(expected).toHaveLength(500);
    await until('all 500 mutations are queued', () => pendingNow(r) === 500);

    await pbHandle.restore();
    const started = Date.now();
    await drained(r, 240_000);

    expect(failures).toEqual([]);
    expect(ok).toEqual(expected); // strictly ascending seq, exactly one per entry
    // Scoped by id: this file shares one server across tests.
    const remoteProjects = await r.pb.collection('projects').getFullList();
    const byId = new Map(remoteProjects.map((p) => [p.id, p]));
    expect(projectIds.filter((id) => byId.has(id))).toHaveLength(100);
    expect(projectIds.every((id) => String(byId.get(id)?.['title']).endsWith('-v2'))).toBe(true);
    expect(doomed.some((id) => byId.has(id))).toBe(false);

    const remoteTasks = await r.pb.collection('tasks').getFullList();
    const taskById = new Set(remoteTasks.map((t) => t.id));
    expect(taskIds.filter((id) => taskById.has(id))).toHaveLength(200);
    // A declared budget, generous enough for a loaded machine.
    expect(Date.now() - started).toBeLessThan(240_000);
  }, 600_000);

  it('a child created offline may reference a parent created offline (SPEC §4.4)', async () => {
    const r = await rig();
    await goOffline(r);

    const parent = (await r.projects.create({ title: 'offline parent' })) as Project;
    const child = (await r.tasks.create({ title: 'offline child', project: parent.id })) as Task;
    // No fixup pass, no localId: the relation is already the final PB id.
    expect(child.project).toBe(parent.id);

    await pbHandle.restore();
    await r.engine.sync();
    await drained(r);

    const remote = await r.pb.collection('tasks').getOne(child.id);
    expect(remote['project']).toBe(parent.id);
    expect(remote['id']).toBe(child.id);
  }, 180_000);

  it('S2 · a delete made elsewhere reaches this client through the tombstone pull', async () => {
    // Realtime off: this is the half of S2 where the client was NOT connected
    // when the delete happened, so the tombstone is the only path there is. The
    // connected half — the same delete arriving over SSE — is asserted in
    // `realtime.spec.ts` (SPEC §9.6).
    const r = await rig({ realtime: { enabled: false } });
    const project = (await r.projects.create({ title: 'doomed' })) as Project;
    await r.engine.sync();
    await until('created remotely', async () => {
      try {
        await r.pb.collection('projects').getOne(project.id);
        return true;
      } catch {
        return false;
      }
    });

    // Another device deletes it.
    const other = await client();
    await other.collection('projects').delete(project.id);

    const pulls: Array<{ collection: string; deleted: number }> = [];
    r.engine.on('pull', (e) => pulls.push({ collection: e.collection, deleted: e.deleted }));

    await r.engine.sync();
    await until('the local row disappeared', async () => (await r.projects.get(project.id)) === undefined);

    const tombstonePull = pulls.find((p) => p.collection === 'deletions');
    expect(tombstonePull?.deleted).toBeGreaterThanOrEqual(1);
    // The record page could not have carried it: PocketBase hard-deletes.
    expect(pulls.find((p) => p.collection === 'projects')?.deleted).toBe(0);
  }, 180_000);

  it('S4 · echo never loops: a pushed write comes back suppressed', async () => {
    const r = await rig();
    const synced: string[] = [];
    r.engine.on('record-synced', (e) => synced.push(`${e.id}@${e.updated}`));

    const project = (await r.projects.create({ title: 'mine' })) as Project;
    await r.engine.sync();
    await drained(r);
    const afterPush = synced.length;
    const local = (await r.projects.get(project.id)) as Project;

    // Repeated pulls see this device's own write and drop it every time.
    for (let i = 0; i < 10; i++) {
      const before = await r.engine.pull(['projects']);
      expect(before.byCollection['projects']?.applied ?? 0).toBe(0);
    }
    expect(synced).toHaveLength(afterPush);
    expect(await r.projects.get(project.id)).toEqual(local);
    expect(pendingNow(r)).toBe(0);
  }, 180_000);

  it('S3 · a stale tombstone cursor forces a full resync that preserves the queue', async () => {
    const r = await rig();
    const kept = (await r.projects.create({ title: 'on the server' })) as Project;
    await r.engine.sync();
    await drained(r);

    // Poke the tombstone cursor back beyond the retention window, and leave an
    // unsent mutation in the queue.
    await goOffline(r);
    const unsent = (await r.projects.create({ title: 'never sent yet' })) as Project;

    const side = await openForOwner(resolveConfig(r.config), user.id);
    const row = await side.db.pbo.cursors.get('deletions');
    await side.db.pbo.cursors.put({
      collection: 'deletions',
      updated: row?.updated ?? null,
      id: row?.id ?? null,
      lastPullAt: Date.now() - DEFAULT_RETENTION_MS - 60_000,
    });
    side.db.close();

    const phases: string[] = [];
    r.engine.on('full-resync', (e) => phases.push(e.phase));

    await pbHandle.restore();
    await r.engine.sync();
    await until('the resync finished', () => phases.includes('done'), 60_000);

    expect(phases[0]).toBe('start');
    // The mirror was rebuilt from scratch…
    expect(await r.projects.get(kept.id)).toBeDefined();
    // …and the queue survived it, then drained.
    await drained(r);
    const remote = await r.pb.collection('projects').getOne(unsent.id);
    expect(remote['title']).toBe('never sent yet');
  }, 300_000);

  it('§6.5 · a denied collection halts alone while the other keeps syncing', async () => {
    const r = await rig();
    const codes: string[] = [];
    r.engine.on('error', (e) => codes.push(e.error.code));

    // Superuser-only listing: a non-superuser list is a 403, not an empty page.
    const admin = new PocketBase(pbHandle.url);
    admin.authStore.save(pbHandle.adminToken, null);
    await admin.collections.update('projects', { listRule: null });
    try {
      const task = (await r.tasks.create({ title: 'still syncing' })) as Task;
      await r.engine.sync();

      await until('the denial was recorded', () => codes.includes('pull/denied'), 30_000);
      let status = 'online';
      r.engine.status.subscribe((v) => (status = v))();
      expect(status).toBe('error');

      // The other collection went on syncing regardless.
      await until('the unaffected collection still drained', async () => {
        try {
          await r.pb.collection('tasks').getOne(task.id);
          return true;
        } catch {
          return false;
        }
      });
      const pulled = await r.engine.pull();
      expect(pulled.byCollection['tasks']).toBeDefined();
    } finally {
      await admin.collections.update('projects', { listRule: 'owner = @request.auth.id' });
    }
  }, 180_000);
});
