/**
 * Shared unit-tier scaffolding: a resolved config, an open database and a set of
 * collection deps, with every database name unique per test so `fake-indexeddb`
 * state cannot leak between them.
 */
import Dexie from 'dexie';
import { resolveConfig } from '../../src/config.js';
import { openForOwner, type OpenHandle } from '../../src/db/open.js';
import type { CollectionDeps } from '../../src/collections/define.js';
import type { PBODexie } from '../../src/db/schema.js';
import type { PushDeps } from '../../src/sync/push.js';
import type { PullDeps } from '../../src/sync/pull.js';
import { asPB, type FakePB } from '../fakes/pb.js';
import type {
  PBOErrorShape,
  PBOEvents,
  PBOfflineConfig,
  PendingEntry,
  ResolvedConfig,
} from '../../src/types.js';

let n = 0;
export function uniqueAppId(prefix = 'app'): string {
  n += 1;
  return `${prefix}${n}-${Math.floor(Math.random() * 1e6)}`;
}

export function testConfig(overrides?: Partial<PBOfflineConfig>): ResolvedConfig {
  return resolveConfig({
    appId: uniqueAppId(),
    collections: [{ name: 'projects' }, { name: 'tasks', indexes: ['project'] }],
    ...overrides,
  } as PBOfflineConfig);
}

export interface TestDb {
  cfg: ResolvedConfig;
  handle: OpenHandle;
  deps(collection?: string, over?: Partial<CollectionDeps>): CollectionDeps;
  nudges: number;
  destroy(): Promise<void>;
}

export async function openTestDb(cfg: ResolvedConfig = testConfig(), ownerKey = 'user1'): Promise<TestDb> {
  const handle = await openForOwner(cfg, ownerKey);

  const state: TestDb = {
    cfg,
    handle,
    nudges: 0,
    deps(collection = 'projects', over?: Partial<CollectionDeps>) {
      const collectionCfg = cfg.collections.find((c) => c.name === collection)!;
      return {
        getDb: () => handle.db,
        cfg,
        collectionCfg,
        getOwnerId: () => ownerKey,
        nudge: () => {
          state.nudges += 1;
        },
        ...over,
      };
    },
    async destroy() {
      handle.db.close();
      await Dexie.delete(handle.dbName);
    },
  };
  return state;
}

/** Let Dexie's liveQuery observers flush. */
export async function tick(ms = 5): Promise<void> {
  await new Promise((r) => setTimeout(r, ms));
}

// ── sync-tier scaffolding (SPEC §5–§11) ──────────────────────────────────────

export interface RecordedEvent {
  type: keyof PBOEvents;
  payload: unknown;
}

export interface SyncHarness {
  db: PBODexie;
  cfg: ResolvedConfig;
  pb: FakePB;
  events: RecordedEvent[];
  halted: Map<string, PBOErrorShape>;
  controller: AbortController;
  online: boolean;
  emit: <E extends keyof PBOEvents>(event: E, payload: PBOEvents[E]) => void;
  of(type: keyof PBOEvents): unknown[];
  push(over?: Partial<PushDeps>): PushDeps;
  pull(over?: Partial<PullDeps>): PullDeps;
  /** Append a `_pending` entry the way crud.ts would, without a mirror write. */
  enqueue(entry: Partial<PendingEntry> & Pick<PendingEntry, 'collection' | 'recordId' | 'op'>): Promise<number>;
}

/** Build the dependency bundles push/pull take, over an open test database. */
export function syncHarness(t: TestDb, pb: FakePB, ownerId: string | null = 'user1'): SyncHarness {
  const events: RecordedEvent[] = [];
  const harness: SyncHarness = {
    db: t.handle.db,
    cfg: t.cfg,
    pb,
    events,
    halted: new Map<string, PBOErrorShape>(),
    controller: new AbortController(),
    online: true,
    emit: (type, payload) => {
      events.push({ type, payload });
    },
    of: (type) => events.filter((e) => e.type === type).map((e) => e.payload),
    push: (over?: Partial<PushDeps>) => ({
      pb: asPB(pb),
      db: t.handle.db,
      cfg: t.cfg,
      isDrainer: () => true,
      isOnline: () => harness.online,
      abort: harness.controller.signal,
      emit: harness.emit,
      observeRequest: () => {},
      isAuthValid: () => true,
      ...over,
    }),
    pull: (over?: Partial<PullDeps>) => ({
      pb: asPB(pb),
      db: t.handle.db,
      cfg: t.cfg,
      getOwnerId: () => ownerId,
      abort: harness.controller.signal,
      emit: harness.emit,
      halted: harness.halted,
      isOnline: () => harness.online,
      observeRequest: () => {},
      ...over,
    }),
    async enqueue(entry) {
      return (await t.handle.db.pbo.pending.add({
        payload: null,
        clientUpdatedAt: Date.now(),
        state: 'queued',
        attempts: 0,
        nextAttemptAt: 0,
        lastError: null,
        ...entry,
      } as PendingEntry)) as number;
    },
  };
  return harness;
}

/** Every `_pending` entry, in `seq` order. */
export async function queue(db: PBODexie): Promise<PendingEntry[]> {
  return db.pbo.pending.orderBy('seq').toArray();
}
