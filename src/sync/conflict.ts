/**
 * Applying a remote record — LWW against a SERVER-stamped clock (SPEC §10).
 *
 * PocketBase's `updated` is an autodate: the server stamps it, so between a local
 * edit and a successful push the client has no comparable value of its own. The
 * resolution is to never compare a client value with a server value:
 *
 *   - only server-minted `updated` strings are ever ordered against each other;
 *   - the client's unpushed intent is protected by the QUEUE, not by a timestamp.
 *
 * Consequence, stated honestly: this is last-write-TO-REACH-THE-SERVER-wins, not
 * last-edit-in-wall-time-wins. Clock skew on any device cannot promote or demote
 * a write.
 *
 * The module is split into a PLAN phase and a COMMIT phase. That is not
 * decoration: `onConflict` is application code and may return a promise, and
 * awaiting a foreign promise inside a Dexie transaction commits it early. So the
 * reads and the hook run outside a transaction, and the writes — a whole pull
 * page of them plus the cursor advance — run inside one (SPEC §6.3).
 */
import type { Table } from 'dexie';
import type {
  ConflictResolver,
  PBOEvents,
  PBRecord,
  PendingEntry,
  ResolvedConfig,
} from '../types.js';
import type { PBODexie } from '../db/schema.js';
import { pendingFor, readMeta, refreshState, writeMeta } from '../db/meta.js';
import { compareTimestamps } from '../util/time.js';
import { isEchoOf } from './echo.js';

export type ApplyOutcome =
  | 'echo-dropped'
  | 'inserted'
  | 'applied'
  | 'kept-local'
  | 'no-op'
  | 'merged';

export type PlannedEvent =
  | { type: 'conflict'; payload: PBOEvents['conflict'] }
  | { type: 'record-synced'; payload: PBOEvents['record-synced'] };

export interface ApplyPlan {
  collection: string;
  id: string;
  outcome: ApplyOutcome;
  /** The row to write into the mirror, when the plan writes one. */
  write?: PBRecord;
  /** `_meta.serverUpdated` to record. `undefined` ⇒ leave it alone. */
  serverUpdated?: string | null;
  /** Clear `_meta.lastPushedUpdated` (SPEC §7.4). */
  clearEcho?: boolean;
  /** `_pending.seq` values the hook's 'remote' verdict discards (SPEC §10.5). */
  dropPending?: number[];
  /** A queued payload the hook's merged record replaces (SPEC §10.5). */
  rewritePayload?: { seq: number; payload: Record<string, unknown> };
  /** Emitted by the caller AFTER the transaction commits. */
  events: PlannedEvent[];
}

/** Fields the client never authors, and never copies out of a hook's answer. */
function fieldsOf(record: PBRecord): Record<string, unknown> {
  const out: Record<string, unknown> = { ...record };
  delete out['id'];
  delete out['created'];
  delete out['updated'];
  return out;
}

function tableOf(db: PBODexie, collection: string): Table<PBRecord, string> {
  return db.table(collection) as Table<PBRecord, string>;
}

/** The local row as the user sees it: the mirror plus everything still queued. */
function localView(local: PBRecord | undefined, id: string, pending: PendingEntry[]): PBRecord {
  const base: PBRecord = local ? { ...local } : { id };
  for (const e of pending) {
    if (e.op === 'delete') continue;
    Object.assign(base, e.payload ?? {});
  }
  base.id = id;
  return base;
}

/** Resolve the effective hook for a collection (per-collection overrides global). */
export function resolverFor(cfg: ResolvedConfig, collection: string): ConflictResolver | undefined {
  const c = cfg.collections.find((x) => x.name === collection);
  return c?.onConflict ?? cfg.onConflict;
}

/**
 * The ordering primitive: compare two SERVER-minted `updated` strings.
 *
 * A plain string comparison. PocketBase's autodate format is fixed-width and
 * zero-padded, so lexicographic order IS chronological order. MUST NOT parse to
 * Date — parsing introduces a rounding boundary the server's own filter does not
 * have, and the two disagreeing is a skipped record.
 */
export function compareUpdated(a: string | null, b: string | null): -1 | 0 | 1 {
  return compareTimestamps(a, b);
}

/**
 * Decide what an incoming record does, without writing anything.
 *
 * Case order is SPEC §10's, exactly: echo, dirty protection (with the hook),
 * absent-insert, clean LWW, and the `serverUpdated === null` fallthrough.
 */
export async function planApply(
  db: PBODexie,
  cfg: ResolvedConfig,
  collection: string,
  remote: PBRecord,
  onError?: (err: unknown) => void,
): Promise<ApplyPlan> {
  const id = String(remote.id);
  const remoteUpdated = typeof remote.updated === 'string' ? remote.updated : null;
  const meta = await readMeta(db, collection, id);

  // 1 · ECHO — this device's own write coming back (SPEC §7).
  if (isEchoOf(meta, remote.updated)) {
    return { collection, id, outcome: 'echo-dropped', events: [] };
  }

  const local = await tableOf(db, collection).get(id);
  const pending = await pendingFor(db, collection, id);

  // 2 · DIRTY PROTECTION — the queue is the authority on local intent.
  if (pending.length > 0) {
    return planDirty(cfg, collection, id, local, remote, remoteUpdated, pending, onError);
  }

  // 3 · ABSENT — nothing local to protect.
  if (!local) {
    return {
      collection,
      id,
      outcome: 'inserted',
      write: { ...remote, id },
      serverUpdated: remoteUpdated,
      clearEcho: true,
      events: remoteUpdated === null
        ? []
        : [{ type: 'record-synced', payload: { collection, id, updated: remoteUpdated } }],
    };
  }

  // 4 · CLEAN LOCAL — compare two server-minted strings, and nothing else.
  const known = meta?.serverUpdated ?? null;
  const applies = known === null || compareUpdated(remoteUpdated, known) > 0;
  if (!applies) return { collection, id, outcome: 'no-op', events: [] };

  return {
    collection,
    id,
    outcome: 'applied',
    write: { ...remote, id },
    serverUpdated: remoteUpdated,
    clearEcho: true,
    events: remoteUpdated === null
      ? []
      : [{ type: 'record-synced', payload: { collection, id, updated: remoteUpdated } }],
  };
}

/**
 * SPEC §10.2 and §10.5.
 *
 * The default is that the local row survives untouched and `serverUpdated` is
 * recorded so the client knows what it is about to overwrite. The queued
 * mutation reaches the server, the server stamps it later than `R.updated` by
 * construction, and that later value wins through the ordinary mechanism.
 */
async function planDirty(
  cfg: ResolvedConfig,
  collection: string,
  id: string,
  local: PBRecord | undefined,
  remote: PBRecord,
  remoteUpdated: string | null,
  pending: PendingEntry[],
  onError?: (err: unknown) => void,
): Promise<ApplyPlan> {
  const view = localView(local, id, pending);
  const resolver = resolverFor(cfg, collection);

  let decision: PBRecord | 'local' | 'remote' = 'local';
  if (resolver) {
    try {
      decision = await resolver({ collection, local: view, remote, pending });
    } catch (err) {
      // SPEC §2.4 / TESTPLAN §1.8: a throwing hook routes to 'error' and the
      // engine falls back to the default. It never unwinds the pull.
      onError?.(err);
      decision = 'local';
    }
  }

  if (decision === 'remote') {
    return {
      collection,
      id,
      outcome: 'applied',
      write: { ...remote, id },
      serverUpdated: remoteUpdated,
      clearEcho: true,
      dropPending: pending.filter((e) => e.state === 'queued').map((e) => e.seq),
      events: [
        { type: 'conflict', payload: { collection, id, local: view, remote, resolution: 'remote' } },
        ...(remoteUpdated === null
          ? []
          : ([{ type: 'record-synced', payload: { collection, id, updated: remoteUpdated } }] as PlannedEvent[])),
      ],
    };
  }

  if (decision !== 'local' && decision !== null && typeof decision === 'object') {
    const merged: PBRecord = { ...view, ...fieldsOf(decision), id };
    // The merge replaces the QUEUED payload — that is what eventually reaches
    // the server. The last queued create/update carries it; a record whose only
    // queued entry is a delete has nothing to merge into.
    const target = [...pending].reverse().find((e) => e.state === 'queued' && e.op !== 'delete');
    const plan: ApplyPlan = {
      collection,
      id,
      outcome: 'merged',
      write: merged,
      serverUpdated: remoteUpdated,
      events: [{ type: 'conflict', payload: { collection, id, local: merged, remote, resolution: 'merged' } }],
    };
    if (target) {
      plan.rewritePayload = {
        seq: target.seq,
        payload: { ...(target.payload ?? {}), ...fieldsOf(decision) },
      };
    }
    return plan;
  }

  const plan: ApplyPlan = {
    collection,
    id,
    outcome: 'kept-local',
    serverUpdated: remoteUpdated,
    events: [{ type: 'conflict', payload: { collection, id, local: view, remote, resolution: 'local' } }],
  };

  // Dirty protection exists to stop the remote OVERWRITING a local row. When
  // there is no local row at all it has nothing to protect, and discarding the
  // remote instead SEEDS NOTHING — which is data loss, not protection.
  //
  // The path that gets here is §11: a full resync clears the mirror (step 3) but
  // deliberately preserves `_pending`, then pulls (step 4) with the queue still
  // full. Every record carrying a queued partial `update` would take the branch
  // above, the cursor would advance past the server's copy (§7.3), and step 5
  // would re-apply a two-field patch onto nothing — leaving `{id, ...patch}` in
  // the mirror with every other field gone, permanently if that entry later
  // dead-letters. So: write the server's record as the BASE and let the queued
  // fields win on top of it, which is what the queue was protecting all along.
  //
  // A queued `delete` is the exception. There the absent local row is the user's
  // intent, and seeding would resurrect a record they deleted.
  if (local === undefined && !pending.some((e) => e.op === 'delete')) {
    plan.write = localView(remote, id, pending);
  }
  return plan;
}

/**
 * Perform a plan's writes.
 *
 * MUST be called inside a transaction whose scope covers the collection table,
 * `_pending`, `_meta` and `_deadletter` (the derived record state reads the last
 * of those). It opens none of its own, so a whole page commits atomically with
 * its cursor advance.
 */
export async function commitApply(db: PBODexie, plan: ApplyPlan): Promise<void> {
  if (plan.outcome === 'echo-dropped' || plan.outcome === 'no-op') return;

  if (plan.dropPending && plan.dropPending.length > 0) {
    await db.pbo.pending.bulkDelete(plan.dropPending);
  }
  if (plan.rewritePayload) {
    const row = await db.pbo.pending.get(plan.rewritePayload.seq);
    if (row) await db.pbo.pending.put({ ...row, payload: plan.rewritePayload.payload });
  }
  if (plan.write) {
    await tableOf(db, plan.collection).put(plan.write);
  }

  const patch: Parameters<typeof writeMeta>[3] = {};
  if (plan.serverUpdated !== undefined) patch.serverUpdated = plan.serverUpdated;
  if (plan.clearEcho) patch.lastPushedUpdated = null;
  if (Object.keys(patch).length > 0) await writeMeta(db, plan.collection, plan.id, patch);
  await refreshState(db, plan.collection, plan.id);
}

export interface ApplyDeps {
  emit?: <E extends keyof PBOEvents>(event: E, payload: PBOEvents[E]) => void;
  onError?: (err: unknown) => void;
}

/**
 * Apply one incoming record — from a pull page or an SSE event, one path.
 *
 * The pull loop uses `planApply` + `commitApply` directly so a page and its
 * cursor share one transaction; this is the convenience wrapper for a single
 * record.
 */
export async function applyRemote(
  db: PBODexie,
  cfg: ResolvedConfig,
  collection: string,
  remote: PBRecord,
  deps?: ApplyDeps,
): Promise<ApplyOutcome> {
  const plan = await planApply(db, cfg, collection, remote, deps?.onError);
  if (plan.outcome !== 'echo-dropped' && plan.outcome !== 'no-op') {
    await db.transaction(
      'rw',
      db.table(collection),
      db.pbo.pending,
      db.pbo.meta,
      db.pbo.deadletter,
      async () => {
        await commitApply(db, plan);
      },
    );
  }
  for (const e of plan.events) deps?.emit?.(e.type, e.payload as never);
  return plan.outcome;
}
