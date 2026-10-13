/**
 * Pull — the `(updated, id)` cursor (SPEC §6).
 *
 * The tie-break on `id` is mandatory. A batch write stamps many records with an
 * identical `updated`; a bare `updated >` cursor either loops on them forever or
 * skips the tail of the page.
 */
import type PocketBase from 'pocketbase';
import type { Table } from 'dexie';
import type {
  Cursor,
  CursorRow,
  PBOErrorShape,
  PBOEvents,
  PBRecord,
  PullResult,
  PushResult,
  ResolvedConfig,
} from '../types.js';
import type { PBODexie } from '../db/schema.js';
import { classifySdkError, PBOError } from '../errors.js';
import { collectionConfig, expandFilter } from '../config.js';
import { advanceCursor, buildPullFilter, EMPTY_CURSOR, PULL_SORT } from '../util/cursor.js';
import { commitApply, planApply, type ApplyPlan } from './conflict.js';

export interface PullDeps {
  pb: PocketBase;
  db: PBODexie;
  cfg: ResolvedConfig;
  getOwnerId: () => string | null;
  abort: AbortSignal;
  emit: <E extends keyof PBOEvents>(event: E, payload: PBOEvents[E]) => void;
  /**
   * Collections halted per SPEC §6.5, by name. A halted collection is skipped;
   * the others keep syncing. Owned by the engine so a halt outlives one pass.
   */
  halted: Map<string, PBOErrorShape>;
  isOnline?: () => boolean;
  observeRequest?: (ok: boolean, networkFailure: boolean) => void;
  onError?: (err: unknown) => void;
  /**
   * SPEC §5.10, applied to the pull side: is the token still one the server would
   * accept? A 401 — or a 403 with a dead store — is a SESSION condition, not the
   * configuration fault §6.5 halts for. Absent, the engine assumes a live token,
   * which is the same benefit of the doubt `auth/session.ts` gives.
   */
  isAuthValid?: () => boolean;
  /** SPEC §5.10: the session died mid-pull; the engine emits `offline`/'auth'. */
  onAuthLost?: () => void;
  /** Used by SPEC §11 steps 2 and 5. */
  drain?: () => Promise<PushResult>;
  now?: () => number;
}

export interface CollectionPullStats {
  applied: number;
  skipped: number;
  deleted: number;
  pages: number;
  /** The pass stopped because the link went down. */
  linkDown?: boolean;
  /** SPEC §6.5 — this collection is halted, the others continue. */
  halted?: boolean;
  /** SPEC §11.3 — the server rejected the cursor; a full resync is due. */
  resync?: boolean;
}

const zero = (): CollectionPullStats => ({ applied: 0, skipped: 0, deleted: 0, pages: 0 });

// ── cursor rows (SPEC §18.2: written here and in tombstones.ts, nowhere else) ──

export async function readCursor(db: PBODexie, collection: string): Promise<Cursor> {
  const row = await db.pbo.cursors.get(collection);
  return row ? { updated: row.updated, id: row.id } : { ...EMPTY_CURSOR };
}

export async function readCursorRow(db: PBODexie, collection: string): Promise<CursorRow | undefined> {
  return db.pbo.cursors.get(collection);
}

/** MUST be called inside the transaction that applied the page (SPEC §6.3). */
export async function writeCursor(db: PBODexie, collection: string, cursor: Cursor, at: number): Promise<void> {
  await db.pbo.cursors.put({ collection, updated: cursor.updated, id: cursor.id, lastPullAt: at });
}

/** SPEC §11.3 — cleared as part of a full resync, and nowhere else. */
export async function clearCursors(db: PBODexie): Promise<void> {
  await db.pbo.cursors.clear();
}

// ── the page loop ────────────────────────────────────────────────────────────

/** The tables one page's transaction has to cover. */
export function pageScope(db: PBODexie, collection: string): Table<unknown, unknown>[] {
  return [
    db.table(collection) as unknown as Table<unknown, unknown>,
    db.pbo.pending as unknown as Table<unknown, unknown>,
    db.pbo.meta as unknown as Table<unknown, unknown>,
    db.pbo.deadletter as unknown as Table<unknown, unknown>,
    db.pbo.cursors as unknown as Table<unknown, unknown>,
  ];
}

interface Page {
  items: PBRecord[];
}

export async function fetchPage(
  deps: PullDeps,
  collection: string,
  userFilter: string | null,
  cursor: Cursor,
): Promise<Page> {
  const filter = buildPullFilter(userFilter, cursor);
  const options: Record<string, unknown> = { sort: PULL_SORT, skipTotal: true, requestKey: null };
  if (filter !== undefined) options['filter'] = filter;
  const page = (await deps.pb.collection(collection).getList(1, deps.cfg.pullPageSize, options)) as {
    items?: PBRecord[];
  };
  return { items: Array.isArray(page?.items) ? page.items : [] };
}

/**
 * One page: fetch, plan every record outside a transaction (the conflict hook is
 * application code and may return a foreign promise), then apply the whole page
 * AND its cursor advance in one transaction so a crash resumes from the last
 * fully applied page (SPEC §6.3).
 */
export async function pullPage(
  deps: PullDeps,
  collection: string,
  cursor: Cursor,
): Promise<{ applied: number; skipped: number; next: Cursor | null; short: boolean }> {
  const cc = collectionConfig(deps.cfg, collection);
  const userFilter = expandFilter(cc.filter, deps.getOwnerId());
  const { items } = await fetchPage(deps, collection, userFilter, cursor);
  deps.observeRequest?.(true, false);

  const next = advanceCursor(items);
  // TERMINATION (SPEC §6.3). The loop below only exits on a short page, so a full
  // page that cannot advance the cursor is an unbounded request loop that
  // re-applies the same rows for ever. `advanceCursor` returns null for a page
  // whose last row has no well-formed `updated` — the §2.6.1 precondition the
  // client never otherwise verifies. Report it and let §11.3 resync, rather than
  // writing a null cursor that reads back as "no cursor at all".
  if (items.length > 0 && next === null) {
    throw new PBOError(
      'pull/malformed',
      `a page of "${collection}" ended in a record with no usable "updated" — the collection is missing the autodate field required by SPEC §2.6.1`,
      { context: { collection, id: items[items.length - 1]?.id ?? null } },
    );
  }

  const plans: ApplyPlan[] = [];
  for (const item of items) {
    plans.push(await planApply(deps.db, deps.cfg, collection, item, deps.onError));
  }
  const at = (deps.now ?? Date.now)();

  await deps.db.transaction('rw', pageScope(deps.db, collection), async () => {
    for (const plan of plans) await commitApply(deps.db, plan);
    // A dropped echo still advances the cursor: it was consumed (SPEC §7.3).
    if (next) await writeCursor(deps.db, collection, next, at);
  });

  for (const plan of plans) for (const e of plan.events) deps.emit(e.type, e.payload as never);

  const applied = plans.filter((p) => p.outcome === 'inserted' || p.outcome === 'applied' || p.outcome === 'merged').length;
  return {
    applied,
    skipped: plans.length - applied,
    next,
    short: items.length < deps.cfg.pullPageSize,
  };
}

/**
 * SPEC §5.10, mirrored on the pull side (SPEC §6.5 halts only for CONFIGURATION
 * faults).
 *
 * A 401 is never a configuration fault — it is an expired access token. A 403 is
 * one only while the store still holds a token the server would accept; with a
 * dead store it is the same session condition wearing a different status.
 *
 * Exported so `tombstones.ts` keeps exactly one copy of the rule.
 */
export function isAuthCondition(deps: PullDeps, status: number): boolean {
  if (status === 401) return true;
  if (status === 403) return (deps.isAuthValid?.() ?? true) === false;
  return false;
}

/**
 * Page one collection until a short page.
 *
 * Absence of a record from a page NEVER deletes it locally (SPEC §6.5): a cursor
 * pull is an incremental change feed, not a snapshot, and cannot tell "unchanged"
 * from "no longer visible". Deletes come from tombstones and local intent only.
 */
export async function pullCollection(deps: PullDeps, collection: string): Promise<CollectionPullStats> {
  const stats = zero();
  for (;;) {
    if (deps.abort.aborted) return stats;
    const cursor = await readCursor(deps.db, collection);
    let page: { applied: number; skipped: number; next: Cursor | null; short: boolean };
    try {
      page = await pullPage(deps, collection, cursor);
    } catch (err) {
      if (err instanceof PBOError && err.code === 'config/unknown-collection') throw err;
      if (err instanceof PBOError && err.code === 'pull/malformed') {
        deps.emit('error', { error: err.toShape() });
        return { ...stats, resync: true };
      }
      const e = classifySdkError(err, { phase: 'pull', collection });
      const status = Number((e.context?.['status'] as number | undefined) ?? 0);
      deps.observeRequest?.(status > 0, status === 0);

      if (e.code === 'link/offline' || e.code === 'link/timeout') {
        return { ...stats, linkDown: true };
      }
      if (isAuthCondition(deps, status)) {
        // SPEC §5.10 on the pull side. An expired access token is the single most
        // ordinary transient auth event; halting for it would wedge the
        // collection for the life of the engine, since `halted` is consulted
        // before every pass and a halted collection is never requested again.
        // Treat it as the link condition it is: stop, let the watcher probe, and
        // resume when the session is back.
        deps.onAuthLost?.();
        return { ...stats, linkDown: true };
      }
      if (e.code === 'pull/denied') {
        // SPEC §6.5: a configuration fault — a wrong `filter`, a missing
        // `listRule`. Halt THIS collection loudly and let the others keep syncing.
        deps.halted.set(collection, e.toShape());
        deps.emit('error', { error: e.toShape() });
        return { ...stats, halted: true };
      }
      // Unparsable page / a cursor the server rejects ⇒ SPEC §11.3.
      deps.emit('error', { error: e.toShape() });
      return { ...stats, resync: true };
    }

    stats.pages += 1;
    stats.applied += page.applied;
    stats.skipped += page.skipped;
    if (page.short || page.next === null) break;
  }

  const cursor = await readCursor(deps.db, collection);
  deps.emit('pull', { collection, applied: stats.applied, deleted: 0, cursor });
  return stats;
}

/**
 * Pull the named collections (default: all configured), then the tombstones.
 *
 * Tombstones pull LAST (SPEC §3.1, §8.5) so a delete cannot be resurrected by a
 * record page fetched afterwards in the same pass — but only when the caller
 * asked for the whole set; an explicit `pull(['projects'])` is a targeted refresh.
 */
export async function pull(deps: PullDeps, collections?: string[]): Promise<PullResult> {
  const names = collections ?? deps.cfg.collections.map((c) => c.name);
  const result: PullResult = { byCollection: {}, fullResyncTriggered: false };
  let linkDown = false;

  for (const name of names) {
    if (deps.abort.aborted) break;
    if (deps.halted.has(name)) {
      result.byCollection[name] = zero();
      continue;
    }
    const stats = await pullCollection(deps, name);
    result.byCollection[name] = {
      applied: stats.applied,
      skipped: stats.skipped,
      deleted: stats.deleted,
      pages: stats.pages,
    };
    if (stats.resync) result.fullResyncTriggered = true;
    if (stats.linkDown) {
      linkDown = true;
      break;
    }
  }

  if (collections === undefined && !linkDown && !deps.abort.aborted) {
    const { pullTombstones } = await import('./tombstones.js');
    const t = await pullTombstones(deps);
    result.byCollection[deps.cfg.deletionsCollection] = {
      applied: t.applied,
      skipped: t.skipped,
      deleted: t.deleted,
      pages: t.pages,
    };
    if (t.resync) result.fullResyncTriggered = true;
  }

  return result;
}
