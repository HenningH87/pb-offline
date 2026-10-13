/**
 * `_state` — the cross-tab observable sync state (SPEC §4.2, §13.4).
 *
 * Written ONLY by the drainer tab; read reactively by every tab through
 * `liveQuery`. That, and not a BroadcastChannel message, is what makes status,
 * queue depth and dead-letter count observable in a tab that does no syncing.
 */
import type { PBOErrorShape, SyncState, Readable } from '../types.js';
import type { PBODexie, StateRow } from './schema.js';
import { fromLiveQuery } from '../store/readable.js';
import { PBOError } from '../errors.js';

export const STATE_KEY = 'sync' as const;

export const INITIAL_STATE: SyncState = {
  status: 'offline',
  online: false,
  pendingCount: 0,
  deadLetterCount: 0,
  lastSyncedAt: null,
  lastError: null,
  isDrainer: false,
  ownerId: null,
  started: false,
};

/** Read the singleton row. */
export async function readState(db: PBODexie): Promise<StateRow | undefined> {
  return db.pbo.state.get(STATE_KEY);
}

/**
 * The `SyncState` a row carries, as seen by the tab identified by `tabId`.
 *
 * `isDrainer` and `started` are properties of the READER, not of the row, and
 * are never taken from it: the row records which tab drains (compared here to
 * this tab's id), and `started` is per-instance lifecycle that `index.ts`
 * overlays. A stale `started: true` written by another tab must never leak into
 * this one's state (added v0.1 after first-consumer feedback).
 */
export function projectState(row: StateRow | undefined, tabId: string): SyncState {
  const base = (row?.state as SyncState | undefined) ?? INITIAL_STATE;
  return { ...INITIAL_STATE, ...base, isDrainer: row?.drainerTabId === tabId, started: false };
}

function sameError(a: PBOErrorShape | null, b: PBOErrorShape | null): boolean {
  if (a === b) return true;
  if (a === null || b === null) return false;
  return a.code === b.code && a.message === b.message && a.retryable === b.retryable && a.at === b.at;
}

/**
 * Field-wise equality of two `SyncState`s.
 *
 * SPEC §2.5.4 lets consumers rely on identity change to re-render, which only
 * means anything while identity changes when the VALUE does. Every projection
 * builds a fresh object, so `Object.is` in the store primitives can never fire
 * on its own — this is the comparison that gives it teeth.
 */
export function sameSyncState(a: SyncState | undefined, b: SyncState): boolean {
  if (a === undefined) return false;
  if (a === b) return true;
  return (
    a.status === b.status &&
    a.online === b.online &&
    a.pendingCount === b.pendingCount &&
    a.deadLetterCount === b.deadLetterCount &&
    a.lastSyncedAt === b.lastSyncedAt &&
    a.isDrainer === b.isDrainer &&
    a.ownerId === b.ownerId &&
    a.started === b.started &&
    sameError(a.lastError, b.lastError)
  );
}

/**
 * Merge a patch into the state row.
 *
 * The row is the drainer's: SPEC §13.4 makes it the one writer, and passing
 * `isDrainer: false` is a bug that must be loud rather than a silent no-op.
 * `isDrainer` is never persisted — it is a property of the reader, not the row.
 */
export async function writeState(
  db: PBODexie,
  patch: Partial<SyncState>,
  opts?: { tabId?: string; isDrainer?: boolean },
): Promise<void> {
  if (opts?.isDrainer === false) {
    throw new PBOError('internal/invariant', '_state is written only by the drainer tab (SPEC §13.4)', {
      context: { tabId: opts.tabId ?? null, patch },
    });
  }
  await db.transaction('rw', db.pbo.state, async () => {
    const row = await db.pbo.state.get(STATE_KEY);
    const prev = (row?.state as SyncState | undefined) ?? INITIAL_STATE;
    // Neither `isDrainer` nor `started` is a property of the row — both belong
    // to the tab reading it — so neither is ever persisted, whatever a caller
    // put in the patch.
    const { isDrainer: _ignoredRole, started: _ignoredStarted, ...rest } = patch;
    const nextState: SyncState = { ...INITIAL_STATE, ...prev, ...rest, isDrainer: false, started: false };
    const drainerTabId = opts?.tabId ?? row?.drainerTabId ?? null;

    // A write that changes nothing MUST NOT happen. `publish()` is called on
    // drain start, drain end, pull start, pull end, onUp, onDown and every
    // sequence's `finally`; stamping `heartbeatAt` unconditionally made the row
    // change on every one of them, so `liveQuery` re-emitted in EVERY tab and
    // every `$state`/`useSyncExternalStore` consumer re-rendered continuously —
    // for state that was byte-identical. The lease heartbeat (§13.5) writes
    // `heartbeatAt` on its own; it is not this function's job.
    if (row && row.drainerTabId === drainerTabId && sameSyncState(row.state as SyncState, nextState)) return;

    await db.pbo.state.put({ key: STATE_KEY, state: nextState, drainerTabId, heartbeatAt: Date.now() });
  });
}

/**
 * A store over the `_state` row, so every tab sees the drainer's state.
 *
 * `isDrainer` is overlaid per tab (it is a property of the reader, not of the
 * row) — the row records WHICH tab drains; each tab compares it to its own id.
 */
export function liveState(db: PBODexie, tabId: string): Readable<SyncState> {
  // `fromLiveQuery` dedupes with `Object.is`, and `projectState` builds a fresh
  // object every time, so the guard could never fire. Hand back the PREVIOUS
  // object when nothing changed — a liveQuery re-emission the row's own
  // `heartbeatAt` caused (the §13.5 lease renews every 2 s, for ever) must not
  // become a re-render in every tab.
  let last: SyncState | undefined;
  return fromLiveQuery(async () => {
    const next = projectState(await readState(db), tabId);
    if (sameSyncState(last, next)) return last as SyncState;
    last = next;
    return next;
  }, { ...INITIAL_STATE });
}

// ── local bookkeeping rows (not the sync state, not the drainer's) ──────────

export const SIGNOUT_KEY = 'signout' as const;
export const OPENED_KEY = 'opened' as const;

/** What a short-drained sign-out left behind for the next `start()`. */
export interface SignOutPending {
  /** The owner whose sign-out this was, for the log line. */
  ownerId: string | null;
  /** How long the retry may wait for the queue to drain, ms. */
  flushTimeoutMs: number;
  at: number;
}

/**
 * Mark this mirror as one a sign-out could not finish (SPEC §15).
 *
 * `signOut()` without `force` returns `wiped: false` when writes are still
 * queued, and the application clears its token anyway — that is the ordinary
 * shape of every sign-out flow. The mirror then sits on the device holding the
 * whole account's data, with nothing left in the process that remembers it was
 * supposed to be gone. This row is that memory, and it is written INSIDE the
 * database it describes so a successful wipe erases it for free.
 */
export async function markSignOutPending(db: PBODexie, pending: SignOutPending): Promise<void> {
  await db.pbo.state.put({ key: SIGNOUT_KEY, state: pending, drainerTabId: null, heartbeatAt: pending.at });
}

/** The unfinished sign-out this mirror owes, or null. */
export async function readSignOutPending(db: PBODexie): Promise<SignOutPending | null> {
  const row = await db.pbo.state.get(SIGNOUT_KEY);
  const value = row?.state as Partial<SignOutPending> | undefined;
  if (!value || typeof value.flushTimeoutMs !== 'number') return null;
  return { ownerId: value.ownerId ?? null, flushTimeoutMs: value.flushTimeoutMs, at: value.at ?? 0 };
}

/**
 * Stamp "this mirror was opened now" — the clock `staleOwnerDays` reads.
 *
 * Written by the db manager on a real open, and deliberately NOT by
 * `openForOwner`: the retention sweep opens other owners' databases to read
 * this value, and a stamp on every open would reset the very clock it came to
 * check.
 */
export async function stampOpenedAt(db: PBODexie, at: number = Date.now()): Promise<void> {
  await db.pbo.state.put({ key: OPENED_KEY, state: { at }, drainerTabId: null, heartbeatAt: at });
}

/** When this mirror was last opened, or null when nothing ever stamped it. */
export async function readOpenedAt(db: PBODexie): Promise<number | null> {
  const row = await db.pbo.state.get(OPENED_KEY);
  const at = (row?.state as { at?: unknown } | undefined)?.at;
  return typeof at === 'number' ? at : null;
}

/** Drainer lease heartbeat, for the Web-Locks-unavailable fallback (SPEC §13.5). */
export async function heartbeat(db: PBODexie, tabId: string): Promise<void> {
  await db.transaction('rw', db.pbo.state, async () => {
    const row = await db.pbo.state.get(STATE_KEY);
    if (!row || row.drainerTabId !== tabId) return;
    await db.pbo.state.put({ ...row, heartbeatAt: Date.now() });
  });
}

/**
 * Hand the drainer lease back (SPEC §13.5).
 *
 * A tab that knows it is leaving clears its stamp so the next claimant does not
 * have to wait out the stale window for a departure that already happened. A tab
 * that is not the holder is a no-op — losing the lease is not the same as being
 * entitled to clear someone else's.
 */
export async function releaseLease(db: PBODexie, tabId: string): Promise<void> {
  await db.transaction('rw', db.pbo.state, async () => {
    const row = await db.pbo.state.get(STATE_KEY);
    if (!row || row.drainerTabId !== tabId) return;
    await db.pbo.state.put({ ...row, drainerTabId: null, heartbeatAt: 0 });
  });
}

/**
 * Compare-and-set claim of the drainer lease. Returns true if this tab won.
 *
 * The whole read-compare-write runs inside one Dexie transaction, which is the
 * serialisation point: two tabs racing cannot both observe a free lease.
 */
export async function claimLease(db: PBODexie, tabId: string, staleAfterMs: number): Promise<boolean> {
  return db.transaction('rw', db.pbo.state, async () => {
    const now = Date.now();
    const row = await db.pbo.state.get(STATE_KEY);
    const held = row?.drainerTabId != null && row.drainerTabId !== tabId;
    const fresh = held && now - (row?.heartbeatAt ?? 0) < staleAfterMs;
    if (fresh) return false;
    await db.pbo.state.put({
      key: STATE_KEY,
      state: row?.state ?? { ...INITIAL_STATE },
      drainerTabId: tabId,
      heartbeatAt: now,
    });
    return true;
  });
}
