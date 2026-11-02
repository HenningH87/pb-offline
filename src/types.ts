/**
 * Every exported type. No runtime code lives here (SPEC §1.1).
 *
 * Signatures are normative — SPEC §2 is the contract they encode.
 */
import type PocketBase from 'pocketbase';
import type { Table } from 'dexie';

// ────────────────────────────── records ──────────────────────────────

export interface PBRecord {
  id: string;
  created?: string;
  updated?: string;
  [k: string]: unknown;
}

// ────────────────────────────── config ───────────────────────────────

export interface PBOfflineConfig {
  /** Namespace for IndexedDB / lock names. MUST be stable for the app's life. */
  appId: string;
  /** Collections to mirror. Order is the push/pull order — parents first. */
  collections: CollectionConfig[];
  /** Tombstone collection name on the server. Default 'deletions'. */
  deletionsCollection?: string;
  /** Field naming the owning user on every synced collection. Default 'owner'. */
  ownerField?: string;
  /** What happens with no session. Default 'local'. SPEC §14.3. */
  anonymousMode?: 'local' | 'disabled';
  /** Merge consecutive local updates to one record inside this window. Default 250. */
  writeCoalesceMs?: number;
  /** Page size for cursor pulls. Default 200. MUST be ≤ the server's list cap. */
  pullPageSize?: number;
  /** Health-ping backoff bounds, ms. Default { min: 1000, max: 60000 }. */
  backoff?: { min?: number; max?: number };
  /** Force a full resync if the last successful pull is older than this. Default 0 (off). */
  staleAfterMs?: number;
  /**
   * Delete ANOTHER owner's mirror once it has gone this many days without being
   * opened. Default `null` ⇒ off, and every mirror lives on the device for ever.
   *
   * An account switch closes the outgoing owner's database and never wipes it
   * (SPEC §14.2) — correct for switching, and it means a device accumulates one
   * unencrypted copy of every account that has ever signed in on it. This is the
   * eviction the library cannot pick a default for: how long a second account's
   * data may sit on a shared machine is the application's call, not ours.
   *
   * `0` means "the moment the engine starts as somebody else" — the right answer
   * for a one-account-per-visitor product. The anonymous mirror is never swept:
   * only `promoteAnonymousData()` may consume it and the engine cannot know
   * whether the application still intends to (SPEC §14.4).
   */
  staleOwnerDays?: number | null;
  /** Realtime. Default { enabled: true }. */
  realtime?: { enabled?: boolean };
  /** Conflict hook. Default undefined ⇒ pure LWW (SPEC §10). */
  onConflict?: ConflictResolver;
  /** Structured logging sink. Default undefined ⇒ silent. */
  logger?: (e: PBOLogEvent) => void;
}

export interface CollectionConfig {
  /** PB collection name. Also the Dexie table name. */
  name: string;
  /** Extra Dexie indexes, Dexie syntax, beyond the mandatory ones (SPEC §4.2). */
  indexes?: string[];
  /**
   * Server-side filter appended to every pull and SSE subscription for this
   * collection, in PB filter syntax. `{owner}` expands to the current user id.
   * Default `${ownerField} = "{owner}"`. `null` ⇒ no filter (shared data).
   */
  filter?: string | null;
  /** Id shape, if the collection overrides PB's default. */
  idFormat?: IdFormat;
  /** Per-collection conflict hook; overrides the global one. */
  onConflict?: ConflictResolver;
}

export interface IdFormat {
  length: number;
  alphabet: string;
}

/** Config after defaults are applied. Internal, but exported for testability. */
export interface ResolvedConfig extends Required<Omit<PBOfflineConfig, 'onConflict' | 'logger' | 'backoff' | 'realtime'>> {
  backoff: { min: number; max: number };
  realtime: { enabled: boolean };
  onConflict?: ConflictResolver;
  logger?: (e: PBOLogEvent) => void;
  collections: ResolvedCollectionConfig[];
}

export interface ResolvedCollectionConfig extends Required<Omit<CollectionConfig, 'onConflict' | 'filter'>> {
  filter: string | null;
  onConflict?: ConflictResolver;
}

// ────────────────────────────── stores ───────────────────────────────

export type Unsubscriber = () => void;

/**
 * The Svelte store contract, and nothing more (SPEC §2.5).
 *
 * MUST emit synchronously on subscribe, MUST stop after unsubscribe, MUST NOT
 * emit a reference-equal value twice.
 */
export interface Readable<T> {
  subscribe(run: (value: T) => void, invalidate?: () => void): Unsubscriber;
}

// ────────────────────────────── sync state ───────────────────────────

export type SyncStatus = 'offline' | 'connecting' | 'syncing' | 'online' | 'error';

export interface SyncState {
  status: SyncStatus;
  online: boolean;
  pendingCount: number;
  deadLetterCount: number;
  lastSyncedAt: number | null;
  lastError: PBOErrorShape | null;
  /** True in the tab holding the drainer lock (SPEC §13). */
  isDrainer: boolean;
  ownerId: string | null;
  /**
   * `start()` has been called on THIS instance and neither `stop()`, `signOut()`
   * (when it wiped) nor `destroy()` has undone it. Added v0.1 after
   * first-consumer feedback — SPEC §2.2.
   *
   * A property of the instance, not of the mirror: like `isDrainer` it is never
   * persisted to `_state`, so a second tab's `started` says nothing about this
   * one. It is true across an auth change, and true while `anonymousMode:
   * 'disabled'` holds a signed-out engine open with no database — "start() is in
   * effect" is the question it answers, not "a database is open".
   */
  started: boolean;
}

export type RecordSyncState = 'synced' | 'pending' | 'dirty' | 'conflicted' | 'dead';

export interface RecordMeta {
  id: string;
  state: RecordSyncState;
  /** Last server `updated` string this client has seen for the record. */
  serverUpdated: string | null;
  /** Client wall-clock ms of the last local mutation. Ordering only — never LWW input. */
  clientUpdatedAt: number;
  pendingOps: number;
  lastError: PBOErrorShape | null;
}

// ────────────────────────────── queue rows ───────────────────────────

export type PendingOp = 'create' | 'update' | 'delete';

export interface PendingEntry {
  seq: number;
  collection: string;
  /** Always the final PocketBase id — there is no localId (SPEC §4.4). */
  recordId: string;
  op: PendingOp;
  payload: Record<string, unknown> | null;
  clientUpdatedAt: number;
  state: 'queued' | 'inflight';
  attempts: number;
  nextAttemptAt: number;
  lastError: PBOErrorShape | null;
}

export interface CursorRow {
  collection: string;
  updated: string | null;
  id: string | null;
  lastPullAt: number | null;
}

export type Cursor = { updated: string | null; id: string | null };

export interface DeadLetter {
  id: number;
  collection: string;
  recordId: string;
  op: PendingOp;
  payload: Record<string, unknown> | null;
  error: PBOErrorShape;
  at: number;
}

export interface MetaRow {
  collection: string;
  recordId: string;
  serverUpdated: string | null;
  clientUpdatedAt: number;
  /** Echo suppression (SPEC §7). */
  lastPushedUpdated: string | null;
  state: RecordSyncState;
  lastError: PBOErrorShape | null;
}

// ────────────────────────────── queries ──────────────────────────────

export interface LocalQuery<T> {
  where?: Partial<Record<keyof T & string, unknown>>;
  sort?: {
    field: keyof T & string;
    dir?: 'asc' | 'desc';
    /**
     * Where a record with no value for `field` gets its sort key. Added v0.1
     * after first-consumer feedback — full semantics in SPEC §2.3.
     *
     * `'clientUpdatedAt'` takes it from the record's `_meta.clientUpdatedAt`, so
     * a record created offline — which has no server-minted `created`/`updated`
     * yet — still lands somewhere sensible in a list sorted by one of them.
     * Omitted, sorting is unchanged.
     */
    localFallback?: 'clientUpdatedAt';
  };
  limit?: number;
  offset?: number;
}

export interface WriteOpts {
  /** Write the mirror, enqueue nothing. Device-local, overwritten by remote (SPEC §4.3). */
  transient?: boolean;
  /** Skip coalescing for this write. */
  noCoalesce?: boolean;
}

export type BatchOp<T extends PBRecord> =
  | { op: 'create'; data: Omit<T, 'id' | 'created' | 'updated'> & { id?: string } }
  | { op: 'update'; id: string; patch: Partial<T>; opts?: WriteOpts }
  | { op: 'delete'; id: string };

// ────────────────────────────── conflicts ────────────────────────────

export type ConflictResolver = (ctx: ConflictContext) =>
  | PBRecord
  | 'local'
  | 'remote'
  | Promise<PBRecord | 'local' | 'remote'>;

export interface ConflictContext {
  collection: string;
  /** The mirror row with queued mutations applied. */
  local: PBRecord;
  /** What the server currently has. */
  remote: PBRecord;
  /** This record's queued mutations, in order. */
  pending: PendingEntry[];
}

// ────────────────────────────── errors ───────────────────────────────

export type PBOErrorCode =
  | 'config/unknown-collection'
  | 'config/bad-id'
  | 'config/invalid'
  | 'auth/unauthenticated'
  | 'auth/changed-mid-flight'
  | 'link/offline'
  | 'link/timeout'
  | 'push/rejected'
  | 'push/vanished'
  | 'push/conflict'
  | 'pull/denied'
  | 'pull/cursor-stale'
  | 'pull/malformed'
  | 'db/open-failed'
  | 'db/quota'
  | 'db/version-conflict'
  | 'lock/unavailable'
  | 'internal/invariant';

export interface PBOErrorShape {
  code: PBOErrorCode;
  message: string;
  retryable: boolean;
  at: number;
}

export interface PBOLogEvent {
  level: 'debug' | 'info' | 'warn' | 'error';
  msg: string;
  data?: Record<string, unknown>;
  at: number;
}

// ────────────────────────────── events ───────────────────────────────

export interface PBOEvents {
  'online': { at: number };
  'offline': { at: number; reason: 'browser' | 'sse' | 'ping' | 'auth' };
  'status': { from: SyncStatus; to: SyncStatus };
  'push': { collection: string; op: PendingOp; id: string; ok: boolean; attempt: number };
  'pull': { collection: string; applied: number; deleted: number; cursor: Cursor };
  'record-synced': { collection: string; id: string; updated: string };
  'conflict': {
    collection: string;
    id: string;
    local: PBRecord;
    remote: PBRecord;
    resolution: 'local' | 'remote' | 'merged';
  };
  'dead-letter': { entry: DeadLetter };
  'full-resync': { phase: 'start' | 'done' | 'failed'; reason: string };
  'auth-change': { ownerId: string | null; previous: string | null };
  'error': { error: PBOErrorShape };
}

// ────────────────────────────── results ──────────────────────────────

export interface PushResult {
  pushed: number;
  failed: number;
  deadLettered: number;
  remaining: number;
  stoppedBy: 'empty' | 'link' | 'auth' | 'stopped';
}

export interface PullResult {
  byCollection: Record<string, { applied: number; skipped: number; deleted: number; pages: number }>;
  fullResyncTriggered: boolean;
}

export interface SignOutResult {
  /** The queue (`_pending`) emptied. */
  drained: boolean;
  wiped: boolean;
  /**
   * Writes the server does not have: still queued PLUS parked as dead letters
   * (SPEC §15.3). Above 0 without `force`, the mirror is kept and marked.
   */
  pending: number;
}

export interface PromotionResult {
  promoted: number;
  byCollection: Record<string, number>;
  strategy: 'copy' | 'move';
}

// ────────────────────────────── public surface ───────────────────────

export interface Collection<T extends PBRecord = PBRecord> {
  readonly name: string;

  // one-shot reads, from the mirror, never the network
  get(id: string): Promise<T | undefined>;
  getAll(): Promise<T[]>;
  find(query?: LocalQuery<T>): Promise<T[]>;
  count(query?: LocalQuery<T>): Promise<number>;

  // reactive reads
  live(query?: LocalQuery<T>): Readable<T[]>;
  liveOne(id: string): Readable<T | undefined>;
  liveCount(query?: LocalQuery<T>): Readable<number>;
  liveRaw<R>(fn: (table: Table<T, string>) => Promise<R>): Readable<R>;

  // local-first writes
  create(data: Omit<T, 'id' | 'created' | 'updated'> & { id?: string }): Promise<T>;
  update(id: string, patch: Partial<T>, opts?: WriteOpts): Promise<T>;
  delete(id: string): Promise<void>;
  batch(ops: BatchOp<T>[]): Promise<void>;

  // per-record sync meta
  meta(id: string): Promise<RecordMeta | undefined>;
  liveMeta(id: string): Readable<RecordMeta | undefined>;
  liveDirty(): Readable<Array<{ id: string; state: RecordSyncState }>>;
}

export interface PBOffline {
  collection<T extends PBRecord = PBRecord>(name: string): Collection<T>;

  readonly status: Readable<SyncStatus>;
  readonly pendingCount: Readable<number>;
  readonly lastSyncedAt: Readable<number | null>;
  readonly deadLetters: Readable<DeadLetter[]>;
  readonly online: Readable<boolean>;
  readonly state: Readable<SyncState>;

  on<E extends keyof PBOEvents>(event: E, cb: (payload: PBOEvents[E]) => void): Unsubscriber;
  once<E extends keyof PBOEvents>(event: E, cb: (payload: PBOEvents[E]) => void): Unsubscriber;

  start(): Promise<void>;
  stop(): Promise<void>;
  push(): Promise<PushResult>;
  pull(collections?: string[]): Promise<PullResult>;
  sync(): Promise<void>;
  fullResync(reason?: string): Promise<void>;
  signOut(opts?: { flushTimeoutMs?: number; force?: boolean }): Promise<SignOutResult>;
  promoteAnonymousData(opts?: { strategy?: 'copy' | 'move' }): Promise<PromotionResult>;

  retryDeadLetter(id: number): Promise<void>;
  discardDeadLetter(id: number): Promise<void>;

  destroy(): Promise<void>;
}

/** Re-exported so consumers can type the SDK instance they hand us. */
export type { PocketBase };
