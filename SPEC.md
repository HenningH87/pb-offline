# SPEC — offline-first sync engine for PocketBase

Status: **pre-release, v0 build contract.** Normative. MUST / MUST NOT / SHOULD /
MAY carry RFC 2119 force. Written for coding agents implementing the library and
for external contributors reviewing it.

Working package name: `pb-offline`. Inside `src/` the name appears in exactly one
place — `src/brand.ts` — and every runtime string that needs it (Dexie database
names, Web Lock names, `BroadcastChannel` names, `localStorage` keys) derives from
`BRAND` there. Outside `src/` it appears only in `package.json`, `LICENSE`,
`README.md` and `server/go/go.mod`'s module path. Renaming the project is those
five files plus a repository rename, and nothing else. Contributors MUST NOT
hardcode the name anywhere else; a unit test enforces it for `src/`.

---

## 0 · What this is

A general-purpose offline-first sync engine for [PocketBase](https://pocketbase.io),
MIT-licensed, in two shipped halves:

- a **client TypeScript package** (this repo's npm package) that mirrors chosen PB
  collections into IndexedDB via [Dexie](https://dexie.org), serves every read and
  write from that mirror, and reconciles with the server in the background;
- **server helpers** — a Go module (`server/go/`) and a JSVM equivalent
  (`pb_hooks/`) — that install the one thing PocketBase does not provide and the
  client cannot synthesise: **delete tombstones**.

The identity of the library is **reactivity and a self-healing link**. Every piece
of state the engine holds — record data, queue depth, connection status, per-record
sync state, dead letters — is observable and re-emits on any source of change. When
the link to PocketBase drops, the engine keeps working locally; when it returns,
the engine drains, backfills and resubscribes without being asked, and says so
through its stores and events.

Design premise, inherited from the reference implementation this replaces: **a UI
path never awaits the network.** A write hits Dexie and returns. Everything else is
the engine's problem.

### 0.1 Prior art and provenance

There is no turnkey offline-sync solution for PocketBase (RxDB has no PB plugin,
PowerSync does not support SQLite backends, ElectricSQL is Postgres-only). The
mechanics specified here follow the pattern the PocketBase community converges on —
Dexie mirror + ordered pending queue + `updated`-cursor pull + a `deletions`
tombstone collection + last-write-wins — hardened against a 359-LOC production
Firestore sync layer whose semantics (echo suppression, apply-if-newer, the
`updatedAt` round-trip invariant, transient ticks, sign-out flush-then-wipe) are
the behavioural reference for §5 and §6.

---

## 1 · Architecture

```
  ┌───────────────────────── application ─────────────────────────┐
  │  svelte stores / react useSyncExternalStore / vanilla         │
  └───────┬───────────────────────────────────────────▲───────────┘
          │ create/update/delete (sync, never awaits net)          │ subscribe()
          ▼                                                       │
  ┌───────────────────────── public API ──────────────────────────┐
  │  createPBOffline(pb, config) → PBOffline                      │
  │    .collection<T>(name)  .status  .pendingCount  .on(...)     │
  └───────┬───────────────────────────────────────────▲───────────┘
          │                                                       │
  ┌───────▼───────────────────────────────────────────┴───────────┐
  │                    Dexie (IndexedDB), ONE DB PER OWNER        │
  │  <collection> tables …  _pending  _cursors  _deadletter       │
  │  _meta  _state                                                │
  │  liveQuery() ⇒ every read re-emits on ANY write, any tab      │
  └───────▲───────────────────┬───────────────────────▲───────────┘
          │ apply-if-newer    │ drain in seq order    │ apply-if-newer
          │                   ▼                       │
  ┌───────┴───────────────────────────────────────────┴───────────┐
  │                        sync engine                            │
  │  push ──▶ POST/PATCH/DELETE  ◀── pull (updated,id cursor)      │
  │  echo suppression · LWW+hook · tombstones · full-resync        │
  │  connectivity watcher (navigator.onLine + SSE state + ping)    │
  │  Web Locks: exactly ONE drainer tab; all tabs reactive         │
  └───────────────────────────────┬───────────────────────────────┘
                                  │ pocketbase JS SDK (peer dep)
  ┌───────────────────────────────▼───────────────────────────────┐
  │  PocketBase   /api/collections/{c}/records   +  SSE realtime   │
  │  server helpers: deletions collection, delete hooks, prune cron│
  └───────────────────────────────────────────────────────────────┘
```

Data flows in exactly two directions and they are kept from feeding into each other
by one mechanism only: **echo suppression keyed on the server's own `updated`
value** (§7). There is no second guard, and contributors MUST NOT add one — two
overlapping guards is how the reference implementation's predecessor deadlocked.

### 1.1 Module map (`src/`)

| Path | Owns |
|---|---|
| `brand.ts` | `BRAND` — the only place the package name reaches runtime. |
| `index.ts` | `createPBOffline`, public re-exports. Nothing else. |
| `types.ts` | Every exported type. No runtime code. |
| `config.ts` | Config normalisation, defaults, validation errors. |
| `errors.ts` | `PBOError`, the code taxonomy (§12). |
| `ids.ts` | PB-compatible id generation and validation (§4.4). |
| `db/schema.ts` | Dexie schema declaration and version migrations. |
| `db/open.ts` | Per-owner database open / close / wipe (§4.1). |
| `db/meta.ts` | `_meta` row read/write, per-record sync state derivation. |
| `db/state.ts` | `_state` singleton row — cross-tab observable sync state. |
| `collections/define.ts` | `defineCollection<T>` and the typed handle. |
| `collections/crud.ts` | Local-first create/update/delete, coalescing, transient. |
| `collections/query.ts` | `liveQuery`-backed reactive reads. |
| `sync/engine.ts` | Orchestration, the status state machine, scheduling. |
| `sync/push.ts` | `_pending` drain (§5). |
| `sync/pull.ts` | Cursor pull (§6). |
| `sync/tombstones.ts` | `deletions` pull and local apply (§8). |
| `sync/realtime.ts` | SSE subscribe/resubscribe (§9). |
| `sync/echo.ts` | Echo suppression map (§7). |
| `sync/conflict.ts` | LWW comparison and the pluggable hook (§10). |
| `sync/deadletter.ts` | Dead-letter park, list, retry, discard. |
| `sync/resync.ts` | Full resync, automatic and manual (§11). |
| `sync/locks.ts` | Drainer election via Web Locks + lease fallback (§13). |
| `net/connectivity.ts` | The link watcher (§3). |
| `net/backoff.ts` | Jittered exponential backoff. |
| `auth/session.ts` | `pb.authStore` binding, owner identity, switch handling. |
| `auth/promotion.ts` | Anonymous → owned promotion (§14). |
| `store/readable.ts` | Svelte store contract helpers. |
| `store/emitter.ts` | Typed event emitter. |
| `util/time.ts` | PB `updated` parsing and comparison. |
| `util/cursor.ts` | `(updated,id)` cursor encode/decode/compare. |

---

## 2 · Public API

All signatures are normative. Implementations MUST NOT widen them in v0.

### 2.1 Entry point

```ts
import PocketBase from 'pocketbase';

export function createPBOffline(pb: PocketBase, config: PBOfflineConfig): PBOffline;

export interface PBOfflineConfig {
  /** Namespace for IndexedDB / lock names. MUST be stable for the app's life. */
  appId: string;
  /** Collections to mirror. Order is the push/pull order — put parents first. */
  collections: CollectionConfig[];
  /** Tombstone collection name on the server. Default 'deletions'. */
  deletionsCollection?: string;
  /** Field on every synced collection naming the owning user. Default 'owner'.
   *  MUST be a bare field identifier (letters/digits/underscore) — it is
   *  interpolated into PB filters. */
  ownerField?: string;
  /**
   * Where anonymous (signed-out) data lives, and whether it exists at all.
   *  'local'   — a db named for the anonymous owner; promotable on sign-in (§14).
   *  'disabled'— reads return empty and writes throw until authenticated.
   * Default 'local'.
   */
  anonymousMode?: 'local' | 'disabled';
  /** Merge consecutive local updates to one record inside this window. Default 250. */
  writeCoalesceMs?: number;
  /** Page size for cursor pulls. Default 200, clamped to 500 (PB's list cap)
   *  with a logger warning. */
  pullPageSize?: number;
  /** Health-ping backoff bounds, in ms. Default { min: 1000, max: 60000 }. */
  backoff?: { min?: number; max?: number };
  /** Force a full resync if the last successful pull is older than this. Default 0 (off). */
  staleAfterMs?: number;
  /** Delete ANOTHER owner's mirror after this many days unopened (§14.2).
   *  Default null ⇒ off: every mirror lives on the device for ever. 0 means
   *  "as soon as somebody else starts the engine". Never sweeps the anonymous
   *  mirror, and never the current owner's. */
  staleOwnerDays?: number | null;
  /** Realtime. Default { enabled: true }. */
  realtime?: { enabled?: boolean };
  /** Conflict hook. Default undefined ⇒ pure LWW (§10). */
  onConflict?: ConflictResolver;
  /** Structured logging sink. Default undefined ⇒ silent. */
  logger?: (e: PBOLogEvent) => void;
}

export interface CollectionConfig {
  /** PB collection name. Also the Dexie table name. */
  name: string;
  /** Extra Dexie indexes, Dexie syntax, beyond the mandatory ones (§4.2). */
  indexes?: string[];
  /**
   * Server-side filter appended to every pull and SSE subscription for this
   * collection, in PB filter syntax. `{owner}` expands to the current user id.
   * Default: `${ownerField} = "{owner}"`. Pass `null` for no filter (shared /
   * public reference data).
   */
  filter?: string | null;
  /** Id shape, if the collection overrides PB's default. Default {length:15, alphabet:'a-z0-9'}. */
  idFormat?: { length: number; alphabet: string };
  /** Per-collection conflict hook; overrides the global one. */
  onConflict?: ConflictResolver;
}
```

`createPBOffline` MUST be synchronous and MUST NOT perform I/O. It returns
immediately with every store already emitting an initial value. Actual database
opening, queue inspection and network work happen on `start()`.

### 2.2 The engine handle

```ts
export interface PBOffline {
  /** Typed handle for one configured collection. Throws if not configured. */
  collection<T extends PBRecord = PBRecord>(name: string): Collection<T>;

  // ── observable state (all are Svelte-contract stores, see §2.5) ──
  readonly status: Readable<SyncStatus>;
  readonly pendingCount: Readable<number>;
  readonly lastSyncedAt: Readable<number | null>;
  readonly deadLetters: Readable<DeadLetter[]>;
  readonly online: Readable<boolean>;
  /** Everything above in one object, for a single subscription. */
  readonly state: Readable<SyncState>;

  // ── events ──
  on<E extends keyof PBOEvents>(event: E, cb: (payload: PBOEvents[E]) => void): Unsubscriber;
  once<E extends keyof PBOEvents>(event: E, cb: (payload: PBOEvents[E]) => void): Unsubscriber;

  // ── lifecycle & control ──
  start(): Promise<void>;
  stop(): Promise<void>;
  /** Drain now if possible. Resolves when the queue is empty or the attempt fails. */
  push(): Promise<PushResult>;
  /** Cursor-pull now. `collections` defaults to all. */
  pull(collections?: string[]): Promise<PullResult>;
  /** push() then pull() then resubscribe. What reconnection runs. */
  sync(): Promise<void>;
  /** Wipe local mirror + cursors and refetch from scratch (§11). Queue is preserved. */
  fullResync(reason?: string): Promise<void>;
  /** Flush the queue, then wipe this owner's database (§15). */
  signOut(opts?: { flushTimeoutMs?: number; force?: boolean }): Promise<SignOutResult>;
  /** Promote anonymous local data to the signed-in owner (§14). */
  promoteAnonymousData(opts?: { strategy?: 'copy' | 'move' }): Promise<PromotionResult>;

  // ── dead letters ──
  retryDeadLetter(id: number): Promise<void>;
  discardDeadLetter(id: number): Promise<void>;

  /** Release every resource. The instance is unusable afterwards. */
  destroy(): Promise<void>;
}

export type SyncStatus = 'offline' | 'connecting' | 'syncing' | 'online' | 'error';

export interface SyncState {
  status: SyncStatus;
  online: boolean;
  pendingCount: number;
  deadLetterCount: number;
  lastSyncedAt: number | null;
  lastError: PBOErrorShape | null;
  /** True in the tab that holds the drainer lock (§13). */
  isDrainer: boolean;
  ownerId: string | null;
  /** `start()` is in effect on this instance. Added v0.1 after first-consumer feedback. */
  started: boolean;
}
```

**`started`** (added v0.1 after first-consumer feedback) is normative as follows.
It is `false` on a fresh handle; it becomes `true` **synchronously when `start()`
is called**, before the returned promise settles, because a start in flight is a
start; it returns to `false` when that promise rejects, when `stop()` resolves,
when `signOut()` wipes, and on `destroy()`. It is unaffected by an auth change —
§14.2 replaces the mirror under a started engine, it does not unstart it — and it
is `true` for a signed-out engine under `anonymousMode: 'disabled'`, which opens
no database but is watching the auth store for the sign-in that will.

Like `isDrainer` it is a property of the READER, not of the mirror: it MUST NOT be
persisted to `_state` (§4.2) and MUST NOT be adopted from the row another tab
wrote. A consumer that starts the engine from a framework effect has no other way
to ask — `start()` is idempotent and returns the same promise, so calling it again
answers nothing.

**Status semantics** (normative, one line each):

| Status | Means |
|---|---|
| `offline` | The link is known-down. Local reads and writes work; nothing is in flight. |
| `connecting` | The link is being re-established (backoff wait, health ping, SSE handshake). |
| `syncing` | A push, pull or resync is in flight. |
| `online` | Link up, queue empty, cursors current, SSE subscribed. |
| `error` | A condition the engine cannot clear by itself (dead letters present, or a collection halted per §6.5). Sync of unaffected collections continues. |

`error` MUST NOT be a transient state: 5xx and network failures are `connecting`,
not `error`. The slot holds one value: when dead letters exist AND the link is
down, `error` wins — dead letters need the user's attention regardless of the
link, and `online`/`state.online` still reports the link truthfully.

### 2.3 Collections

```ts
export interface PBRecord {
  id: string;
  created?: string;
  updated?: string;
  [k: string]: unknown;
}

export interface Collection<T extends PBRecord> {
  readonly name: string;

  // ── reads (one-shot, from the mirror; never touch the network) ──
  get(id: string): Promise<T | undefined>;
  getAll(): Promise<T[]>;
  find(query?: LocalQuery<T>): Promise<T[]>;
  count(query?: LocalQuery<T>): Promise<number>;

  // ── reactive reads (Svelte-contract stores over Dexie liveQuery) ──
  live(query?: LocalQuery<T>): Readable<T[]>;
  liveOne(id: string): Readable<T | undefined>;
  liveCount(query?: LocalQuery<T>): Readable<number>;
  /** Arbitrary Dexie work, re-run on any change to this collection's table. */
  liveRaw<R>(fn: (table: Dexie.Table<T, string>) => Promise<R>): Readable<R>;

  // ── writes (local-first; resolve after the Dexie write, never after the network) ──
  create(data: Omit<T, 'id' | 'created' | 'updated'> & { id?: string }): Promise<T>;
  update(id: string, patch: Partial<T>, opts?: WriteOpts): Promise<T>;
  delete(id: string): Promise<void>;
  /** One Dexie transaction, one contiguous run of queue entries. */
  batch(ops: BatchOp<T>[]): Promise<void>;

  // ── per-record sync meta ──
  meta(id: string): Promise<RecordMeta | undefined>;
  liveMeta(id: string): Readable<RecordMeta | undefined>;
  /** Every record with pending or dead-lettered mutations. */
  liveDirty(): Readable<Array<{ id: string; state: RecordSyncState }>>;
}

export interface WriteOpts {
  /**
   * Write the mirror but enqueue nothing. The change is device-local and is
   * overwritten by the next remote version of the record. For live/ticking
   * derived values that must never become server state.
   */
  transient?: boolean;
  /** Skip coalescing for this write; enqueue its own entry. */
  noCoalesce?: boolean;
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
```

`LocalQuery<T>` in v0 is deliberately thin — equality filters, a sort key, limit and
offset — because everything harder is available through `liveRaw`:

```ts
export interface LocalQuery<T> {
  where?: Partial<Record<keyof T & string, unknown>>;
  sort?: {
    field: keyof T & string;
    dir?: 'asc' | 'desc';
    /** Added v0.1 after first-consumer feedback. See "Local ordering key" below. */
    localFallback?: 'clientUpdatedAt';
  };
  limit?: number;
  offset?: number;
}
```

#### Local ordering key — `sort.localFallback` (added v0.1 after first-consumer feedback)

`created` and `updated` are **server** stamps (§4.4, §10). A record written
offline has neither until a push comes back, so a list sorted on one of them
scatters the app's own newest writes to whichever end `undefined` happens to land
on — which is precisely when the user is looking at them. The engine MUST NOT
mint a `created`/`updated` of its own to paper over this: §10's whole conflict
story rests on those two fields being server-minted and nothing else.

`sort.localFallback: 'clientUpdatedAt'` is the sanctioned substitute. It borrows
the one clock the client already keeps honestly, `_meta.clientUpdatedAt` (§4.2) —
the wall-clock millisecond of the record's last local mutation, which is already
declared "ordering only, never LWW input". The semantics are normative:

1. A row's sort key is `record[field]`. A row whose value is `undefined`, `null`
   or `''` is a **fallback row**; `0` and `false` are values, not absences.
2. A fallback row's key is its `_meta.clientUpdatedAt`, or `0` when the record has
   no `_meta` row at all.
3. **Interleaved (date) domain.** If every row that HAS a value carries a
   date-shaped string — `YYYY-MM-DD` with an optional time part, which matches
   both PocketBase's `2026-01-01 09:00:00.000Z` and ISO-8601's `…T09:00:00.000Z` —
   the whole sort runs in milliseconds: present values are parsed (the space
   separator normalised to `T`), fallback rows use their `clientUpdatedAt`
   unchanged. Fallback rows therefore **interleave** with mirrored rows in true
   time order. String comparison MUST NOT be used here: `' ' < 'T'` would sort
   every ISO row after every PocketBase row of the same day.
4. **Partitioned (non-date) domain.** Otherwise the two key sources are not
   comparable, so they are kept apart: valued rows sort among themselves by the
   field (the same comparison a plain sort uses), fallback rows among themselves
   by `clientUpdatedAt`, and the fallback group goes **last** in `asc`.
5. `dir` reverses the comparison as a whole, the group split in rule 4 included —
   so `dir: 'desc'` puts unsynced rows first, which is the "newest first, my own
   writes on top" list an application actually builds.
6. Equal keys break by `id` ascending, in both directions, so a fallback list is
   fully deterministic rather than dependent on IndexedDB's read order.

`find`, `count`, `live` and `liveCount` MUST share one implementation of this, so
a list and a `limit`ed count of that same list cannot disagree. Sorting does not
change a count, so `liveCount` with no `where`/`limit`/`offset` MAY still take its
fast `Table.count()` path and read no `_meta`. When the fallback IS applied, the
`_meta` read MUST happen inside the query's own `liveQuery` function, which makes
`_meta` part of the observed set: a write that moves a record's `clientUpdatedAt`
re-emits the list. Omitting `localFallback` MUST leave sorting byte-identical to
v0 — including where empty values land — and MUST cost no `_meta` read.

#### Reactive reads outlive the mirror (added v0.1 after first-consumer feedback)

A collection handle is safe to construct and subscribe to at any time, and that
guarantee is normative for the reactive reads — `live`, `liveOne`, `liveCount`,
`liveRaw`, `liveMeta`, `liveDirty` — as well as the one-shot ones. **A reactive
read created before `start()` MUST emit its declared empty value (§2.5.1) and
MUST come alive when the mirror opens; a reactive read held across an auth change
MUST follow the incoming owner's mirror (§14.2), and MUST empty when the mirror
is closed or wiped (§15).** None of that may require the consumer to rebuild the
store, to await `start()` before subscribing, or to re-subscribe on `auth-change`.

This is not free, and getting it wrong is silent. Dexie's `liveQuery` re-runs a
querier only when a write touches something that querier READ on its previous
run, and the observability keys it records name the **database**, not the Dexie
instance. So a querier that ran while no database was open recorded an EMPTY
observed set — nothing can ever overlap nothing, and the subscription is dead for
the life of the handle while writes go on working perfectly. A querier that last
ran against the outgoing owner's database is dead in the same way: the incoming
owner's writes touch a differently-named database. Resolving the database lazily
inside the querier is therefore necessary and NOT sufficient.

The engine's answer is a **mirror revision**: a counter the database manager steps
each time the open handle changes identity — the first open, both halves of an
account switch (the close and the open), a `close()`, a `wipe()`. Re-opening the
SAME owner is not an identity change and MUST NOT step it. Every reactive read
subscribes to that revision for as long as it has subscribers, and each step
DISCARDS its `liveQuery` and builds a new one, which re-runs the querier against
whatever mirror is open now and records a fresh observed set. Implementations MUST
NOT introduce a second notification path for data (§2.5, and the module note in
`collections/query.ts`): the revision carries no values and nothing renders it —
its only job is to invalidate an observed set that Dexie can no longer reach.

The revision subscription belongs to the store's `liveQuery` subscription, not to
the store: releasing the last subscriber MUST release it too, so an idle store is
not woken by an account switch and leaks no listener. A rebuild MUST NOT emit on
its own — the rebuilt querier's result arrives as an ordinary frame, and §2.5's
four properties hold across it unchanged.

Whether a read is BLOCKED (`anonymousMode: 'disabled'` with no session, §14.3) is
likewise decided on every run and never snapshotted when the store is built: a
store created while signed out returns empty and comes alive on sign-in like any
other.

### 2.4 Events

```ts
export interface PBOEvents {
  'online': { at: number };
  'offline': { at: number; reason: 'browser' | 'sse' | 'ping' | 'auth' };
  'status': { from: SyncStatus; to: SyncStatus };
  'push': { collection: string; op: PendingOp; id: string; ok: boolean; attempt: number };
  'pull': { collection: string; applied: number; deleted: number; cursor: Cursor };
  'record-synced': { collection: string; id: string; updated: string };
  'conflict': { collection: string; id: string; local: PBRecord; remote: PBRecord; resolution: 'local' | 'remote' | 'merged' };
  'dead-letter': { entry: DeadLetter };
  'full-resync': { phase: 'start' | 'done' | 'failed'; reason: string };
  'auth-change': { ownerId: string | null; previous: string | null };
  'error': { error: PBOErrorShape };
}
```

`on()` returns an unsubscriber. Handlers MUST NOT throw; the emitter catches and
routes a thrown handler error to `'error'` rather than unwinding the engine.

### 2.5 The store contract

Every `Readable<T>` this library exposes is a **Svelte store**, and nothing more:

```ts
export interface Readable<T> {
  subscribe(run: (value: T) => void, invalidate?: () => void): Unsubscriber;
}
export type Unsubscriber = () => void;
```

Normative properties:

1. `subscribe` MUST call `run` **synchronously** with the current value before
   returning. For a `live()` query whose first Dexie read has not resolved, the
   current value is the declared empty value (`[]`, `undefined`, `0`).
2. It MUST return a function that stops delivery and releases the underlying
   `liveQuery` subscription.
3. It MUST NOT emit after its unsubscriber has been called.
4. It MUST NOT emit a value equal by reference to the previous one; consumers rely
   on identity change to re-render.
5. *(added v0.1 after first-consumer feedback)* A store over the mirror MUST stay
   live across the mirror's own lifecycle — subscribed before `start()`, held
   across an auth change, emptied by a wipe. Properties 1–4 hold across each of
   those transitions unchanged: the rebuild described in §2.3, "Reactive reads
   outlive the mirror", emits nothing of its own and delivers nothing to a
   handler that has already unsubscribed.

That contract is deliberately the whole framework story for v0:

- **Svelte** — `$store` works natively.
- **React** — `useSyncExternalStore(store.subscribe, getSnapshot)` over a one-line
  local adapter; the README carries the six-line hook. No React package ships in
  v0 (§16).
- **Vanilla** — `const off = store.subscribe(v => render(v))`.

### 2.6 Server helper contracts

Go (`server/go/`, module `github.com/HenningH87/pb-offline/server/go`, package `pbo`):

```go
type Options struct {
    // Collections to install tombstone hooks on. Required, non-empty.
    Collections []string
    // Tombstone collection name. Default "deletions".
    DeletionsCollection string
    // Field naming the owner on the source collections and on the tombstone.
    // Default "owner".
    OwnerField string
    // Tombstone retention. Rows older than this are pruned. Default 30 * 24h.
    // MUST be documented to clients: a client whose cursor predates it MUST
    // full-resync.
    Retention time.Duration
    // Prune cron expression. Default "0 3 * * *".
    PruneCron string
    // Create/patch the deletions collection at boot. nil ⇒ true (Go zero
    // values cannot default a bool to true; same pattern as PB's *string rules).
    EnsureSchema *bool
}

// Register installs the tombstone hooks, the deletions collection migration and
// the prune cron. It MUST be called before app.Start(). It is idempotent.
func Register(app core.App, opts Options) error

// Retention returns the configured window so an app can expose it to clients.
func Retention(app core.App) time.Duration
```

JSVM (`pb_hooks/pb-offline.pb.js`): a single file, copied into a PB project's
`pb_hooks/`, configured by editing the `CONFIG` object at its top. Same behaviour,
same tombstone shape. It is the fallback for teams not embedding PB as a Go
framework and MUST stay behaviourally identical to the Go module; any divergence is
a bug in whichever one is wrong.

Its `ensureSchema` ships **off**, which is the one place the two helpers'
DEFAULTS differ on purpose: the Go module is wired up by someone writing Go, the
JSVM file is installed by copying it into a directory, and a copy can reach a
running server before anybody has edited `CONFIG`. Boot-time schema mutation
driven by a still-placeholder config is not something a copy-paste install
should be able to do. The integration harness turns it on the way an operator
does, by editing the copied file.

**`ensureSchema` is additive about API RULES, not just fields.** On a collection
it is creating it writes the rules above. On one that already exists it may only
set a rule that is *unset*: a rule the operator has written is KEPT, and a
difference from the prescribed value is reported as a warning naming both. Rules
are the security boundary of a collection, and silently resetting a boundary
somebody tightened on purpose — at every boot, with no log line — is a
downgrade, not a migration. An operator who wants pb-offline's rules back clears
theirs and reboots.

**Required server-side schema conventions** (both helpers assume them; the client
requires them):

1. Every synced collection MUST have PocketBase's autodate `updated` field, set to
   update on both create and update. This is the sync clock. Without it nothing
   works.
2. Every synced collection SHOULD have a `${ownerField}` relation to the auth
   collection, and **exactly these** API rules:

   ```
   listRule   = "owner = @request.auth.id"
   viewRule   = "owner = @request.auth.id"
   createRule = "@request.body.owner = @request.auth.id"
   updateRule = "owner = @request.auth.id && @request.body.owner:isset = false"
   deleteRule = "owner = @request.auth.id"
   ```

   Collections configured with `filter: null` are exempt (shared reference
   data), and are then pull-only in practice.

   **Why the update rule is not the same as the other four.** PocketBase
   evaluates an `updateRule` as a FILTER against the record in its **existing**
   state. It does not re-evaluate the rule against the post-update record, and
   it does not restrict which fields the request body may set. Under a bare
   `owner = @request.auth.id`, then:

   > `PATCH /api/collections/projects/records/<mine>` with body
   > `{"owner": "<victim-id>"}` **succeeds**. The rule passed — the record was
   > mine when it was read — and the write hands it to somebody else. It then
   > appears in the victim's `listRule`-filtered pulls and replicates into their
   > pb-offline mirror on every device.

   The attacker loses read access in the same instant, so it is a one-way plant
   rather than exfiltration: content injected into another account, quota
   inflated on a per-record-priced product, and an integrity break in anything
   that trusts "this record is in my account, so I made it". `:isset = false`
   is the second clause that closes it — an update body may not so much as
   MENTION the owner field.

   `createRule` needs no such clause: PocketBase evaluates a create rule against
   the submitted record, so `@request.body.owner = @request.auth.id` refuses a
   create naming a foreign owner outright.

   The client half of the same boundary is §4.3: `create()` forces the session
   owner and `update()` refuses a patch that touches `${ownerField}` at all, so
   a hand-off is rejected locally rather than queued and dead-lettered. Because
   an update body may not carry the owner, the §5.7 create→update conversion
   strips it before retrying.
3. The tombstone collection MUST have fields:
   `collection` (text, indexed), `record_id` (text, indexed),
   `${ownerField}` (relation to the auth collection, indexed), plus PB's `created`
   autodate — and the autodate `updated` (the client pulls tombstones with the same
   cursor mechanism as anything else, so a tombstone MUST carry `updated`).
   Recommended index: `(owner, updated, id)`.
4. Tombstone API rules: `listRule` and `viewRule` = `${ownerField} = @request.auth.id`;
   `createRule`, `updateRule`, `deleteRule` = `nil` (hooks write it as superuser).
5. A collection whose records may become invisible to a user without being deleted
   (ownership transfer, rule change) is a **§6.5 hazard**; see there.

---

## 3 · The link watcher — self-healing connectivity

`net/connectivity.ts` owns one boolean (`online`) derived from three inputs. No
other module may decide the link is up or down.

**Invariants.**

1. Inputs, in decreasing authority:
   a. **A completed HTTP round trip** to PB (any request the engine made, success
      *or* an HTTP status — a 4xx proves the link works). Authoritative: up.
   b. **A network-level failure** of such a request (`TypeError` / fetch abort that
      is not a user cancel). Authoritative: down.
   c. **SSE connection state** from the PB SDK's realtime client: an open
      subscription implies up; a disconnect implies suspect, not down.
   d. **`navigator.onLine`** and its `online`/`offline` events. `false` is
      authoritative for down. `true` is a hint only — it means "the OS has an
      interface", not "PocketBase is reachable".
2. While down, the watcher MUST run a **health ping** on jittered exponential
   backoff between `backoff.min` and `backoff.max`. The ping SHOULD be
   `pb.health.check()`; implementations MUST NOT ping more often than
   `backoff.min` and MUST NOT hold more than one ping in flight.
3. The watcher MUST ping immediately, ignoring the backoff timer, on: a
   `navigator.onLine` `online` event, `document.visibilitychange` to visible, and
   any explicit `sync()`/`push()`/`pull()` call.
4. A transition down→up MUST emit `'online'` and MUST trigger the **reconnect
   sequence** (§3.1). A transition up→down MUST emit `'offline'` with its reason and
   MUST cancel in-flight pushes at the entry boundary (§5, invariant 9).
5. Backoff state MUST reset to `backoff.min` on any transition to up.
6. The watcher MUST NOT be the drainer's gate in non-drainer tabs: those tabs
   observe `online` from `_state` (§13) rather than pinging on their own.

### 3.1 The reconnect sequence

On every down→up transition, and on `sync()`, the drainer tab MUST run, in this
order, and MUST NOT interleave the steps:

1. **Drain `_pending`** in `seq` order until empty, dead-lettered, or interrupted
   by a link failure (§5).
2. **Cursor pull** every configured collection, then the tombstone collection
   (§6, §8). Tombstones pull **last** so a delete cannot be resurrected by a record
   page fetched afterwards in the same pass.
3. **Resubscribe SSE** for every configured collection (§9).
4. Set `lastSyncedAt`, status → `online`.

Ordering is normative. Pushing before pulling is what makes the queue the
authority on local intent; pulling before resubscribing is what closes the gap
between the last cursor and the first live event.

---

## 4 · Local storage

### 4.1 One database per owner

**Decision: per-owner IndexedDB databases, named `${BRAND.dbPrefix}_${appId}_${ownerKey}`,**
where `ownerKey` is the PB user record id, or `${BRAND.anonOwner}` (`"anon"`) for
the unauthenticated store. Not a shared database with an owner column.

Justification (this is a load-bearing choice, not a preference):

- **Sign-out wipe is atomic and total.** `Dexie.delete(name)` cannot leave a row
  behind. Row-scoped deletion across N tables plus a queue plus cursors is a sweep,
  and a sweep can be interrupted. Leaking one user's record into the next user's
  session is the worst failure this library could have; the storage layer is where
  it gets made impossible.
- **No query can leak.** With no owner column there is no owner predicate to forget.
  A `live()` query written by an application developer cannot accidentally observe
  another account's rows, because those rows are in a different database.
- **Account switching is free.** Multi-account applications (which PB's single
  `authStore` otherwise makes awkward) switch by closing one Dexie instance and
  opening another. Both accounts' offline data survives the switch, which a shared
  database with a wipe-on-switch policy cannot offer.
- **Lock and channel names scope naturally.** The drainer lock is per database name,
  so two accounts open in two tabs each get their own drainer with no extra design.

Accepted costs, stated plainly: reference data configured `filter: null` is
duplicated per account on the same device; and promotion of anonymous data is a
copy between databases rather than an UPDATE of an owner column (§14).

`db/open.ts` MUST expose open/close/wipe keyed by `ownerKey`, MUST hold at most one
open Dexie instance per engine instance, and MUST close the previous instance
before opening the next on an auth change.

### 4.2 Schema

Dexie version 1. Per configured collection, one table:

```
<collection>: 'id, *<user indexes…>'
```

`id` is the primary key and is always the PocketBase record id (§4.4). Mandatory
implicit indexes: none beyond `id` — user records are read by id, by full scan
through `find`/`liveRaw`, or by whatever `CollectionConfig.indexes` declares.

Engine tables:

```
_pending:    '++seq, [collection+recordId], recordId, collection, state'
_cursors:    'collection'
_deadletter: '++id, collection, recordId, at'
_meta:       '[collection+recordId], collection, state'
_state:      'key'
```

Row shapes:

```ts
type PendingOp = 'create' | 'update' | 'delete';

interface PendingEntry {
  seq: number;                 // auto-increment; THE ordering
  collection: string;
  recordId: string;            // always the final PB id — see §4.4
  op: PendingOp;
  payload: Record<string, unknown> | null;  // null for delete
  clientUpdatedAt: number;     // ordering + diagnostics only
  state: 'queued' | 'inflight';
  attempts: number;
  nextAttemptAt: number;       // 0 when immediately eligible
  lastError: PBOErrorShape | null;
}

interface CursorRow {
  collection: string;          // also the tombstone collection's own row
  updated: string | null;      // last applied server `updated`
  id: string | null;           // tie-break id at that `updated`
  lastPullAt: number | null;
}

interface DeadLetter {
  id: number;
  collection: string;
  recordId: string;
  op: PendingOp;
  payload: Record<string, unknown> | null;
  error: PBOErrorShape;        // the 4xx that parked it
  at: number;
}

interface MetaRow {
  collection: string;
  recordId: string;
  serverUpdated: string | null;  // null ⇒ never confirmed by the server
  clientUpdatedAt: number;
  lastPushedUpdated: string | null;  // echo suppression, §7
  state: RecordSyncState;
  lastError: PBOErrorShape | null;
}
```

`_state` holds exactly one row, `key: 'sync'`, carrying the `SyncState` shape plus
the drainer's tab id and a heartbeat timestamp. **It is written only by the drainer
tab** and read reactively (via `liveQuery`) by every tab — that, and not a
`BroadcastChannel` message, is what makes sync status observable in non-drainer
tabs with no extra machinery.

### 4.3 Writes

`create`/`update`/`delete` MUST, in one Dexie read-write transaction spanning the
collection table, `_pending` and `_meta`:

1. Apply the change to the collection table.
2. Update `_meta` (`clientUpdatedAt`, `state`).
3. Append or coalesce a `_pending` entry (unless `transient`).

and MUST resolve as soon as that transaction commits. They MUST NOT await any
network call, and MUST NOT be gated on `online`.

**Ownership is not a writable field.** With a session owner, `create()` MUST
FORCE `${ownerField}` to that owner, and MUST throw rather than accept a
different one — filling it in only when the caller left it blank makes the
owner caller-supplied, and spreading a fetched record into a "duplicate this"
feature is the ordinary way that happens. `update()` MUST refuse a patch that
mentions `${ownerField}` at all: reassigning it is a hand-off, not an edit, and
the server rule set of §2.6 refuses it too, so a patch carrying it would
otherwise apply to the mirror and dead-letter on the wire — the worst of the
three places to find out. Both throw `internal/invariant`.

**Coalescing** (`writeCoalesceMs`, default 250, `0` disables): an `update` MUST
merge into an existing `_pending` entry for the same `(collection, recordId)` when
that entry is `state: 'queued'`, is an `update` or a `create`, and was appended
within the window. Merging is a shallow field merge into `payload`; the entry keeps
its original `seq` (so ordering relative to other records is preserved) and its
`clientUpdatedAt` advances. This reproduces the reference implementation's
per-entity 800 ms save debounce as a queue-level behaviour, without making the
local write itself late.

Ordering rules that MUST hold:

- A `delete` MUST remove every queued `create`/`update` entry for that record and
  then append its own entry — except when a `create` for that record has never been
  pushed, in which case the whole record's queue collapses to nothing and no
  request is ever made (an offline create-then-delete is a no-op on the server).
- A `create` MUST NOT coalesce into a `delete`, and vice versa.
- `batch()` MUST write all ops in one transaction and MUST append their entries
  contiguously.

**Transient writes** (`{transient: true}`) update the collection table and `_meta.clientUpdatedAt`
and enqueue nothing. They MUST NOT set the record `dirty`, and the next remote
version of the record overwrites them per §10. This is the mechanism for live-mode
ticks and other derived values that must never become server state.

### 4.4 Record ids: client-generated, no rewriting

**Decision: the client generates the PocketBase id at create time and sends it in
the create request body.** There is no `localId`, and there is no id-rewriting pass.

PocketBase accepts a client-supplied `id` on record creation provided it matches
the collection's id pattern (default: 15 characters of `[a-z0-9]`). `ids.ts` MUST
generate ids from `crypto.getRandomValues` over the configured alphabet, MUST
default to 15/`abcdefghijklmnopqrstuvwxyz0123456789`, and MUST validate any
caller-supplied `id` against `CollectionConfig.idFormat` at `create()` time,
throwing `config/bad-id` rather than enqueuing a request that will 400.

Justification:

- **Relations resolve immediately.** A child record created offline can reference a
  parent created offline in the same session, with no fixup. Under localId
  rewriting, every payload in the queue and every foreign key in every mirror table
  has to be rewritten when the parent's create succeeds — a sweep across unrelated
  tables, mid-drain, that must itself be transactional and idempotent. That sweep is
  the single largest source of bugs in the alternative design, and this decision
  deletes it outright.
- **Retry after an ambiguous failure is decidable.** A create whose response was lost
  (timeout after the server committed) retries and returns 400 on the unique `id`.
  The engine reads that specific failure as "already created" and converts the entry
  to an `update` (§5, invariant 7) rather than dead-lettering a write the user made.
  Where the earlier attempt is known to be ambiguous, the engine asks the server
  first and converts before sending (§5, invariant 12).
  With server-assigned ids that same failure is indistinguishable from a genuine
  validation error, and the honest response is a duplicate record.
- **Tombstones, cursors and the mirror all key on one id for a record's whole life.**
  No table ever holds a temporary key.

Collision probability at 36¹⁵ ≈ 2.2 × 10²³ is not a practical concern; PocketBase's
own ids are drawn from the same space.

**This is a deliberate divergence from the migration plan's §5.2**, which described
`{localId, serverId?}` in the queue. The divergence is the recommendation, taken.
The rest of §5.2 is implemented as written.

---

## 5 · Push — draining `_pending`

Only the drainer tab (§13) executes this. Invariants:

1. Entries MUST be processed in strictly ascending `seq` order, one request at a
   time. No parallelism. The queue is a log of user intent and reordering it can
   violate referential integrity the application relies on.
2. Before each request the engine MUST re-read the entry from Dexie (another tab may
   have coalesced into it) and MUST mark it `state:'inflight'`.
3. Request mapping: `create` → `pb.collection(c).create(payload)` with `id` in the
   payload; `update` → `pb.collection(c).update(id, payload)`; `delete` →
   `pb.collection(c).delete(id)`.
4. `pb.autoCancellation(false)` MUST be set on the SDK instance by the engine at
   `start()` — the PB SDK cancels concurrent requests to the same collection by
   default, which would silently kill a drain that overlaps an application read.
   The engine MUST document that it does this.
5. **On success**, in one Dexie transaction: delete the entry; write the server's
   returned record into the mirror **only if** the record is not dirty from a *later*
   queue entry (a newer `_pending` entry for the same record means the user has moved
   on — merge server-authored fields but keep the queued local fields); set
   `_meta.serverUpdated` and `_meta.lastPushedUpdated` to the response's `updated`
   (§7); recompute `_meta.state`. Emit `push` (ok) and — for `create`/`update`
   only — `record-synced` (a delete response carries no `updated` to report).
   A `delete` success removes the mirror row and the `_meta` row.
6. **On 4xx (except the cases in 7 and 8)**: move the entry to `_deadletter`,
   set `_meta.state = 'dead'`, emit `dead-letter`, set status `error`, and **continue
   with the next entry**. A 4xx is a permanent rejection of that specific mutation;
   halting the whole queue for it would strand every unrelated write behind it.
   The engine MUST NOT silently drop it. The local mirror row is **left as the user
   left it** — the library never reverts a user's data behind their back; the
   application decides what to do with a dead letter.
7. **On 400 for a `create` whose failure identifies the `id` as already taken**, the
   entry MUST be converted in place to an `update` and retried once. If that update
   also fails, normal 4xx handling applies.
8. **On 404 for an `update` or `delete`**, the record is already gone server-side.
   The engine MUST treat it as success for a `delete` (idempotent), and for an
   `update` MUST dead-letter with code `push/vanished` — an update to a record the
   server no longer has is data the user will otherwise lose silently.
9. **On 5xx, 429 or a network failure**: leave the entry `queued`, increment
   `attempts`, set `nextAttemptAt` from the backoff, **stop the drain**, and hand
   control to the link watcher. Later entries MUST NOT be attempted — they may
   depend on this one. The backoff ladder is the wait for `429` too: the PB JS
   SDK exposes no response headers, so a `Retry-After` header is unreachable
   without wrapping the application's own SDK instance, and the engine MUST NOT
   do that.
10. **On 401/403**: if `pb.authStore` is no longer valid, treat as a link condition
    (stop, wait for re-auth) and emit `offline` with reason `auth`. If the token is
    valid, it is a rules rejection: dead-letter per 6.
11. A drain MUST be re-entrant-safe: at most one drain runs per engine instance, and
    a `push()` called during a drain returns the in-flight drain's promise.
12. **An ambiguous create asks before it is re-sent** (added 7 Oct 2026). A `create`
    is ambiguous when an earlier request for it may have reached the server without
    an answer being recorded: the entry is found `inflight` when the drain picks it
    up (its sender died or navigated away mid-request), or `attempts > 0`. Before
    re-sending it the engine MUST read `getList(1, 1, {filter: id = <id>})` — a list
    read, so a miss is a 200 and not a 404 in the console. A row with that id ⇒
    the entry is converted to an `update` in place (invariant 7's conversion, the
    entry kept `inflight`) and sent as one; no row ⇒ the create is sent; a link
    failure ⇒ invariant 9; any other refusal of the read ⇒ the create is sent as
    before and invariant 7 still applies. Why: invariant 7 relies on the server
    answering a taken id with a 400, and a create hook that refuses the record's
    SHAPE first (an application's "one of these per account" rule, which counts the
    row the lost request made) answers 403 instead, which invariant 10 would
    dead-letter as a rules rejection.
13. **A refused write's row outlives a resync** (added 8 Oct 2026). Invariant 6
    leaves the mirror row as the user left it; a full resync (§11) MUST NOT be the
    thing that drops it. Step 5 of §11 re-applies every dead letter's `create`
    payload (and an `update`'s over the row the pull brought back, never as a stub)
    with `_meta.state = 'dead'` and its `lastError`. Why: a create refused with a
    4xx exists nowhere but this mirror, and a resync that cleared the tables and
    re-applied only `_pending` made it vanish with no word.

```ts
export interface PushResult {
  pushed: number; failed: number; deadLettered: number; remaining: number;
  stoppedBy: 'empty' | 'link' | 'auth' | 'stopped';
}
```

---

## 6 · Pull — the `(updated, id)` cursor

1. Request shape, per collection:

   ```
   GET /api/collections/{c}/records
       ?page=1&perPage={pullPageSize}
       &sort=updated,id
       &filter=({userFilter}) && (updated > "{cur.updated}" ||
                (updated = "{cur.updated}" && id > "{cur.id}"))
       &skipTotal=1
   ```

   With no cursor yet, the `updated`/`id` clause is omitted entirely.
2. **The tie-break on `id` is mandatory.** A batch write stamps many records with an
   identical `updated`; a bare `updated > cursor` either loops on them forever or
   skips the tail of the page. `sort=updated,id` plus the compound predicate is the
   only correct form, and `util/cursor.ts` MUST be the one place it is built.
3. Page until a page returns fewer than `pullPageSize` rows. Advance the cursor to
   the `(updated, id)` of the **last row of each applied page**, written in the same
   Dexie transaction as that page's records. A crash mid-pull therefore resumes from
   the last fully applied page and never from the start.
4. Each incoming record is applied per §10 (apply-if-newer, dirty-protected) after
   passing echo suppression (§7).
5. `updated` values are compared as **strings**. PocketBase's autodate format
   (`YYYY-MM-DD HH:MM:SS.sssZ`) is fixed-width and zero-padded, so lexicographic
   order is chronological order. Implementations MUST NOT parse to `Date` for
   comparison — parsing introduces a rounding boundary that the server's own
   filter does not have, and the two disagreeing is a skipped record. The
   fixed-width premise is enforced: exactly three fractional digits. A page whose
   last row carries no well-formed `updated` MUST raise `pull/malformed` for that
   collection instead of advancing the cursor — advancing on a malformed value
   corrupts the cursor stream.

### 6.5 When the server stops returning a record

A record can leave a user's pull results without being deleted: an API rule change,
an ownership transfer, a changed `filter`. Normative handling:

- **Absence from a pull page MUST NEVER cause a local delete.** Deletes come from
  tombstones (§8) and from local intent, and from nothing else. A cursor pull is an
  incremental change feed, not a snapshot, and cannot distinguish "unchanged" from
  "no longer visible".
- Such a record therefore goes stale in the mirror. The remedies, in order: a
  tombstone if the server actually deleted it; `staleAfterMs` forcing a periodic
  full resync; the manual `fullResync()`. Applications with mutable visibility
  SHOULD set `staleAfterMs`.
- **If the pull request itself is rejected** as a rules/configuration fault
  (403 with a still-valid session, or 400 from a filter the collection's rules
  reject), the engine MUST halt sync **for that collection only**, record the
  error on `_state.lastError`, set status `error`, and emit `error` with code
  `pull/denied`. Other collections MUST keep syncing. This is a configuration
  fault — wrong `filter`, missing `listRule` — and the engine's job is to say so
  loudly rather than to retry a request that will never succeed. A **401**, or a
  403 when `pb.authStore` is no longer valid, is NOT a halt: it is a link/auth
  condition handled exactly as §5.10 handles it on the push side (stop, wait for
  re-auth). A halt MUST clear on link re-establishment as well as at `start()` —
  a fixed server rule must not require an app restart to be noticed.
- A pushed record that the server accepts but the user cannot then read back (write
  rule broader than read rule) is a server misconfiguration. The engine MUST leave
  the local row in place with `serverUpdated` from the push response and MUST NOT
  loop trying to fetch it.

```ts
export interface PullResult {
  byCollection: Record<string, { applied: number; skipped: number; deleted: number; pages: number }>;
  fullResyncTriggered: boolean;
}
```

---

## 7 · Echo suppression

The direct analogue of the reference implementation's "record it as already
persisted before applying".

1. `_meta.lastPushedUpdated` holds the `updated` value the server returned for this
   client's most recent successful push of that record. It MUST be written in the
   same transaction as the push success (§5, invariant 5).
2. An incoming record from **either** a pull page or an SSE event MUST be dropped
   before it reaches the mirror if `remote.updated === meta.lastPushedUpdated`.
   That value is, by construction, this device's own write coming back.
3. Dropping an echo MUST still advance the pull cursor past that record (it was
   consumed) and MUST NOT emit `record-synced` a second time.
4. `lastPushedUpdated` MUST be cleared when a newer remote `updated` for that record
   is applied, and on full resync.
5. **Known, accepted cost**: `updated` has millisecond resolution, so a
   *different device's* write landing in the same millisecond as this device's
   last push of the same record is indistinguishable from the echo and is
   dropped until something else touches the record. This is the honest price of
   a single ms-resolution guard and is documented in the README; it MUST NOT be
   patched with a second guard.
6. **This is the only echo guard.** The engine MUST NOT additionally suppress by
   timestamp window, by request id, or by comparing payload equality. The reference
   implementation's invariant — *the sync clock round-trips unchanged; only one
   party stamps it* — holds here in its PocketBase form: **only the server stamps
   `updated`, and the client never writes it**. Every other guard would need its own
   correctness argument, and two guards can disagree.

---

## 8 · Tombstones

1. The server helper installs `OnRecordAfterDeleteSuccess` on every configured
   collection and writes one row to the tombstone collection:
   `{collection, record_id, owner, created, updated}`. It MUST copy the owner from
   the deleted record's `ownerField` so the client's owner filter reaches it. If the
   deleted record has no such field, the helper writes the tombstone **unowned** and
   logs a warning — an unrecorded delete is unrecoverable, while an unowned row is
   inert to owner-filtered clients (it never matches their filter).
2. The client treats the tombstone collection as an ordinary synced collection for
   pull purposes: same `(updated,id)` cursor, same paging, its own `_cursors` row.
3. Applying a tombstone means: delete the mirror row for `(collection, record_id)`,
   delete its `_meta`, and **cancel any queued `_pending` entries for it that are not
   `create`** — an update to a record deleted elsewhere is dead on arrival. A queued
   `create` for that id MUST be preserved: id reuse after a delete is legal, and the
   user's create is newer intent than a remote delete of a previous incarnation.
   Emit `pull` with a nonzero `deleted`.
4. **A tombstone MUST NOT delete a record with pending local mutations other than as
   in 3.** If the record is dirty, the engine MUST emit `conflict` with
   `resolution: 'remote'` and then apply the delete — a delete is not mergeable, and
   losing it silently is worse than losing an edit loudly.
5. Tombstones are pulled **after** the record collections in every pass (§3.1).
6. **Retention.** The server prunes tombstones older than `Options.Retention`
   (default 30 days). A client whose tombstone cursor is older than the retention
   window has provably missed deletes and MUST full-resync (§11).
7. **A stale tombstone is ignored.** A tombstone (or an SSE `delete`) whose
   `updated` is OLDER than the `serverUpdated` the mirror holds for that id is a
   previous incarnation's: the id was deleted, made again (3), and the new
   create confirmed before the old delete was seen. It MUST NOT be applied:
   doing so drops a row the server still holds, and the record cursor has
   already moved past it.

---

## 9 · Realtime (SSE)

1. Granularity: **one subscription per configured collection, `'*'`, with the
   collection's owner filter**, plus one for the tombstone collection. Never a
   collection-wide unfiltered subscription: PocketBase evaluates every connected
   client's subscriptions on every mutation, so an unfiltered subscription on a busy
   collection is a server-side cost multiplied by the number of connected clients.
2. Subscribing is done **only by the drainer tab** (§13). Other tabs receive the
   effects through Dexie's cross-tab `liveQuery`. A per-tab SSE connection would
   multiply the server's connection count by the user's tab count for no benefit.
3. An SSE event is applied through **exactly the same path** as a pulled record:
   echo suppression (§7) then §10. There is no SSE-specific apply path.
4. **An SSE event MUST NOT advance the pull cursor.** SSE delivery is unordered with
   respect to the cursor stream and can drop events across a reconnect; advancing the
   cursor from an event would skip every record between the cursor and that event.
   This is the single most consequential rule in this section.
5. SSE is an optimisation for freshness and **never the only path**. Every reconnect
   runs the cursor pull (§3.1) regardless of whether SSE reported anything.
6. `delete` actions arriving over SSE MAY be applied directly as local deletes
   (per §8.3–8.4) — the event carries the same authority as a tombstone. The
   tombstone pull remains mandatory: the SSE delete is what the client sees when
   connected, and the tombstone is what it sees when it was not.
7. Subscriptions MUST be torn down on `stop()`, on auth change, and on losing the
   drainer lock.

---

## 10 · Applying a remote record — LWW against a server-stamped clock

This is where PocketBase's autodate `updated` being **server-stamped** is
reconciled with last-write-wins. The problem is real: after a local edit and before
a successful push, the client cannot know what `updated` the server will assign, so
it has no comparable value of its own.

The resolution is to **never compare a client value with a server value.** Only
server-minted `updated` strings are ever ordered against each other, and the client's
own unpushed intent is protected by the queue rather than by a timestamp.

Algorithm for an incoming remote record `R` in collection `c`, with local row `L`
and meta `M`:

1. **Echo** — if `R.updated === M.lastPushedUpdated`, drop (§7). Done.
2. **Dirty protection** — if `_pending` holds any entry for `(c, R.id)`, the local
   row MUST NOT be overwritten. Emit `conflict` with `resolution: 'local'`, record
   `M.serverUpdated = R.updated` (so the client knows what it is about to overwrite),
   and leave the mirror alone. The queued mutation will reach the server, the server
   will stamp it later than `R.updated` by construction, and that later value becomes
   the winner **through the same mechanism as every other write** — no special case.
3. **Absent** — if `L` does not exist and no tombstone has been applied for it in
   this pass, insert `R`, set `M.serverUpdated = R.updated`, `M.state = 'synced'`.
   Emit `record-synced`.
4. **Clean local** — compare `R.updated` with `M.serverUpdated` as strings:
   - `R.updated > M.serverUpdated` → apply `R` wholesale, set `M.serverUpdated`,
     emit `record-synced`.
   - `R.updated === M.serverUpdated` → no-op.
   - `R.updated < M.serverUpdated` → no-op. (Possible when SSE and a pull page race;
     the older event loses.)
   - `M.serverUpdated === null` and `L` exists → the local row is a transient-only or
     never-confirmed row; apply `R` (server data beats unconfirmed local data that
     nothing has queued).
5. **Conflict hook** — when configured, `onConflict` is invoked *only* in case 2,
   before the queue's mutation is pushed, and may return a merged record. Signature:

   ```ts
   export type ConflictResolver = (ctx: {
     collection: string;
     local: PBRecord;              // the mirror row, with queued mutations applied
     remote: PBRecord;             // what the server currently has
     pending: PendingEntry[];      // this record's queued mutations, in order
   }) => PBRecord | 'local' | 'remote' | Promise<PBRecord | 'local' | 'remote'>;
   ```

   Returning a record replaces the queued mutation's payload with a field-merge of
   the returned record; `'remote'` discards the queued mutations for that record and
   applies `R`; `'local'` is the default behaviour. The hook MUST NOT perform network
   I/O; it MUST be pure and fast.

**Why this is safe.** Every ordering decision the engine makes compares two strings
minted by one clock — the PocketBase server's. Client clock skew cannot promote or
demote a write; the client clock only orders that device's own queue, which is FIFO
by `seq` anyway and needs no wall clock at all. Two devices editing the same clean
record concurrently both push; the server stamps them in arrival order; the later
arrival's `updated` is greater and wins on both devices. That is last-write-wins,
honestly stated: **it is last-write-*to-reach-the-server*-wins**, not
last-edit-in-wall-time-wins, and applications with a different requirement must
supply `onConflict` or not use this library.

Field-level merge is **not** the default and v0 ships none (§16).

---

## 11 · Full resync

The escape hatch that bounds the worst case of any bug above.

Triggers — automatic:

1. The tombstone cursor is older than the server's retention window. The engine
   learns the window from `staleAfterMs` if configured, and otherwise infers the
   condition from `_cursors.lastPullAt` for the tombstone collection being older
   than 30 days (the documented default). Either way the condition MUST be checked
   before every pull pass.
2. `staleAfterMs` elapsed since the last successful pull. **Warning documented
   to users**: because trigger 1 infers the retention window from `staleAfterMs`
   when set, a small value (say 60s) makes every short offline spell a FULL
   resync of the whole mirror. Choose it as "how stale may the mirror get before
   a from-scratch refetch is worth it", never as a freshness knob — freshness is
   SSE + reconnect pulls.
3. A pull returns a cursor the server rejects as malformed (a schema change under
   the client's feet).

Manual: `fullResync(reason?)`.

Procedure — normative:

1. Emit `full-resync {phase:'start'}`; status → `syncing`.
2. **Drain `_pending` first if the link is up.** Local intent is not what is being
   resynced. If it cannot drain, the queue is **preserved through the resync**, not
   discarded.
3. In one Dexie transaction: clear every collection table, `_cursors`, and `_meta`.
   MUST NOT clear `_pending` or `_deadletter`.
4. Pull every collection from a null cursor, then the tombstone collection.
   Tombstones from before the wipe are harmless no-ops.
5. Re-apply every dead letter (state `dead`, §5 invariant 13), then any surviving
   `_pending` entries, to the mirror so the UI does not blink the user's unsent or
   refused work out of existence, then drain.
6. Emit `full-resync {phase:'done'}`.

A failure at any step leaves the mirror empty-but-consistent and emits
`{phase:'failed'}`; the next reconnect retries from step 3.

---

## 12 · Error taxonomy

```ts
export class PBOError extends Error {
  readonly code: PBOErrorCode;
  readonly retryable: boolean;
  readonly cause?: unknown;
  readonly context?: Record<string, unknown>;
}
export interface PBOErrorShape { code: PBOErrorCode; message: string; retryable: boolean; at: number }
```

| Code | Retryable | Meaning / engine response |
|---|---|---|
| `config/unknown-collection` | no | `collection(name)` for a name not in config. Throws at call site. |
| `config/bad-id` | no | Caller-supplied id fails the collection's id format. Throws at `create()`. |
| `config/invalid` | no | Bad config at `createPBOffline`. Throws synchronously. |
| `auth/unauthenticated` | no | Write attempted with `anonymousMode:'disabled'` and no session. |
| `auth/changed-mid-flight` | yes | Owner changed during an operation; the operation is abandoned. |
| `link/offline` | yes | The link is down. Not an error state; informational. |
| `link/timeout` | yes | A request exceeded its deadline. |
| `push/rejected` | no | 4xx from the server. Dead-letters the entry. |
| `push/vanished` | no | 404 on update — the record no longer exists. Dead-letters. |
| `push/conflict` | no | 4xx identified as an id collision, after the §5.7 retry failed. |
| `pull/denied` | no | 403/400 on a pull. Halts that collection (§6.5). |
| `pull/cursor-stale` | yes | Cursor predates tombstone retention. Triggers full resync. |
| `pull/malformed` | yes | Unparseable page. Triggers full resync for that collection. |
| `db/open-failed` | no | IndexedDB unavailable (private mode, quota, corruption). Fatal to the instance. |
| `db/quota` | yes | Write rejected for quota. Surfaced; the app must free space. |
| `db/version-conflict` | no | Another tab holds an older schema version open. |
| `lock/unavailable` | yes | Web Locks unavailable; the lease fallback is in use (§13). |
| `internal/invariant` | no | A bug in this library. MUST include enough context to reproduce. |

Every error the engine surfaces MUST carry one of these codes. Implementations MUST
NOT throw bare `Error` across the public API.

---

## 13 · Multi-tab

1. Exactly one tab per open database performs **push, pull and SSE**. Election is via
   the Web Locks API: the drainer holds `${BRAND.lockPrefix}:drain:${dbName}`
   indefinitely; other tabs `request` the same lock and become drainer if the holder
   goes away (tab closed, crashed, navigated). No polling, no heartbeat, no
   split-brain — the browser owns the liveness question.
2. **Every tab is fully reactive** with no extra work: Dexie's `liveQuery` observes
   IndexedDB changes across tabs, so a record written by the drainer's pull re-emits
   in every tab's `live()` query.
3. **Every tab may write.** Writes go to Dexie and `_pending` from any tab; the
   drainer picks them up. After a local write a non-drainer tab SHOULD post a
   `BroadcastChannel` nudge (`${BRAND.channelPrefix}:${dbName}`, message `{t:'nudge'}`)
   so the drainer wakes immediately rather than on its next tick. The nudge is an
   optimisation; correctness MUST NOT depend on it (the drainer polls `_pending` on
   a timer as well).
4. Sync **state** is observable in every tab because the drainer writes it to
   `_state` and every tab `liveQuery`s that row (§4.2). `isDrainer` distinguishes
   them.
5. **Fallback where Web Locks is unavailable** (older Safari, insecure context): a
   lease row in `_state` — `{drainerTabId, heartbeatAt}` — claimed by compare-and-set
   inside a Dexie transaction and renewed every 2 s; a lease unrenewed for 6 s may be
   stolen. Implementations MUST prefer Web Locks and MUST emit `lock/unavailable`
   once when falling back. The fallback is best-effort: a duplicate drain under it is
   possible and is survivable because every push is either idempotent (client-supplied
   ids, §4.4) or resolved by §5.7.
6. On losing the drainer role a tab MUST abort its SSE subscriptions and MUST NOT
   start a new drain; an in-flight request is allowed to complete and record its
   result (its Dexie transaction is the serialisation point).

---

## 14 · Auth-awareness and promotion

1. The engine subscribes to `pb.authStore.onChange`. The **owner identity** is
   `pb.authStore.record?.id ?? null`.
2. On owner change (`A → B`, `A → null`, `null → B`) the engine MUST, in order:
   stop the drain, tear down SSE, close the current Dexie instance, open the database
   for the new owner, emit `auth-change`, and (if authenticated) run the reconnect
   sequence. It MUST NOT wipe the outgoing owner's database — that is `signOut()`'s
   job and only `signOut()`'s (§15). Merely switching accounts preserves both.
   Collection handles and the reactive reads built from them survive this: they
   follow the incoming owner's mirror without being rebuilt, and MUST NOT keep
   showing the outgoing owner's rows across the switch (§2.3, "Reactive reads
   outlive the mirror").
3. **Retention (`staleOwnerDays`).** Because clause 2 never wipes the outgoing
   owner, a device accumulates one unencrypted copy of every account that has
   ever signed in on it, for ever. `staleOwnerDays` is the opt-in eviction: at
   `start()`, every `${dbPrefix}_${appId}_*` database that is neither the
   current owner's nor the anonymous one, and that has not been opened for that
   many days, is deleted. `0` means "the moment somebody else starts the
   engine". The default is `null` — off — because how long a second account's
   offline work may sit on a shared machine is the application's call, not the
   library's. The anonymous mirror is NEVER swept: only `promoteAnonymousData()`
   may consume it and the engine cannot know whether the application still
   intends to. A mirror with no last-opened stamp (one written before this
   option existed) is stamped rather than deleted — unknown age is not old. The
   sweep is best-effort: a database another tab holds open is logged and left,
   and never fails `start()`.
4. `anonymousMode: 'local'` (default) opens the `anon` database when there is no
   session. Every read and write works; nothing is ever pushed; `status` is
   `offline` with `ownerId: null` and this MUST be documented as the honest state —
   the data is local, and the engine will not pretend otherwise.
5. **Promotion.** `promoteAnonymousData()` copies the `anon` database's records into
   the signed-in owner's database and enqueues a `create` for each:
   - It MUST be explicit — the engine MUST NOT promote automatically, because
     "the last person to use this browser signed in" is not consent to upload their
     work to a new account.
   - `strategy: 'copy'` (default) leaves the anonymous database intact;
     `'move'` deletes it after a successful copy.
   - Each promoted record keeps its id (they are already PB-compatible, §4.4), gains
     `ownerField = <new owner id>`, and is enqueued as a `create`. A collision with an
     existing server record resolves through §5.7.
   - Records are promoted in configured collection order, so parents precede children.
   - It MUST be a no-op returning `{promoted: 0}` when the anonymous database is
     absent or empty.

```ts
export interface PromotionResult { promoted: number; byCollection: Record<string, number>; strategy: 'copy' | 'move' }
```

---

## 15 · Sign-out

`signOut({flushTimeoutMs = 5000, force = false})`:

1. Attempt a drain bounded by `flushTimeoutMs`.
2. If the queue empties and `_deadletter` is empty: close and `Dexie.delete()` the owner's database, clear
   in-memory state, emit `auth-change`. Return `{drained: true, wiped: true, pending: 0}`.
3. If it does not, or if `_deadletter` holds a write after the drain (refused
   before it or during it — parked, not pushed, and the only copy): **do not
   wipe** unless `force`, and mark the mirror (4). Return
   `{drained, wiped: false, pending: N}`, where N counts queued AND parked
   writes and `drained` says whether `_pending` emptied, so the application can
   tell the user that N changes stay on the device. `force: true` wipes anyway, and is the escape
   hatch for a shared or borrowed machine, where the device matters more than
   the N unpushed changes do.
4. **An unwiped mirror MUST be marked.** The application is about to clear its
   token and forget the sign-out ever happened; the mirror must not. A
   `signout` row in `_state` (inside the database it describes, so a later wipe
   erases it for free) records the attempt, and the next `start()` for that
   owner MUST retry: drain again, and wipe once the queue is empty, then open a
   fresh mirror for the session that is now running. The retry waits
   `max(flushTimeoutMs, 5000)`: a sign-out that could not wait at all (no
   link, `0`) says nothing about the session now running. A queue that still
   will not drain keeps the marker rather than forcing the wipe — destroying
   unpushed work is the caller's decision (`force`), never a retry's.

   The retry wipes only when `_pending` AND `_deadletter` are both empty. A
   write the server refuses during the retry's drain is parked, which empties
   the queue, and a wipe would destroy the only copy of it. With a dead letter
   the mirror stays open for the owner who is signed in again (who sees the
   parked write as on any other day) and the marker is CLEARED: that sign-out
   is over, and a later `start()` must not wipe a mirror its owner is using.

   Without this, "sign out" on a dead link leaves a complete copy of the
   account on the device permanently, while the UI reports a completed
   sign-out and nothing left running remembers otherwise.
5. `signOut()` MUST NOT call `pb.authStore.clear()` — token lifetime is the
   application's decision. It MUST be safe to call before or after the app clears the
   auth store.

```ts
export interface SignOutResult { drained: boolean; wiped: boolean; pending: number }
```

---

## 16 · Non-goals (v0)

Explicitly out of scope. A PR implementing one of these is a scope change, not a
feature, and needs the decision reversed first.

1. **No CRDTs, no operational transforms, no peer-to-peer.** The conflict model is
   LWW plus a hook, and the hook is where an application puts its own semantics.
2. **No automatic field-level merge.** Two devices editing different fields of one
   record concurrently: the later push wins the whole record. Applications needing
   per-field merge write it in `onConflict` for the specific records that need it.
3. **No non-PocketBase backends.** The `(updated,id)` cursor, the tombstone hook API
   and the SSE semantics are PocketBase's. A generic adapter layer would cost more
   than it buys and would be a different library.
4. **No React (or Vue, or Solid) package in v0.** The Svelte store contract plus
   `useSyncExternalStore` is six lines in the README. A framework package is a second
   release cadence and a second bug surface.
5. **No file/attachment sync.** PB file fields sync as filenames; the bytes are not
   mirrored. Offline file upload needs its own queue and its own quota story.
6. **No server-side query pushdown.** `live()` queries run over the mirror. If a
   collection is too large to mirror, this library is the wrong tool for it.
7. **No schema discovery or migration generation.** The application declares its
   collections; the server helper only creates the tombstone collection.
8. **No encryption at rest.** IndexedDB is as private as the browser profile.
9. **No auth UI, no token refresh scheduling.** The `pocketbase` SDK owns auth.

---

## 17 · Packaging

**Decision: one npm package for the client; the server helpers ship separately and
are not npm artifacts.**

- The client is a single package with one entry point. Splitting it into
  `core` / `svelte` / `react` workspaces would triple release overhead to publish a
  store adapter that the store contract (§2.5) already makes unnecessary. There is no
  second consumer that needs an independent version.
- **The Go module lives at `server/go/` with its own `go.mod`** and is consumed as
  `github.com/<org>/<repo>/server/go` — Go modules are fetched from a repository
  path, so a Go consumer never downloads the TypeScript and an npm consumer never
  downloads the Go. One repository, two ecosystems, no monorepo tooling.
- **The JSVM hook is a single file copied by hand.** PocketBase's `pb_hooks/` has no
  package manager; a file to copy is the idiomatic distribution and keeps the hook
  auditable in the consumer's own repo.
- `examples/` are not published (`.npmignore`), and are not workspaces — they pin the
  published package name and are built against the built `dist/`, so they exercise
  what a consumer actually gets.

Published files: `dist/`, `README.md`, `LICENSE`, `SPEC.md`. Peer dependencies:
`pocketbase` `^0.28`, `dexie` `^4`. Both are peers, not dependencies: two copies of
Dexie means two IndexedDB connection pools and two `liveQuery` observers that cannot
see each other, and two copies of the PB SDK means two auth stores.

### 17.4 The dual-peer tripwire (added v0.1 after first-consumer feedback)

Peer resolution does what the paragraph above assumes for a normal install. It does
**not** for `file:` or `npm link`: the linked directory carries its own
`node_modules`, its devDependency copy of `dexie` is right there, and a bundler
resolving from the real path takes it. The result is the silent failure — writes
reach IndexedDB, no observer registered by the app's copy ever hears about them, and
the UI simply stops updating. Nothing throws and no request fails.

`createPBOffline` therefore MUST, synchronously and with no I/O:

1. Read `globalThis[Symbol.for('<brand>.dexie')]`. If unset, stamp it with this
   copy's `Dexie` constructor and return.
2. If it holds this copy's constructor, return.
3. Otherwise warn — **once per page**, guarded by a second `globalThis` flag so N
   copies cannot report one problem N times — naming the hazard, its symptom, and
   the README's "Consuming via file: / link" section. The warning goes to
   `PBOfflineConfig.logger` at `level: 'warn'` when one is configured, and to
   `console.warn` otherwise.

The stamp is never replaced: the first copy loaded stays the record. The guard is a
diagnostic and MUST NOT throw, refuse to construct, or change any other behaviour —
a mismatched peer is a consumer build problem, and the engine's job is to say so
rather than to fail closed on a heuristic.

---

## 18 · Conventions for implementers

1. No module outside `src/net/connectivity.ts` decides whether the link is up.
2. `_pending` entries are appended/coalesced only by `src/collections/crud.ts`
   (§4.3) and consumed/state-transitioned only by `src/sync/push.ts` (§5) — with
   the narrow exceptions the spec itself grants: tombstone application cancels
   queued entries (§8.3), §5.7 converts a create to an update in place,
   dead-letter retry re-enqueues at the tail (§2.2), and a `'remote'`/merged
   `onConflict` outcome rewrites or drops that record's entries (§10.5). No
   `_cursors` writes outside `src/sync/pull.ts` + `src/sync/tombstones.ts`.
3. Every Dexie write that touches more than one table MUST be in one transaction.
4. Every public async method MUST be safe to call concurrently with itself.
5. No `setInterval` outside `sync/engine.ts` and `net/connectivity.ts`.
6. No string containing the package name outside `src/brand.ts` (§0).
7. Every invariant in this document that can be asserted in a test MUST have one;
   `TESTPLAN.md` maps them.
