# TESTPLAN

Companion to `SPEC.md`. Every normative invariant in the SPEC that can be asserted
is mapped here to a test. A change to the SPEC that is not accompanied by a change
here is incomplete.

Three tiers, three commands:

| Tier | Runner | Environment | Command |
|---|---|---|---|
| Unit | vitest | `fake-indexeddb`, mocked `PocketBase` | `npm run test:unit` |
| Integration | vitest | `fake-indexeddb`, **real PB binary** on an ephemeral port | `npm run test:integration` |
| Browser scenarios | Playwright | real Chromium, real IndexedDB, real PB, real tabs | `npm run test:e2e` |

`npm test` runs unit + integration. `test:e2e` is separate because it needs
browsers installed.

---

## 1 · Unit tier

`fake-indexeddb` provides IndexedDB; the `pocketbase` SDK is replaced by a
hand-written fake (`test/fakes/pb.ts`) that records requests, replays scripted
responses, and can be told to fail with a given status, to hang, or to drop the
"connection". No network, no binary, no timers longer than the test.

### 1.1 `ids.ts` — SPEC §4.4

- generates 15 chars from `[a-z0-9]` by default; 10k samples, no character outside
  the alphabet, no length drift.
- honours a custom `idFormat`.
- `validateId` accepts PB-shaped ids and rejects uppercase, punctuation, wrong length.
- `create({id})` with an invalid id throws `config/bad-id` **before** any Dexie write
  (assert the table is untouched).
- uses `crypto.getRandomValues` (spy), never `Math.random`.

### 1.2 `util/time.ts`, `util/cursor.ts` — SPEC §6.2, §6.5

- string comparison of PB `updated` values orders chronologically across: sub-second
  digits, midnight, month and year rollover, and the fixed-width padding boundaries.
- **regression:** comparison never parses to `Date` (spy on the `Date` constructor).
- cursor filter construction: no cursor ⇒ filter has no `updated` clause; with a
  cursor ⇒ exactly the `(updated > X || (updated = X && id > Y))` disjunction, and
  the user filter is parenthesised around it (a bare `&&` with an `||` inside is the
  classic precedence bug — assert the parentheses literally).
- cursor advances to the last row of a page, not the max `updated` on the page.

### 1.3 `collections/crud.ts` — SPEC §4.3

- `create`/`update`/`delete` resolve without any network call being made (assert the
  fake PB recorded zero requests).
- one Dexie transaction covers table + `_pending` + `_meta` (assert atomicity by
  failing the `_meta` write and observing the table roll back).
- **coalescing:** two updates inside `writeCoalesceMs` produce one `_pending` entry
  with merged payload and the original `seq`; outside the window, two entries.
- `noCoalesce` forces a second entry.
- coalescing never merges across a `delete`, and never merges `create` into `update`
  in a way that loses the create.
- **create-then-delete offline collapses to nothing** — zero `_pending` entries, zero
  requests ever made.
- `delete` removes queued updates for that record but keeps a queued `create` (SPEC
  §8.3 companion case).
- **transient writes** update the table, do not enqueue, do not set `dirty`, and are
  overwritten by the next remote apply.
- `batch()` writes one transaction and contiguous `seq` values.
- **ownership is forced, never accepted (SPEC §4.3, added after the 10 Sep audit):**
  `create()` with a session owner rejects a caller-supplied owner naming anybody
  else — before any Dexie write — and accepts a redundant one naming the session
  owner; `update()` (and the same op inside a `batch()`) rejects any patch that
  mentions `ownerField` at all; with no session owner the field is left alone.

### 1.4 `collections/query.ts` + `store/readable.ts` — SPEC §2.5

- `subscribe` emits synchronously before returning, with the declared empty value
  when the first Dexie read has not resolved.
- unsubscribe stops delivery; no emission after it (assert with a deferred Dexie
  write).
- re-emission on **each** source: local create, local update, local delete, queue
  drain reconciliation, cursor pull apply, SSE apply, tombstone delete. Seven tests,
  one per source — this is the library's headline claim and it gets seven assertions.
- never emits a reference-equal value twice.
- `liveOne`, `liveCount`, `liveMeta`, `liveDirty` each re-emit on a relevant change
  and stay quiet on an irrelevant one.
- double-subscribe/unsubscribe is safe; the underlying `liveQuery` is released only
  when the last subscriber leaves.
- **`sort.localFallback` (SPEC §2.3, added v0.1):** an unsynced row interleaves into
  a date-shaped sort by its `clientUpdatedAt`, in both directions; PocketBase's space
  separator and ISO's `T` compare as the same instant; a non-date field partitions
  instead (unsynced trailing in `asc`, leading in `desc`); `''`/`null` are absences
  and `0` is a value; a missing `_meta` row reads as 0; ties break by `id`; the
  fallback applies before `limit`/`offset` so `find`, `count`, `live` and `liveCount`
  agree; a list re-emits and reorders when a local write moves `clientUpdatedAt`, and
  when only `_meta` moves; **omitting it leaves the default sort untouched.**
- **Reactive reads outlive the mirror (SPEC §2.3, added v0.1):** all six reactive
  reads (`live`, `liveOne`, `liveCount`, `liveRaw`, `liveMeta`, `liveDirty`)
  subscribed BEFORE the mirror opens emit the declared empty value and then come
  alive on the first write; the same six follow an account switch and show the new
  owner's writes while showing none of the outgoing owner's; `liveDirty` is not
  frozen by the session it was built in (§14.3); unsubscribing before the mirror
  ever opens leaks no revision listener and emits nothing afterwards; a rebuild
  delivers nothing to a handler that left mid-flight; a store nobody is subscribed
  to runs no querier on a revision step. `createDbManager` steps its revision on
  every identity change and only on one (re-opening the same owner does not). At
  engine level, through the public API only: a live query built before `start()`
  shows the first write; it follows `setOwner`; it empties on `signOut()`; and a
  second tab that subscribed before its OWN `start()` sees the first tab's write,
  cross-tab, with no extra wiring.

### 1.5 `sync/push.ts` — SPEC §5

- strict `seq` ordering, one request in flight at a time (the fake asserts no
  overlap).
- entry re-read from Dexie immediately before the request (another tab coalesced into
  it mid-drain).
- `pb.autoCancellation(false)` is called at `start()`.
- success path: entry removed, mirror updated, `serverUpdated` **and**
  `lastPushedUpdated` set to the response `updated`, `record-synced` emitted, all in
  one transaction.
- success where a **later** queue entry exists for the same record: server-authored
  fields merge, queued local fields survive.
- **4xx ⇒ dead-letter, continue with the next entry** (assert entry N+1 is attempted).
- 4xx leaves the local mirror row untouched (the library never reverts user data).
- **400 create-id-taken ⇒ converted to update and retried once**; a second failure
  dead-letters with `push/conflict`.
- 404 on delete ⇒ treated as success; 404 on update ⇒ `push/vanished` dead-letter.
- **5xx / network failure ⇒ stop the drain**, entry stays `queued`, `attempts`
  incremented, later entries untouched.
- 429 honours `Retry-After`.
- 401 with an invalid authStore ⇒ `offline` with reason `auth`, no dead-letter.
- concurrent `push()` calls return the same in-flight promise.
- `state:'inflight'` is cleared on every exit path including a thrown error.

### 1.6 `sync/pull.ts` — SPEC §6

- pages until a short page; cursor written in the same transaction as each page.
- crash mid-pull (reject on page 3) resumes from the end of page 2, not page 1.
- **tie-break:** a page where every row shares one `updated` value advances by `id`
  and the next request returns the remainder — the naive-cursor loop and the
  naive-cursor skip are both asserted absent, over a 3-page fixture.
- absence of a record from a page **never** deletes it locally (SPEC §6.5).
- 403 on a pull halts that collection only; other collections keep syncing; status
  `error`, code `pull/denied`.
- a pushed-but-unreadable record does not loop.

### 1.7 `sync/echo.ts` — SPEC §7

- an incoming record whose `updated` equals `lastPushedUpdated` is dropped before the
  mirror write.
- a dropped echo **still advances the cursor**.
- a dropped echo does not emit a second `record-synced`.
- `lastPushedUpdated` is cleared when a strictly newer remote `updated` is applied,
  and by `fullResync`.
- **echo never loops:** a scripted round trip — local write → push → SSE echo → pull
  echo — leaves the queue empty, produces exactly one push request, and emits exactly
  one `record-synced`. Run 50 iterations to catch a slow oscillation.

### 1.8 `sync/conflict.ts` — SPEC §10

Table-driven over the five cases:

| local | pending? | `M.serverUpdated` | `R.updated` | expected |
|---|---|---|---|---|
| absent | – | – | any | insert |
| present | yes | any | any | keep local, `conflict{resolution:'local'}`, `serverUpdated` recorded |
| present | no | `T1` | `T2 > T1` | apply remote |
| present | no | `T1` | `T1` | no-op |
| present | no | `T1` | `T0 < T1` | no-op |
| present | no | `null` | any | apply remote |

Plus: `onConflict` is invoked only in the pending case; returning `'remote'` discards
that record's queued mutations; returning a record merges into the queued payload;
a hook that throws routes to `'error'` and falls back to `'local'`.

**Property test:** for random interleavings of local writes and remote applies, the
final mirror row always equals the record whose server `updated` is greatest among
those actually pushed. 500 cases.

### 1.9 `sync/tombstones.ts` — SPEC §8

- applying a tombstone deletes the mirror row and `_meta`, cancels queued
  update/delete entries, **preserves a queued create**.
- a tombstone for a dirty record emits `conflict{resolution:'remote'}` and then
  deletes.
- tombstones are pulled after record collections in a pass (assert request order).
- a tombstone cursor older than retention sets `pull/cursor-stale` and triggers a full
  resync.

### 1.10 `sync/resync.ts` — SPEC §11

- clears collection tables, `_cursors`, `_meta`; **preserves `_pending` and
  `_deadletter`** (explicit assertion — the tempting bug is `db.tables.forEach(clear)`).
- surviving pending entries are re-applied to the mirror before the drain, so a
  `live()` subscriber never observes the user's unsent work disappear (assert the
  emitted sequence contains no empty frame for that record).
- failure mid-resync emits `{phase:'failed'}` and leaves the mirror consistent;
  the next reconnect retries.
- `staleAfterMs` elapsed triggers a resync before the next pull.

### 1.11 `net/connectivity.ts` — SPEC §3

- authority order: a 4xx response marks the link **up** even though the request
  failed; `navigator.onLine === false` marks it down; `navigator.onLine === true`
  alone does not mark it up.
- backoff is jittered, bounded by `min`/`max`, resets to `min` on recovery.
- at most one ping in flight; no ping more often than `min`.
- an immediate ping on: `online` event, `visibilitychange` to visible, explicit
  `sync()`.
- down→up emits `'online'` and runs the reconnect sequence **in order**
  (drain → pull → tombstones → subscribe); assert the recorded request order.
- up→down emits `'offline'` with the right reason for each of the four sources.

### 1.12 `sync/locks.ts` — SPEC §13

- with Web Locks present, exactly one of N engine instances against one db name
  becomes drainer; releasing it promotes another.
- non-drainer instances perform no push, no pull, no SSE (assert zero requests).
- non-drainer writes still land in `_pending` and still emit a `BroadcastChannel`
  nudge.
- with Web Locks absent, the lease fallback elects one drainer, renews, and allows a
  steal after the expiry; `lock/unavailable` is emitted exactly once.
- losing the lock mid-drain aborts SSE and starts no new drain; the in-flight request
  is still allowed to record its result.

### 1.13 `auth/` — SPEC §14, §15

- owner change closes one db and opens another, in that order; the outgoing db is
  **not** wiped.
- `A → B → A` restores A's queue and mirror intact.
- anonymous mode `local`: reads and writes work, zero requests, `ownerId: null`.
- anonymous mode `disabled`: writes throw `auth/unauthenticated`.
- promotion is never automatic (assert no promotion on sign-in without the call).
- promotion copies in configured collection order, sets the owner field, enqueues
  creates, and is a no-op on an empty/absent anon db. `'move'` deletes the anon db
  only after a successful copy.
- `signOut` with an empty queue wipes; with a non-empty queue that cannot drain, does
  **not** wipe and reports `pending: N`; `force` wipes anyway.
- `signOut` does not call `authStore.clear()`.
- **the unwiped mirror is marked, and the next `start()` finishes the job**
  (SPEC §15, added after the 10 Sep audit): an offline `signOut` that reports
  `wiped: false` leaves a `signout` row in `_state`; a later engine for the same
  owner, with the link back, drains the queue to the server, deletes the old
  mirror and comes up on a fresh empty one with `started: true`. Still offline,
  the retry keeps both the marker and the data — destroying unpushed work is
  `force`'s decision, never a retry's.
- **`staleOwnerDays`** (SPEC §14.2, same audit): off by default (the sweep
  reports nothing at any age); with a window set, another owner's mirror older
  than it is deleted and a fresher one kept; the CURRENT owner's and the
  ANONYMOUS mirror are never touched even at `0`; a mirror with no `opened`
  stamp is stamped rather than deleted, and evicted on a later pass — unknown
  age is not old.

### 1.14 `errors.ts`, `config.ts`, `brand.ts`

- every error thrown across the public API is a `PBOError` with a code in the taxonomy
  (walk the API with fault injection).
- invalid config throws `config/invalid` synchronously from `createPBOffline`.
- `createPBOffline` performs no I/O (spy on `indexedDB.open` and the fake PB).
- **brand tripwire:** a source scan asserts the package name string appears in no file
  under `src/` other than `brand.ts` (SPEC §0, §18.6).
- `deletionsCollection` is rejected unless it is a bare identifier — it is pasted
  into the prune query's `{{table}}` by both server helpers (added after the
  10 Sep audit).
- an `idFormat` that could mint the reserved anonymous owner key (`anon`) is
  rejected: that user's mirror would BE the anonymous sandbox (SPEC §4.1, §14.3).
- `staleOwnerDays` defaults to `null`, takes `0`, and rejects a negative.

### 1.15 `db/peer.ts` — SPEC §17.4 (added v0.1)

- one Dexie: `createPBOffline` stamps the constructor on `globalThis` and says
  nothing, however many engines are constructed.
- two Dexies: it warns exactly ONCE across every engine constructed afterwards, and
  the message names the symptom (`liveQuery`), the cause (`file:`) and the README
  section carrying the fix.
- a configured `logger` receives it at `level: 'warn'` and the console does not.
- the existing stamp is never replaced, and the check performs no I/O.

### 1.16 `SyncState.started` — SPEC §2.2 (added v0.1)

- false before `start()`, true after, and the transition is emitted by `state`.
- false again after `stop()`, after `destroy()`, after a `signOut()` that wiped, and
  after a `start()` that rejected; true again on a restart.
- **survives an auth change** (both directions, including to signed-out), and is true
  for a signed-out `anonymousMode: 'disabled'` engine that opened no database.
- never persisted: the `_state` row another tab reads carries `started: false`.

---

## 2 · Integration tier — a real PocketBase

### 2.1 The harness

`scripts/fetch-pb.mjs` downloads the PocketBase binary matching
`process.platform`/`process.arch` into `.cache/pocketbase/<version>/` and verifies
the published checksum. The version is pinned in `package.json` under
`pbOffline.pocketbaseVersion`. `.cache/` is gitignored; CI caches it by that key.

`test/harness/pb-server.ts` exposes:

```ts
startPB(opts?: { migrations?: string[] }): Promise<{
  url: string;            // http://127.0.0.1:<ephemeral>
  admin: { email: string; password: string };
  stop(): Promise<void>;
  /** Cut the socket without a clean close — simulates a link drop. */
  sever(): Promise<void>;
  restore(): Promise<void>;
}>
```

Each run gets a fresh `pb_data` in a temp dir, an ephemeral port (bind :0, read the
port back), a seeded superuser, the fixture schema, and the tombstone helper
installed. Teardown removes the directory. Tests MUST NOT share an instance across
files unless they declare `serial`.

Fixture schema (`test/fixtures/schema.json`): two owner-scoped collections with a
parent/child relation (`projects`, `tasks`), one shared collection with
`filter: null` (`tags`), plus the `deletions` collection created by the helper.

### 2.2 Server helper conformance — SPEC §2.6, §8

Run the **same** suite twice, once against the Go helper (compiled into a test PB
binary via `server/go/`'s example main) and once against `pb_hooks/pb-offline.pb.js`
in the stock binary. Behavioural parity is the point; a divergence fails both.

- `Register` creates the tombstone collection with the specified fields, indexes and
  rules; running it twice is idempotent.
- **rules are additive too (added after the 10 Sep audit):** a rule the operator
  wrote on an EXISTING tombstone collection survives the next boot and is reported
  as a warning naming both values; a rule that is unset is still set. A boot-time
  reset of a boundary somebody tightened on purpose is a downgrade, not a migration.
- the JSVM helper ships `ensureSchema: false` — a copied file can reach a running
  server before CONFIG is edited — and the harness turns it on the way an operator
  does, by editing the copy.
- deleting a record writes exactly one tombstone carrying the deleted record's owner.
- deleting a record in a non-configured collection writes none.
- a superuser delete still writes a tombstone.
- a cascade delete (relation with cascade) writes a tombstone per deleted record.
- the prune cron removes rows older than `Retention` and nothing newer.
- tombstone list rules: user A cannot list user B's tombstones.
- the tombstone collection carries `updated` and is cursor-pullable.

### 2.3 Engine against the real server

- create/update/delete round-trips; the mirror matches the server after each pass.
- `updated` values returned by the server are exactly what echo suppression compares
  (guards against a serialisation difference between the SDK and the REST payload).
- client-supplied ids are accepted by PB on create for the default id pattern
  **and** for a collection with a custom id length (SPEC §4.4).
- a duplicate-id create returns the 400 shape §5.7 keys on — this test is what pins
  the engine to PB's actual error body, and it MUST assert the body, not just the
  status.
- an owner-filtered pull returns only the caller's rows; the shared collection
  returns all.
- **the prescribed API rule set refuses a record hand-off** (`test/integration/
  rules.spec.ts`, added after the 10 Sep audit): with two real users, a
  `PATCH {owner: other}` on the attacker's own record is refused, the record stays
  theirs, and it never appears in the victim's list. Plain `fetch`, not the client
  library — what is under test is the SERVER's answer to a request the client
  would never send. Reverting `:isset = false` in `test/fixtures/schema.json`
  turns that refusal into a 200, and nothing else in the suite notices.
- rules rejection on pull produces `pull/denied` (temporarily tighten the listRule).
- `pullPageSize` above the server's list cap is clamped or errors visibly, not
  silently truncated.
- SSE: subscribe, mutate from a second SDK client, observe the event applied and the
  cursor **not** advanced (SPEC §9.4) — then pull and observe the cursor advance.
- token expiry mid-drain: the drain stops with reason `auth` and resumes after
  `authRefresh`.
- `sort.localFallback` against server-minted `created`: two synced records and two
  made offline order newest-first with the offline pair on top, and the order is
  unchanged once the drain gives them real server stamps (SPEC §2.3, added v0.1).

---

## 3 · Scenario matrix (the P4 gate)

These are the acceptance scenarios. Each MUST exist and MUST pass before the library
is considered functionally complete. Tier is noted per scenario; the Playwright ones
run against a real browser and a real PB with the network manipulated through CDP.

| # | Scenario | Tier | Asserts |
|---|---|---|---|
| S1 | **Write offline, drain on reconnect, in order** | E2E | Go offline (CDP `Network.emulateNetworkConditions` offline). Make 12 interleaved writes across two collections including a parent and its children. Reconnect. Assert: the server received the mutations in `seq` order; the parent create preceded every child create; the final server state equals the final local state; `pendingCount` went N→0; `status` walked `offline → connecting → syncing → online`. |
| S2 | **Delete on A is seen on B** | Integration + E2E | Two engine instances (two dbs, same user). A deletes a record while B is offline. B reconnects. Assert: B's mirror loses the row via the **tombstone pull** (assert the tombstone request happened and the record page did not carry it); B's `live()` subscriber re-emitted; and with B *online*, the same delete arrives over SSE and is applied identically. |
| S3 | **Stale cursor forces a full resync** | Integration | Set B's tombstone cursor to a timestamp older than the retention window. Trigger a pull. Assert: `pull/cursor-stale`, `full-resync{start}` then `{done}`, mirror rebuilt from scratch, `_pending` and `_deadletter` survived, and a record deleted during the offline window is absent afterwards. |
| S4 | **Echo never loops** | Unit + Integration | 50 rounds of local write → push → SSE echo → pull echo. Assert: exactly one request per write, exactly one `record-synced` per write, queue empty, and — the real target — the process terminates. A failing implementation hangs; the test MUST have a timeout that fails rather than a retry that masks it. |
| S5 | **500 queued mutations drain in order** | Integration | Offline, enqueue 500 mutations (mixed create/update/delete across two collections, including updates to records created in the same batch). Reconnect. Assert: exactly 500 requests (coalescing disabled for this test), strictly ascending `seq`, zero dead letters, final server state matches, and the drain completes within a declared budget. Also assert memory: the drain does not materialise all 500 payloads at once (spy on the Dexie cursor usage). |
| S6 | **Multi-tab, single drainer** | E2E | Three tabs on the same origin and user. Assert: exactly one holds the drainer lock; writes made in tabs 2 and 3 are pushed by tab 1; **all three** re-render from a pull applied by tab 1; closing tab 1 promotes another tab within one lock turnaround and the drain resumes with no lost and no duplicated mutation. Repeat with Web Locks stubbed out to exercise the lease fallback. |
| S7 | **Auth switch wipes / preserves correctly** | E2E | Sign in as A, create data, sign out with an empty queue → A's db is gone. Sign in as B → B sees only B's data (assert A's db name is absent from `indexedDB.databases()`). Separately: switch A→B **without** signing out → both databases exist and A's queue survives. Separately: sign out with an undrainable queue → not wiped, `pending: N` reported. |
| S8 | **SSE drop → pull backfill** | E2E | Subscribed and online. Sever the SSE connection only (leave HTTP up), mutate records from a second client during the gap, restore. Assert: the missed mutations arrive via the **cursor pull** on resubscribe, not via SSE; no record is skipped; the cursor is contiguous; and a record mutated during the gap and again after it is applied exactly once at its final value. |

Supporting scenarios, same gate:

| # | Scenario | Tier | Asserts |
|---|---|---|---|
| S9 | **Dead letter is surfaced, never dropped** | Integration | Queue a mutation the rules reject. Assert: it lands in `_deadletter`, `deadLetters` store emits, `status` becomes `error`, the **next** queued mutation still drains, the local row is unchanged, and `retryDeadLetter` re-enqueues it at the tail. |
| S10 | **Promotion** | E2E | Anonymous session, create data across both collections, sign in, call `promoteAnonymousData()`. Assert: records appear on the server owned by the new user, ids preserved, parents before children, anon db intact under `'copy'` and gone under `'move'`, and no promotion happened before the explicit call. |
| S11 | **Cold offline start** | E2E | Sync, close the tab, go offline, reopen. Assert: the app renders previously synced data with zero network requests, writes still work, and `status` is `offline` from the first frame. |
| S12 | **Quota / IndexedDB unavailable** | Unit | `indexedDB.open` rejecting produces `db/open-failed`, the engine reports it through `status: 'error'` and does not throw asynchronously into the app. A quota-exceeded write produces `db/quota` on the write's promise. |

---

## 4 · What is deliberately not tested

- PocketBase's own correctness (rules evaluation, autodate stamping). The integration
  tier asserts the *contract* it depends on — `updated` monotonicity per record and
  client-id acceptance — and nothing more.
- Dexie's `liveQuery` cross-tab delivery mechanism. S6 asserts the behaviour the
  library promises; it does not test Dexie internals.
- Performance beyond S5's declared budget. There is no benchmark suite in v0.

## 5 · CI

Unit + integration on every push (Linux, Node LTS + current). E2E on pull requests
and on `main`. The PB binary is fetched once and cached by the pinned version key.
The brand tripwire (§1.14) and `tsc --noEmit` run as part of unit.
