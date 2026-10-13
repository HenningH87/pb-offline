# pb-offline

> **Pre-release.** The client is feature-complete against `SPEC.md` — local
> layer, sync core, realtime, drainer election, auth switching, promotion —
> and covered by 407 unit and integration tests. The multi-tab browser
> scenarios (TESTPLAN S6, S11) still need the Playwright tier. Do not depend
> on this yet.

pb-offline is a TypeScript sync engine for [PocketBase](https://pocketbase.io).
It mirrors the collections you choose into IndexedDB via
[Dexie](https://dexie.org), serves every read and write from that mirror, and
reconciles with the server in the background. Deletes need one thing
PocketBase does not provide on its own: a server-side tombstone. A small Go
module or a single JSVM file installs it for you.

## 30 seconds

```ts
import PocketBase from 'pocketbase';
import { createPBOffline } from 'pb-offline';

const pb = new PocketBase('https://example.com');
const db = createPBOffline(pb, {
  appId: 'myapp',
  collections: [{ name: 'tasks' }],
});
await db.start();

const tasks = db.collection('tasks');

// re-emits on every local write, every pull, every realtime event
tasks.live({ sort: { field: 'created', dir: 'desc' } }).subscribe(render);

// resolves on the IndexedDB commit; this line needs no network
await tasks.create({ title: 'Ship it', owner: pb.authStore.record.id });
```

Turn the network off. That last line still works, and still shows up in
`render`. Turn it back on and the write reaches the server without anything
else in your app noticing.

## What it does

- **Reactive live queries.** `live()`, `liveOne()`, `liveCount()` and
  `liveRaw()` are Dexie `liveQuery` underneath, so they re-emit on any source
  of change: a local write, a queue drain, a cursor pull, a realtime event, a
  tombstone delete. No manual refetch, no cache invalidation to get wrong.
  Subscribe whenever you like: a live query built before `start()` shows the
  declared empty value and comes alive when the mirror opens, and one you keep
  across a sign-in follows the new account's mirror on its own. You never have
  to await `start()` before rendering, or rebuild a store on `auth-change`.
- **A sort key that works offline.** `created` and `updated` are the server's,
  so a record written offline has neither and a list sorted on one scatters
  your own newest writes. `sort: { field: 'created', dir: 'desc',
  localFallback: 'clientUpdatedAt' }` falls back to the record's local edit
  clock for exactly the rows the server has not stamped yet — they interleave
  in true time order rather than piling up at one end. The engine will not fake
  a server timestamp to achieve this; the semantics are in
  [SPEC.md §2.3](./SPEC.md).
- **Self-healing reconnect.** A connectivity watcher combines the browser's
  online/offline events, the realtime connection's own state, and a backoff
  health ping; `navigator.onLine` counts for "down" but never alone for "up."
  When the link returns, the engine drains the pending queue in order, pulls
  by cursor, re-applies tombstones, and resubscribes, in that order, without
  being asked.
- **An ordered offline queue.** Writes queue in `_pending` in the order you
  made them, coalesced per record inside a short window, and drained one
  request at a time. A 5xx or a dropped connection stops the drain rather
  than reordering it, because later writes can depend on earlier ones.
- **Tombstoned deletes.** PocketBase gives a client no way to learn a record
  was deleted out from under it. The server helper writes one row to a
  `deletions` collection on every delete; the client pulls it with the same
  cursor mechanism as everything else.
- **Last-write-wins, honestly stated.** Conflicts resolve on the server's own
  `updated` timestamp, not wall-clock time, which two devices never agree on.
  That makes it last-write-*to-reach-the-server*-wins, and an `onConflict`
  hook is there for the records where that isn't the right answer.
- **One drainer, every tab reactive.** Web Locks elect exactly one open tab
  to push, pull and hold the realtime subscription. Every tab still sees
  every change immediately, because Dexie's `liveQuery` crosses tabs on its
  own; no `BroadcastChannel` message has to carry the actual data.
- **Per-owner isolation.** Each signed-in user gets their own IndexedDB
  database, and signed-out use gets a separate one. Signing out deletes a
  whole database in one call; there's no owner column a query could forget
  to filter on. Switching accounts *keeps* the outgoing mirror, so offline
  work survives the switch — set `staleOwnerDays` if a device shouldn't
  accumulate other people's data (`0` evicts every other owner's mirror at
  the next start). A sign-out that can't drain leaves the mirror in place and
  marks it, and the next start for that owner finishes the job.
- **Anonymous-to-owned promotion, explicit.** Data made before sign-in stays
  in its own local database. It does not follow a new sign-in automatically.
  The app has to call `promoteAnonymousData()` itself, because the last
  person to use a browser signing in is not consent to upload their work to
  that account.

## Server setup

Every synced collection needs PocketBase's `updated` autodate field (the
sync clock) and, normally, an `owner` relation with these API rules:

```
listRule   = owner = @request.auth.id
viewRule   = owner = @request.auth.id
createRule = @request.body.owner = @request.auth.id
updateRule = owner = @request.auth.id && @request.body.owner:isset = false
deleteRule = owner = @request.auth.id
```

The update rule is the one to copy carefully. PocketBase checks an
`updateRule` against the record as it is *now* and never re-checks the
result, so under a bare `owner = @request.auth.id` anyone can `PATCH` a
record they own with `{"owner": "<somebody-else>"}` and plant it in that
person's account — where it shows up in their pulls and mirrors onto all of
their devices. `:isset = false` says an update body may not mention the
owner field at all. `createRule` needs no such clause; create rules are
evaluated against the record being submitted.

Then install the tombstone helper, in whichever form matches how you run
PocketBase.

Embedding PocketBase as a Go framework:

```go
import pbo "github.com/HenningH87/pb-offline/server/go"

pbo.Register(app, pbo.Options{
    Collections: []string{"tasks"},
    Retention:   30 * 24 * time.Hour,
})
```

Running the stock PocketBase binary: copy `pb_hooks/pb-offline.pb.js` into
your project's `pb_hooks/` directory and edit the `CONFIG` object at its top.
Same tombstone shape, same retention and prune-cron behavior, no build step.
`ensureSchema` ships **off** there — a copied file can reach a running server
before anyone has edited `CONFIG`, and that is not a moment to be mutating a
schema — so turn it on once `collections` names yours. Either helper leaves an
API rule you have written on the tombstone collection alone and warns instead
of resetting it.

Full field list, API rule conventions, and the reasoning behind the
tombstone design are in [SPEC.md §2.6](./SPEC.md) and [§8](./SPEC.md).

## Using it outside Svelte

Every store this library exposes is a Svelte store and nothing more:
`subscribe(fn)` calls `fn` synchronously with the current value and returns
an unsubscribe function. That's native in Svelte. In React, wrap it once:

```ts
function useStore<T>(store: Readable<T>): T {
  let value!: T;
  store.subscribe((v) => (value = v))();
  return useSyncExternalStore(store.subscribe, () => value);
}
```

In anything else, `const off = store.subscribe((v) => render(v))`.

## Consuming via `file:` / link

Both peer dependencies must resolve to **one** copy each. `dexie` matters most:
Dexie's `liveQuery` registry lives inside the module, so an observer registered
by copy A never hears about a write made through copy B. Two copies produce an
app where every write lands in IndexedDB correctly and nothing re-renders — no
error, no failed request, just a UI frozen at whatever it read first.

A registry install resolves peers from your tree and this never comes up. A
`file:` dependency or `npm link` does not: the linked directory carries its own
`node_modules`, its devDependency copy of Dexie is sitting right there, and a
bundler resolving from the real path takes that one. `createPBOffline` warns once
on the console when it detects this, but the fix is in your build config.

Pin the resolution in **both** places — TypeScript for types, the bundler and
installer for the runtime module:

```jsonc
// tsconfig.json — so `Dexie` is one type, not two structurally-identical ones
{
  "compilerOptions": {
    "paths": {
      "dexie": ["./node_modules/dexie"],
      "pocketbase": ["./node_modules/pocketbase"]
    }
  }
}
```

```jsonc
// package.json — npm 8.3+; use `resolutions` for yarn, `overrides` for pnpm
{
  "overrides": {
    "pb-offline": {
      "dexie": "$dexie",
      "pocketbase": "$pocketbase"
    }
  }
}
```

Vite resolves through the real path by default, so add the same pin there:

```ts
// vite.config.ts
resolve: {
  dedupe: ['dexie', 'pocketbase'],
  // needed as well when the package is a `file:` dependency
  alias: { dexie: fileURLToPath(new URL('./node_modules/dexie', import.meta.url)) },
}
```

The cheapest structural fix, if you control both repos, is to delete the linked
package's own `node_modules/dexie` after installing it — then there is only one
copy to find. To verify: `createPBOffline` stays silent, and a write made in one
tab re-renders a `live()` query in another.

## Limitations

No CRDTs, no operational transforms, no peer-to-peer sync. No automatic
field-level merge: two devices editing different fields of the same record
concurrently, the later push still wins the whole record, and an app that
needs per-field merge writes it in `onConflict`. No backend but PocketBase;
the `(updated, id)` cursor and the tombstone hooks are PocketBase's own
shape, and a generic adapter layer would be a different library. No React
package. The store contract above is the whole framework story for now. No
file-content sync; PocketBase file fields sync as filenames, not bytes. No
encryption at rest beyond what the browser profile already gives you. No
auth UI and no token-refresh scheduling. That's the `pocketbase` SDK's job,
not this one's.

Two sharp edges to know about, both deliberate. Echo suppression works on
PocketBase's millisecond-resolution `updated` stamp, so if another device
writes the same record in the same millisecond as your last push, that write
is treated as your echo and dropped until the record changes again — the
cost of keeping exactly one guard in the pipeline. And `staleAfterMs` does
double duty as the assumed tombstone-retention window: set it to a small
value and every short offline spell triggers a full wipe-and-refetch of the
mirror. Pick it as "how stale may the mirror get before refetching from
scratch is worth it" — freshness while online is already handled by SSE and
reconnect pulls.

The full list, with the reasoning behind each item, is
[SPEC.md §16](./SPEC.md).

## Repository layout

```
SPEC.md            the build contract: architecture, API, algorithms, invariants
TESTPLAN.md        unit / integration / scenario matrix
src/               the client package
server/go/         Go module: tombstone hooks, deletions migration, prune cron
pb_hooks/          JSVM equivalent of the above, one file to copy
examples/          runnable consumers, not published
```

For anyone contributing: `SPEC.md` is normative and `TESTPLAN.md` maps every
invariant in it to a test. Read those before a PR, not this file.

## Requirements

PocketBase v0.23+ (developed against v0.40.x), `pocketbase` JS SDK `^0.28`,
`dexie` `^4`. Both are peer dependencies, not bundled: two copies of either
would mean two auth stores or two IndexedDB observers that can't see each
other.

## Contributing

Contributions are welcome, from a failing test to a new tier of the test plan.
`CONTRIBUTING.md` has the setup, the rules and the licence of contributions
(MIT, the same as the project).

## License

MIT © 2026 Hendrik Henning
