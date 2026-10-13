# Running the tests

Three tiers, per `TESTPLAN.md` §1–3. This file documents invocation and the
harness's moving parts; the *contract* each test asserts lives in `TESTPLAN.md`.

## Layout

| Path | What |
|---|---|
| `test/unit/**` | vitest, `fake-indexeddb` + a scripted PocketBase fake. No network. |
| `test/integration/**` | vitest against a **real PocketBase binary** on an ephemeral port. |
| `test/harness.ts` | The harness's public surface (`startHarness`, `hasPocketBase`). |
| `test/harness/pb-server.ts` | Its implementation: process, temp `pb_data`, fixture schema, severable proxy. |
| `test/fixtures/schema.json` | The integration fixture collections (`projects`, `tasks`, `tags`). |
| `scripts/fetch-pb.mjs` | Downloads and checksum-verifies the pinned PocketBase binary into `.cache/`. |

## Commands

```sh
node scripts/fetch-pb.mjs      # fetch + verify the pinned binary (idempotent)
npm run test:unit
npm run test:integration       # `pretest:integration` fetches the binary first
npm test                       # unit + integration

cd server/go && go vet ./... && go test ./...   # the Go server helper
```

### npm scripts to merge into `package.json`

`package.json` is owned elsewhere; these are the lines the server-side work
needs. `test:integration`, `pretest:integration` and `pretest:e2e` already exist
and are unchanged.

```json
"pb:fetch": "node scripts/fetch-pb.mjs",
"test:server": "cd server/go && go vet ./... && go test ./..."
```

If `test:server` is added to CI, note that it needs a Go toolchain (1.27+, which
PocketBase v0.40 requires) but **not** the fetched binary — the Go tests use
PocketBase's own `tests.NewTestApp`.

`vitest.config.ts`'s integration project now includes `test/integration/**/*.spec.ts`
alongside `*.test.ts`.

## What the harness actually starts

`startHarness()` gives every call its own everything — there is no shared
instance and no fixed port:

1. A fresh `pb_data` under `os.tmpdir()`, removed on `stop()`.
2. A seeded superuser (`pocketbase superuser upsert`), before the server starts.
3. A copy of the **shipped** `pb_hooks/pb-offline.pb.js` in the instance's
   `pb_hooks/`, with `CONFIG.collections` rewritten to the collections that run
   syncs. `--hooksWatch=false`, so a file-watcher restart can never be mistaken
   for a link drop.
4. `pocketbase serve` on a free port, waited on until `/api/health` answers.
5. The fixture schema from `test/fixtures/schema.json`, applied over the admin API.
6. A TCP passthrough proxy in front of PocketBase. `pb.url` points at the proxy;
   `sever()` destroys every live socket and refuses new ones **without stopping
   the process**, which is what a dropped link looks like to a client. A clean
   shutdown is not — that distinction is the point (SPEC §3).

```ts
const pb = await startHarness();
pb.url;               // http://127.0.0.1:<ephemeral>
pb.adminToken;        // superuser token
const user = await pb.createUser();   // { id, email, password, token }
await pb.sever(); await pb.restore();
await pb.stop();
```

## Where server-helper behaviour is tested

The two server helpers are required to behave identically (SPEC §2.6), and they
are tested in the two places that can reach them:

- **Go module** — `server/go/pbo_test.go`, against PocketBase's own
  `tests.NewTestApp`. Tombstone-on-delete with the record's owner, cascade
  deletes, unregistered collections, `Register` idempotency, the created schema,
  and the prune window (including a batch boundary). Run with
  `cd server/go && go test ./...`.
- **JSVM helper** — the integration tier, because the harness boots the real file
  in the real binary. `test/integration/harness.spec.ts` asserts the collection
  it creates at bootstrap, the tombstone it writes on delete, and that one user
  cannot list another's tombstones.

The harness does **not** fake tombstones. If `pb_hooks/pb-offline.pb.js` breaks,
the integration tier fails — which is the whole reason it is copied rather than
reimplemented.

## Skipping

Integration specs call `describe.skipIf(!hasPocketBase())`, so a checkout that
has never run `scripts/fetch-pb.mjs` reports them skipped instead of failing.
Run the fetch (or `npm run test:integration`, which does it for you) to actually
exercise them.
