# Example: svelte-todo

A minimal Svelte 5 + Vite todo app. One collection, `todos`. It exists to make
the library's claims visible, not to look like a product — there is no
component library, no CSS framework, and no state management beyond the
stores `pb-offline` already gives you.

It imports the library by its package name, resolved by an alias in
`vite.config.ts` to the repo's own **built** `dist/`, exactly like a real
consumer would import from `node_modules` (SPEC §17). It does not reach into
`src/`.

```
examples/svelte-todo/
  src/
    lib/
      db.ts       the one createPBOffline call
      types.ts    the Todo record shape
    App.svelte    login form, todo list, sync status bar, dead-letter drawer
    main.ts
  vite.config.ts   aliases 'pb-offline' -> ../../dist/index.js
```

## What it shows

- **Anonymous-first.** Before you log in, the todo list already works — it
  reads and writes an `anon` IndexedDB database (SPEC §14.3) and pushes
  nothing. Add a few todos before signing in and watch them stay put.
- **`authWithPassword` + promotion.** Logging in switches the engine to a
  per-owner database automatically. Your anonymous todos do **not** follow
  you — that's a deliberate consent boundary (SPEC §14.4). Click
  **"Bring my offline todos"** to copy them in explicitly, via
  `promoteAnonymousData()`.
- **Local-first CRUD.** `create`, the checkbox (`update`), and delete all
  resolve against IndexedDB immediately — there is no spinner on a write
  because there is nothing to wait for.
- **A live status bar** bound to `status`, `pendingCount`, `online`, and
  `lastSyncedAt`. Nothing in the app calls `sync()`; the bar walks
  `offline → connecting → syncing → online` on its own.
- **A dead-letter drawer.** The "Simulate a rejected write" button creates a
  todo with empty text, which the collection's own API rule rejects. It lands
  in `deadLetters` with Retry and Discard, exactly as any other rejected
  write would.

## 1. Run PocketBase

Download a PocketBase binary matching this project's tested version
(`pocketbase serve` needs v0.23+; this repo is developed against v0.40.x) and
start it from an empty data directory:

```sh
./pocketbase serve
```

Open `http://127.0.0.1:8090/_/` and create an admin account.

### Install the tombstone hook

Copy the JSVM helper into your PocketBase project, then restart it:

```sh
cp ../../pb_hooks/pb-offline.pb.js ./pb_hooks/
```

Edit the `CONFIG` object at the top of that file so `collections` reads
`['todos']`, then restart `pocketbase serve`. It creates the `deletions`
collection and installs the delete hooks and prune cron for you.

**Go alternative** — if you embed PocketBase as a Go framework instead of
running the stock binary, skip the file copy and call the module before
`app.Start()`:

```go
import pbo "github.com/HenningH87/pb-offline/server/go"

pbo.Register(app, pbo.Options{
    Collections: []string{"todos"},
    Retention:   30 * 24 * time.Hour,
})
```

### Create the `todos` collection

In the admin UI (Collections → New collection → Base collection), name it
`todos`, and add these fields:

| Field | Type | Notes |
|---|---|---|
| `text` | Plain text | Required, min length 1 |
| `done` | Bool | |
| `owner` | Relation → `users` | Required, single, cascade delete off |

PocketBase adds `id`, `created` and `updated` automatically — `updated` is
the sync clock this library relies on; do not remove it.

Set the API rules (List/Search, View, Create, Update, Delete) all to:

```
owner = @request.auth.id
```

Create a normal user account under Collections → `users` → New record (or
sign up your own login screen — this example only ships the login form, per
SPEC §16.9: no auth UI is this library's job).

## 2. Run the example

From the **repo root** (not this directory), build the library once:

```sh
npm install
npm run build
```

Then, in this directory:

```sh
npm install
npm run dev
```

Open the printed localhost URL. If PocketBase is not on
`http://127.0.0.1:8090`, set `VITE_PB_URL` in a `.env` file in this
directory.

## 3. Try airplane mode

With the app open and a todo or two on screen, open DevTools → Network and
switch to **Offline** (or turn off Wi-Fi). Keep adding, checking off, and
deleting todos — every write still resolves instantly, and the status bar
drops to `offline`. Nothing is queued to memory only: reload the page while
still offline and the todos and the pending count survive, because they were
never anywhere but IndexedDB.

Turn the network back on. The status bar walks
`connecting → syncing → online` without you touching anything, the pending
writes drain in the order you made them, and the todos you added while
offline show up in the PocketBase admin UI.

## Building for production

```sh
npm run build
npm run preview
```
