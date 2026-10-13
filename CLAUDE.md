# pb-offline — notes for coding agents

Offline-first sync engine for PocketBase: a local-first Dexie mirror, reactive
queries, and a self-healing link to the server. MIT licensed; contributions
welcome (`CONTRIBUTING.md`).

## Where the truth lives

- `SPEC.md` is the contract. Every behaviour has a numbered section; tests,
  commits and pull requests cite those numbers. Change the spec before the
  behaviour.
- `TESTPLAN.md` lists the scenarios per tier and the ones still open.
- `test/README.md` explains the harness (unit tier on fakes, integration tier
  against a real PocketBase binary fetched by `scripts/fetch-pb.mjs`).
- `server/go/` is the Go helper that installs the tombstone collection;
  `pb_hooks/pb-offline.pb.js` is the same for the JSVM.

## Working rules

- Every fixed bug gets a regression test that names its spec section before it
  counts as fixed.
- Keep the public API in `src/index.ts` small; prefer an option on an existing
  call over a new entry point.
- Run `npm run typecheck`, `npm test` and, for `server/go/`, `npm run
  test:server` before committing.
- Never commit `.cache/`, `pb_data/` or `dist/` (see `.gitignore`).
