# Contributing to pb-offline

Thank you for looking in. pb-offline is meant to be built by more than one
person, and contributions of every size are welcome: a bug report with a
failing test, a doc fix, a new integration scenario, or a feature from the
roadmap in `TESTPLAN.md`.

## Licence of contributions

pb-offline is released under the MIT licence (see `LICENSE`). By submitting a
pull request you agree that your contribution is licensed under the same MIT
licence, with no further terms. There is no contributor agreement to sign.

If you prefer to make that explicit per commit, add a sign-off line
(`git commit -s`), which appends `Signed-off-by: Your Name <you@example.com>`.
It is appreciated but not required.

## Before you start

- Read `SPEC.md`. It is the contract: every behaviour the engine promises has
  a numbered section there, and tests and pull requests refer to those
  numbers. A change in behaviour is a change in the spec first.
- Read `TESTPLAN.md`. It lists the scenarios each tier covers and the ones
  still open. An open scenario is a good first contribution.
- Open an issue before a large change so the design can be discussed. Small
  fixes can go straight to a pull request.

## Setting up

```sh
git clone https://github.com/HenningH87/pb-offline.git
cd pb-offline
npm install
npm run pb:fetch        # downloads and verifies the pinned PocketBase binary into .cache/
npm run typecheck
npm test                # unit + integration (the integration tier starts a real PocketBase)
npm run test:server     # the Go server helper; needs a Go toolchain
```

`test/README.md` explains the harness and how to run one tier at a time. The
example app in `examples/svelte-todo/` is the quickest way to see the engine
in a browser.

## Making a change

1. Branch from `main`.
2. Add or change the test first. Every fixed bug gets a regression test that
   names the spec section it guards; every new behaviour gets a spec section
   and a test.
3. Keep the public API in `src/index.ts` small. Prefer an option on an
   existing call over a new entry point.
4. Run `npm run typecheck`, `npm test` and, if you touched `server/go/`,
   `npm run test:server`. Pull requests are checked the same way.
5. Write the commit message for the next reader: what changed and why, with
   the spec section in the first line when one applies.

## Pull requests

- One topic per pull request. A refactor and a behaviour change are two.
- Say which spec sections the change touches and how you tested it.
- Expect a review that asks questions. Discussion is how the design stays
  coherent; it is not a verdict on the work.

## Reporting a bug

Open an issue with the PocketBase version, the SDK and Dexie versions, the
browser, and the smallest sequence of operations that reproduces it. A
failing unit test under `test/unit/` is the best report there is.

## Code of conduct

Be kind, be specific, and assume good faith. Disagreements are about the
code, never about the person. Anyone who cannot keep to that will be asked
to leave the project.
