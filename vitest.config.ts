import { defineConfig } from 'vitest/config';

/**
 * Two projects, deliberately separated (TESTPLAN §1, §2):
 *
 *  unit        — fake-indexeddb + a scripted PocketBase fake. No network, fast.
 *  integration — fake-indexeddb + a REAL PocketBase binary on an ephemeral port.
 *                `pretest:integration` downloads the binary into .cache/.
 *
 * Browser scenarios live in Playwright (`npm run test:e2e`), not here.
 */
export default defineConfig({
  test: {
    projects: [
      {
        test: {
          name: 'unit',
          include: ['test/unit/**/*.test.ts'],
          environment: 'node',
          setupFiles: ['test/setup/indexeddb.ts'],
        },
      },
      {
        test: {
          name: 'integration',
          include: ['test/integration/**/*.test.ts', 'test/integration/**/*.spec.ts'],
          environment: 'node',
          // SSE needs an EventSource; Node hides its own behind a flag, so the
          // integration tier installs a small fetch-backed one (TESTPLAN §2.3).
          setupFiles: ['test/setup/indexeddb.ts', 'test/setup/eventsource.ts'],
          // A real binary boots per file; give it room and keep files serial so
          // ephemeral ports and pb_data directories cannot collide.
          testTimeout: 30_000,
          hookTimeout: 30_000,
          // One worker, one file at a time: every integration file boots its
          // own binary, and ephemeral ports and pb_data directories must not
          // collide. (`poolOptions` was removed in vitest 4 — these are the
          // top-level options that replaced it.)
          maxWorkers: 1,
          minWorkers: 1,
          fileParallelism: false,
        },
      },
    ],
  },
});
