/**
 * Vitest setup: a real-enough IndexedDB in Node (TESTPLAN §1).
 *
 * `fake-indexeddb` implements the spec including cross-connection change
 * notification, which is what lets the unit tier exercise Dexie's `liveQuery`
 * without a browser. It does NOT implement cross-TAB behaviour — the multi-tab
 * scenarios (S6) belong to Playwright, not here.
 */
import 'fake-indexeddb/auto';

// TODO: reset databases between test files so a leaked db name cannot make one
// test depend on another's ordering.
