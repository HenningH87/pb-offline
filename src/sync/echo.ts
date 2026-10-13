/**
 * Echo suppression (SPEC §7).
 *
 * The direct analogue of the reference implementation's "record it as already
 * persisted before applying". `_meta.lastPushedUpdated` holds the `updated` value
 * the server returned for THIS device's most recent successful push of a record;
 * anything arriving with that exact value is this device's own write coming back.
 *
 * THIS IS THE ONLY ECHO GUARD. Do not add suppression by timestamp window, by
 * request id, or by payload equality. The invariant that makes one guard
 * sufficient is that only the server ever stamps `updated`, and the client never
 * writes it.
 */
import type { MetaRow, PBRecord } from '../types.js';
import type { PBODexie } from '../db/schema.js';
import { readMeta, writeMeta } from '../db/meta.js';

/**
 * The pure predicate, so the decision can be made from a meta row already read
 * inside a transaction without a second round trip to Dexie.
 */
export function isEchoOf(meta: Pick<MetaRow, 'lastPushedUpdated'> | undefined, remoteUpdated: unknown): boolean {
  const known = meta?.lastPushedUpdated ?? null;
  if (known === null) return false;
  return typeof remoteUpdated === 'string' && remoteUpdated === known;
}

/**
 * True when `remote` is this device's own write returning.
 *
 * A dropped echo MUST still advance the pull cursor (it was consumed) and MUST
 * NOT emit a second 'record-synced' — both are the caller's job, and both are
 * asserted in TESTPLAN §1.7.
 */
export async function isEcho(db: PBODexie, collection: string, remote: PBRecord): Promise<boolean> {
  const meta = await readMeta(db, collection, String(remote.id));
  return isEchoOf(meta, remote.updated);
}

/**
 * Record what the server returned for a successful push.
 *
 * MUST run in the same transaction as the push success (SPEC §7.1) — a crash
 * between the two would leave the client treating its own write as a remote one.
 */
export async function recordPush(
  db: PBODexie,
  collection: string,
  recordId: string,
  serverUpdated: string | null,
): Promise<void> {
  await writeMeta(db, collection, recordId, {
    serverUpdated,
    lastPushedUpdated: serverUpdated,
  });
}

/** Clear on applying a strictly newer remote `updated`, and on full resync. */
export async function clearEcho(db: PBODexie, collection: string, recordId: string): Promise<void> {
  await writeMeta(db, collection, recordId, { lastPushedUpdated: null });
}
