/**
 * Anonymous → owned promotion (SPEC §14.4).
 *
 * With one database per owner, promotion is a copy between databases rather than
 * an UPDATE of an owner column. That is the accepted cost of the isolation model,
 * and it buys an explicit, inspectable, resumable operation.
 *
 * Sign-out (SPEC §15) is NOT here: it needs the live drain, the open handle and
 * the event emitter, all of which belong to the engine handle, so it lives in
 * `index.ts` next to them.
 */
import Dexie from 'dexie';
import type { PBRecord, PendingEntry, PromotionResult, ResolvedConfig } from '../types.js';
import type { PBODexie } from '../db/schema.js';
import { BRAND, dbNameFor } from '../brand.js';
import { deleteDatabase, openForOwner } from '../db/open.js';
import { writeMeta } from '../db/meta.js';

export interface PromotionOptions {
  strategy?: 'copy' | 'move';
  /**
   * The owner's already-open database. The engine passes its own handle so the
   * copy lands in the instance the application is reading from, rather than in a
   * second connection whose writes it would only see through liveQuery.
   */
  targetDb?: PBODexie;
}

/** Fields the client never authors — the server owns this clock (SPEC §10). */
function payloadOf(record: PBRecord, ownerField: string, ownerId: string): Record<string, unknown> {
  const out: Record<string, unknown> = { ...record };
  delete out['created'];
  delete out['updated'];
  out[ownerField] = ownerId;
  return out;
}

/**
 * Copy the anonymous database's records into the signed-in owner's database and
 * enqueue a create for each.
 *
 *   - EXPLICIT ONLY. The engine never calls this on its own: "the last person to
 *     use this browser signed in" is not consent to upload their work to a new
 *     account.
 *   - 'copy' (default) leaves the anonymous database intact; 'move' deletes it
 *     only after a successful copy.
 *   - Each record keeps its id (already PB-compatible, SPEC §4.4), gains the owner
 *     field, and is enqueued as a create. A collision with an existing server
 *     record resolves through SPEC §5.7 — the create comes back as a taken id, is
 *     converted to an update in place, and is retried once.
 *   - Records are promoted in CONFIGURED COLLECTION ORDER, so parents precede
 *     children and a relation is never queued before its target.
 *   - A no-op returning `{promoted: 0}` when the anonymous database is absent or
 *     empty — including its side effects: an empty 'move' deletes nothing.
 */
export async function promoteAnonymousData(
  cfg: ResolvedConfig,
  ownerId: string,
  opts?: PromotionOptions,
): Promise<PromotionResult> {
  const strategy = opts?.strategy ?? 'copy';
  const result: PromotionResult = { promoted: 0, byCollection: {}, strategy };

  const anonName = dbNameFor(cfg.appId, BRAND.anonOwner);
  if (!(await Dexie.exists(anonName))) return result;

  const anon = await openForOwner(cfg, BRAND.anonOwner);
  let target: PBODexie | undefined = opts?.targetDb;
  let opened: { db: PBODexie } | undefined;

  try {
    // Read everything first: an empty anonymous database must not so much as open
    // the target, let alone delete itself under 'move'.
    const byCollection = new Map<string, PBRecord[]>();
    let total = 0;
    for (const c of cfg.collections) {
      const rows = (await anon.db.table(c.name).toArray()) as PBRecord[];
      byCollection.set(c.name, rows);
      total += rows.length;
    }
    if (total === 0) return result;

    if (!target) {
      opened = await openForOwner(cfg, ownerId);
      target = opened.db;
    }

    const now = Date.now();
    for (const c of cfg.collections) {
      const rows = byCollection.get(c.name) ?? [];
      if (rows.length === 0) {
        result.byCollection[c.name] = 0;
        continue;
      }
      const table = target.table(c.name);
      await target.transaction('rw', table, target.pbo.pending, target.pbo.meta, async () => {
        for (const row of rows) {
          const id = String(row.id);
          const payload = payloadOf(row, cfg.ownerField, ownerId);
          await table.put({ ...payload, id });
          await writeMeta(target!, c.name, id, {
            clientUpdatedAt: now,
            state: 'dirty',
            serverUpdated: null,
            lastPushedUpdated: null,
            lastError: null,
          });
          // `seq` is Dexie's auto-increment: the queue's order is the promotion's
          // order, which is the configured collection order (parents first).
          const entry: Omit<PendingEntry, 'seq'> = {
            collection: c.name,
            recordId: id,
            op: 'create',
            payload: { ...payload, id },
            clientUpdatedAt: now,
            state: 'queued',
            attempts: 0,
            nextAttemptAt: 0,
            lastError: null,
          };
          await target!.pbo.pending.add(entry as PendingEntry);
        }
      });
      result.byCollection[c.name] = rows.length;
      result.promoted += rows.length;
    }

    if (strategy === 'move') {
      anon.db.close();
      try {
        // BOUNDED. `deleteDatabase` blocks for as long as another tab holds the
        // anonymous database open, and multi-tab is a first-class feature (§13).
        await deleteDatabase(anonName);
      } catch {
        // The copy already succeeded and is queued; throwing here would lose the
        // result and invite a re-promotion that duplicates the queue. What
        // actually happened is a copy, so SAY so — `strategy` is the honest
        // report, and the anonymous database is still there to move later.
        result.strategy = 'copy';
      }
    }
    return result;
  } finally {
    // 'move' already closed it; closing twice is a no-op in Dexie.
    anon.db.close();
    opened?.db.close();
  }
}
