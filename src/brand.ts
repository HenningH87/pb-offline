/**
 * The ONLY place the package name reaches runtime (SPEC §0, §18.6).
 *
 * Every Dexie database name, Web Lock name, BroadcastChannel name, localStorage
 * key and error-code prefix derives from here. Renaming the project is an edit to
 * this file plus package.json, LICENSE and README.md — nothing else in `src/` may
 * contain the name as a literal, and a unit test asserts that (TESTPLAN §1.14).
 */
import { PBOError } from './errors.js';

export const BRAND = {
  /** Short, lowercase, no punctuation. Used to build storage identifiers. */
  id: 'pbo',
  /** Human-readable, for log lines and error messages. */
  name: 'pb-offline',
  /** Dexie database name prefix: `${dbPrefix}_${appId}_${ownerKey}`. */
  dbPrefix: 'pbo',
  /** Web Lock name prefix: `${lockPrefix}:drain:${dbName}`. */
  lockPrefix: 'pbo',
  /** BroadcastChannel name prefix: `${channelPrefix}:${dbName}`. */
  channelPrefix: 'pbo',
  /** Owner key used for the unauthenticated, purely local database. */
  anonOwner: 'anon',
} as const;

/**
 * Character class for a name segment. `_` is the segment separator, so it is
 * excluded: allowing it would let `appId="a_b", owner="c"` and `appId="a",
 * owner="b_c"` produce the same database name.
 */
export const NAME_SEGMENT = /^[A-Za-z0-9][A-Za-z0-9.-]*$/;

export function isValidNameSegment(s: string): boolean {
  return typeof s === 'string' && s.length > 0 && s.length <= 64 && NAME_SEGMENT.test(s);
}

/** Dexie database name for one owner. SPEC §4.1. */
export function dbNameFor(appId: string, ownerKey: string): string {
  if (!isValidNameSegment(appId) || !isValidNameSegment(ownerKey)) {
    throw new PBOError('config/invalid', `invalid database name segment: ${appId}/${ownerKey}`, {
      context: { appId, ownerKey },
    });
  }
  return `${BRAND.dbPrefix}_${appId}_${ownerKey}`;
}

/** Prefix every database of one app shares. Used to enumerate owners (§14). */
export function dbPrefixFor(appId: string): string {
  return `${BRAND.dbPrefix}_${appId}_`;
}

/** The owner key a database name encodes, or null when it is not ours. */
export function ownerKeyFromDbName(appId: string, dbName: string): string | null {
  const prefix = dbPrefixFor(appId);
  return dbName.startsWith(prefix) ? dbName.slice(prefix.length) : null;
}

/** Web Lock name electing the single drainer tab for one database. SPEC §13. */
export function drainLockNameFor(dbName: string): string {
  return `${BRAND.lockPrefix}:drain:${dbName}`;
}

/** BroadcastChannel name for cross-tab nudges. SPEC §13.3. */
export function channelNameFor(dbName: string): string {
  return `${BRAND.channelPrefix}:${dbName}`;
}
