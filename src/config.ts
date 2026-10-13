/**
 * Config normalisation, defaults and validation (SPEC §2.1).
 *
 * `resolveConfig` is pure and synchronous — `createPBOffline` performs no I/O, so
 * every config fault MUST surface here as a thrown `config/invalid`.
 */
import type { PBOfflineConfig, ResolvedConfig, ResolvedCollectionConfig, IdFormat } from './types.js';
import { PBOError } from './errors.js';
import { BRAND, isValidNameSegment } from './brand.js';
import { assertFilterSafe } from './util/time.js';
import { ENGINE_TABLES } from './db/schema.js';

export const DEFAULT_ID_FORMAT: IdFormat = {
  length: 15,
  alphabet: 'abcdefghijklmnopqrstuvwxyz0123456789',
};

export const DEFAULTS = {
  deletionsCollection: 'deletions',
  ownerField: 'owner',
  anonymousMode: 'local' as const,
  writeCoalesceMs: 250,
  pullPageSize: 200,
  backoff: { min: 1_000, max: 60_000 },
  staleAfterMs: 0,
  staleOwnerDays: null,
  realtime: { enabled: true },
} as const;

/**
 * A PB identifier. `ownerField` is interpolated into a filter unquoted, and
 * `deletionsCollection` becomes a SQL table identifier in the JSVM server
 * helper's prune query, which builds it by concatenation because PocketBase's
 * `{{table}}` placeholder is a quoting convention and not a bind parameter.
 * Both are therefore identifier-injection surfaces, and both are checked here
 * rather than only where they are used.
 */
const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * PocketBase caps `perPage` at 500 (`api/list` clamps it server-side).
 *
 * SPEC §2.1 says `pullPageSize` MUST be ≤ the server's list cap but nothing
 * enforced it, and exceeding it is not harmless: every page comes back with ≤500
 * rows, so the short-page test in §6.3 is ALWAYS true and the loop stops after
 * one page per pass. A first sync of a large collection then looks stuck.
 */
export const MAX_PULL_PAGE_SIZE = 500;

function bad(msg: string, context?: Record<string, unknown>): never {
  throw new PBOError('config/invalid', msg, context === undefined ? undefined : { context });
}

/**
 * Cheap balance check on a PB filter expression. The server is the real judge;
 * this only catches the transposition that would otherwise be reported as a
 * mystery 400 on every pull.
 */
export function isBalancedFilter(filter: string): boolean {
  let depth = 0;
  let quote: string | null = null;
  for (let i = 0; i < filter.length; i++) {
    const ch = filter[i] as string;
    if (quote !== null) {
      if (ch === '\\') i++;
      else if (ch === quote) quote = null;
      continue;
    }
    if (ch === '"' || ch === "'") quote = ch;
    else if (ch === '(') depth++;
    else if (ch === ')' && --depth < 0) return false;
  }
  return depth === 0 && quote === null;
}

/**
 * Could this id format mint the literal owner key the anonymous mirror uses?
 *
 * The anonymous database and an owner's database share ONE namespace
 * (`pbo_<appId>_<ownerKey>`), separated only by the reserved key `anon`
 * (SPEC §4.1, §14.3). Stock PocketBase ids are 15 lowercase alphanumerics so
 * nothing can collide — but a custom auth collection with `idFormat.length: 4`
 * over an alphabet containing a, n and o CAN issue the id `anon`, and that
 * user's mirror would then be the anonymous sandbox: same rows, same queue,
 * `promoteAnonymousData()` copying data into the account it came from and a
 * sign-out wiping both.
 */
function canMintAnonKey(format: IdFormat): boolean {
  const anon = BRAND.anonOwner;
  if (format.length !== anon.length) return false;
  return [...anon].every((ch) => format.alphabet.includes(ch));
}

function resolveIdFormat(raw: IdFormat | undefined, collection: string): IdFormat {
  if (raw === undefined) return { ...DEFAULT_ID_FORMAT };
  if (!Number.isInteger(raw.length) || raw.length < 1) {
    bad(`collection "${collection}": idFormat.length must be a positive integer`, { idFormat: raw });
  }
  if (typeof raw.alphabet !== 'string' || raw.alphabet.length < 2) {
    bad(`collection "${collection}": idFormat.alphabet must hold at least two symbols`, { idFormat: raw });
  }
  const format = { length: raw.length, alphabet: raw.alphabet };
  if (canMintAnonKey(format)) {
    bad(
      `collection "${collection}": idFormat can mint the reserved owner key "${BRAND.anonOwner}", which names the anonymous mirror; widen the length or drop a symbol from the alphabet`,
      { idFormat: raw, reserved: BRAND.anonOwner },
    );
  }
  return format;
}

/**
 * Apply defaults and validate. Pure and synchronous: `createPBOffline` performs
 * no I/O, so every config fault surfaces here as `config/invalid` (SPEC §2.1).
 *
 * `{owner}` is NOT expanded here — the owner changes over an engine's life, so
 * expansion happens at request time (`expandFilter`).
 */
export function resolveConfig(config: PBOfflineConfig): ResolvedConfig {
  if (config === null || typeof config !== 'object') bad('config must be an object');

  const appId = config.appId;
  if (typeof appId !== 'string' || !isValidNameSegment(appId)) {
    bad(`appId must match ${'[A-Za-z0-9][A-Za-z0-9.-]*'} (it becomes part of a database name)`, { appId });
  }

  const ownerField = config.ownerField ?? DEFAULTS.ownerField;
  // Interpolated raw into `defaultFilter`, so it is a filter-injection surface as
  // much as a name. "Non-empty string" was not enough of a check for something
  // that ends up inside a PB filter expression.
  if (typeof ownerField !== 'string' || !IDENTIFIER.test(ownerField)) {
    bad('ownerField must be a field identifier matching [A-Za-z_][A-Za-z0-9_]* (it is interpolated into a PB filter)', {
      ownerField,
    });
  }

  const deletionsCollection = config.deletionsCollection ?? DEFAULTS.deletionsCollection;
  // "Non-empty string" was not enough: this name is pasted into a SQL identifier
  // by the prune query in both server helpers, so it is held to the same
  // identifier shape as `ownerField` rather than to a length check.
  if (typeof deletionsCollection !== 'string' || !IDENTIFIER.test(deletionsCollection)) {
    bad(
      'deletionsCollection must be a collection identifier matching [A-Za-z_][A-Za-z0-9_]* (it becomes a SQL identifier in the prune query)',
      { deletionsCollection },
    );
  }

  const anonymousMode = config.anonymousMode ?? DEFAULTS.anonymousMode;
  if (anonymousMode !== 'local' && anonymousMode !== 'disabled') {
    bad(`anonymousMode must be 'local' or 'disabled'`, { anonymousMode });
  }

  const writeCoalesceMs = config.writeCoalesceMs ?? DEFAULTS.writeCoalesceMs;
  if (!Number.isFinite(writeCoalesceMs) || writeCoalesceMs < 0) {
    bad('writeCoalesceMs must be a non-negative number', { writeCoalesceMs });
  }

  const requestedPageSize = config.pullPageSize ?? DEFAULTS.pullPageSize;
  if (!Number.isInteger(requestedPageSize) || requestedPageSize < 1) {
    bad('pullPageSize must be an integer ≥ 1', { pullPageSize: requestedPageSize });
  }
  // Clamped rather than rejected: a server MAY be configured with a different
  // cap, and a client that pages 500 at a time against one that allows more is
  // merely slower. A client that asks for more than the server returns is BROKEN.
  const pullPageSize = Math.min(requestedPageSize, MAX_PULL_PAGE_SIZE);
  if (pullPageSize !== requestedPageSize) {
    config.logger?.({
      level: 'warn',
      msg: `pullPageSize ${requestedPageSize} exceeds PocketBase's list cap of ${MAX_PULL_PAGE_SIZE}; clamped (SPEC §2.1)`,
      data: { requested: requestedPageSize, used: pullPageSize },
      at: Date.now(),
    });
  }

  const staleAfterMs = config.staleAfterMs ?? DEFAULTS.staleAfterMs;
  if (!Number.isFinite(staleAfterMs) || staleAfterMs < 0) {
    bad('staleAfterMs must be a non-negative number', { staleAfterMs });
  }

  // Opt-in, and `null` is the default rather than a large number: an eviction
  // policy nobody asked for deletes a second account's offline work.
  const staleOwnerDays = config.staleOwnerDays ?? DEFAULTS.staleOwnerDays;
  if (staleOwnerDays !== null && (!Number.isFinite(staleOwnerDays) || staleOwnerDays < 0)) {
    bad('staleOwnerDays must be null or a non-negative number', { staleOwnerDays });
  }

  const min = config.backoff?.min ?? DEFAULTS.backoff.min;
  const max = config.backoff?.max ?? DEFAULTS.backoff.max;
  if (!Number.isFinite(min) || !Number.isFinite(max) || min <= 0 || max <= 0) {
    bad('backoff bounds must be positive numbers', { min, max });
  }
  if (min > max) bad('backoff.min must be ≤ backoff.max', { min, max });

  if (!Array.isArray(config.collections) || config.collections.length === 0) {
    bad('collections must be a non-empty array');
  }
  if (config.onConflict !== undefined && typeof config.onConflict !== 'function') {
    bad('onConflict must be a function');
  }
  if (config.logger !== undefined && typeof config.logger !== 'function') {
    bad('logger must be a function');
  }

  const seen = new Set<string>();
  const collections: ResolvedCollectionConfig[] = config.collections.map((c) => {
    if (c === null || typeof c !== 'object') bad('every collection must be an object');
    const name = c.name;
    if (typeof name !== 'string' || name.length === 0) bad('collection.name must be a non-empty string', { name });
    if (seen.has(name)) bad(`duplicate collection "${name}"`, { name });
    if ((ENGINE_TABLES as readonly string[]).includes(name)) {
      bad(`collection "${name}" collides with an engine table`, { name, engineTables: ENGINE_TABLES });
    }
    if (name === deletionsCollection) {
      bad(`collection "${name}" collides with deletionsCollection`, { name });
    }
    seen.add(name);

    const indexes = c.indexes ?? [];
    if (!Array.isArray(indexes) || indexes.some((i) => typeof i !== 'string' || i.length === 0)) {
      bad(`collection "${name}": indexes must be an array of non-empty strings`, { indexes });
    }
    if (indexes.includes('id')) {
      bad(`collection "${name}": 'id' is the primary key and MUST NOT be repeated in indexes`);
    }

    let filter: string | null;
    if (c.filter === undefined) filter = defaultFilter(ownerField);
    else if (c.filter === null) filter = null;
    else if (typeof c.filter !== 'string' || c.filter.length === 0) {
      return bad(`collection "${name}": filter must be a non-empty string or null`, { filter: c.filter });
    } else if (!isBalancedFilter(c.filter)) {
      return bad(`collection "${name}": filter has unbalanced parentheses or quotes`, { filter: c.filter });
    } else filter = c.filter;

    if (c.onConflict !== undefined && typeof c.onConflict !== 'function') {
      bad(`collection "${name}": onConflict must be a function`);
    }

    const resolved: ResolvedCollectionConfig = {
      name,
      indexes: [...indexes],
      idFormat: resolveIdFormat(c.idFormat, name),
      filter,
    };
    if (c.onConflict !== undefined) resolved.onConflict = c.onConflict;
    return resolved;
  });

  const out: ResolvedConfig = {
    appId,
    collections,
    deletionsCollection,
    ownerField,
    anonymousMode,
    writeCoalesceMs,
    pullPageSize,
    staleAfterMs,
    staleOwnerDays,
    backoff: { min, max },
    realtime: { enabled: config.realtime?.enabled ?? DEFAULTS.realtime.enabled },
  };
  if (config.onConflict !== undefined) out.onConflict = config.onConflict;
  if (config.logger !== undefined) out.logger = config.logger;
  return out;
}

/** The default per-collection server filter. SPEC §2.1. */
export function defaultFilter(ownerField: string): string {
  return `${ownerField} = "{owner}"`;
}

/**
 * Substitute `{owner}` in a resolved filter. Returns null for an unfiltered
 * collection. A filter that needs an owner while signed out is unsatisfiable, so
 * the empty owner is substituted verbatim and the request will legitimately
 * return nothing rather than silently widening to every row.
 *
 * The owner id is checked with the SAME guard `quoteForFilter` applies to cursor
 * values, and for the same reason: substitution lands inside a quoted literal
 * (`owner = "{owner}"`), so an id carrying a `"` or a `\` closes that literal
 * early and silently widens the result set to other users' rows. PB ids are
 * `[a-z0-9]{15}` today; a custom auth collection with a permissive id pattern is
 * not, and this boundary must not depend on which one the application uses.
 */
export function expandFilter(filter: string | null, ownerId: string | null): string | null {
  if (filter === null) return null;
  if (!filter.includes('{owner}')) return filter;
  const owner = ownerId ?? '';
  assertFilterSafe(owner, 'an owner id substituted into a filter');
  return filter.split('{owner}').join(owner);
}

/** Look up a resolved collection config by name, or throw `config/unknown-collection`. */
export function collectionConfig(cfg: ResolvedConfig, name: string): ResolvedCollectionConfig {
  const found = cfg.collections.find((c) => c.name === name);
  if (!found) {
    throw new PBOError('config/unknown-collection', `collection "${name}" is not configured`, {
      context: { name, configured: cfg.collections.map((c) => c.name) },
    });
  }
  return found;
}
