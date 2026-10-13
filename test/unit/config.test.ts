/** SPEC §2.1 — config normalisation and validation. TESTPLAN §1.14. */
import { describe, expect, it } from 'vitest';
import {
  collectionConfig,
  DEFAULTS,
  defaultFilter,
  expandFilter,
  isBalancedFilter,
  MAX_PULL_PAGE_SIZE,
  resolveConfig,
} from '../../src/config.js';
import { PBOError } from '../../src/errors.js';
import type { PBOfflineConfig } from '../../src/types.js';

const base: PBOfflineConfig = { appId: 'todo', collections: [{ name: 'projects' }] };

function expectInvalid(config: unknown): PBOError {
  try {
    resolveConfig(config as PBOfflineConfig);
  } catch (e) {
    expect(e).toBeInstanceOf(PBOError);
    expect((e as PBOError).code).toBe('config/invalid');
    return e as PBOError;
  }
  throw new Error('config was accepted but should not have been');
}

describe('resolveConfig defaults', () => {
  it('fills every documented default', () => {
    const cfg = resolveConfig(base);
    expect(cfg.deletionsCollection).toBe(DEFAULTS.deletionsCollection);
    expect(cfg.ownerField).toBe(DEFAULTS.ownerField);
    expect(cfg.anonymousMode).toBe('local');
    expect(cfg.writeCoalesceMs).toBe(250);
    expect(cfg.pullPageSize).toBe(200);
    expect(cfg.staleAfterMs).toBe(0);
    expect(cfg.backoff).toEqual({ min: 1_000, max: 60_000 });
    expect(cfg.realtime).toEqual({ enabled: true });
    expect(cfg.collections[0]).toEqual({
      name: 'projects',
      indexes: [],
      idFormat: { length: 15, alphabet: 'abcdefghijklmnopqrstuvwxyz0123456789' },
      filter: defaultFilter('owner'),
    });
  });

  it('keeps a null filter as null (shared reference data)', () => {
    const cfg = resolveConfig({ ...base, collections: [{ name: 'tags', filter: null }] });
    expect(cfg.collections[0]!.filter).toBeNull();
  });

  it('derives the default filter from a custom ownerField', () => {
    const cfg = resolveConfig({ ...base, ownerField: 'account' });
    expect(cfg.collections[0]!.filter).toBe('account = "{owner}"');
  });

  it('does not expand {owner} — the owner changes over the engine’s life', () => {
    expect(resolveConfig(base).collections[0]!.filter).toContain('{owner}');
  });
});

describe('resolveConfig validation', () => {
  it('rejects an appId that cannot be part of a database name', () => {
    for (const appId of ['', 'has space', 'a_b', undefined, 42]) {
      expectInvalid({ ...base, appId });
    }
  });

  it('rejects an empty collections array and duplicates', () => {
    expectInvalid({ ...base, collections: [] });
    expectInvalid({ ...base, collections: [{ name: 'p' }, { name: 'p' }] });
  });

  it('rejects a collection colliding with an engine table or the tombstone table', () => {
    for (const name of ['_pending', '_cursors', '_deadletter', '_meta', '_state']) {
      expectInvalid({ ...base, collections: [{ name }] });
    }
    expectInvalid({ ...base, collections: [{ name: 'deletions' }] });
    expectInvalid({ ...base, deletionsCollection: 'graves', collections: [{ name: 'graves' }] });
  });

  it('rejects out-of-range numbers', () => {
    expectInvalid({ ...base, pullPageSize: 0 });
    expectInvalid({ ...base, pullPageSize: 1.5 });
    expectInvalid({ ...base, writeCoalesceMs: -1 });
    expectInvalid({ ...base, staleAfterMs: -1 });
    expectInvalid({ ...base, staleOwnerDays: -1 });
    expectInvalid({ ...base, backoff: { min: 5_000, max: 1_000 } });
  });

  it('takes staleOwnerDays, and defaults it to off', () => {
    expect(resolveConfig(base).staleOwnerDays).toBeNull();
    expect(resolveConfig({ ...base, staleOwnerDays: 0 }).staleOwnerDays).toBe(0);
    expect(resolveConfig({ ...base, staleOwnerDays: 30 }).staleOwnerDays).toBe(30);
  });

  it('rejects an unusable idFormat', () => {
    expectInvalid({ ...base, collections: [{ name: 'p', idFormat: { length: 0, alphabet: 'ab' } }] });
    expectInvalid({ ...base, collections: [{ name: 'p', idFormat: { length: 5, alphabet: '' } }] });
  });

  /**
   * The anonymous mirror and an owner's mirror share ONE namespace
   * (`pbo_<appId>_<ownerKey>`), separated only by the literal `anon`. Stock PB
   * ids cannot collide with it — but a custom auth collection whose ids are
   * four characters over an alphabet containing a, n and o can BE it, and that
   * user's mirror would then be the anonymous sandbox: same rows, same queue,
   * promotion copying data into the account it came from.
   */
  it('rejects an idFormat that could mint the reserved anonymous owner key', () => {
    const err = expectInvalid({ ...base, collections: [{ name: 'p', idFormat: { length: 4, alphabet: 'anor' } }] });
    expect(err.message).toContain('anon');
    // One symbol short, one character short, or wide enough to be safe.
    expect(() => resolveConfig({ ...base, collections: [{ name: 'p', idFormat: { length: 4, alphabet: 'abcd' } }] })).not.toThrow();
    expect(() => resolveConfig({ ...base, collections: [{ name: 'p', idFormat: { length: 5, alphabet: 'anor' } }] })).not.toThrow();
  });

  /**
   * `deletionsCollection` is pasted into the prune query's `{{table}}` by both
   * server helpers — PocketBase's `{{ }}` is a quoting convention, not a bind
   * parameter — so it is held to an identifier shape, not to "non-empty".
   */
  it('rejects a deletionsCollection that is not a bare identifier', () => {
    expectInvalid({ ...base, deletionsCollection: '' });
    expectInvalid({ ...base, deletionsCollection: 'deletions}} WHERE 1=1 --' });
    expectInvalid({ ...base, deletionsCollection: 'my deletions' });
    expectInvalid({ ...base, deletionsCollection: '1deletions' });
    expect(resolveConfig({ ...base, deletionsCollection: '_pbo_graves2' }).deletionsCollection).toBe('_pbo_graves2');
  });

  it('rejects an unbalanced filter, cheaply', () => {
    expectInvalid({ ...base, collections: [{ name: 'p', filter: 'owner = "{owner}" && (a = 1' }] });
    expect(isBalancedFilter('(a = 1 || b = "x)")')).toBe(true);
    expect(isBalancedFilter('a = "unterminated')).toBe(false);
  });

  it('rejects an id index duplicated by the caller', () => {
    expectInvalid({ ...base, collections: [{ name: 'p', indexes: ['id'] }] });
  });

  it('rejects non-function hooks', () => {
    expectInvalid({ ...base, onConflict: 'nope' });
    expectInvalid({ ...base, logger: 'nope' });
  });

  /**
   * REGRESSION (review finding 15) — `ownerField` is interpolated RAW into
   * `defaultFilter`, so "non-empty string" was not the check it needed to be.
   */
  it('rejects an ownerField that is not a field identifier — it lands inside a filter', () => {
    expect(resolveConfig({ ...base, ownerField: 'account_id' }).ownerField).toBe('account_id');
    expectInvalid({ ...base, ownerField: 'owner" || id != "' });
    expectInvalid({ ...base, ownerField: 'owner.id' });
    expectInvalid({ ...base, ownerField: '1owner' });
    expectInvalid({ ...base, ownerField: '' });
  });

  /**
   * REGRESSION (review finding 17) — SPEC §2.1 says `pullPageSize` MUST be ≤ the
   * server's list cap and nothing enforced it. PocketBase caps `perPage` at 500,
   * so a configured 1000 made every page come back short, the §6.3 loop stop
   * after one page per pass, and a first sync of a large collection look stuck.
   */
  it('clamps pullPageSize to PocketBase’s list cap, and says so', () => {
    const logged: Array<{ level: string; msg: string }> = [];
    const cfg = resolveConfig({ ...base, pullPageSize: 1_000, logger: (e) => logged.push(e) });
    expect(cfg.pullPageSize).toBe(MAX_PULL_PAGE_SIZE);
    expect(logged).toHaveLength(1);
    expect(logged[0]).toMatchObject({ level: 'warn' });
    expect(logged[0]!.msg).toContain('list cap');

    // At or under the cap nothing is clamped and nothing is logged.
    const quiet: unknown[] = [];
    expect(resolveConfig({ ...base, pullPageSize: 500, logger: (e) => quiet.push(e) }).pullPageSize).toBe(500);
    expect(quiet).toEqual([]);
  });
});

describe('expandFilter / collectionConfig', () => {
  it('substitutes the owner at request time', () => {
    expect(expandFilter('owner = "{owner}"', 'u1')).toBe('owner = "u1"');
    expect(expandFilter(null, 'u1')).toBeNull();
    expect(expandFilter('owner = "{owner}"', null)).toBe('owner = ""');
  });

  /**
   * REGRESSION (review finding 15) — `quoteForFilter` refuses to interpolate a
   * value carrying a quote or a backslash, precisely because a filter that
   * escapes its own quoting is a SILENTLY WIDENED result set rather than a parse
   * error. `{owner}` lands inside the same quoted literal and had no guard at
   * all, so an app with a custom auth collection and a permissive id pattern
   * would have pulled other users' rows into the mirror.
   */
  it('refuses an owner id that would escape the quoted literal it lands in', () => {
    expect(() => expandFilter('owner = "{owner}"', 'u1" || id != "')).toThrowError(PBOError);
    expect(() => expandFilter('owner = "{owner}"', 'u1\\')).toThrowError(PBOError);
    // A filter with no `{owner}` in it is not the owner's business at all.
    expect(expandFilter('archived = false', 'u1" || 1=1')).toBe('archived = false');
  });

  it('throws config/unknown-collection for a name not in config', () => {
    const cfg = resolveConfig(base);
    expect(collectionConfig(cfg, 'projects').name).toBe('projects');
    try {
      collectionConfig(cfg, 'nope');
      throw new Error('should have thrown');
    } catch (e) {
      expect((e as PBOError).code).toBe('config/unknown-collection');
    }
  });
});
