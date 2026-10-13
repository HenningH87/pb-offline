/** SPEC §0, §4.1, §13 — the brand is the only place the package name lives. */
import { describe, expect, it } from 'vitest';
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  BRAND,
  channelNameFor,
  dbNameFor,
  dbPrefixFor,
  drainLockNameFor,
  isValidNameSegment,
  ownerKeyFromDbName,
} from '../../src/brand.js';
import { PBOError } from '../../src/errors.js';

const SRC = fileURLToPath(new URL('../../src', import.meta.url));
const PKG = JSON.parse(readFileSync(new URL('../../package.json', import.meta.url), 'utf8')) as { name: string };

function tsFiles(dir: string): string[] {
  return readdirSync(dir).flatMap((entry) => {
    const full = join(dir, entry);
    if (statSync(full).isDirectory()) return tsFiles(full);
    return entry.endsWith('.ts') ? [full] : [];
  });
}

describe('brand tripwire (SPEC §0, §18.6)', () => {
  it('the package name appears in no file under src/ other than brand.ts', () => {
    const offenders = tsFiles(SRC)
      .filter((f) => !f.endsWith('/brand.ts'))
      .filter((f) => readFileSync(f, 'utf8').includes(PKG.name));
    expect(offenders).toEqual([]);
  });

  it('brand.ts does carry it, so the tripwire is testing something', () => {
    expect(readFileSync(join(SRC, 'brand.ts'), 'utf8')).toContain(PKG.name);
  });
});

describe('storage identifiers (SPEC §4.1, §13)', () => {
  it('names a database per owner', () => {
    expect(dbNameFor('todo', 'u123')).toBe(`${BRAND.dbPrefix}_todo_u123`);
    expect(dbNameFor('todo', BRAND.anonOwner)).toBe(`${BRAND.dbPrefix}_todo_anon`);
  });

  it('rejects a segment that could make two owners collide', () => {
    for (const bad of ['', 'has space', 'under_score', 'a/b', '-lead']) {
      expect(() => dbNameFor(bad, 'u1')).toThrowError(PBOError);
      try {
        dbNameFor(bad, 'u1');
      } catch (e) {
        expect((e as PBOError).code).toBe('config/invalid');
      }
    }
    expect(isValidNameSegment('ok-1.2')).toBe(true);
  });

  it('round-trips the owner key out of a database name', () => {
    expect(ownerKeyFromDbName('todo', dbNameFor('todo', 'u9'))).toBe('u9');
    expect(ownerKeyFromDbName('todo', 'other_db')).toBeNull();
    expect(dbPrefixFor('todo')).toBe(`${BRAND.dbPrefix}_todo_`);
  });

  it('scopes the lock and the channel to the database name', () => {
    const db = dbNameFor('todo', 'u1');
    expect(drainLockNameFor(db)).toBe(`${BRAND.lockPrefix}:drain:${db}`);
    expect(channelNameFor(db)).toBe(`${BRAND.channelPrefix}:${db}`);
  });
});
