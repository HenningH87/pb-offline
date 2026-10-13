/** SPEC §4.4 — client-generated PocketBase ids. TESTPLAN §1.1. */
import { describe, expect, it, vi } from 'vitest';
import { assertValidId, generateId, isValidId } from '../../src/ids.js';
import { DEFAULT_ID_FORMAT } from '../../src/config.js';
import { PBOError } from '../../src/errors.js';

describe('generateId', () => {
  it('produces 15 chars of [a-z0-9] by default, over 10k samples', () => {
    const seen = new Set<string>();
    for (let i = 0; i < 10_000; i++) {
      const id = generateId();
      expect(id).toHaveLength(15);
      expect(id).toMatch(/^[a-z0-9]{15}$/);
      seen.add(id);
    }
    expect(seen.size).toBe(10_000); // no collisions at this scale
  });

  it('covers the alphabet roughly uniformly (rejection sampling, not modulo)', () => {
    const counts = new Map<string, number>();
    for (let i = 0; i < 4_000; i++) {
      for (const ch of generateId()) counts.set(ch, (counts.get(ch) ?? 0) + 1);
    }
    expect(counts.size).toBe(DEFAULT_ID_FORMAT.alphabet.length);
    const expected = (4_000 * 15) / DEFAULT_ID_FORMAT.alphabet.length;
    for (const c of counts.values()) {
      expect(c).toBeGreaterThan(expected * 0.75);
      expect(c).toBeLessThan(expected * 1.25);
    }
  });

  it('honours a custom idFormat', () => {
    const id = generateId({ length: 8, alphabet: 'ABC' });
    expect(id).toHaveLength(8);
    expect(id).toMatch(/^[ABC]{8}$/);
  });

  it('uses crypto.getRandomValues and never Math.random', () => {
    const crypto = vi.spyOn(globalThis.crypto, 'getRandomValues');
    const random = vi.spyOn(Math, 'random');
    generateId();
    expect(crypto).toHaveBeenCalled();
    expect(random).not.toHaveBeenCalled();
    crypto.mockRestore();
    random.mockRestore();
  });

  it('rejects an unusable format with config/invalid', () => {
    for (const format of [{ length: 0, alphabet: 'ab' }, { length: 4, alphabet: 'a' }]) {
      try {
        generateId(format);
        throw new Error('should have thrown');
      } catch (e) {
        expect((e as PBOError).code).toBe('config/invalid');
      }
    }
  });
});

describe('isValidId / assertValidId', () => {
  it('accepts PB-shaped ids', () => {
    expect(isValidId('abc123def456ghi')).toBe(true);
    expect(isValidId(generateId())).toBe(true);
  });

  it('rejects uppercase, punctuation and the wrong length', () => {
    expect(isValidId('ABC123DEF456GHI')).toBe(false);
    expect(isValidId('abc-123def456gh')).toBe(false);
    expect(isValidId('short')).toBe(false);
    expect(isValidId('abc123def456ghij')).toBe(false);
    expect(isValidId('')).toBe(false);
  });

  it('validates against a per-collection format', () => {
    expect(isValidId('ABCD', { length: 4, alphabet: 'ABCD' })).toBe(true);
    expect(isValidId('abcd', { length: 4, alphabet: 'ABCD' })).toBe(false);
  });

  it('throws config/bad-id, naming the collection', () => {
    try {
      assertValidId('nope', 'projects');
      throw new Error('should have thrown');
    } catch (e) {
      expect(e).toBeInstanceOf(PBOError);
      expect((e as PBOError).code).toBe('config/bad-id');
      expect((e as PBOError).retryable).toBe(false);
      expect((e as PBOError).message).toContain('projects');
    }
  });
});
