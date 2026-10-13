/** SPEC §6.2, §6.5, §3.2 — the cursor, the clock and the backoff. */
import { describe, expect, it, vi } from 'vitest';
import { compareTimestamps, isPBTimestamp, quoteForFilter, toEpochMs } from '../../src/util/time.js';
import {
  advanceCursor,
  buildPullFilter,
  compareCursors,
  decodeCursor,
  encodeCursor,
  EMPTY_CURSOR,
  isEmptyCursor,
  PULL_SORT,
} from '../../src/util/cursor.js';
import { createBackoff } from '../../src/net/backoff.js';
import { PBOError } from '../../src/errors.js';

const T = (s: string): string => s;

describe('util/time — PB `updated` as a string (SPEC §6.5)', () => {
  it('orders chronologically across every boundary that matters', () => {
    const ordered = [
      '2025-12-31 23:59:59.998Z',
      '2025-12-31 23:59:59.999Z',
      '2026-01-01 00:00:00.000Z', // year + midnight rollover
      '2026-01-01 00:00:00.001Z', // sub-second
      '2026-01-09 00:00:00.000Z',
      '2026-01-10 00:00:00.000Z', // zero-padding boundary in the day
      '2026-02-01 00:00:00.000Z', // month rollover
      '2026-10-01 00:00:00.000Z', // zero-padding boundary in the month
    ];
    for (let i = 1; i < ordered.length; i++) {
      expect(compareTimestamps(ordered[i - 1]!, ordered[i]!)).toBe(-1);
      expect(compareTimestamps(ordered[i]!, ordered[i - 1]!)).toBe(1);
      expect(compareTimestamps(ordered[i]!, ordered[i]!)).toBe(0);
    }
    // A sort by the comparator reproduces the written order exactly.
    const shuffled = [...ordered].reverse();
    expect([...shuffled].sort(compareTimestamps)).toEqual(ordered);
  });

  it('sorts null first — never having seen a server value is older than any', () => {
    expect(compareTimestamps(null, '2026-01-01 00:00:00.000Z')).toBe(-1);
    expect(compareTimestamps('2026-01-01 00:00:00.000Z', null)).toBe(1);
    expect(compareTimestamps(null, null)).toBe(0);
  });

  it('regression: comparison never parses to Date (SPEC §6.5)', () => {
    const ctor = vi.spyOn(globalThis, 'Date');
    const parse = vi.spyOn(Date, 'parse');
    compareTimestamps('2026-01-01 00:00:00.000Z', '2026-01-01 00:00:00.001Z');
    expect(ctor).not.toHaveBeenCalled();
    expect(parse).not.toHaveBeenCalled();
    // toEpochMs is the ONLY sanctioned parse, and it is retention arithmetic.
    expect(toEpochMs('2026-01-01 00:00:00.000Z')).toBe(Date.UTC(2026, 0, 1));
    ctor.mockRestore();
    parse.mockRestore();
  });

  it('requires the full fixed width, fraction included — that IS the premise', () => {
    expect(isPBTimestamp('2026-01-01 00:00:00.000Z')).toBe(true);
    // PB's DefaultDateLayout is the literal `2006-01-02 15:04:05.000Z`, so the
    // fraction is never elided. Accepting a value without one would break the
    // premise the whole cursor rests on: '.' is 0x2E and 'Z' is 0x5A, so
    // "…00:00Z" sorts AFTER every fractional value in the same second, and
    // lexicographic order would stop being chronological order.
    expect('2026-01-01 00:00:00Z' > '2026-01-01 00:00:00.999Z').toBe(true);
    expect(isPBTimestamp('2026-01-01 00:00:00Z')).toBe(false);
    expect(isPBTimestamp('2026-01-01 00:00:00.1Z')).toBe(false);
    expect(isPBTimestamp('2026-01-01 00:00:00.000000Z')).toBe(false);
    expect(isPBTimestamp('2026-01-01T00:00:00.000Z')).toBe(false);
    expect(isPBTimestamp('2026-01-01 00:00:00.000')).toBe(false);
    expect(isPBTimestamp(17)).toBe(false);
  });

  it('refuses to quote a value that could escape the filter', () => {
    expect(quoteForFilter('2026-01-01 00:00:00.000Z')).toBe('"2026-01-01 00:00:00.000Z"');
    expect(() => quoteForFilter('a"b')).toThrowError(PBOError);
    expect(() => quoteForFilter('a\\b')).toThrowError(PBOError);
  });
});

describe('util/cursor — the compound filter (SPEC §6.1, §6.2)', () => {
  const cursor = { updated: T('2026-01-02 03:04:05.678Z'), id: 'abcdefghijklmno' };

  it('omits the updated clause entirely when there is no cursor', () => {
    expect(buildPullFilter('owner = "u1"', EMPTY_CURSOR)).toBe('owner = "u1"');
    expect(buildPullFilter('owner = "u1"', EMPTY_CURSOR)).not.toContain('updated');
    expect(buildPullFilter(null, EMPTY_CURSOR)).toBeUndefined();
  });

  it('builds the exact disjunction, with the user filter parenthesised around it', () => {
    // The literal string. A bare `&&` with an `||` inside binds the other way and
    // silently widens the result set to the whole collection.
    expect(buildPullFilter('owner = "u1"', cursor)).toBe(
      '(owner = "u1") && (updated > "2026-01-02 03:04:05.678Z" || ' +
        '(updated = "2026-01-02 03:04:05.678Z" && id > "abcdefghijklmno"))',
    );
  });

  it('drops the user half when the collection is unfiltered', () => {
    expect(buildPullFilter(null, cursor)).toBe(
      '(updated > "2026-01-02 03:04:05.678Z" || (updated = "2026-01-02 03:04:05.678Z" && id > "abcdefghijklmno"))',
    );
  });

  it('sorts by updated then id, in that order', () => {
    expect(PULL_SORT).toBe('updated,id');
  });

  it('advances to the LAST ROW of a page, not the maximum updated on it', () => {
    // A page that ends mid-tie: three rows share one `updated`, and the last row
    // is not the one with the greatest id.
    const page = [
      { id: 'c', updated: '2026-01-01 00:00:00.000Z' },
      { id: 'z', updated: '2026-01-01 00:00:00.000Z' },
      { id: 'd', updated: '2026-01-01 00:00:00.000Z' },
    ];
    expect(advanceCursor(page)).toEqual({ updated: '2026-01-01 00:00:00.000Z', id: 'd' });
    expect(advanceCursor([])).toBeNull();
  });

  it('round-trips through encode/decode and orders by (updated, id)', () => {
    expect(decodeCursor(encodeCursor(cursor))).toEqual(cursor);
    expect(decodeCursor(encodeCursor(EMPTY_CURSOR))).toEqual(EMPTY_CURSOR);
    expect(isEmptyCursor(EMPTY_CURSOR)).toBe(true);
    expect(isEmptyCursor(cursor)).toBe(false);
    expect(compareCursors(cursor, { ...cursor, id: 'z' })).toBe(-1);
    expect(compareCursors(cursor, { updated: '2026-01-03 00:00:00.000Z', id: 'a' })).toBe(-1);
    expect(compareCursors(cursor, cursor)).toBe(0);
  });
});

describe('net/backoff (SPEC §3.2)', () => {
  it('is bounded by min and max and grows exponentially', () => {
    const b = createBackoff({ min: 100, max: 1_000 });
    const seen: number[] = [];
    for (let i = 0; i < 40; i++) seen.push(b.next());
    expect(Math.min(...seen)).toBeGreaterThanOrEqual(100);
    expect(Math.max(...seen)).toBeLessThanOrEqual(1_000);
    expect(b.attempt).toBe(40);
  });

  it('is jittered — full jitter, not a fixed ladder', () => {
    const b = createBackoff({ min: 1, max: 100_000 });
    const seen = new Set<number>();
    for (let i = 0; i < 20; i++) seen.add(b.next());
    // A fixed ladder over this range would produce 20 distinct but predictable
    // values; the point is only that the sequence is not deterministic.
    const b2 = createBackoff({ min: 1, max: 100_000 });
    const second: number[] = [];
    for (let i = 0; i < 20; i++) second.push(b2.next());
    expect(second.some((v) => !seen.has(v))).toBe(true);
  });

  it('scales the ceiling as 2^attempt and resets to min on recovery', () => {
    const b = createBackoff({ min: 100, max: 10_000, random: () => 1 });
    expect(b.peek()).toBe(100);
    expect(b.next()).toBe(100);
    expect(b.next()).toBe(200);
    expect(b.next()).toBe(400);
    b.reset();
    expect(b.attempt).toBe(0);
    expect(b.next()).toBe(100);
  });

  it('never returns less than min, even with a jitter of zero', () => {
    const b = createBackoff({ min: 250, max: 5_000, random: () => 0 });
    expect(b.next()).toBe(250);
    expect(b.next()).toBe(250);
  });

  // There is deliberately no `parseRetryAfter` test here, because there is no
  // `parseRetryAfter`: the SDK surfaces no response headers, so nothing could
  // ever feed one. See `errors.test.ts` — "cannot read Retry-After off a 429".
});
