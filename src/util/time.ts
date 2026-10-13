/**
 * PocketBase `updated` handling (SPEC §6.5, §10).
 *
 * PB autodate format is `YYYY-MM-DD HH:MM:SS.sssZ` — fixed width, zero padded.
 * Lexicographic order is therefore chronological order, and the engine compares
 * these values AS STRINGS everywhere.
 *
 * Parsing to Date for comparison is forbidden: it introduces a rounding boundary
 * the server's own filter predicate does not have, and the two disagreeing is a
 * skipped record. A unit test spies on the Date constructor to enforce this.
 */
import { PBOError } from '../errors.js';

/**
 * The canonical PB autodate shape — `YYYY-MM-DD HH:MM:SS.sssZ`, exactly three
 * fractional digits, always present.
 *
 * The width is not cosmetic, it is the premise of §6.5. PocketBase's
 * `DefaultDateLayout` is the literal `2006-01-02 15:04:05.000Z` and `MarshalJSON`
 * uses the same layout, so the fraction is never elided — and it MUST NOT be
 * tolerated if it ever were: `"…00:00Z"` compares on `'Z'` (0x5A) against `'.'`
 * (0x2E), which sorts a zero-millisecond value AFTER every fractional value in
 * the same second. Lexicographic order would stop being chronological order and
 * the cursor would skip records. This regex is where that premise is enforced.
 */
const PB_TIMESTAMP = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}Z$/;

/** True when the string is a well-formed PB autodate value. */
export function isPBTimestamp(s: unknown): s is string {
  return typeof s === 'string' && PB_TIMESTAMP.test(s);
}

/**
 * String comparison, chronological by construction. Nulls sort first.
 *
 * MUST NOT parse to Date — see the module header. `null` sorting first means
 * "never seen a server value", which is older than any server value.
 */
export function compareTimestamps(a: string | null | undefined, b: string | null | undefined): -1 | 0 | 1 {
  const x = a ?? null;
  const y = b ?? null;
  if (x === y) return 0;
  if (x === null) return -1;
  if (y === null) return 1;
  return x < y ? -1 : x > y ? 1 : 0;
}

/**
 * Quote a value for embedding in a PB filter string, quotes included.
 *
 * The values this is used on are server-minted (`updated`) or PB ids, and
 * neither can contain a quote or a backslash — but assert that rather than trust
 * it, because a filter that escapes the quoting is a silently widened result set,
 * not a parse error.
 */
export function quoteForFilter(s: string): string {
  assertFilterSafe(s, 'a filter value');
  return `"${s}"`;
}

/**
 * The same guard, for a value substituted into a filter WITHOUT quotes of its
 * own — `{owner}` inside `owner = "{owner}"`, say.
 *
 * Substitution is exactly as dangerous as quoting: an owner id carrying a `"`
 * closes the literal early and silently widens the result set to rows the user
 * does not own. PB ids cannot contain one today, but "cannot today" is not an
 * argument a security boundary should rest on, and a custom auth collection with
 * a permissive id pattern walks straight into it.
 */
export function assertFilterSafe(value: string, what: string): void {
  if (typeof value !== 'string' || /["\\]/.test(value)) {
    throw new PBOError('internal/invariant', `${what} contained a quote or a backslash`, {
      context: { value },
    });
  }
}

/**
 * Milliseconds since epoch, for retention arithmetic and diagnostics ONLY.
 *
 * MUST NOT be used for any ordering decision — that is what compareTimestamps is
 * for, and mixing the two is how a clock-skew bug gets in.
 */
export function toEpochMs(s: string): number {
  const iso = s.replace(' ', 'T');
  return Date.parse(iso.endsWith('Z') ? iso : `${iso}Z`);
}
