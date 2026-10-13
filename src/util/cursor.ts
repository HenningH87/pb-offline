/**
 * The `(updated, id)` cursor (SPEC §6).
 *
 * THE one place the pull filter is built. The tie-break on `id` is mandatory: a
 * batch write stamps many records with an identical `updated`, and a bare
 * `updated >` cursor either loops on them forever or skips the tail of the page.
 */
import type { Cursor } from '../types.js';
import { compareTimestamps, isPBTimestamp, quoteForFilter } from './time.js';

/** The cursor of a collection that has never been pulled. */
export const EMPTY_CURSOR: Cursor = { updated: null, id: null };

/** The sort parameter. Always `updated,id`, in that order. */
export const PULL_SORT = 'updated,id' as const;

export function isEmptyCursor(c: Cursor | null | undefined): boolean {
  return c === null || c === undefined || c.updated === null || c.updated === undefined;
}

/**
 * The `(updated, id)` half of the pull predicate, parenthesised.
 *
 * Undefined when there is no cursor yet: SPEC §6.1 requires the clause to be
 * OMITTED ENTIRELY rather than written as a comparison against an empty string,
 * which PocketBase rejects.
 */
function cursorClause(cursor: Cursor | null | undefined): string | undefined {
  if (isEmptyCursor(cursor)) return undefined;
  const t = quoteForFilter((cursor as Cursor).updated as string);
  const id = (cursor as Cursor).id;
  if (id === null || id === undefined) return `(updated > ${t})`;
  return `(updated > ${t} || (updated = ${t} && id > ${quoteForFilter(id)}))`;
}

/**
 * Build the filter string for a page request.
 *
 * With a cursor present the shape is exactly:
 *
 *   (<userFilter>) && (updated > "T" || (updated = "T" && id > "I"))
 *
 * BOTH halves parenthesised. A bare `&&` with an `||` inside binds the other way
 * and silently widens the result set to the whole collection — a unit test
 * asserts the parentheses literally.
 */
export function buildPullFilter(userFilter: string | null, cursor: Cursor): string | undefined {
  const clause = cursorClause(cursor);
  if (userFilter === null || userFilter.length === 0) return clause;
  if (clause === undefined) return userFilter;
  return `(${userFilter}) && ${clause}`;
}

/**
 * The cursor after applying a page.
 *
 * The `(updated, id)` of the LAST ROW of the page — not the maximum `updated` on
 * it. Those differ exactly when a page ends mid-tie, which is the case the
 * tie-break exists for. Returns null for an empty page (nothing was consumed, so
 * nothing advances).
 *
 * ALSO returns null when the last row carries no well-formed `updated`. Writing
 * `{updated: null}` into `_cursors` would be worse than not advancing at all:
 * `isEmptyCursor` reports such a row as ABSENT, so the next request omits the
 * cursor clause entirely (§6.1) and a full page repeats for ever. The caller
 * turns a null return on a NON-EMPTY page into `pull/malformed` — SPEC §2.6.1
 * makes the autodate `updated` a precondition, and a collection lacking one is a
 * server misconfiguration to report, not an unbounded request loop to run.
 */
export function advanceCursor(page: ReadonlyArray<{ id: string; updated?: string }>): Cursor | null {
  if (page.length === 0) return null;
  const last = page[page.length - 1] as { id: string; updated?: string };
  if (!isPBTimestamp(last.updated)) return null;
  return { updated: last.updated, id: last.id };
}

/** Order two cursors: by `updated` first, then by the tie-break id. */
export function compareCursors(a: Cursor, b: Cursor): -1 | 0 | 1 {
  const byUpdated = compareTimestamps(a.updated, b.updated);
  if (byUpdated !== 0) return byUpdated;
  return compareTimestamps(a.id, b.id);
}

/**
 * A cursor as one opaque string, for logs and diagnostics payloads.
 *
 * The separator is a unit separator rather than a space: PB's `updated` carries
 * a space of its own, and splitting on that decodes to a truncated timestamp.
 */
const SEP = '\u001f';

export function encodeCursor(c: Cursor): string {
  if (isEmptyCursor(c)) return '';
  return `${c.updated}${SEP}${c.id ?? ''}`;
}

export function decodeCursor(s: string): Cursor {
  if (s === '') return { ...EMPTY_CURSOR };
  const at = s.indexOf(SEP);
  if (at < 0) return { updated: s, id: null };
  const id = s.slice(at + SEP.length);
  return { updated: s.slice(0, at), id: id === '' ? null : id };
}
