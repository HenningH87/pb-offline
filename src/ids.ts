/**
 * PocketBase-compatible record ids, generated on the CLIENT (SPEC §4.4).
 *
 * The engine mints the id at create time and sends it in the create body.
 * PocketBase accepts a client-supplied `id` when it matches the collection's id
 * pattern (default 15 chars of [a-z0-9]). This is why there is no `localId` and
 * no id-rewriting pass anywhere in the codebase: relations resolve immediately,
 * an ambiguous create retry is decidable (SPEC §5.7), and one id serves a record
 * for its whole life across the mirror, the queue, the cursor and the tombstone.
 */
import type { IdFormat } from './types.js';
import { DEFAULT_ID_FORMAT } from './config.js';
import { PBOError } from './errors.js';

/** Uint8 rejection sampling only works up to 256 symbols. */
const MAX_ALPHABET = 256;

function assertFormat(format: IdFormat): void {
  if (!Number.isInteger(format.length) || format.length < 1) {
    throw new PBOError('config/invalid', `id length must be a positive integer, got ${format.length}`);
  }
  if (format.alphabet.length < 2) {
    throw new PBOError('config/invalid', 'id alphabet must hold at least two symbols');
  }
  if (format.alphabet.length > MAX_ALPHABET) {
    throw new PBOError('config/invalid', `id alphabet must hold at most ${MAX_ALPHABET} symbols`);
  }
}

function randomBytes(n: number): Uint8Array {
  const out = new Uint8Array(n);
  // `crypto` is a global in browsers and in Node ≥19. Math.random is never an
  // acceptable substitute: ids are the collision domain AND the retry key (§5.7).
  const c = globalThis.crypto;
  if (!c || typeof c.getRandomValues !== 'function') {
    throw new PBOError('internal/invariant', 'crypto.getRandomValues is unavailable');
  }
  c.getRandomValues(out);
  return out;
}

/**
 * Generate an id.
 *
 * Uniform over the alphabet by rejection sampling — a plain `byte % n` would
 * over-weight the first `256 % n` symbols, which is a real (if small) collision
 * amplifier at 15 characters.
 */
export function generateId(format: IdFormat = DEFAULT_ID_FORMAT): string {
  assertFormat(format);
  const alphabet = format.alphabet;
  const n = alphabet.length;
  const limit = MAX_ALPHABET - (MAX_ALPHABET % n);

  let out = '';
  while (out.length < format.length) {
    const bytes = randomBytes(Math.max(8, (format.length - out.length) * 2));
    for (let i = 0; i < bytes.length && out.length < format.length; i++) {
      const b = bytes[i] as number;
      if (b >= limit) continue;
      out += alphabet[b % n];
    }
  }
  return out;
}

/**
 * True when `id` matches the format exactly (length and character class).
 *
 * Used by `create()` to reject a caller-supplied id BEFORE any Dexie write, so a
 * bad id never reaches the queue (TESTPLAN §1.1).
 */
export function isValidId(id: string, format: IdFormat = DEFAULT_ID_FORMAT): boolean {
  if (typeof id !== 'string' || id.length !== format.length) return false;
  for (let i = 0; i < id.length; i++) {
    if (!format.alphabet.includes(id[i] as string)) return false;
  }
  return true;
}

/** Throw `config/bad-id` when invalid. */
export function assertValidId(id: string, collection: string, format: IdFormat = DEFAULT_ID_FORMAT): void {
  if (!isValidId(id, format)) {
    throw new PBOError(
      'config/bad-id',
      `id ${JSON.stringify(id)} does not match the id format for collection "${collection}" ` +
        `(length ${format.length}, alphabet ${JSON.stringify(format.alphabet)})`,
      { context: { collection, id, format } },
    );
  }
}
