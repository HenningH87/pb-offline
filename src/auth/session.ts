/**
 * Auth binding (SPEC §14).
 *
 * The owner identity is `pb.authStore.record?.id ?? null`. On an owner change the
 * engine stops the drain, tears down SSE, closes the current database, opens the
 * new owner's, and reconnects.
 *
 * It MUST NOT wipe the outgoing owner's database. That is `signOut()`'s job and
 * only `signOut()`'s — merely switching accounts preserves both, which is what
 * makes multi-account offline usable at all.
 */
import type PocketBase from 'pocketbase';
import type { Unsubscriber } from '../types.js';
import { BRAND } from '../brand.js';

export interface Session {
  readonly ownerId: string | null;
  /** Database key: the owner id, or BRAND.anonOwner when signed out. */
  readonly ownerKey: string;
  readonly authenticated: boolean;
  onChange(cb: (ownerId: string | null, previous: string | null) => void): Unsubscriber;
  stop(): void;
}

/** The owner identity, straight from the SDK. SPEC §14.1. */
export function ownerIdOf(pb: PocketBase): string | null {
  const store = (pb as { authStore?: { record?: { id?: unknown } | null } }).authStore;
  const id = store?.record?.id;
  return typeof id === 'string' && id.length > 0 ? id : null;
}

/** The database key for an owner. `anon` is not a user id, and cannot collide. */
export function ownerKeyOf(ownerId: string | null): string {
  return ownerId ?? BRAND.anonOwner;
}

/**
 * Bind to `pb.authStore`.
 *
 * The SDK fires `onChange` on token refresh as well, and a refresh is NOT an
 * owner change: the ids are compared, never the tokens. A refresh that fired an
 * owner change would close and reopen the database — and abort the drain — every
 * time the token rotated.
 */
export function createSession(pb: PocketBase): Session {
  const listeners = new Set<(ownerId: string | null, previous: string | null) => void>();
  let current = ownerIdOf(pb);
  let off: Unsubscriber | null = null;

  const store = (pb as { authStore?: { onChange?: (cb: () => void) => Unsubscriber } }).authStore;
  if (typeof store?.onChange === 'function') {
    const unsubscribe = store.onChange(() => {
      const next = ownerIdOf(pb);
      if (next === current) return;
      const previous = current;
      current = next;
      for (const cb of [...listeners]) cb(next, previous);
    });
    off = typeof unsubscribe === 'function' ? unsubscribe : null;
  }

  return {
    get ownerId() {
      return current;
    },
    get ownerKey() {
      return ownerKeyOf(current);
    },
    get authenticated() {
      return current !== null;
    },
    onChange(cb) {
      listeners.add(cb);
      return () => listeners.delete(cb);
    },
    stop() {
      listeners.clear();
      off?.();
      off = null;
    },
  };
}

/**
 * True when the store holds a token the server would still accept.
 *
 * Used by push error handling to tell "the rules rejected this" (dead-letter)
 * from "we are not authenticated any more" (a link condition) — SPEC §5.10.
 *
 * An SDK that does not expose `isValid` is given the benefit of the doubt: the
 * consequence of guessing "valid" is a dead letter the user can see and retry,
 * and the consequence of guessing "invalid" is a queue that silently stops.
 */
export function isAuthValid(pb: PocketBase): boolean {
  const store = (pb as { authStore?: { isValid?: boolean } }).authStore;
  return store?.isValid !== false;
}
