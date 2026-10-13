/**
 * Jittered exponential backoff (SPEC §3.2).
 *
 * Used by the health ping and by per-entry push retries. Jitter is not optional:
 * without it, every tab and every device that lost the link at the same moment
 * retries at the same moment.
 */

export interface Backoff {
  /** Delay in ms for the next attempt, and advance the state. */
  next(): number;
  /** Current ceiling without advancing. */
  peek(): number;
  /** Back to the minimum. Called on any transition to up. */
  reset(): void;
  readonly attempt: number;
}

/**
 * Full jitter — `random(0, min(max, min * 2^attempt))` — with a floor of `min`.
 *
 * The floor matters: a jittered zero would make the ping loop hot, and SPEC §3.2
 * forbids pinging more often than `backoff.min`.
 */
export function createBackoff(opts: { min: number; max: number; random?: () => number }): Backoff {
  const min = Math.max(1, opts.min);
  const max = Math.max(min, opts.max);
  const random = opts.random ?? Math.random;
  let attempt = 0;

  const ceiling = (): number => {
    // 2^attempt overflows to Infinity long before it matters; Math.min clamps it.
    const scaled = min * Math.pow(2, attempt);
    return Math.min(max, Number.isFinite(scaled) ? scaled : max);
  };

  return {
    get attempt() {
      return attempt;
    },
    peek: ceiling,
    next() {
      const delay = Math.min(max, Math.max(min, Math.floor(random() * ceiling())));
      attempt += 1;
      return delay;
    },
    reset() {
      attempt = 0;
    },
  };
}

/*
 * There is deliberately no `parseRetryAfter` here.
 *
 * SPEC §5.9 asks a `429` to honour `Retry-After`, but the PocketBase JS SDK never
 * surfaces response headers: `ClientResponseError` carries `{url, status,
 * response, isAbort, originalError}` and `response` is the PARSED JSON BODY
 * (`{code, message, data}`) — see `node_modules/pocketbase/dist/pocketbase.es.mjs`,
 * where `send()` throws `new ClientResponseError({url, status, data})` with the
 * body and nothing else. A parser here would only ever be fed values the engine
 * cannot obtain, so the ladder above IS the 429 wait. See src/errors.ts.
 */
