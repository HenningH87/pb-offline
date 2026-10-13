/**
 * The error taxonomy (SPEC §12).
 *
 * Nothing in this library throws a bare Error across the public API. Every code
 * in `PBOErrorCode` has exactly one meaning and one documented engine response.
 */
import type { PBOErrorCode, PBOErrorShape } from './types.js';

/** Codes whose condition clears by itself given time or a restored link. */
const RETRYABLE: ReadonlySet<PBOErrorCode> = new Set<PBOErrorCode>([
  'auth/changed-mid-flight',
  'link/offline',
  'link/timeout',
  'pull/cursor-stale',
  'pull/malformed',
  'db/quota',
  'lock/unavailable',
]);

export class PBOError extends Error {
  readonly code: PBOErrorCode;
  readonly retryable: boolean;
  override readonly cause?: unknown;
  readonly context?: Record<string, unknown>;

  constructor(
    code: PBOErrorCode,
    message: string,
    opts?: { cause?: unknown; context?: Record<string, unknown>; retryable?: boolean },
  ) {
    super(message);
    this.name = 'PBOError';
    this.code = code;
    this.retryable = opts?.retryable ?? RETRYABLE.has(code);
    this.cause = opts?.cause;
    this.context = opts?.context;
  }

  toShape(): PBOErrorShape {
    return { code: this.code, message: this.message, retryable: this.retryable, at: Date.now() };
  }
}

/** True for a code the engine may retry without user or developer action. */
export function isRetryable(code: PBOErrorCode): boolean {
  return RETRYABLE.has(code);
}

/**
 * The subset of `ClientResponseError` this module reads.
 *
 * The real class (pocketbase@0.28) is exactly
 * `{url, status, response, isAbort, originalError}` plus a `data` getter aliasing
 * `response`, and `response` is the PARSED JSON BODY — `{code, message, data}`.
 * There are NO response headers on it: `send()` throws
 * `new ClientResponseError({url, status, data: <body>})` and the `Response`
 * object never leaves the SDK. Nothing below may read a field that does not
 * appear here, and nothing here may be added without checking the SDK first.
 */
interface SdkErrorLike {
  url?: string;
  status?: number;
  isAbort?: boolean;
  name?: string;
  message?: string;
  response?: { code?: number; message?: string; data?: Record<string, { code?: string; message?: string }> };
  data?: { code?: number; message?: string; data?: Record<string, { code?: string; message?: string }> };
  originalError?: unknown;
}

function asSdkError(err: unknown): SdkErrorLike | null {
  return err !== null && typeof err === 'object' ? (err as SdkErrorLike) : null;
}

function statusOf(err: SdkErrorLike): number {
  return typeof err.status === 'number' ? err.status : 0;
}

function fieldErrors(err: SdkErrorLike): Record<string, { code?: string; message?: string }> {
  return err.response?.data ?? err.data?.data ?? {};
}

function messageOf(err: unknown): string {
  const sdk = asSdkError(err);
  return sdk?.response?.message ?? sdk?.data?.message ?? sdk?.message ?? String(err);
}

/**
 * True for the specific 400 that SPEC §5.7 keys on: the id we supplied is
 * already taken. PocketBase reports it as a per-field validation error on `id`
 * (`validation_not_unique`), which is why this reads the body and not the status
 * — a bare 400 is an ordinary rejection.
 */
export function isIdTakenError(err: unknown): boolean {
  const sdk = asSdkError(err);
  if (!sdk || statusOf(sdk) !== 400) return false;
  const idErr = fieldErrors(sdk)['id'];
  if (!idErr) return false;
  const code = (idErr.code ?? '').toLowerCase();
  const msg = (idErr.message ?? '').toLowerCase();
  return code.includes('unique') || code.includes('exists') || msg.includes('unique') || msg.includes('exists');
}

/**
 * Classify a thrown value from the PocketBase SDK into the taxonomy (SPEC §12).
 *
 * Two things this deliberately does NOT decide:
 *  - whether a 401 means "the session died" (a link condition, SPEC §5.10) or
 *    "the rules said no": that needs `pb.authStore` validity, which belongs to
 *    the caller. A 401 classifies as a rejection and push.ts overrides it.
 *  - 5xx/429 map to `link/offline`, not to a push code: the taxonomy has no
 *    server-error member, and the engine's response to both is identical —
 *    stop, back off, stay `connecting` (SPEC §2.2). `context.status` preserves
 *    what actually happened.
 *
 * DEVIATION from SPEC §5.9, recorded here rather than hidden: a `429` cannot
 * honour `Retry-After`, because the SDK surfaces no response headers at all (see
 * `SdkErrorLike` above). The jittered backoff ladder is the wait, and there is no
 * code here pretending otherwise.
 */
export function classifySdkError(
  err: unknown,
  ctx: { phase: 'push' | 'pull' | 'ping' | 'subscribe'; collection?: string },
): PBOError {
  const sdk = asSdkError(err);
  const status = sdk ? statusOf(sdk) : 0;
  const message = messageOf(err);
  const context: Record<string, unknown> = { phase: ctx.phase, status };
  if (ctx.collection !== undefined) context['collection'] = ctx.collection;

  const mk = (code: PBOErrorCode, extra?: Record<string, unknown>): PBOError =>
    new PBOError(code, message, { cause: err, context: { ...context, ...extra } });

  if (sdk?.isAbort === true || sdk?.name === 'AbortError' || sdk?.name === 'TimeoutError') {
    return mk('link/timeout');
  }
  // Status 0 is the SDK's shape for "the request never completed" — a fetch
  // TypeError, a DNS failure, a severed socket. Authoritative down (SPEC §3.1b).
  if (status === 0) return mk('link/offline');

  if (status === 429 || status >= 500) return mk('link/offline');

  if (ctx.phase === 'pull' || ctx.phase === 'subscribe') {
    // 401 classifies here alongside 403/400 for the same reason it does on the
    // push side: whether it means "the session died" (a link condition, §5.10)
    // or "the rules said no" needs `pb.authStore` validity, which belongs to the
    // caller. `pull.ts` reads `status` off the context and overrides.
    if (status === 403 || status === 401 || status === 400) return mk('pull/denied');
    if (status === 404) return mk('pull/denied');
    return mk('pull/malformed');
  }

  if (ctx.phase === 'ping') return mk('link/offline');

  // push
  if (isIdTakenError(err)) return mk('push/conflict');
  if (status === 404) return mk('push/vanished');
  return mk('push/rejected');
}

/** Coerce anything thrown into a PBOError so no bare Error escapes the API. */
export function toPBOError(err: unknown, fallback: PBOErrorCode = 'internal/invariant'): PBOError {
  if (err instanceof PBOError) return err;
  const message = messageOf(err);
  return new PBOError(fallback, message, { cause: err });
}

/** Throw for a violated internal invariant, with enough context to reproduce. */
export function invariant(cond: unknown, msg: string, context?: Record<string, unknown>): asserts cond {
  if (!cond) {
    throw new PBOError('internal/invariant', msg, context === undefined ? undefined : { context });
  }
}
