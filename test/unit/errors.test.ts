/** SPEC §12 — the error taxonomy. TESTPLAN §1.14. */
import { describe, expect, it } from 'vitest';
import { classifySdkError, isIdTakenError, isRetryable, invariant, PBOError, toPBOError } from '../../src/errors.js';
import { FakeClientResponseError, networkError } from '../fakes/pb.js';
import type { PBOErrorCode } from '../../src/types.js';

const ALL_CODES: PBOErrorCode[] = [
  'config/unknown-collection',
  'config/bad-id',
  'config/invalid',
  'auth/unauthenticated',
  'auth/changed-mid-flight',
  'link/offline',
  'link/timeout',
  'push/rejected',
  'push/vanished',
  'push/conflict',
  'pull/denied',
  'pull/cursor-stale',
  'pull/malformed',
  'db/open-failed',
  'db/quota',
  'db/version-conflict',
  'lock/unavailable',
  'internal/invariant',
];

/**
 * The shape the PB SDK's ClientResponseError ACTUALLY presents.
 *
 * Built through the structural copy in `test/fakes/pb.ts` rather than by hand, so
 * this file cannot drift into asserting against a field the SDK does not have —
 * which is exactly how the `Retry-After` clause below stayed green for so long.
 */
function sdkError(
  status: number,
  data?: Record<string, { code?: string; message?: string }>,
  extra?: { isAbort?: boolean; name?: string },
): unknown {
  return new FakeClientResponseError({
    status,
    ...extra,
    response: { code: status, message: 'failed', data: data ?? {} },
  });
}

describe('PBOError', () => {
  it('marks exactly the documented codes retryable', () => {
    const retryable = ALL_CODES.filter(isRetryable);
    expect(retryable.sort()).toEqual(
      ['auth/changed-mid-flight', 'db/quota', 'link/offline', 'link/timeout', 'lock/unavailable', 'pull/cursor-stale', 'pull/malformed'].sort(),
    );
  });

  it('serialises to the wire shape', () => {
    const shape = new PBOError('push/rejected', 'no').toShape();
    expect(shape).toMatchObject({ code: 'push/rejected', message: 'no', retryable: false });
    expect(typeof shape.at).toBe('number');
  });
});

describe('classifySdkError', () => {
  it('treats a request that never completed as a link condition', () => {
    expect(classifySdkError(sdkError(0), { phase: 'push' }).code).toBe('link/offline');
    expect(classifySdkError(sdkError(0), { phase: 'push' }).retryable).toBe(true);
  });

  it('treats an abort/timeout as link/timeout', () => {
    expect(classifySdkError(sdkError(0, {}, { isAbort: true }), { phase: 'pull' }).code).toBe('link/timeout');
  });

  it('treats 5xx and 429 as link conditions, never as an error state', () => {
    for (const status of [500, 502, 503, 429]) {
      expect(classifySdkError(sdkError(status), { phase: 'push' }).code).toBe('link/offline');
    }
  });

  /**
   * REGRESSION (review finding 5). The SDK surfaces no response headers, so a
   * `Retry-After` is unreachable and SPEC §5.9's clause cannot be honoured. This
   * asserts the reality — a 429 carries the body and NOTHING else — so nobody
   * re-adds a reader for a field that will never arrive.
   */
  it('cannot read Retry-After off a 429: the SDK surfaces no headers', () => {
    const err = new FakeClientResponseError({
      status: 429,
      response: { code: 429, message: 'Too Many Requests.', data: {} },
    });
    // The whole error surface, enumerated. No headers anywhere on it.
    expect(Object.keys(err.response).sort()).toEqual(['code', 'data', 'message']);
    expect((err as unknown as Record<string, unknown>)['headers']).toBeUndefined();
    expect((err.response as Record<string, unknown>)['retryAfter']).toBeUndefined();

    const classified = classifySdkError(err, { phase: 'push' });
    expect(classified.code).toBe('link/offline');
    expect(classified.context).not.toHaveProperty('retryAfterMs');
  });

  it('mirrors the real ClientResponseError surface, `data` alias included', () => {
    const err = networkError();
    expect(err.status).toBe(0);
    expect(err.isAbort).toBe(false);
    // The SDK WRAPS a fetch TypeError rather than rethrowing it, so the name is
    // the class's own and the TypeError survives as `originalError`.
    expect(err.name).toBe('ClientResponseError 0');
    expect((err.originalError as Error).name).toBe('TypeError');
    expect(err.data).toBe(err.response);
    expect(classifySdkError(err, { phase: 'pull' }).code).toBe('link/offline');
  });

  it('keys the create-id-collision on the BODY, not the status (SPEC §5.7)', () => {
    const taken = sdkError(400, { id: { code: 'validation_not_unique', message: 'Value must be unique.' } });
    expect(isIdTakenError(taken)).toBe(true);
    expect(classifySdkError(taken, { phase: 'push' }).code).toBe('push/conflict');

    const ordinary = sdkError(400, { title: { code: 'validation_required', message: 'Missing.' } });
    expect(isIdTakenError(ordinary)).toBe(false);
    expect(classifySdkError(ordinary, { phase: 'push' }).code).toBe('push/rejected');
  });

  it('maps 404 on a push to push/vanished and other 4xx to push/rejected', () => {
    expect(classifySdkError(sdkError(404), { phase: 'push' }).code).toBe('push/vanished');
    expect(classifySdkError(sdkError(403), { phase: 'push' }).code).toBe('push/rejected');
    expect(classifySdkError(sdkError(400), { phase: 'push' }).code).toBe('push/rejected');
  });

  it('maps a rejected pull to pull/denied and an unreadable page to pull/malformed', () => {
    expect(classifySdkError(sdkError(403), { phase: 'pull', collection: 'projects' }).code).toBe('pull/denied');
    expect(classifySdkError(sdkError(400), { phase: 'pull' }).code).toBe('pull/denied');
    expect(classifySdkError(sdkError(418), { phase: 'pull' }).code).toBe('pull/malformed');
  });

  it('carries enough context to reproduce', () => {
    const e = classifySdkError(sdkError(403), { phase: 'pull', collection: 'tasks' });
    expect(e.context).toMatchObject({ phase: 'pull', collection: 'tasks', status: 403 });
    expect(e.cause).toBeDefined();
  });
});

describe('toPBOError / invariant', () => {
  it('passes a PBOError through untouched', () => {
    const original = new PBOError('db/quota', 'full');
    expect(toPBOError(original)).toBe(original);
  });

  it('coerces anything else so no bare Error escapes the API', () => {
    expect(toPBOError(new Error('boom')).code).toBe('internal/invariant');
    expect(toPBOError('boom').message).toBe('boom');
    expect(toPBOError(new Error('boom'), 'db/open-failed').code).toBe('db/open-failed');
  });

  it('throws internal/invariant with context, and is a no-op when the condition holds', () => {
    expect(() => invariant(true, 'fine')).not.toThrow();
    try {
      invariant(false, 'broken', { seq: 7 });
      throw new Error('should have thrown');
    } catch (e) {
      expect((e as PBOError).code).toBe('internal/invariant');
      expect((e as PBOError).context).toMatchObject({ seq: 7 });
    }
  });
});
