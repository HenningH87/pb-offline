/**
 * A hand-written PocketBase SDK fake (TESTPLAN §1).
 *
 * It records every request, so the unit tier can assert the thing the library
 * promises hardest: that a local write makes no network call at all.
 *
 * Beyond recording it is a small, honest server: an in-memory record store that
 * stamps a monotonically increasing `updated` exactly the way PocketBase's
 * autodate does (the client never writes that field, so the fake must), plus
 * scripting hooks for the failure modes SPEC §5 and §6 enumerate — 4xx, 5xx,
 * timeouts, a taken id, a severed link.
 *
 * FIDELITY RULE (TESTPLAN §1, and a review finding that cost two green tests):
 * this file MUST NOT invent an SDK affordance. A fake that grows a field the real
 * SDK does not have certifies behaviour that cannot occur — which is worse than
 * no test, because it reads as coverage. Every shape below is checked against
 * `node_modules/pocketbase/dist/pocketbase.es.mjs`. Notably there is NO
 * `Retry-After` anywhere: `ClientResponseError` is `{url, status, response,
 * isAbort, originalError}` with a `data` getter aliasing `response`, `response`
 * is the PARSED JSON BODY, and the `Response` object never leaves the SDK.
 */
import type PocketBase from 'pocketbase';

export interface FakeRequest {
  method: 'create' | 'update' | 'delete' | 'getList' | 'health' | 'subscribe' | 'unsubscribe';
  collection: string;
  args: unknown[];
}

/** One live SSE subscription, as the fake sees it (TESTPLAN §1.12, §2.3). */
export interface FakeSubscription {
  collection: string;
  topic: string;
  filter: string | null;
  cb: (data: unknown) => void;
}

export interface FakeRecord {
  id: string;
  created: string;
  updated: string;
  [k: string]: unknown;
}

/** A PocketBase error BODY — what `response` actually holds on a real error. */
export interface FakeErrorBody {
  code?: number;
  message?: string;
  data?: Record<string, { code?: string; message?: string }>;
}

/** Fields the real `ClientResponseError` constructor accepts. */
export interface FakeErrorInit {
  url?: string;
  status?: number;
  isAbort?: boolean;
  name?: string;
  message?: string;
  response?: FakeErrorBody;
  originalError?: unknown;
}

type Method = FakeRequest['method'];
type Scripted = { kind: 'error'; error: unknown } | { kind: 'value'; value: unknown };

export interface FakePB {
  requests: FakeRequest[];
  autoCancellationCalls: boolean[];
  authStore: {
    record: { id: string } | null;
    token: string;
    isValid: boolean;
    onChange(cb: (token: string, record: unknown) => void): () => void;
    clear(): void;
  };
  setOwner(id: string | null): void;
  autoCancellation(v: boolean): void;
  collection(name: string): {
    create(payload: unknown): Promise<unknown>;
    update(id: string, payload: unknown): Promise<unknown>;
    delete(id: string): Promise<unknown>;
    getList(page: number, perPage: number, opts?: unknown): Promise<unknown>;
    subscribe(topic: string, cb: unknown, opts?: unknown): Promise<() => Promise<void>>;
  };
  health: { check(): Promise<{ code: number }> };

  // ── realtime (SPEC §9) ───────────────────────────────────────────────────
  /** The realtime client surface `sync/realtime.ts` binds to. */
  realtime: {
    isConnected: boolean;
    onDisconnect?: (activeSubscriptions: string[]) => void;
    subscribe(topic: string, cb: (data: unknown) => void): Promise<() => Promise<void>>;
  };
  /** Live subscriptions, in the order they were made. */
  subscriptions: FakeSubscription[];
  /** Called as each subscription is torn down — a step marker for order tests. */
  onUnsubscribe: ((sub: FakeSubscription) => void) | null;
  /** Deliver an SSE event to every subscription on `collection`. */
  emitEvent(collection: string, action: 'create' | 'update' | 'delete', record: unknown): void;
  /** Simulate the SSE socket dropping while subscriptions are still live. */
  dropConnection(): void;
  /** Simulate the SDK reconnecting and re-submitting its subscriptions. */
  reconnect(): void;

  // ── scripting ────────────────────────────────────────────────────────────
  /** When true every request fails at the network level (status 0). */
  offline: boolean;
  /**
   * Called with every recorded request BEFORE it is answered. The hook may write
   * to Dexie, which is how a test simulates another tab coalescing into the
   * queue mid-drain (SPEC §5, invariant 2).
   */
  onRequest: ((req: FakeRequest) => void | Promise<void>) | null;
  /** True while a request is in flight — asserts SPEC §5.1's "one at a time". */
  inFlight: number;
  maxInFlight: number;
  /** Queue one scripted outcome for the next matching request. */
  failNext(collection: string, method: Method, error: unknown): void;
  valueNext(collection: string, method: Method, value: unknown): void;
  /** Successive `getList` responses for a collection. */
  setPages(collection: string, pages: FakeRecord[][]): void;
  /** The in-memory server store. */
  records: Map<string, Map<string, FakeRecord>>;
  seed(collection: string, records: Array<Partial<FakeRecord> & { id: string }>): FakeRecord[];
  get(collection: string, id: string): FakeRecord | undefined;
  /** Requests, filtered to the record-bearing ones (health pings excluded). */
  recordRequests(): FakeRequest[];
  /** The next `updated` the fake will stamp, without consuming it. */
  clock: number;
  stamp(): string;
}

/**
 * A structural copy of `ClientResponseError`, field for field.
 *
 * Transcribed from the SDK's own constructor: `response` defaults to `{}`,
 * `isAbort` is true when the source was an AbortError, `name` is
 * `"ClientResponseError " + status` (NOT a bare class name — engine code that
 * matched on the bare name would be matching nothing), `message` comes from the
 * BODY, `data` is a getter aliasing `response`, and `cause` is `originalError`.
 * There is no header, no `retryAfter`, and no `Response`.
 */
export class FakeClientResponseError extends Error {
  url = '';
  status = 0;
  response: FakeErrorBody = {};
  isAbort = false;
  originalError: unknown = null;

  constructor(init: FakeErrorInit = {}) {
    super('ClientResponseError');
    this.url = init.url ?? '';
    this.status = typeof init.status === 'number' ? init.status : 0;
    this.isAbort = init.isAbort === true || init.name === 'AbortError';
    this.originalError = init.originalError ?? null;
    this.response = init.response ?? {};
    this.name = init.name ?? `ClientResponseError ${this.status}`;
    this.message = this.response.message ?? init.message ?? 'Something went wrong.';
    this.cause = this.originalError;
  }

  get data(): FakeErrorBody {
    return this.response;
  }

  override toString(): string {
    return `${this.name}: ${this.message}`;
  }
}

/** Kept as the exported name the tests import. */
export type FakeError = FakeClientResponseError;

/** A PocketBase-shaped error, as `send()` would throw it for an HTTP failure. */
export function sdkError(status: number, extra?: FakeErrorInit): FakeClientResponseError {
  return new FakeClientResponseError({
    url: 'http://127.0.0.1:8090/api/collections/x/records',
    ...extra,
    status,
    response: { code: status, message: `fake ${status}`, data: {}, ...extra?.response },
  });
}

/** The exact body SPEC §5.7 keys on: PB's `validation_not_unique` on `id`. */
export function idTakenError(): FakeClientResponseError {
  return sdkError(400, {
    response: {
      code: 400,
      message: 'Failed to create record.',
      data: { id: { code: 'validation_not_unique', message: 'Value must be unique.' } },
    },
  });
}

/**
 * A network-level failure.
 *
 * The SDK's `send()` catches the fetch `TypeError` and rethrows it WRAPPED:
 * `.catch(e => { throw new ClientResponseError(e) })`. So what reaches the engine
 * has status 0, an empty `response`, and the TypeError as `originalError` — not
 * the bare TypeError the fake used to hand over.
 */
export function networkError(): FakeClientResponseError {
  return new FakeClientResponseError({
    status: 0,
    originalError: Object.assign(new TypeError('Failed to fetch'), { name: 'TypeError' }),
    message: 'Failed to fetch',
  });
}

/** An aborted / autocancelled request, as the SDK reports it. */
export function abortError(): FakeClientResponseError {
  return new FakeClientResponseError({
    status: 0,
    isAbort: true,
    message: 'The request was aborted (most likely autocancelled).',
  });
}

export function createFakePB(opts?: { ownerId?: string | null; offline?: boolean }): FakePB {
  const requests: FakeRequest[] = [];
  const listeners = new Set<(token: string, record: unknown) => void>();
  const script = new Map<string, Scripted[]>();
  const pages = new Map<string, FakeRecord[][]>();
  const records = new Map<string, Map<string, FakeRecord>>();
  const subscriptions: FakeSubscription[] = [];
  const connectListeners = new Set<(data: unknown) => void>();
  let record: { id: string } | null = opts?.ownerId != null ? { id: opts.ownerId } : null;

  const key = (collection: string, method: Method): string => `${collection}:${method}`;
  const tableOf = (collection: string): Map<string, FakeRecord> => {
    const t = records.get(collection) ?? new Map<string, FakeRecord>();
    records.set(collection, t);
    return t;
  };

  const fake: FakePB = {
    requests,
    autoCancellationCalls: [],
    offline: opts?.offline ?? false,
    onRequest: null,
    inFlight: 0,
    maxInFlight: 0,
    records,
    subscriptions,
    onUnsubscribe: null,
    clock: 0,

    realtime: {
      isConnected: false,
      async subscribe(topic, cb) {
        if (topic !== 'PB_CONNECT') throw new Error(`fake realtime.subscribe: ${topic}`);
        connectListeners.add(cb);
        return async () => {
          connectListeners.delete(cb);
        };
      },
    },

    emitEvent(collection, action, rec) {
      for (const s of [...subscriptions]) {
        if (s.collection !== collection) continue;
        s.cb({ action, record: rec });
      }
    },

    dropConnection() {
      fake.realtime.isConnected = false;
      // The SDK reports the still-active subscriptions, which is how a client
      // tells a network drop from its own teardown.
      fake.realtime.onDisconnect?.(subscriptions.map((s) => `${s.collection}/${s.topic}`));
    },

    reconnect() {
      fake.realtime.isConnected = true;
      for (const cb of [...connectListeners]) cb({});
    },

    /**
     * PocketBase's autodate shape, fixed width. Monotonic per call, because the
     * engine's whole ordering story is that only the server stamps this.
     */
    stamp() {
      fake.clock += 1;
      const ms = Date.UTC(2026, 0, 1) + fake.clock;
      const iso = new Date(ms).toISOString(); // 2026-01-01T00:00:00.001Z
      return `${iso.slice(0, 10)} ${iso.slice(11, 23)}Z`;
    },

    authStore: {
      get record() {
        return record;
      },
      get token() {
        return record ? `token-${record.id}` : '';
      },
      get isValid() {
        return record !== null;
      },
      onChange(cb) {
        listeners.add(cb);
        return () => listeners.delete(cb);
      },
      clear() {
        record = null;
        for (const cb of listeners) cb('', null);
      },
    },
    setOwner(id) {
      record = id === null ? null : { id };
      for (const cb of listeners) cb(fake.authStore.token, record);
    },
    autoCancellation(v) {
      fake.autoCancellationCalls.push(v);
    },

    failNext(collection, method, error) {
      const list = script.get(key(collection, method)) ?? [];
      list.push({ kind: 'error', error });
      script.set(key(collection, method), list);
    },
    valueNext(collection, method, value) {
      const list = script.get(key(collection, method)) ?? [];
      list.push({ kind: 'value', value });
      script.set(key(collection, method), list);
    },
    setPages(collection, p) {
      pages.set(collection, p.map((page) => [...page]));
    },
    seed(collection, rows) {
      const t = tableOf(collection);
      const out: FakeRecord[] = [];
      for (const r of rows) {
        const stamped: FakeRecord = {
          created: r.created ?? fake.stamp(),
          updated: r.updated ?? fake.stamp(),
          ...r,
        } as FakeRecord;
        t.set(stamped.id, stamped);
        out.push(stamped);
      }
      return out;
    },
    get(collection, id) {
      return records.get(collection)?.get(id);
    },
    recordRequests() {
      return requests.filter((r) => r.method !== 'health');
    },

    collection(name) {
      const push = (method: Method, args: unknown[]): void => {
        requests.push({ method, collection: name, args });
      };

      /** Every request goes through here: recording, scripting, concurrency. */
      const run = async <T>(method: Method, args: unknown[], real: () => T): Promise<T> => {
        push(method, args);
        fake.inFlight += 1;
        fake.maxInFlight = Math.max(fake.maxInFlight, fake.inFlight);
        try {
          if (fake.onRequest) await fake.onRequest({ method, collection: name, args });
          const scripted = script.get(key(name, method))?.shift();
          if (scripted) {
            await Promise.resolve();
            if (scripted.kind === 'error') throw scripted.error;
            return scripted.value as T;
          }
          if (fake.offline) {
            await Promise.resolve();
            throw networkError();
          }
          await Promise.resolve();
          return real();
        } finally {
          fake.inFlight -= 1;
        }
      };

      return {
        async create(payload) {
          return run('create', [payload], () => {
            const body = { ...(payload as Record<string, unknown>) };
            const id = String(body['id'] ?? '');
            const t = tableOf(name);
            if (id !== '' && t.has(id)) throw idTakenError();
            const stamp = fake.stamp();
            const rec = { ...body, id, created: stamp, updated: stamp } as FakeRecord;
            t.set(id, rec);
            return { ...rec };
          });
        },
        async update(id, payload) {
          return run('update', [id, payload], () => {
            const t = tableOf(name);
            const existing = t.get(id);
            if (!existing) throw sdkError(404);
            const rec: FakeRecord = { ...existing, ...(payload as Record<string, unknown>), id, updated: fake.stamp() };
            t.set(id, rec);
            return { ...rec };
          });
        },
        async delete(id) {
          return run('delete', [id], () => {
            const t = tableOf(name);
            if (!t.has(id)) throw sdkError(404);
            t.delete(id);
            return true;
          });
        },
        async getList(page, perPage, o) {
          return run('getList', [page, perPage, o], () => {
            const scripted = pages.get(name);
            if (scripted) return { items: scripted.shift() ?? [], page, perPage };
            // SPEC §5.12's probe: a read filtered on one id answers from the table.
            const byId = /^id = "([^"]+)"$/.exec(String((o as { filter?: unknown } | undefined)?.filter ?? ''));
            if (byId) {
              const rec = tableOf(name).get(byId[1]!);
              return { items: rec ? [{ ...rec }] : [], page, perPage };
            }
            return { items: [], page, perPage };
          });
        },
        /**
         * SPEC §9.1 — the fake records the topic AND the filter, because "one
         * filtered subscription per collection" is the invariant under test.
         */
        async subscribe(topic, cb, o) {
          push('subscribe', [topic, o]);
          const scripted = script.get(key(name, 'subscribe'))?.shift();
          if (scripted) {
            await Promise.resolve();
            if (scripted.kind === 'error') throw scripted.error;
          }
          if (fake.offline) {
            await Promise.resolve();
            throw networkError();
          }
          const entry: FakeSubscription = {
            collection: name,
            topic,
            filter: ((o as { filter?: string } | undefined)?.filter ?? null) as string | null,
            cb: cb as (data: unknown) => void,
          };
          subscriptions.push(entry);
          fake.realtime.isConnected = true;
          return async () => {
            const at = subscriptions.indexOf(entry);
            if (at >= 0) subscriptions.splice(at, 1);
            fake.onUnsubscribe?.(entry);
            requests.push({ method: 'unsubscribe', collection: name, args: [topic] });
            if (subscriptions.length === 0) fake.realtime.isConnected = false;
          };
        },
      };
    },

    health: {
      /** Scripted as collection `''`, e.g. `failNext('', 'health', sdkError(503))`. */
      async check() {
        requests.push({ method: 'health', collection: '', args: [] });
        await Promise.resolve();
        const scripted = script.get(key('', 'health'))?.shift();
        if (scripted) {
          if (scripted.kind === 'error') throw scripted.error;
          return scripted.value as { code: number };
        }
        if (fake.offline) throw networkError();
        return { code: 200 };
      },
    },
  };
  return fake;
}

/** The fake, typed as the SDK for call sites that want the real signature. */
export function asPB(fake: FakePB): PocketBase {
  return fake as unknown as PocketBase;
}
