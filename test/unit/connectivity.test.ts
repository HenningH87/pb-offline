/** SPEC §3 — the link watcher is the single authority on `online`. */
import { afterEach, describe, expect, it } from 'vitest';
import { createConnectivityWatcher, type ConnectivityWatcher } from '../../src/net/connectivity.js';
import { asPB, createFakePB, networkError, sdkError, type FakePB } from '../fakes/pb.js';
import { tick } from './_helpers.js';

/** A minimal event target, because Node's globalThis is not one. */
function installDom(): { fire: (target: 'global' | 'document', type: string) => void; restore: () => void; setVisible: (v: string) => void } {
  const handlers = new Map<string, Set<() => void>>();
  const docHandlers = new Map<string, Set<() => void>>();
  const g = globalThis as unknown as Record<string, unknown>;
  const before = {
    add: g['addEventListener'],
    remove: g['removeEventListener'],
    doc: g['document'],
  };
  const visibility = { state: 'visible' };

  const bind = (map: Map<string, Set<() => void>>) => ({
    addEventListener: (t: string, f: () => void) => {
      const set = map.get(t) ?? new Set<() => void>();
      set.add(f);
      map.set(t, set);
    },
    removeEventListener: (t: string, f: () => void) => map.get(t)?.delete(f),
  });

  const globalBinding = bind(handlers);
  g['addEventListener'] = globalBinding.addEventListener;
  g['removeEventListener'] = globalBinding.removeEventListener;
  g['document'] = {
    ...bind(docHandlers),
    get visibilityState() {
      return visibility.state;
    },
  };

  return {
    fire(target, type) {
      const map = target === 'global' ? handlers : docHandlers;
      for (const f of [...(map.get(type) ?? [])]) f();
    },
    setVisible(v) {
      visibility.state = v;
    },
    restore() {
      g['addEventListener'] = before.add;
      g['removeEventListener'] = before.remove;
      if (before.doc === undefined) delete g['document'];
      else g['document'] = before.doc;
    },
  };
}

function installNavigator(onLine: boolean | undefined): () => void {
  const descriptor = Object.getOwnPropertyDescriptor(globalThis, 'navigator');
  Object.defineProperty(globalThis, 'navigator', {
    configurable: true,
    value: onLine === undefined ? {} : { onLine },
  });
  return () => {
    if (descriptor) Object.defineProperty(globalThis, 'navigator', descriptor);
    else delete (globalThis as unknown as Record<string, unknown>)['navigator'];
  };
}

interface Harness {
  watcher: ConnectivityWatcher;
  pb: FakePB;
  ups: number;
  downs: string[];
}

const cleanups: Array<() => void> = [];
afterEach(() => {
  while (cleanups.length > 0) cleanups.pop()?.();
});

function make(opts?: { offline?: boolean; isDrainer?: () => boolean; min?: number; max?: number }): Harness {
  const pb = createFakePB({ ownerId: 'u1', offline: opts?.offline ?? false });
  const h: Harness = { watcher: null as unknown as ConnectivityWatcher, pb, ups: 0, downs: [] };
  h.watcher = createConnectivityWatcher({
    pb: asPB(pb),
    backoff: { min: opts?.min ?? 20, max: opts?.max ?? 40 },
    onUp: () => {
      h.ups += 1;
    },
    onDown: (reason) => {
      h.downs.push(reason);
    },
    ...(opts?.isDrainer ? { isDrainer: opts.isDrainer } : {}),
  });
  cleanups.push(() => h.watcher.stop());
  return h;
}

describe('authority order (SPEC §3.1)', () => {
  it('a 4xx marks the link UP even though the request failed', () => {
    const h = make();
    expect(h.watcher.isOnline()).toBe(false);
    // ok:false, networkFailure:false — an HTTP status arrived, so the link works.
    h.watcher.observeRequest(false, false);
    expect(h.watcher.isOnline()).toBe(true);
    expect(h.ups).toBe(1);
  });

  it('a network-level failure marks it DOWN', () => {
    const h = make();
    h.watcher.observeRequest(true, false);
    expect(h.watcher.isOnline()).toBe(true);
    h.watcher.observeRequest(false, true);
    expect(h.watcher.isOnline()).toBe(false);
    expect(h.downs).toEqual(['ping']);
  });

  it('navigator.onLine === false is authoritative down at start', async () => {
    cleanups.push(installNavigator(false));
    const h = make();
    h.watcher.start();
    await tick(5);
    expect(h.watcher.isOnline()).toBe(false);
    // Not one ping was made: the OS already answered the question.
    expect(h.pb.requests).toEqual([]);
  });

  it('navigator.onLine === true alone does NOT mark it up — only a round trip does', async () => {
    cleanups.push(installNavigator(true));
    const h = make({ offline: true });
    h.watcher.start();
    expect(h.watcher.isOnline()).toBe(false);
    await tick(10);
    // A ping WAS attempted (that is what start means) and it failed.
    expect(h.pb.requests.map((r) => r.method)).toContain('health');
    expect(h.watcher.isOnline()).toBe(false);
  });

  it('an SSE disconnect is SUSPECT, not down: it probes and the probe decides', async () => {
    const h = make();
    h.watcher.start();
    await tick(10);
    expect(h.watcher.isOnline()).toBe(true);

    h.pb.offline = true;
    h.watcher.observeSse(false);
    await tick(10);
    expect(h.watcher.isOnline()).toBe(false);
    expect(h.downs).toEqual(['sse']);

    h.pb.offline = false;
    h.watcher.observeSse(true);
    expect(h.watcher.isOnline()).toBe(true);
  });

  it('a lost session is down with reason auth (SPEC §5.10)', async () => {
    const h = make();
    h.watcher.observeRequest(true, false);
    h.watcher.observeAuthLoss();
    expect(h.watcher.isOnline()).toBe(false);
    expect(h.downs).toEqual(['auth']);
  });

  it('emits offline with the right reason for the browser source', async () => {
    const dom = installDom();
    cleanups.push(dom.restore);
    const h = make();
    h.watcher.start();
    await tick(10);
    expect(h.watcher.isOnline()).toBe(true);
    dom.fire('global', 'offline');
    expect(h.downs).toEqual(['browser']);
  });
});

describe('the health ping (SPEC §3.2, §3.3)', () => {
  it('holds at most one ping in flight', async () => {
    const h = make({ offline: true });
    const all = await Promise.all([h.watcher.pingNow(), h.watcher.pingNow(), h.watcher.pingNow()]);
    expect(all).toEqual([false, false, false]);
    expect(h.pb.requests.filter((r) => r.method === 'health')).toHaveLength(1);
  });

  it('retries on the ladder while down, and stops on recovery', async () => {
    const h = make({ offline: true, min: 5, max: 10 });
    h.watcher.start();
    await tick(60);
    const attempts = h.pb.requests.filter((r) => r.method === 'health').length;
    expect(attempts).toBeGreaterThan(1);

    h.pb.offline = false;
    await tick(40);
    expect(h.watcher.isOnline()).toBe(true);
    const settled = h.pb.requests.filter((r) => r.method === 'health').length;
    await tick(40);
    // Up: no more pinging.
    expect(h.pb.requests.filter((r) => r.method === 'health').length).toBe(settled);
  });

  it('resets the backoff to min on any transition to up (SPEC §3.5)', async () => {
    const h = make({ offline: true, min: 5, max: 5_000 });
    h.watcher.start();
    await tick(60);
    expect(h.watcher.backoff.attempt).toBeGreaterThan(0);
    h.watcher.observeRequest(true, false);
    expect(h.watcher.backoff.attempt).toBe(0);
    expect(h.watcher.backoff.peek()).toBe(5);
  });

  it('pings immediately on an online event and on becoming visible', async () => {
    const dom = installDom();
    cleanups.push(dom.restore);
    // A long ladder, so anything that arrives promptly can only be the
    // immediate trigger and not the timer.
    const h = make({ offline: true, min: 10_000, max: 10_000 });
    h.watcher.start();
    await tick(10);
    const afterStart = h.pb.requests.length;

    dom.fire('global', 'online');
    await tick(10);
    expect(h.pb.requests.length).toBe(afterStart + 1);

    dom.setVisible('hidden');
    dom.fire('document', 'visibilitychange');
    await tick(10);
    expect(h.pb.requests.length).toBe(afterStart + 1); // hidden: no ping

    dom.setVisible('visible');
    dom.fire('document', 'visibilitychange');
    await tick(10);
    expect(h.pb.requests.length).toBe(afterStart + 2);
  });

  it('treats a 5xx health response as proof the link works (authority 1a)', async () => {
    const h = make({ offline: true });
    h.pb.offline = false;
    h.pb.failNext('', 'health', sdkError(503));
    // The fake scripts per (collection, method); health has no collection.
    const ok = await h.watcher.pingNow();
    expect(ok).toBe(true);
    expect(h.watcher.isOnline()).toBe(true);
  });

  it('a non-drainer tab never pings — it adopts the drainer verdict (SPEC §3.6)', async () => {
    const h = make({ offline: false, isDrainer: () => false });
    h.watcher.start();
    await tick(30);
    expect(h.pb.requests).toEqual([]);
    expect(h.watcher.isOnline()).toBe(false);

    h.watcher.adopt(true);
    expect(h.watcher.isOnline()).toBe(true);
    expect(h.ups).toBe(1);
    expect(h.pb.requests).toEqual([]);
  });
});

describe('transitions', () => {
  it('down→up fires onUp exactly once per transition', async () => {
    const h = make();
    h.watcher.observeRequest(true, false);
    h.watcher.observeRequest(true, false);
    h.watcher.observeRequest(true, false);
    expect(h.ups).toBe(1);
    h.watcher.observeRequest(false, true);
    h.watcher.observeRequest(false, true);
    expect(h.downs).toEqual(['ping']);
    h.watcher.observeRequest(true, false);
    expect(h.ups).toBe(2);
  });

  it('a network error object from the SDK classifies as down, an HTTP one does not', async () => {
    const h = make();
    h.pb.offline = false;
    h.pb.failNext('', 'health', networkError());
    await h.watcher.pingNow();
    expect(h.watcher.isOnline()).toBe(false);
  });
});
