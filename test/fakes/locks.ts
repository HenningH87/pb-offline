/**
 * An in-process Web Locks manager (TESTPLAN §1.12).
 *
 * `fake-indexeddb` gives the unit tier IndexedDB but nothing gives it
 * `navigator.locks` on every supported Node, so the election tests drive this
 * double instead. It implements the slice `sync/locks.ts` uses — exclusive mode,
 * `ifAvailable`, and a FIFO queue that promotes the next waiter when the holder's
 * callback settles — and nothing else.
 */

interface Waiter {
  resolve: (v: unknown) => void;
  reject: (err: unknown) => void;
  callback: (lock: unknown) => unknown;
}

export interface FakeLockManager {
  request(
    name: string,
    options: { mode?: 'exclusive' | 'shared'; ifAvailable?: boolean },
    callback: (lock: unknown) => unknown,
  ): Promise<unknown>;
  /** Names currently held. Test introspection only. */
  held(): string[];
  /** How many requests are queued behind the holder of `name`. */
  waiting(name: string): number;
}

export function createLockManager(): FakeLockManager {
  const holders = new Set<string>();
  const queues = new Map<string, Waiter[]>();

  const queueOf = (name: string): Waiter[] => {
    const q = queues.get(name) ?? [];
    queues.set(name, q);
    return q;
  };

  function grant(name: string, waiter: Waiter): void {
    holders.add(name);
    let result: unknown;
    try {
      result = waiter.callback({ name, mode: 'exclusive' });
    } catch (err) {
      holders.delete(name);
      waiter.reject(err);
      queueMicrotask(() => pump(name));
      return;
    }
    // The lock is held for exactly as long as the callback's promise is pending —
    // that is the whole contract, and what a drainer relies on.
    void Promise.resolve(result).then(
      (v) => {
        holders.delete(name);
        waiter.resolve(v);
        pump(name);
      },
      (err) => {
        holders.delete(name);
        waiter.reject(err);
        pump(name);
      },
    );
  }

  function pump(name: string): void {
    if (holders.has(name)) return;
    const next = queueOf(name).shift();
    if (next) grant(name, next);
  }

  return {
    request(name, options, callback) {
      return new Promise((resolve, reject) => {
        const waiter: Waiter = { resolve, reject, callback };
        if (!holders.has(name)) {
          grant(name, waiter);
          return;
        }
        if (options?.ifAvailable) {
          // Not available: the callback is handed null and never holds anything.
          try {
            resolve(callback(null));
          } catch (err) {
            reject(err);
          }
          return;
        }
        queueOf(name).push(waiter);
      });
    },
    held: () => [...holders],
    waiting: (name) => queueOf(name).length,
  };
}
