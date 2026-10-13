/**
 * Typed event emitter (SPEC §2.4).
 *
 * Events are the imperative half of the reactive story: stores say what the state
 * IS, events say what just HAPPENED. Both are first-class.
 */
import type { PBOEvents, Unsubscriber } from '../types.js';
import { toPBOError } from '../errors.js';

export interface Emitter {
  on<E extends keyof PBOEvents>(event: E, cb: (payload: PBOEvents[E]) => void): Unsubscriber;
  once<E extends keyof PBOEvents>(event: E, cb: (payload: PBOEvents[E]) => void): Unsubscriber;
  emit<E extends keyof PBOEvents>(event: E, payload: PBOEvents[E]): void;
  clear(): void;
}

type AnyHandler = (payload: never) => void;

/**
 * A handler that throws MUST NOT unwind the engine (SPEC §2.4): the throw is
 * caught and routed to the 'error' event instead. A throwing 'error' handler is
 * swallowed — there is nowhere left to send it.
 */
export function createEmitter(logger?: (err: unknown) => void): Emitter {
  const handlers = new Map<keyof PBOEvents, Set<AnyHandler>>();

  function off(event: keyof PBOEvents, cb: AnyHandler): void {
    const set = handlers.get(event);
    if (!set) return;
    set.delete(cb);
    if (set.size === 0) handlers.delete(event);
  }

  function on<E extends keyof PBOEvents>(event: E, cb: (payload: PBOEvents[E]) => void): Unsubscriber {
    const set = handlers.get(event) ?? new Set<AnyHandler>();
    handlers.set(event, set);
    set.add(cb as AnyHandler);
    let live = true;
    return () => {
      if (!live) return;
      live = false;
      off(event, cb as AnyHandler);
    };
  }

  function emit<E extends keyof PBOEvents>(event: E, payload: PBOEvents[E]): void {
    const set = handlers.get(event);
    if (!set || set.size === 0) return;
    for (const cb of [...set]) {
      try {
        (cb as (p: PBOEvents[E]) => void)(payload);
      } catch (err) {
        if (event === 'error') {
          logger?.(err);
          continue;
        }
        try {
          emit('error', { error: toPBOError(err).toShape() });
        } catch {
          logger?.(err);
        }
      }
    }
  }

  return {
    on,
    once(event, cb) {
      const unsub = on(event, (payload) => {
        unsub();
        cb(payload);
      });
      return unsub;
    },
    emit,
    clear() {
      handlers.clear();
    },
  };
}
