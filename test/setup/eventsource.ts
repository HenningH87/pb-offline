/**
 * A minimal `EventSource` for the integration tier (TESTPLAN §2.3).
 *
 * Node ships one only behind `--experimental-eventsource`, and the flag is not
 * available on every Node this package supports — so rather than pin the test
 * command to a runtime flag, the SSE scenarios get a small implementation over
 * `fetch`. It covers exactly what the PocketBase SDK's realtime client uses:
 * `new EventSource(url)`, `onerror`, `addEventListener`/`removeEventListener` by
 * event NAME (PocketBase dispatches each subscription topic as its own event
 * type), `close()`, and events carrying `data` and `lastEventId`.
 *
 * It is a test fixture, not a product: no reconnection, no `withCredentials`,
 * no `readyState` transitions beyond the two the SDK observes.
 */

interface SseEvent {
  type: string;
  data: string;
  lastEventId: string;
}

type Listener = (ev: SseEvent) => void;

class NodeEventSource {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSED = 2;

  readonly url: string;
  readyState = 0;
  onerror: ((ev: unknown) => void) | null = null;
  onopen: ((ev: unknown) => void) | null = null;
  onmessage: Listener | null = null;

  private listeners = new Map<string, Set<Listener>>();
  private controller = new AbortController();
  private closed = false;

  constructor(url: string) {
    this.url = url;
    void this.run();
  }

  addEventListener(type: string, listener: Listener): void {
    const set = this.listeners.get(type) ?? new Set<Listener>();
    set.add(listener);
    this.listeners.set(type, set);
  }

  removeEventListener(type: string, listener: Listener): void {
    this.listeners.get(type)?.delete(listener);
  }

  close(): void {
    this.closed = true;
    this.readyState = NodeEventSource.CLOSED;
    this.controller.abort();
  }

  private dispatch(ev: SseEvent): void {
    if (this.closed) return;
    for (const listener of [...(this.listeners.get(ev.type) ?? [])]) listener(ev);
    if (ev.type === 'message') this.onmessage?.(ev);
  }

  private fail(err: unknown): void {
    if (this.closed) return;
    this.readyState = NodeEventSource.CLOSED;
    this.onerror?.(err);
  }

  private async run(): Promise<void> {
    let response: Response;
    try {
      response = await fetch(this.url, {
        headers: { Accept: 'text/event-stream', 'Cache-Control': 'no-cache' },
        signal: this.controller.signal,
      });
    } catch (err) {
      this.fail(err);
      return;
    }
    if (!response.ok || !response.body) {
      this.fail(new Error(`EventSource: ${response.status}`));
      return;
    }

    this.readyState = NodeEventSource.OPEN;
    this.onopen?.({});

    const reader = response.body.getReader();
    const decode = new TextDecoder();
    let buffer = '';
    let lastEventId = '';

    const flush = (frame: string): void => {
      let type = 'message';
      let id = lastEventId;
      const data: string[] = [];
      for (const raw of frame.split('\n')) {
        const line = raw.endsWith('\r') ? raw.slice(0, -1) : raw;
        if (line === '' || line.startsWith(':')) continue;
        const at = line.indexOf(':');
        const field = at === -1 ? line : line.slice(0, at);
        const value = at === -1 ? '' : line.slice(at + 1).replace(/^ /, '');
        if (field === 'event') type = value;
        else if (field === 'data') data.push(value);
        else if (field === 'id') id = value;
      }
      lastEventId = id;
      if (data.length === 0 && type === 'message') return;
      this.dispatch({ type, data: data.join('\n'), lastEventId: id });
    };

    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decode.decode(value, { stream: true });
        let split = buffer.indexOf('\n\n');
        while (split !== -1) {
          flush(buffer.slice(0, split));
          buffer = buffer.slice(split + 2);
          split = buffer.indexOf('\n\n');
        }
      }
      // The server closed the stream: that is a connection error to the SDK.
      this.fail(new Error('EventSource: stream ended'));
    } catch (err) {
      this.fail(err);
    }
  }
}

/** Install it globally unless the runtime already provides a real one. */
export function installEventSource(): void {
  const g = globalThis as { EventSource?: unknown };
  if (typeof g.EventSource === 'function') return;
  g.EventSource = NodeEventSource as unknown as typeof g.EventSource;
}

installEventSource();
