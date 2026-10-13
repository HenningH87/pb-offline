/**
 * SPEC §17.4 — the dual-peer tripwire.
 *
 * Two copies of Dexie in one page is the failure this guards: each copy has its
 * own `liveQuery` registry, so writes made through one never re-emit queries
 * created by the other. Nothing throws, nothing logs, no request fails — the UI
 * simply stops updating. A `file:` or `npm link` install produces it by default,
 * which is exactly how the first consumer met it.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import Dexie from 'dexie';
import { createPBOffline } from '../../src/index.js';
import {
  checkDexiePeer,
  DEXIE_IDENTITY_KEY,
  DEXIE_WARNED_KEY,
  DUAL_DEXIE_MESSAGE,
} from '../../src/db/peer.js';
import { asPB, createFakePB } from '../fakes/pb.js';
import { uniqueAppId } from './_helpers.js';
import type { PBOffline, PBOfflineConfig, PBOLogEvent } from '../../src/types.js';

type Stamps = Record<symbol, unknown>;
const g = globalThis as unknown as Stamps;

const engines: PBOffline[] = [];

/** A second Dexie, as a `file:` install would resolve one. Identity is the point. */
class OtherDexie {
  static readonly semVer = '4.0.11';
}

function makeEngine(over?: Partial<PBOfflineConfig>): PBOffline {
  const engine = createPBOffline(asPB(createFakePB({ ownerId: 'user1' })), {
    appId: uniqueAppId(),
    collections: [{ name: 'projects' }],
    ...over,
  } as PBOfflineConfig);
  engines.push(engine);
  return engine;
}

beforeEach(() => {
  delete g[DEXIE_IDENTITY_KEY];
  delete g[DEXIE_WARNED_KEY];
});

afterEach(async () => {
  while (engines.length > 0) await engines.pop()?.destroy();
  delete g[DEXIE_IDENTITY_KEY];
  delete g[DEXIE_WARNED_KEY];
  vi.restoreAllMocks();
});

describe('one copy of Dexie — the normal install', () => {
  it('stamps the constructor on globalThis and says nothing', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    makeEngine();
    expect(g[DEXIE_IDENTITY_KEY]).toBe(Dexie);
    expect(warn).not.toHaveBeenCalled();
  });

  it('stays silent for every further engine on the same constructor', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    makeEngine();
    makeEngine();
    checkDexiePeer();
    expect(warn).not.toHaveBeenCalled();
    expect(g[DEXIE_WARNED_KEY]).toBeUndefined();
  });
});

describe('two copies of Dexie — the file:/link hazard', () => {
  it('warns, ONCE, however many engines are created afterwards', () => {
    g[DEXIE_IDENTITY_KEY] = OtherDexie; // the other copy got there first
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});

    makeEngine();
    makeEngine();
    checkDexiePeer();

    expect(warn).toHaveBeenCalledTimes(1);
    expect(warn.mock.calls[0]![0]).toBe(DUAL_DEXIE_MESSAGE);
  });

  it('says what is wrong and where the fix is written down', () => {
    g[DEXIE_IDENTITY_KEY] = OtherDexie;
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    makeEngine();

    const msg = warn.mock.calls[0]![0] as string;
    expect(msg).toContain('two different copies of `dexie`');
    expect(msg).toContain('liveQuery');
    expect(msg).toContain('file:');
    expect(msg).toContain('Consuming via file: / link');
    expect(msg).toContain('README');
  });

  it('routes through the configured logger instead of the console', () => {
    g[DEXIE_IDENTITY_KEY] = OtherDexie;
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const logged: PBOLogEvent[] = [];

    makeEngine({ logger: (e) => logged.push(e) });

    expect(warn).not.toHaveBeenCalled();
    expect(logged).toHaveLength(1);
    expect(logged[0]).toMatchObject({ level: 'warn', msg: DUAL_DEXIE_MESSAGE });
    expect(logged[0]!.data).toMatchObject({ peer: 'dexie' });
  });

  it('does not replace the stamp it found — the first copy loaded stays the record', () => {
    g[DEXIE_IDENTITY_KEY] = OtherDexie;
    vi.spyOn(console, 'warn').mockImplementation(() => {});
    makeEngine();
    expect(g[DEXIE_IDENTITY_KEY]).toBe(OtherDexie);
  });

  it('costs nothing at construction time — the check performs no I/O', () => {
    g[DEXIE_IDENTITY_KEY] = OtherDexie;
    vi.spyOn(console, 'warn').mockImplementation(() => {});
    const open = vi.spyOn(globalThis.indexedDB, 'open');
    makeEngine();
    expect(open).not.toHaveBeenCalled();
  });
});
