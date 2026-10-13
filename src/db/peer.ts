/**
 * The dual-peer tripwire (SPEC §17.4).
 *
 * `dexie` is a PEER dependency because Dexie's `liveQuery` registry is per copy
 * of the module: an observer registered by copy A never hears about a write made
 * through copy B. Two copies therefore produce an application where writes land
 * in IndexedDB correctly and nothing re-renders — no error, no warning, no
 * failing request, just a UI frozen at whatever it read first.
 *
 * npm resolves that peer from the consumer's tree in a normal install. It does
 * NOT when the package is consumed as `file:` or `npm link`: the linked
 * directory carries its own `node_modules`, its devDependency copy of Dexie is
 * right there, and a bundler resolving from the real path finds that one. The
 * README's "Consuming via file: / link" section has the fix.
 *
 * The guard is a stamp on `globalThis` rather than anything cleverer because
 * that is the one registry both copies can see.
 */
import Dexie from 'dexie';
import { BRAND } from '../brand.js';
import type { PBOLogEvent } from '../types.js';

/** Where the first-loaded copy records which Dexie constructor it is using. */
export const DEXIE_IDENTITY_KEY: symbol = Symbol.for(`${BRAND.name}.dexie`);
/** Set once a mismatch has been reported, so a second engine does not repeat it. */
export const DEXIE_WARNED_KEY: symbol = Symbol.for(`${BRAND.name}.dexie.warned`);

export const DUAL_DEXIE_MESSAGE =
  `${BRAND.name}: two different copies of \`dexie\` are loaded in this page. ` +
  `Dexie's liveQuery registry is per copy, so writes made through one copy do not ` +
  `re-emit queries created by the other: local writes will reach IndexedDB and the ` +
  `UI will silently stop updating. This usually means ${BRAND.name} was installed ` +
  `with \`file:\` or \`npm link\` and resolved its own devDependency copy instead of ` +
  `your app's. Fix it with a tsconfig \`paths\` entry or an npm \`overrides\` entry — ` +
  `see "Consuming via file: / link" in the ${BRAND.name} README.`;

type GlobalStamps = Record<symbol, unknown>;

/**
 * Stamp this copy's Dexie identity, and warn ONCE if it is not the one already
 * stamped. Cheap (two property reads) and called from `createPBOffline`.
 *
 * The warn-once flag lives on `globalThis` too, deliberately: each copy of the
 * library has its own module scope, so a module-local flag would let N copies
 * warn N times about one problem.
 */
export function checkDexiePeer(logger?: (e: PBOLogEvent) => void): void {
  const g = globalThis as unknown as GlobalStamps;
  const stamped = g[DEXIE_IDENTITY_KEY];

  if (stamped === undefined) {
    g[DEXIE_IDENTITY_KEY] = Dexie;
    return;
  }
  if (stamped === Dexie) return;
  if (g[DEXIE_WARNED_KEY] === true) return;
  g[DEXIE_WARNED_KEY] = true;

  const event: PBOLogEvent = {
    level: 'warn',
    msg: DUAL_DEXIE_MESSAGE,
    data: { peer: 'dexie', ours: describe(Dexie), stamped: describe(stamped) },
    at: Date.now(),
  };
  if (logger) logger(event);
  else console.warn(DUAL_DEXIE_MESSAGE);
}

/** Enough to tell two copies apart in a log line, without dumping a constructor. */
function describe(ctor: unknown): string {
  const c = ctor as { semVer?: unknown; version?: unknown } | null;
  const v = typeof c?.semVer === 'string' ? c.semVer : c?.version;
  return v === undefined || v === null ? 'unknown' : `dexie@${String(v)}`;
}
