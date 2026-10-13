/**
 * Integration harness: a real PocketBase binary per test file (TESTPLAN §2.1).
 *
 * A single Go binary boots faster and more deterministically than any emulator
 * suite, which is what makes "test against the real server" affordable here.
 *
 * Tombstone behaviour under test comes from the SHIPPED JSVM helper: the harness
 * copies `pb_hooks/pb-offline.pb.js` next to a fresh `pb_data` and rewrites its
 * CONFIG for the fixture collections, so every integration run is also a
 * conformance run against that file. The Go module's own behaviour is covered by
 * `server/go`'s Go tests (`go test ./server/go/...`); the two are required to
 * agree (SPEC §2.6, TESTPLAN §2.2).
 *
 * No SDK and no npm dependency is used here — plain `fetch` against the REST
 * API — so the harness works before the client package builds.
 */

import { spawn, type ChildProcess } from 'node:child_process';
import { copyFile, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { existsSync, readFileSync } from 'node:fs';
import { createConnection, createServer, type Server, type Socket } from 'node:net';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const repoRoot = join(dirname(fileURLToPath(import.meta.url)), '..', '..');

export interface PBUser {
  id: string;
  email: string;
  password: string;
  /** A fresh auth token for this user. */
  token: string;
}

export interface PBHandle {
  /** http://127.0.0.1:<ephemeral> — the severable proxy, not PocketBase itself. */
  url: string;
  /**
   * PocketBase's own address, bypassing the proxy.
   *
   * A client on this URL keeps working while `sever()` holds — which is what a
   * SECOND DEVICE looks like when only this client's link is down (S8).
   */
  directUrl: string;
  admin: { email: string; password: string };
  /** A superuser token, valid for the life of the instance. */
  adminToken: string;
  /** Create (and authenticate) a `users` record. */
  createUser(email?: string, password?: string): Promise<PBUser>;
  stop(): Promise<void>;
  /** Cut the socket without a clean close — simulates a link drop. */
  sever(): Promise<void>;
  restore(): Promise<void>;
}

export interface StartPBOptions {
  /**
   * Collections the tombstone helper watches. Written into the copied hook
   * file's CONFIG. Defaults to the owner-scoped fixture collections.
   */
  syncedCollections?: string[];
  /** Skip the fixture schema entirely (for tests that build their own). */
  schema?: boolean;
  /** Print PocketBase's stdout/stderr. Useful when a boot fails in CI. */
  verbose?: boolean;
}

const ADMIN = { email: 'harness@pb-offline.test', password: 'pb-offline-harness' };

/** The version pinned in package.json, which is also the .cache directory key. */
export function pinnedPocketBaseVersion(): string {
  const pkg = JSON.parse(readFileSync(join(repoRoot, 'package.json'), 'utf8')) as {
    pbOffline?: { pocketbaseVersion?: string };
  };
  const version = pkg.pbOffline?.pocketbaseVersion;
  if (!version) throw new Error('package.json is missing pbOffline.pocketbaseVersion');
  return version;
}

export function pocketBaseBinary(): string {
  const name = process.platform === 'win32' ? 'pocketbase.exe' : 'pocketbase';
  return join(repoRoot, '.cache', 'pocketbase', pinnedPocketBaseVersion(), name);
}

/**
 * Whether the binary has been fetched. Integration specs skip themselves rather
 * than fail when it has not — `npm run test:integration` fetches it first.
 */
export function hasPocketBase(): boolean {
  try {
    return existsSync(pocketBaseBinary());
  } catch {
    return false;
  }
}

// ── process helpers ──────────────────────────────────────────────────────────

function run(bin: string, args: string[]): Promise<void> {
  return new Promise((resolve, reject) => {
    const child = spawn(bin, args, { stdio: 'pipe' });
    let output = '';
    child.stdout.on('data', (d: Buffer) => (output += d.toString()));
    child.stderr.on('data', (d: Buffer) => (output += d.toString()));
    child.on('error', reject);
    child.on('exit', (code) =>
      code === 0 ? resolve() : reject(new Error(`${args[0]} exited ${code}:\n${output}`)),
    );
  });
}

/** A port nothing is listening on right now. Racy by nature; callers retry. */
function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const probe = createServer();
    probe.once('error', reject);
    probe.listen(0, '127.0.0.1', () => {
      const address = probe.address();
      const port = typeof address === 'object' && address ? address.port : 0;
      probe.close(() => (port ? resolve(port) : reject(new Error('no ephemeral port'))));
    });
  });
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function waitForHealth(url: string, deadlineMs: number, child: ChildProcess, log: () => string) {
  const until = Date.now() + deadlineMs;
  while (Date.now() < until) {
    if (child.exitCode !== null) {
      throw new Error(`PocketBase exited with ${child.exitCode} before becoming healthy:\n${log()}`);
    }
    try {
      const res = await fetch(`${url}/api/health`);
      if (res.ok) return;
    } catch {
      // not up yet
    }
    await sleep(100);
  }
  throw new Error(`PocketBase did not become healthy within ${deadlineMs}ms:\n${log()}`);
}

// ── the severable proxy ──────────────────────────────────────────────────────

interface Proxy {
  port: number;
  sever(): void;
  restore(): void;
  close(): Promise<void>;
}

/**
 * A TCP passthrough in front of PocketBase. `sever()` destroys every live socket
 * and refuses new ones without stopping the process, which is what a link drop
 * looks like to a client — a clean shutdown is not (SPEC §3, TESTPLAN §2.1).
 */
async function startProxy(upstreamPort: number): Promise<Proxy> {
  let severed = false;
  const sockets = new Set<Socket>();

  const server: Server = createServer((client) => {
    if (severed) {
      client.destroy();
      return;
    }
    const upstream = createConnection({ host: '127.0.0.1', port: upstreamPort });
    sockets.add(client);
    sockets.add(upstream);

    const drop = () => {
      sockets.delete(client);
      sockets.delete(upstream);
      client.destroy();
      upstream.destroy();
    };
    client.on('error', drop);
    upstream.on('error', drop);
    client.on('close', drop);
    upstream.on('close', drop);

    client.pipe(upstream);
    upstream.pipe(client);
  });

  const port = await new Promise<number>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      if (typeof address === 'object' && address) resolve(address.port);
      else reject(new Error('proxy did not bind'));
    });
  });

  return {
    port,
    sever() {
      severed = true;
      for (const s of sockets) s.destroy();
      sockets.clear();
    },
    restore() {
      severed = false;
    },
    close() {
      severed = true;
      for (const s of sockets) s.destroy();
      sockets.clear();
      return new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

// ── the hook file ────────────────────────────────────────────────────────────

/**
 * Copies the shipped JSVM helper into the instance's pb_hooks, with CONFIG's
 * collection list swapped for the ones this run syncs and `ensureSchema` turned
 * on — the two edits an operator makes on install.
 *
 * `ensureSchema` SHIPS OFF (a copy-paste install must not mutate a schema
 * before anybody has edited CONFIG), so the harness has to opt in exactly as a
 * real project does; that the tombstone collection then appears is part of what
 * the integration tier asserts.
 *
 * Both rewrites are targeted replacements of a literal, so a change to CONFIG's
 * shape shows up here as a loud failure rather than a silently unconfigured hook.
 */
async function installHooks(hooksDir: string, syncedCollections: string[]) {
  const source = join(repoRoot, 'pb_hooks', 'pb-offline.pb.js');
  const target = join(hooksDir, 'pb-offline.pb.js');
  await copyFile(source, target);

  const original = await readFile(target, 'utf8');
  const withCollections = original.replace(
    /collections:\s*\[[^\]]*\],/,
    `collections: ${JSON.stringify(syncedCollections)},`,
  );
  if (withCollections === original) {
    throw new Error('harness: could not find CONFIG.collections in pb_hooks/pb-offline.pb.js');
  }
  const patched = withCollections.replace(/ensureSchema:\s*false,/, 'ensureSchema: true,');
  if (patched === withCollections) {
    throw new Error('harness: could not find CONFIG.ensureSchema in pb_hooks/pb-offline.pb.js');
  }
  await writeFile(target, patched);
}

// ── the API surface the harness itself needs ─────────────────────────────────

async function api<T>(url: string, path: string, init: RequestInit & { token?: string } = {}): Promise<T> {
  const { token, ...rest } = init;
  const res = await fetch(`${url}${path}`, {
    ...rest,
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { Authorization: token } : {}),
      ...(rest.headers ?? {}),
    },
  });
  const body = await res.text();
  if (!res.ok) throw new Error(`${init.method ?? 'GET'} ${path} → ${res.status}: ${body}`);
  return (body ? JSON.parse(body) : null) as T;
}

async function applyFixtureSchema(url: string, token: string) {
  const raw = await readFile(join(repoRoot, 'test', 'fixtures', 'schema.json'), 'utf8');
  const fixture = JSON.parse(raw) as { collections: Array<Record<string, unknown>> };

  const ids: Record<string, string> = {};
  const users = await api<{ id: string }>(url, '/api/collections/users', { token });
  ids.users = users.id;

  for (const collection of fixture.collections) {
    const resolved = JSON.parse(
      JSON.stringify(collection).replace(/\{\{(\w+)\}\}/g, (_m, key: string) => {
        const id = ids[key];
        if (!id) throw new Error(`harness: fixture references {{${key}}} before it exists`);
        return id;
      }),
    ) as Record<string, unknown>;

    const created = await api<{ id: string; name: string }>(url, '/api/collections', {
      method: 'POST',
      token,
      body: JSON.stringify(resolved),
    });
    ids[created.name] = created.id;
  }
}

// ── entry point ──────────────────────────────────────────────────────────────

/**
 * Boot a fresh instance. Every call gets its own pb_data, its own hooks copy and
 * its own ports, so test files never share state.
 */
export async function startPB(opts: StartPBOptions = {}): Promise<PBHandle> {
  const binary = pocketBaseBinary();
  if (!existsSync(binary)) {
    throw new Error(`PocketBase binary is missing at ${binary}; run \`node scripts/fetch-pb.mjs\``);
  }

  const syncedCollections = opts.syncedCollections ?? ['projects', 'tasks'];
  const root = await mkdtemp(join(tmpdir(), 'pb-offline-'));
  const dataDir = join(root, 'pb_data');
  const hooksDir = join(root, 'pb_hooks');
  await mkdir(dataDir, { recursive: true });
  await mkdir(hooksDir, { recursive: true });
  await installHooks(hooksDir, syncedCollections);

  await run(binary, ['superuser', 'upsert', ADMIN.email, ADMIN.password, '--dir', dataDir]);

  const upstreamPort = await freePort();
  let log = '';
  const child = spawn(
    binary,
    [
      'serve',
      `--http=127.0.0.1:${upstreamPort}`,
      '--dir',
      dataDir,
      '--hooksDir',
      hooksDir,
      // Determinism: a watcher restart mid-test would look like a link drop.
      '--hooksWatch=false',
    ],
    { stdio: 'pipe' },
  );
  const capture = (d: Buffer) => {
    log += d.toString();
    if (opts.verbose) process.stderr.write(d);
  };
  child.stdout.on('data', capture);
  child.stderr.on('data', capture);

  const upstreamURL = `http://127.0.0.1:${upstreamPort}`;
  let proxy: Proxy | undefined;

  const stop = async () => {
    await proxy?.close();
    if (child.exitCode === null) {
      child.kill('SIGKILL');
      await new Promise<void>((resolve) => child.once('exit', () => resolve()));
    }
    await rm(root, { recursive: true, force: true });
  };

  try {
    // Generous: a cold binary on a loaded machine plus the JSVM pool warm-up.
    await waitForHealth(upstreamURL, 30_000, child, () => log);

    proxy = await startProxy(upstreamPort);
    const url = `http://127.0.0.1:${proxy.port}`;

    const auth = await api<{ token: string }>(url, '/api/collections/_superusers/auth-with-password', {
      method: 'POST',
      body: JSON.stringify({ identity: ADMIN.email, password: ADMIN.password }),
    });

    if (opts.schema !== false) {
      await applyFixtureSchema(url, auth.token);
    }

    let seq = 0;
    const createUser = async (email?: string, password = 'harness-password'): Promise<PBUser> => {
      const address = email ?? `user${++seq}-${Date.now()}@pb-offline.test`;
      const record = await api<{ id: string }>(url, '/api/collections/users/records', {
        method: 'POST',
        token: auth.token,
        body: JSON.stringify({ email: address, password, passwordConfirm: password, verified: true }),
      });
      const session = await api<{ token: string }>(url, '/api/collections/users/auth-with-password', {
        method: 'POST',
        body: JSON.stringify({ identity: address, password }),
      });
      return { id: record.id, email: address, password, token: session.token };
    };

    return {
      url,
      directUrl: upstreamURL,
      admin: { ...ADMIN },
      adminToken: auth.token,
      createUser,
      stop,
      async sever() {
        proxy!.sever();
      },
      async restore() {
        proxy!.restore();
      },
    };
  } catch (err) {
    await stop();
    throw err;
  }
}
