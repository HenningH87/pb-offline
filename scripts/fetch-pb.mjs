#!/usr/bin/env node
/**
 * Download the PocketBase binary for this platform into .cache/ (TESTPLAN §2.1).
 *
 * The version is pinned in package.json under `pbOffline.pocketbaseVersion`, so
 * CI caches by that key and a version bump is one edit in one place.
 *
 *   node scripts/fetch-pb.mjs            # fetch if missing, verify, exit 0
 *   node scripts/fetch-pb.mjs --force    # re-download even if cached
 *   node scripts/fetch-pb.mjs --print    # print the resolved binary path only
 *
 * Plain node, no dependencies — including the unzip, which is a ~70 line reader
 * over node:zlib rather than a shell-out to `unzip`/`tar` (GNU tar cannot read
 * zip archives, so shelling out is not actually portable).
 */

import { createHash } from 'node:crypto';
import { chmodSync, existsSync, mkdirSync, readFileSync, realpathSync, writeFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { inflateRawSync } from 'node:zlib';

const repoRoot = join(dirname(fileURLToPath(import.meta.url)), '..');

const PLATFORMS = {
  'darwin:arm64': 'darwin_arm64',
  'darwin:x64': 'darwin_amd64',
  'linux:arm64': 'linux_arm64',
  'linux:x64': 'linux_amd64',
  'linux:arm': 'linux_armv7',
  'win32:x64': 'windows_amd64',
  'win32:arm64': 'windows_arm64',
};

function fail(message) {
  console.error(`fetch-pb: ${message}`);
  process.exit(1);
}

function pinnedVersion() {
  if (process.env.POCKETBASE_VERSION) return process.env.POCKETBASE_VERSION;

  const pkg = JSON.parse(readFileSync(join(repoRoot, 'package.json'), 'utf8'));
  const version = pkg?.pbOffline?.pocketbaseVersion;
  if (!version) fail('package.json is missing pbOffline.pocketbaseVersion');
  return version;
}

export function binaryPath(version = pinnedVersion()) {
  const name = process.platform === 'win32' ? 'pocketbase.exe' : 'pocketbase';
  return join(repoRoot, '.cache', 'pocketbase', version, name);
}

async function download(url) {
  const res = await fetch(url, { redirect: 'follow' });
  if (!res.ok) fail(`GET ${url} → ${res.status} ${res.statusText}`);
  return Buffer.from(await res.arrayBuffer());
}

/**
 * Minimal ZIP reader: locate the end-of-central-directory record, walk the
 * central directory, and inflate the one entry we want. Only the two
 * compression methods PocketBase's releases actually use are supported.
 */
function unzipEntry(zip, wantedName) {
  const EOCD_SIG = 0x06054b50;
  let eocd = -1;
  for (let i = zip.length - 22; i >= 0 && i > zip.length - 22 - 0xffff; i--) {
    if (zip.readUInt32LE(i) === EOCD_SIG) {
      eocd = i;
      break;
    }
  }
  if (eocd < 0) fail('downloaded archive is not a zip file (no end-of-central-directory record)');

  const entryCount = zip.readUInt16LE(eocd + 10);
  let ptr = zip.readUInt32LE(eocd + 16);

  for (let i = 0; i < entryCount; i++) {
    if (zip.readUInt32LE(ptr) !== 0x02014b50) fail('corrupt zip central directory');

    const method = zip.readUInt16LE(ptr + 10);
    const compressedSize = zip.readUInt32LE(ptr + 20);
    const uncompressedSize = zip.readUInt32LE(ptr + 24);
    const nameLen = zip.readUInt16LE(ptr + 28);
    const extraLen = zip.readUInt16LE(ptr + 30);
    const commentLen = zip.readUInt16LE(ptr + 32);
    const localOffset = zip.readUInt32LE(ptr + 42);
    const name = zip.subarray(ptr + 46, ptr + 46 + nameLen).toString('utf8');

    if (name === wantedName) {
      // The local header repeats the name/extra lengths, and they can differ
      // from the central directory's, so they must be re-read here.
      if (zip.readUInt32LE(localOffset) !== 0x04034b50) fail('corrupt zip local header');
      const localNameLen = zip.readUInt16LE(localOffset + 26);
      const localExtraLen = zip.readUInt16LE(localOffset + 28);
      const start = localOffset + 30 + localNameLen + localExtraLen;
      const raw = zip.subarray(start, start + compressedSize);

      if (method === 0) return raw;
      if (method === 8) {
        const out = inflateRawSync(raw);
        if (out.length !== uncompressedSize) fail(`inflate size mismatch for ${name}`);
        return out;
      }
      fail(`unsupported zip compression method ${method} for ${name}`);
    }

    ptr += 46 + nameLen + extraLen + commentLen;
  }

  fail(`the archive does not contain ${wantedName}`);
}

function verifies(target, version) {
  const res = spawnSync(target, ['--version'], { encoding: 'utf8' });
  if (res.error || res.status !== 0) return false;
  return `${res.stdout}${res.stderr}`.includes(version);
}

async function main() {
  const force = process.argv.includes('--force');
  const version = pinnedVersion();
  const target = binaryPath(version);

  if (process.argv.includes('--print')) {
    console.log(target);
    return;
  }

  if (!force && existsSync(target) && verifies(target, version)) {
    console.log(`fetch-pb: PocketBase ${version} already cached at ${target}`);
    return;
  }

  const key = `${process.platform}:${process.arch}`;
  const slug = PLATFORMS[key];
  if (!slug) fail(`no PocketBase release asset for ${key}`);

  const asset = `pocketbase_${version}_${slug}.zip`;
  const base = `https://github.com/pocketbase/pocketbase/releases/download/v${version}`;

  console.log(`fetch-pb: downloading ${asset}`);
  const [zip, checksums] = await Promise.all([
    download(`${base}/${asset}`),
    download(`${base}/checksums.txt`),
  ]);

  // A harness that silently runs an unverified binary is a supply-chain hole in
  // every contributor's checkout, so the published checksum is not optional.
  const expected = checksums
    .toString('utf8')
    .split('\n')
    .map((line) => line.trim().split(/\s+/))
    .find((parts) => parts[1] === asset || parts[1] === `*${asset}`)?.[0];
  if (!expected) fail(`no published checksum for ${asset}`);

  const actual = createHash('sha256').update(zip).digest('hex');
  if (actual !== expected.toLowerCase()) {
    fail(`checksum mismatch for ${asset}\n  expected ${expected}\n  got      ${actual}`);
  }

  const entry = process.platform === 'win32' ? 'pocketbase.exe' : 'pocketbase';
  const binary = unzipEntry(zip, entry);

  mkdirSync(dirname(target), { recursive: true });
  writeFileSync(target, binary);
  chmodSync(target, 0o755);

  if (!verifies(target, version)) {
    fail(`the extracted binary at ${target} does not report version ${version}`);
  }

  console.log(`fetch-pb: PocketBase ${version} ready at ${target}`);
}

// The harness imports binaryPath() from here, so only run when invoked directly.
if (process.argv[1] && realpathSync(process.argv[1]) === realpathSync(fileURLToPath(import.meta.url))) {
  main().catch((err) => fail(err?.stack || String(err)));
}
