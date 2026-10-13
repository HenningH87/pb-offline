/**
 * Smoke test for the integration harness (TESTPLAN §2.1) and, through it, for
 * the shipped JSVM server helper (TESTPLAN §2.2).
 *
 * It proves the loop the rest of the integration tier stands on: a real binary
 * boots on an ephemeral port, a user can be created and authenticated, a record
 * round-trips, deleting it produces a tombstone carrying that user's id — and
 * that tombstone is written by `pb_hooks/pb-offline.pb.js`, which the harness
 * copies next to the binary, not by the harness itself.
 *
 * It skips itself when the PocketBase binary has not been fetched
 * (`node scripts/fetch-pb.mjs`), so a clean checkout running `npm test` offline
 * reports skipped rather than failed.
 */

import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { hasPocketBase, startHarness, type PBHandle, type PBUser } from '../harness';

const BOOT_TIMEOUT = 120_000;

describe.skipIf(!hasPocketBase())('integration harness', () => {
  let pb: PBHandle;

  beforeAll(async () => {
    pb = await startHarness();
  }, BOOT_TIMEOUT);

  afterAll(async () => {
    await pb?.stop();
  }, 30_000);

  const get = async (path: string, token?: string) => {
    const res = await fetch(`${pb.url}${path}`, {
      headers: token ? { Authorization: token } : {},
    });
    const body = await res.text();
    return { status: res.status, body: body ? JSON.parse(body) : null };
  };

  const send = async (method: string, path: string, token: string, payload?: unknown) => {
    const res = await fetch(`${pb.url}${path}`, {
      method,
      headers: { 'Content-Type': 'application/json', Authorization: token },
      ...(payload === undefined ? {} : { body: JSON.stringify(payload) }),
    });
    const body = await res.text();
    return { status: res.status, body: body ? JSON.parse(body) : null };
  };

  it('boots and answers on an ephemeral port', async () => {
    expect(pb.url).toMatch(/^http:\/\/127\.0\.0\.1:\d+$/);

    const health = await get('/api/health');
    expect(health.status).toBe(200);
    expect(pb.adminToken).toBeTruthy();
  });

  it('applies the fixture schema', async () => {
    const collections = await get('/api/collections?perPage=200', pb.adminToken);
    const names = (collections.body as { items: Array<{ name: string }> }).items.map((c) => c.name);
    expect(names).toEqual(expect.arrayContaining(['projects', 'tasks', 'tags']));
  });

  // The harness never creates this collection; the copied hook file does, at
  // bootstrap. Its presence and shape is the JSVM helper's ensure-schema working.
  it('lets the JSVM helper create the deletions collection per SPEC §2.6', async () => {
    const { status, body } = await get('/api/collections/deletions', pb.adminToken);
    expect(status).toBe(200);

    const collection = body as {
      type: string;
      listRule: string | null;
      viewRule: string | null;
      createRule: string | null;
      updateRule: string | null;
      deleteRule: string | null;
      fields: Array<{ name: string; type: string; onCreate?: boolean; onUpdate?: boolean }>;
      indexes: string[];
    };

    expect(collection.type).toBe('base');

    const byName = Object.fromEntries(collection.fields.map((f) => [f.name, f]));
    expect(byName.collection?.type).toBe('text');
    expect(byName.record_id?.type).toBe('text');
    expect(byName.owner?.type).toBe('relation');
    expect(byName.created).toMatchObject({ type: 'autodate', onCreate: true });
    // `updated` is the client's cursor — without OnUpdate the collection is
    // invisible to the sync engine (SPEC §2.6 convention 3).
    expect(byName.updated).toMatchObject({ type: 'autodate', onCreate: true, onUpdate: true });

    expect(collection.listRule).toBe('owner = @request.auth.id');
    expect(collection.viewRule).toBe('owner = @request.auth.id');
    expect(collection.createRule).toBeNull();
    expect(collection.updateRule).toBeNull();
    expect(collection.deleteRule).toBeNull();

    expect(collection.indexes.join('\n')).toContain('(`owner`, `updated`, `id`)');
  });

  it('creates and authenticates a user', async () => {
    const user = await pb.createUser();
    expect(user.id).toMatch(/^[a-z0-9]+$/);
    expect(user.token).toBeTruthy();

    const me = await get(`/api/collections/users/records/${user.id}`, user.token);
    expect(me.status).toBe(200);
  });

  it('writes a tombstone carrying the deleted record owner', async () => {
    const user: PBUser = await pb.createUser();

    const created = await send('POST', '/api/collections/projects/records', user.token, {
      title: 'offline-first',
      owner: user.id,
    });
    expect(created.status).toBe(200);
    const recordId = (created.body as { id: string }).id;

    const deleted = await send('DELETE', `/api/collections/projects/records/${recordId}`, user.token);
    expect(deleted.status).toBe(204);

    const tombstones = await get(
      `/api/collections/deletions/records?filter=${encodeURIComponent(`record_id="${recordId}"`)}`,
      user.token,
    );
    expect(tombstones.status).toBe(200);

    const items = (tombstones.body as { items: Array<Record<string, string>> }).items;
    expect(items).toHaveLength(1);
    expect(items[0]).toMatchObject({
      collection: 'projects',
      record_id: recordId,
      owner: user.id,
    });
    // Cursor-pullable, or the client can never see it.
    expect(items[0]?.updated).toMatch(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}Z$/);
  });

  it('keeps one user out of another user tombstones', async () => {
    const [alice, bob] = await Promise.all([pb.createUser(), pb.createUser()]);

    const created = await send('POST', '/api/collections/projects/records', alice.token, {
      title: 'alice only',
      owner: alice.id,
    });
    const recordId = (created.body as { id: string }).id;
    await send('DELETE', `/api/collections/projects/records/${recordId}`, alice.token);

    const asBob = await get(
      `/api/collections/deletions/records?filter=${encodeURIComponent(`record_id="${recordId}"`)}`,
      bob.token,
    );
    expect((asBob.body as { items: unknown[] }).items).toHaveLength(0);
  });

  it('writes no tombstone for a collection the helper does not watch', async () => {
    const tag = await send('POST', '/api/collections/tags/records', pb.adminToken, { label: 'shared' });
    const tagId = (tag.body as { id: string }).id;
    await send('DELETE', `/api/collections/tags/records/${tagId}`, pb.adminToken);

    const tombstones = await get(
      `/api/collections/deletions/records?filter=${encodeURIComponent(`record_id="${tagId}"`)}`,
      pb.adminToken,
    );
    expect((tombstones.body as { items: unknown[] }).items).toHaveLength(0);
  });

  it('severs and restores the link without stopping the server', async () => {
    await pb.sever();
    await expect(fetch(`${pb.url}/api/health`)).rejects.toThrow();

    await pb.restore();
    const health = await get('/api/health');
    expect(health.status).toBe(200);
  });
});
