/**
 * Conformance: the API rule set SPEC §2.6 prescribes, against a real server.
 *
 * This library does not install rules — it tells applications which ones to
 * write, and everything it promises about isolation rests on them being those
 * and not the obvious ones. Four of the five ARE the obvious ones. The update
 * rule is not, and the reason is a PocketBase behaviour no amount of reading
 * the client code would reveal:
 *
 *   an `updateRule` is evaluated as a FILTER over the record in its EXISTING
 *   state. PocketBase does not re-check it against the post-update record, and
 *   does not restrict which fields the body may set.
 *
 * So under a bare `owner = @request.auth.id`, `PATCH {"owner": "<victim>"}` on
 * a record you own SUCCEEDS: the record was yours when the rule read it, and
 * the write hands it to somebody else — into their pulls, and onto every one
 * of their devices through their pb-offline mirror. `:isset = false` is the
 * clause that closes it, and this file is the proof, because the difference is
 * invisible in every other test: both rule sets pass the whole sync suite.
 *
 * Plain `fetch` against the REST API, deliberately. What is under test is the
 * SERVER's answer to a hostile request, and a request shaped by the client
 * library could only ever send what the client library is willing to send.
 */

import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { hasPocketBase, startHarness, type PBHandle, type PBUser } from '../harness';

const BOOT_TIMEOUT = 120_000;

describe.skipIf(!hasPocketBase())('prescribed API rules (SPEC §2.6)', () => {
  let pb: PBHandle;
  /** The attacker: signs in, owns a record, tries to plant it on `victim`. */
  let attacker: PBUser;
  let victim: PBUser;

  beforeAll(async () => {
    pb = await startHarness();
    attacker = await pb.createUser();
    victim = await pb.createUser();
  }, BOOT_TIMEOUT);

  afterAll(async () => {
    await pb?.stop();
  }, 30_000);

  const send = async (method: string, path: string, token: string, payload?: unknown) => {
    const res = await fetch(`${pb.url}${path}`, {
      method,
      headers: { 'Content-Type': 'application/json', Authorization: token },
      ...(payload === undefined ? {} : { body: JSON.stringify(payload) }),
    });
    const body = await res.text();
    return { status: res.status, body: body ? (JSON.parse(body) as Record<string, unknown>) : null };
  };

  /**
   * A refusal, whatever shape PocketBase gives it.
   *
   * An `updateRule` is a FILTER, so a request the rule excludes is reported as
   * 404 "not found" rather than 403 — the record does not exist as far as that
   * rule is concerned. A createRule rejection is a 400. Pinning either exact
   * number would make this test about PocketBase's error taxonomy instead of
   * about the write being refused.
   */
  const expectRefused = (result: { status: number }) => {
    expect(result.status).not.toBe(200);
    expect([400, 403, 404]).toContain(result.status);
  };

  const mine = async (title: string): Promise<string> => {
    const created = await send('POST', '/api/collections/projects/records', attacker.token, {
      title,
      owner: attacker.id,
    });
    expect(created.status).toBe(200);
    return created.body!['id'] as string;
  };

  it('lets an owner create, read and edit their own record', async () => {
    const id = await mine('ordinary');

    const edited = await send('PATCH', `/api/collections/projects/records/${id}`, attacker.token, {
      title: 'edited',
    });
    expect(edited.status).toBe(200);
    expect(edited.body!['title']).toBe('edited');
  });

  it('refuses a create that names another account as the owner', async () => {
    expectRefused(
      await send('POST', '/api/collections/projects/records', attacker.token, {
        title: 'planted at birth',
        owner: victim.id,
      }),
    );
  });

  /** THE ONE. Reverting `:isset = false` in the fixture turns this refusal into a 200. */
  it('refuses a PATCH that hands a record to another account', async () => {
    const id = await mine('to be planted');

    expectRefused(
      await send('PATCH', `/api/collections/projects/records/${id}`, attacker.token, {
        owner: victim.id,
      }),
    );

    // And nothing moved: the record is still the attacker's, and the victim's
    // list — the thing that would have replicated it onto all their devices —
    // does not contain it.
    const still = await send('GET', `/api/collections/projects/records/${id}`, attacker.token);
    expect(still.status).toBe(200);
    expect(still.body!['owner']).toBe(attacker.id);

    const victimList = await send('GET', '/api/collections/projects/records?perPage=200', victim.token);
    expect(victimList.status).toBe(200);
    expect((victimList.body!['items'] as Array<{ id: string }>).map((r) => r.id)).not.toContain(id);
  });

  it('refuses the hand-off even when the body sets the owner to the value it already has', async () => {
    // `:isset` is about the body MENTIONING the field, not about the value
    // changing — a rule that compared values would have to trust the client's
    // idea of what the current one is.
    const id = await mine('self-assign');
    expectRefused(
      await send('PATCH', `/api/collections/projects/records/${id}`, attacker.token, {
        owner: attacker.id,
        title: 'and an edit alongside',
      }),
    );
  });

  it('still refuses to touch a record owned by somebody else', async () => {
    const theirs = await send('POST', '/api/collections/projects/records', victim.token, {
      title: 'not yours',
      owner: victim.id,
    });
    expect(theirs.status).toBe(200);
    const id = theirs.body!['id'] as string;

    expect((await send('GET', `/api/collections/projects/records/${id}`, attacker.token)).status).toBe(404);
    expect((await send('PATCH', `/api/collections/projects/records/${id}`, attacker.token, { title: 'x' })).status).toBe(404);
    expect((await send('DELETE', `/api/collections/projects/records/${id}`, attacker.token)).status).toBe(404);
  });
});
