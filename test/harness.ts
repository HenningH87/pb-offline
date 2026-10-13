/**
 * The integration harness's public surface, in the shape the specs consume it:
 *
 *   const pb = await startHarness();
 *   pb.url, pb.adminToken, await pb.createUser(), await pb.stop()
 *
 * The implementation, including the severable proxy TESTPLAN §2.1 asks for,
 * lives in `./harness/pb-server`. This file exists so a spec never has to know
 * that, and so the two names TESTPLAN and the build brief use resolve to one
 * thing.
 *
 * What it sets up per instance: a fresh `pb_data` in a temp dir, a seeded
 * superuser, the fixture schema from `test/fixtures/schema.json`, and a copy of
 * the SHIPPED `pb_hooks/pb-offline.pb.js` — so the tombstone rows an
 * integration test sees are written by the JSVM helper itself, not by the
 * harness faking them. The Go module's equivalent behaviour is covered by
 * `go test ./...` inside `server/go`; SPEC §2.6 requires the two to agree, and
 * TESTPLAN §2.2 is where that parity gets asserted.
 */

export { startPB as startHarness, startPB, hasPocketBase, pocketBaseBinary, pinnedPocketBaseVersion } from './harness/pb-server';
export type { PBHandle, PBUser, StartPBOptions } from './harness/pb-server';
