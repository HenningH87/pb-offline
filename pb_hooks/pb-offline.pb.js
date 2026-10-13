/// <reference path="../pb_data/types.d.ts" />
/**
 * pb-offline — JSVM server helper.
 *
 * The equivalent of `server/go/` for projects that run the stock PocketBase
 * binary rather than embedding it as a Go framework. Copy this single file into
 * your project's `pb_hooks/` directory and edit CONFIG below.
 *
 * It MUST stay behaviourally identical to the Go module. The integration suite
 * runs the same conformance tests against both (TESTPLAN §2.2); a divergence is
 * a bug in whichever one is wrong.
 *
 * JSVM caveats that shaped this file:
 *   - a handler's SOURCE is stringified and re-compiled in a separate goja
 *     runtime, so a handler cannot close over anything in this file's scope.
 *     Each one therefore `require`s this file back to reach CONFIG and the
 *     helpers. That works because the hook-registration globals exist only in
 *     the loader runtime, so the `typeof onRecordAfterDeleteSuccess` guard below
 *     makes the re-entrant load a pure module evaluation.
 *   - `$app` inside a hook is the (possibly transactional) app for that event;
 *     helpers take it as an argument rather than reading a global.
 *   - every hook handler must call `e.next()`.
 */

const CONFIG = {
    // Collections to install tombstone hooks on. EDIT THIS.
    collections: ['projects', 'tasks'],
    // Tombstone collection name.
    deletionsCollection: 'deletions',
    // Field naming the owner on the source collections and on the tombstone.
    ownerField: 'owner',
    // Tombstone retention, in days. A CONTRACT WITH CLIENTS: a client whose
    // tombstone cursor is older than this must full-resync.
    retentionDays: 30,
    // Prune schedule.
    pruneCron: '0 3 * * *',
    // Create or patch the tombstone collection at boot. The Go helper's
    // EnsureSchema.
    //
    // OFF BY DEFAULT, and the Go module's `true` default is deliberately not
    // matched here. The install instruction for this file is "copy it into
    // pb_hooks/", which means a copy can reach a running server before anybody
    // has edited CONFIG — and boot-time schema mutation driven by a
    // still-placeholder config is not something a copy-paste install should be
    // able to do. Turn it on once `collections` above names YOUR collections;
    // leave it off when the schema is owned by pb_migrations/.
    ensureSchema: false,
};

/** A SQL/PB identifier. `deletionsCollection` is pasted into the prune query. */
const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * Guard the one CONFIG value that becomes a SQL identifier.
 *
 * The prune query interpolates the collection name into `{{table}}` by string
 * concatenation, because PocketBase's `{{ }}` is a quoting convention and not a
 * bind parameter — values in that query ARE bound (`{:cutoff}`, `{:limit}`),
 * the table name cannot be. The client library applies the same check in
 * `resolveConfig`, so a project whose two halves agree never sees this throw.
 */
function assertIdentifier(name, what) {
    if (typeof name !== 'string' || !IDENTIFIER.test(name)) {
        throw new Error(
            'pb-offline: CONFIG.' + what + ' must match [A-Za-z_][A-Za-z0-9_]* — it is used as a SQL identifier; got ' +
            JSON.stringify(name)
        );
    }
}

const PRUNE_JOB_ID = 'pbo-prune-tombstones';
const PRUNE_BATCH_SIZE = 500;
const FIELD_COLLECTION = 'collection';
const FIELD_RECORD_ID = 'record_id';

/**
 * Reads the owner id off a record, tolerating both shapes a PocketBase relation
 * can hold it in: a single relation (string) and a multiple relation (array).
 */
function readOwner(record, ownerField) {
    const raw = record.get(ownerField);
    if (typeof raw === 'string') {
        return raw;
    }
    if (Array.isArray(raw) && raw.length > 0) {
        return raw[0];
    }
    return '';
}

/** Writes one tombstone row. Never rule-checked — a direct model save is not. */
function writeTombstone(app, config, collectionName, recordId, owner) {
    const deletions = app.findCollectionByNameOrId(config.deletionsCollection);
    const tombstone = new Record(deletions);
    tombstone.set(FIELD_COLLECTION, collectionName);
    tombstone.set(FIELD_RECORD_ID, recordId);
    tombstone.set(config.ownerField, owner);
    app.save(tombstone);
}

/** The 15-char lowercase-alphanumeric id the Go helper pins for this name. */
function pinnedCollectionId(name) {
    return 'pbo' + $security.sha256('pb-offline/deletions/' + name).slice(0, 12);
}

function sanitiseIdentifier(s) {
    return s.toLowerCase().replace(/[^a-z0-9_]/g, '_');
}

/** PocketBase's autodate wire format: `YYYY-MM-DD HH:MM:SS.sssZ`. */
function pbDateTime(date) {
    return date.toISOString().replace('T', ' ');
}

/**
 * Decides which auth collection the tombstone's owner relation points at, by
 * copying the answer off the collections being synced rather than asking the
 * operator to write it down a second time.
 */
function resolveOwnerCollectionId(app, config) {
    const names = config.collections.concat([config.deletionsCollection]);
    for (const name of names) {
        let collection;
        try {
            collection = app.findCollectionByNameOrId(name);
        } catch (_) {
            continue;
        }
        const field = collection.fields.getByName(config.ownerField);
        if (field && field.type() === 'relation' && field.collectionId) {
            return field.collectionId;
        }
    }

    const authCollections = app.findAllCollections('auth');
    const candidates = [];
    for (const c of authCollections) {
        if (c.system) {
            continue; // _superusers and friends never own user data
        }
        if (c.name === 'users') {
            return c.id;
        }
        candidates.push(c);
    }
    if (candidates.length === 1) {
        return candidates[0].id;
    }

    throw new Error(
        'pb-offline: cannot infer the auth collection for the "' + config.ownerField +
        '" relation; create the "' + config.deletionsCollection +
        '" collection yourself and set CONFIG.ensureSchema = false'
    );
}

/**
 * Creates or patches the tombstone collection (SPEC §2.6).
 *
 *   collection   text,     indexed
 *   record_id    text,     indexed
 *   <ownerField> relation → auth collection, indexed
 *   created      autodate (create)
 *   updated      autodate (create + update)     ← REQUIRED, it is the cursor
 *   index        (owner, updated, id)
 *
 *   listRule / viewRule            = "<ownerField> = @request.auth.id"
 *   create / update / deleteRule   = null   (hooks write as superuser)
 *
 * Idempotent and additive: an existing collection is patched in place and never
 * dropped, because dropping it would destroy the tombstones that long-offline
 * clients still need. Additive covers the RULES too — see `applyRule`.
 */
function ensureSchema(app, config) {
    assertIdentifier(config.deletionsCollection, 'deletionsCollection');
    assertIdentifier(config.ownerField, 'ownerField');
    const ownerCollectionId = resolveOwnerCollectionId(app, config);
    const rule = config.ownerField + ' = @request.auth.id';

    let collection = null;
    try {
        collection = app.findCollectionByNameOrId(config.deletionsCollection);
    } catch (_) {
        collection = null;
    }

    if (collection === null) {
        collection = new Collection({
            id: pinnedCollectionId(config.deletionsCollection),
            type: 'base',
            name: config.deletionsCollection,
            listRule: rule,
            viewRule: rule,
            createRule: null,
            updateRule: null,
            deleteRule: null,
            fields: [
                { name: FIELD_COLLECTION, type: 'text', required: true },
                { name: FIELD_RECORD_ID, type: 'text', required: true },
                {
                    name: config.ownerField,
                    type: 'relation',
                    collectionId: ownerCollectionId,
                    maxSelect: 1,
                    // Not required: a collection the client configures with
                    // `filter: null` has no owner, and a tombstone with an empty
                    // owner still beats a delete nobody recorded.
                    required: false,
                    // Never cascade: deleting a user must not erase the evidence
                    // that their records were deleted.
                    cascadeDelete: false,
                },
                { name: 'created', type: 'autodate', onCreate: true, onUpdate: false, system: true },
                { name: 'updated', type: 'autodate', onCreate: true, onUpdate: true, system: true },
            ],
        });
    } else {
        if (collection.type !== 'base') {
            throw new Error(
                'pb-offline: collection "' + config.deletionsCollection + '" already exists with type "' +
                collection.type + '"; the tombstone collection must be a base collection'
            );
        }

        ensureField(collection, FIELD_COLLECTION, 'text', function () {
            return new TextField({ name: FIELD_COLLECTION, required: true });
        });
        ensureField(collection, FIELD_RECORD_ID, 'text', function () {
            return new TextField({ name: FIELD_RECORD_ID, required: true });
        });
        ensureField(collection, config.ownerField, 'relation', function () {
            return new RelationField({
                name: config.ownerField,
                collectionId: ownerCollectionId,
                maxSelect: 1,
                required: false,
                cascadeDelete: false,
            });
        });
        ensureField(collection, 'created', 'autodate', function () {
            return new AutodateField({ name: 'created', onCreate: true, onUpdate: false, system: true });
        });
        ensureField(collection, 'updated', 'autodate', function () {
            return new AutodateField({ name: 'updated', onCreate: true, onUpdate: true, system: true });
        });

        applyRule(app, collection, 'listRule', rule);
        applyRule(app, collection, 'viewRule', rule);
        applyRule(app, collection, 'createRule', null);
        applyRule(app, collection, 'updateRule', null);
        applyRule(app, collection, 'deleteRule', null);
    }

    const prefix = 'idx_pbo_' + sanitiseIdentifier(config.deletionsCollection);
    const owner = '`' + config.ownerField + '`';
    collection.addIndex(prefix + '_collection', false, '`' + FIELD_COLLECTION + '`', '');
    collection.addIndex(prefix + '_record_id', false, '`' + FIELD_RECORD_ID + '`', '');
    collection.addIndex(prefix + '_owner', false, owner, '');
    collection.addIndex(prefix + '_cursor', false, owner + ', `updated`, `id`', '');

    app.save(collection);
}

/**
 * Sets one API rule on an EXISTING tombstone collection — the operator's answer
 * wins (the Go module's `chooseRule`; the two MUST agree, SPEC §2.6).
 *
 * "Idempotent and additive" was true of the fields and false of the rules: this
 * function's predecessor reassigned all five on every boot, so an operator who
 * deliberately tightened `deletions` had it reverted at the next restart with
 * no log line. Rules are the security boundary of a collection; resetting a
 * boundary somebody changed on purpose is a downgrade, not a migration.
 *
 * A rule that is unset is set. A rule that already says what we would have said
 * is left alone. Anything else is KEPT, and both values go in a warning.
 */
function applyRule(app, collection, ruleName, prescribed) {
    const current = collection[ruleName];
    if (current === null || current === undefined) {
        collection[ruleName] = prescribed;
        return;
    }
    if (current === prescribed) {
        return;
    }
    app.logger().warn(
        'pb-offline: keeping the existing API rule on the tombstone collection; pb-offline would have set a different one',
        'collection', collection.name,
        'rule', ruleName,
        'existing', ruleText(current),
        'prescribed', ruleText(prescribed)
    );
}

/**
 * Renders a rule for a log line. `null` is not empty: an empty rule means
 * ANYONE and null means superusers only, and confusing the two in a warning
 * about API rules would be its own security bug.
 */
function ruleText(rule) {
    if (rule === null || rule === undefined) {
        return 'nil (superusers only)';
    }
    if (rule === '') {
        return '"" (public)';
    }
    return rule;
}

/**
 * Adds a field if it is missing; refuses to coerce one that exists with the
 * wrong type, rather than silently reshaping the operator's data.
 */
function ensureField(collection, name, wantType, build) {
    const existing = collection.fields.getByName(name);
    if (!existing) {
        collection.fields.add(build());
        return;
    }
    if (existing.type() !== wantType) {
        throw new Error(
            'pb-offline: collection "' + collection.name + '" already has a field "' + name +
            '" of type "' + existing.type() + '", but the tombstone contract requires "' + wantType +
            '"; rename or remove it rather than letting pb-offline coerce it'
        );
    }
}

/**
 * Deletes tombstones older than the retention window; returns how many went.
 *
 * Keys on `created`, not `updated`: a tombstone is never updated, and keying on
 * a column nothing writes twice keeps the arithmetic obvious.
 */
function pruneTombstones(app, config) {
    // The name is pasted into the SQL below, so it is checked HERE as well as
    // in ensureSchema: a project running with `ensureSchema: false` never
    // reaches that check, and the cron would be the only thing left touching
    // the identifier.
    assertIdentifier(config.deletionsCollection, 'deletionsCollection');
    const table = config.deletionsCollection;
    const cutoff = pbDateTime(new Date(Date.now() - config.retentionDays * 24 * 60 * 60 * 1000));

    const countExpired = function () {
        const row = new DynamicModel({ total: 0 });
        app.db()
            .newQuery('SELECT COUNT(*) AS total FROM {{' + table + '}} WHERE [[created]] < {:cutoff}')
            .bind({ cutoff: cutoff })
            .one(row);
        return row.total;
    };

    let total = 0;
    let remaining = countExpired();

    while (remaining > 0) {
        app.runInTransaction(function (txApp) {
            // The subselect is what bounds the batch: SQLite's DELETE has no
            // LIMIT clause in a default build.
            txApp.db()
                .newQuery(
                    'DELETE FROM {{' + table + '}} WHERE [[id]] IN ' +
                    '(SELECT [[id]] FROM {{' + table + '}} WHERE [[created]] < {:cutoff} LIMIT {:limit})'
                )
                .bind({ cutoff: cutoff, limit: PRUNE_BATCH_SIZE })
                .execute();
        });

        const left = countExpired();
        if (left >= remaining) {
            // Nothing moved — stop rather than spin. Log and return like the Go
            // helper does (SPEC §2.6: the two MUST stay behaviorally identical);
            // a throw here would surface as an uncaught cron error.
            app.logger().warn(
                'pb-offline: prune made no progress',
                'expiredLeft', left
            );
            return total;
        }
        total += remaining - left;
        remaining = left;
    }

    return total;
}

// Handlers cannot see this scope, so they require the file back. The guard above
// keeps that re-entrant load from trying to register anything a second time.
if (typeof module !== 'undefined') {
    module.exports = {
        CONFIG: CONFIG,
        PRUNE_JOB_ID: PRUNE_JOB_ID,
        readOwner: readOwner,
        writeTombstone: writeTombstone,
        ensureSchema: ensureSchema,
        pruneTombstones: pruneTombstones,
        assertIdentifier: assertIdentifier,
        applyRule: applyRule,
    };
}

// The hook-registration globals exist only in the loader runtime. Their absence
// is what tells this file it is being `require`d from inside a handler.
if (typeof onRecordAfterDeleteSuccess === 'function') {
    // ── tombstone hook ───────────────────────────────────────────────────────
    //
    // Fires after the delete commits, so a rolled-back delete leaves no
    // tombstone; fires once per record of a cascade delete; fires for superuser
    // deletes too, which matters — an admin deleting a row in the dashboard must
    // not leave clients holding it forever.
    onRecordAfterDeleteSuccess((e) => {
        const pbo = require(`${__hooks}/pb-offline.pb.js`);
        const config = pbo.CONFIG;

        const owner = pbo.readOwner(e.record, config.ownerField);
        if (owner === '') {
            e.app.logger().warn(
                'pb-offline: deleted record has no owner; writing an unowned tombstone',
                'collection', e.record.collection().name,
                'record', e.record.id,
                'ownerField', config.ownerField
            );
        }

        try {
            pbo.writeTombstone(e.app, config, e.record.collection().name, e.record.id, owner);
        } catch (err) {
            // The delete has already committed; this cannot be rolled back, and
            // it silently costs a client its consistency. It is the one error
            // here that deserves noise.
            e.app.logger().error(
                'pb-offline: FAILED to write a delete tombstone — offline clients will not learn about this delete',
                'collection', e.record.collection().name,
                'record', e.record.id,
                'error', String(err)
            );
        }

        e.next();
    }, ...CONFIG.collections);

    // ── prune cron ───────────────────────────────────────────────────────────
    cronAdd(PRUNE_JOB_ID, CONFIG.pruneCron, () => {
        const pbo = require(`${__hooks}/pb-offline.pb.js`);
        const pruned = pbo.pruneTombstones($app, pbo.CONFIG);
        if (pruned > 0) {
            $app.logger().info(
                'pb-offline: pruned expired tombstones',
                'collection', pbo.CONFIG.deletionsCollection,
                'count', pruned,
                'retentionDays', pbo.CONFIG.retentionDays
            );
        }
    });

    // ── schema ───────────────────────────────────────────────────────────────
    //
    // e.next() first: the collections cache and the database are only ready once
    // the rest of the bootstrap chain has run.
    onBootstrap((e) => {
        e.next();

        const pbo = require(`${__hooks}/pb-offline.pb.js`);
        if (!pbo.CONFIG.ensureSchema) {
            return;
        }
        pbo.ensureSchema(e.app, pbo.CONFIG);
    });
}
