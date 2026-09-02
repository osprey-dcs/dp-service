package com.ospreydcs.dp.service.common.mongo.migration.migrations;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.mongo.MongoClientBase;
import com.ospreydcs.dp.service.common.mongo.migration.Migration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

/**
 * Renames the annotation {@code comment} field to {@code description}, and drops the compound text
 * index built over the old field name (issue #248's D3, delivered by #254).
 *
 * <p><b>The index drop is not optional and is not a performance matter.</b> MongoDB permits only one
 * text index per collection, so on an existing deployment {@code createMongoIndexesAnnotations()}
 * cannot create the replacement while the old index exists — the create fails with
 * {@code IndexOptionsConflict}. This is why the migration runner is ordered ahead of every
 * {@code createMongoIndexes*()} call in {@code MongoClientBase.init()}.
 *
 * <p><b>A text index is not identified by its key document.</b> MongoDB stores one as
 * {@code key: {_fts: "text", _ftsx: 1, ownerId: 1}}, with the indexed text fields moved into a
 * separate {@code weights} document. So the old and new indexes have <i>identical</i> key documents
 * and differ only in {@code weights} — matching on the key would find both, and matching on the
 * literal default name would miss an index that had been created explicitly under another name.
 * This migration therefore identifies the index by the presence of the old field in {@code weights},
 * which is what actually distinguishes it.
 *
 * <p><b>Idempotency.</b> Both halves are safe to re-run. {@code $rename} matches only documents that
 * still have a {@code comment} field, so a second pass matches nothing; and the index scan finds no
 * candidate once the drop has happened. A document that somehow carries both fields is left alone
 * and reported rather than silently resolved — see {@link #renameField}.
 */
public class V1AnnotationCommentToDescription implements Migration {

    private static final Logger logger = LogManager.getLogger();

    static final String FIELD_COMMENT = "comment";
    static final String FIELD_DESCRIPTION = "description";

    static final String INDEX_FIELD_NAME = "name";
    static final String INDEX_FIELD_KEY = "key";
    static final String INDEX_FIELD_WEIGHTS = "weights";

    @Override
    public int version() {
        return 1;
    }

    @Override
    public String description() {
        return "rename annotation 'comment' field to 'description' and replace its text index";
    }

    @Override
    public void apply(MongoDatabase database) throws DpException {
        final MongoCollection<Document> annotations =
                database.getCollection(MongoClientBase.COLLECTION_NAME_ANNOTATIONS);
        dropStaleTextIndex(annotations);
        renameField(annotations);
    }

    /**
     * Renames {@code comment} to {@code description} on every annotation that still has the old
     * field.
     *
     * <p>Documents carrying <i>both</i> fields are excluded and reported rather than renamed. Mongo's
     * {@code $rename} overwrites the target, so renaming such a document would destroy an existing
     * description with no record of it. That state should not arise — nothing ever wrote both — but
     * if it does, losing data silently is worse than leaving it for an operator.
     */
    private void renameField(MongoCollection<Document> annotations) throws DpException {

        try {
            final long conflicting = annotations.countDocuments(
                    Filters.and(
                            Filters.exists(FIELD_COMMENT),
                            Filters.exists(FIELD_DESCRIPTION)));

            if (conflicting > 0) {
                throw new DpException(
                        conflicting + " annotation document(s) have both a '" + FIELD_COMMENT
                                + "' and a '" + FIELD_DESCRIPTION + "' field. Renaming would "
                                + "overwrite the existing description, so the migration stopped "
                                + "instead. Resolve these documents manually, then restart. Find "
                                + "them with: db.annotations.find({comment: {$exists: true}, "
                                + "description: {$exists: true}})");
            }

            final long renamed = annotations.updateMany(
                    Filters.exists(FIELD_COMMENT),
                    Updates.rename(FIELD_COMMENT, FIELD_DESCRIPTION)).getModifiedCount();

            logger.info(
                    "migration v1 renamed '{}' to '{}' on {} annotation document(s)",
                    FIELD_COMMENT, FIELD_DESCRIPTION, renamed);

        } catch (MongoException ex) {
            throw new DpException(
                    "error renaming annotation '" + FIELD_COMMENT + "' field: " + ex.getMessage(), ex);
        }
    }

    /**
     * Drops the text index covering the old field name, if one exists.
     *
     * <p>Finding nothing to drop is a normal outcome, not an error: on a fresh database there is no
     * such index, and on a re-run it has already gone. See the class comment for why {@code weights}
     * rather than the key document is the discriminator.
     */
    private void dropStaleTextIndex(MongoCollection<Document> annotations) throws DpException {

        try {
            String staleIndexName = null;

            for (Document index : annotations.listIndexes()) {

                final Document weights = index.get(INDEX_FIELD_WEIGHTS, Document.class);
                if (weights == null || !weights.containsKey(FIELD_COMMENT)) {
                    continue;
                }

                staleIndexName = index.getString(INDEX_FIELD_NAME);
                logger.info(
                        "migration v1 found stale text index '{}' with key {} and weights {}",
                        staleIndexName,
                        index.get(INDEX_FIELD_KEY),
                        weights.toJson());
                break;
            }

            if (staleIndexName == null) {
                logger.info(
                        "migration v1 found no text index over '{}'; nothing to drop",
                        FIELD_COMMENT);
                return;
            }

            annotations.dropIndex(staleIndexName);
            logger.info("migration v1 dropped stale text index '{}'", staleIndexName);

        } catch (MongoException ex) {
            throw new DpException(
                    "error dropping stale annotation text index: " + ex.getMessage(), ex);
        }
    }
}
