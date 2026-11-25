package com.ospreydcs.dp.service.common.mongo.migration.migrations;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.mongo.MongoClientBase;
import com.ospreydcs.dp.service.common.mongo.migration.Migration;
import com.ospreydcs.dp.service.common.mongo.migration.MigrationContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.TreeSet;

/**
 * Normalizes the {@code tags} array on every annotation to the house convention — lowercase,
 * deduplicated, sorted — matching what {@code AnnotationDocument.fromSaveAnnotationRequest} stores
 * as of #248 Phase 2 (plan D12).
 *
 * <p>Annotation tags were previously stored as-given, unlike the pvMetadata and configuration
 * collections, which have normalized on save since they shipped. With saves now normalizing, a
 * stored mixed-case tag would be unreachable by any normalized {@code TagsCriterion} value — a
 * silent wrong answer of the same class as #197/#243, which is why this is a migration and not a
 * read-side shim.
 *
 * <p><b>Idempotency.</b> Normalization is a fixpoint: applying it to an already-normalized list
 * yields the same list, and the update is only issued for documents whose stored list differs from
 * its normalized form, so a re-run matches nothing and writes nothing.
 *
 * <p><b>Late and concurrent runs (#292).</b> This is a read followed by a write per document, so
 * the write is filtered on the {@code tags} array that was read as well as on {@code _id}. As
 * first shipped it was filtered on {@code _id} alone: a holder displaced by a takeover while
 * stalled could still be in this loop after the services had started, and a {@code saveAnnotation}
 * landing between its read and its write was then overwritten with the normalized form of the
 * <i>old</i> tags, a lost update with no error. With the filter, a document changed since the read
 * matches nothing; whatever replaced it came through the save path and is already normalized. Two
 * runs at once race to the same write and the loser matches nothing. An ordinary run is unchanged.
 *
 * <p>Lowercasing deliberately duplicates {@code DpBsonDocumentBase.normalizedTags()} rather than
 * calling it — a migration's behavior must stay frozen while the live classes evolve — but must
 * agree with it byte-for-byte, including the {@link Locale#ROOT} lowercasing: a default-locale
 * fold would bake locale-variant bytes into stored data that the save path (and any client's
 * normalized query value) could then never match.
 */
public class V2NormalizeAnnotationTags implements Migration {

    private static final Logger logger = LogManager.getLogger();

    static final String FIELD_TAGS = "tags";

    @Override
    public int version() {
        return 2;
    }

    @Override
    public String description() {
        return "normalize annotation tags to lowercase/deduplicated/sorted";
    }

    @Override
    public void apply(MongoDatabase database, MigrationContext context) throws DpException {

        final MongoCollection<Document> annotations =
                database.getCollection(MongoClientBase.COLLECTION_NAME_ANNOTATIONS);

        long normalizedCount = 0;
        try {
            for (Document document : annotations.find(Filters.exists(FIELD_TAGS))
                    .comment(context.operationComment())) {
                // A corrupt tags array (a non-string or null element) throws an unchecked
                // ClassCastException/NullPointerException from getList/toLowerCase. Fail closed,
                // but name the document: an operator repairing a legacy database needs to know
                // which of possibly many annotations to fix (the corrupt-bucket convention).
                try {
                    final List<String> storedTags = document.getList(FIELD_TAGS, String.class);
                    if (storedTags == null || storedTags.isEmpty()) {
                        continue;
                    }
                    final List<String> normalizedTags = new ArrayList<>(new TreeSet<>(
                            storedTags.stream().map(tag -> tag.toLowerCase(Locale.ROOT)).toList()));
                    if (normalizedTags.equals(storedTags)) {
                        continue;
                    }
                    // Filtered on the tags that were read, not on _id alone (#292): see the
                    // class comment. A document saved since the read matches nothing and is
                    // left alone.
                    final UpdateResult result = annotations.updateOne(
                            Filters.and(
                                    Filters.eq("_id", document.getObjectId("_id")),
                                    Filters.eq(FIELD_TAGS, storedTags)),
                            Updates.set(FIELD_TAGS, normalizedTags),
                            new UpdateOptions().comment(context.operationComment()));
                    if (result.getMatchedCount() > 0) {
                        normalizedCount++;
                    }
                } catch (MongoException ex) {
                    // MongoException extends RuntimeException; rethrow so a database failure is
                    // classified by the outer catch, not reported as a malformed document
                    throw ex;
                } catch (RuntimeException ex) {
                    final String errorMsg = "annotation " + document.get("_id")
                            + " has a malformed tags array: " + ex.getMessage();
                    logger.error("V2NormalizeAnnotationTags: {}", errorMsg, ex);
                    throw new DpException(errorMsg, ex);
                }
            }
        } catch (MongoException ex) {
            logger.error("V2NormalizeAnnotationTags: mongo exception normalizing tags: {}", ex.getMessage(), ex);
            throw new DpException("error normalizing annotation tags: " + ex.getMessage());
        }

        logger.info("V2NormalizeAnnotationTags: normalized tags on {} annotation(s)", normalizedCount);
    }
}
