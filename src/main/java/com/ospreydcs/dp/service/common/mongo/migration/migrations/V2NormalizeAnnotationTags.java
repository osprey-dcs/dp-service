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

import java.util.ArrayList;
import java.util.List;
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
    public void apply(MongoDatabase database) throws DpException {

        final MongoCollection<Document> annotations =
                database.getCollection(MongoClientBase.COLLECTION_NAME_ANNOTATIONS);

        long normalizedCount = 0;
        try {
            for (Document document : annotations.find(Filters.exists(FIELD_TAGS))) {
                final List<String> storedTags = document.getList(FIELD_TAGS, String.class);
                if (storedTags == null || storedTags.isEmpty()) {
                    continue;
                }
                final List<String> normalizedTags = new ArrayList<>(new TreeSet<>(
                        storedTags.stream().map(String::toLowerCase).toList()));
                if (normalizedTags.equals(storedTags)) {
                    continue;
                }
                annotations.updateOne(
                        Filters.eq("_id", document.getObjectId("_id")),
                        Updates.set(FIELD_TAGS, normalizedTags));
                normalizedCount++;
            }
        } catch (MongoException ex) {
            logger.error("V2NormalizeAnnotationTags: mongo exception normalizing tags: {}", ex.getMessage(), ex);
            throw new DpException("error normalizing annotation tags: " + ex.getMessage());
        }

        logger.info("V2NormalizeAnnotationTags: normalized tags on {} annotation(s)", normalizedCount);
    }
}
