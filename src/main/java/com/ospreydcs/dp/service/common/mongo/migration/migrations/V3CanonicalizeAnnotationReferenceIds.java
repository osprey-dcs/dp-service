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
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;

/**
 * Canonicalizes the {@code dataSetIds} and {@code annotationIds} reference arrays on every
 * annotation to the lowercase hex form {@code ObjectId.toHexString()} emits, matching what
 * {@code AnnotationDocument.fromSaveAnnotationRequest} stores as of the #248 Phase 2 review fixes.
 *
 * <p>Stored reference ids are matched as <i>strings</i> — deleteDataSet's referential-integrity
 * check and the queryAnnotations dataSets/annotations criteria — while save validation parses them
 * as binary ObjectIds, which accept either hex case. A reference stored in a case variant therefore
 * passed validation but was invisible to every string-matched check: deleteDataSet could remove a
 * dataset such an annotation still references, leaving a dangling reference. Saves now canonicalize;
 * this migration brings previously stored references into line.
 *
 * <p>An entry that is not parseable as an ObjectId at all is left unchanged and logged: it can never
 * have referenced a real document, so rewriting it would destroy evidence without fixing anything.
 *
 * <p><b>Idempotency.</b> Canonicalization is a fixpoint — {@code toHexString()} output is already
 * canonical — and the update is only issued for documents whose stored arrays differ from their
 * canonical form, so a re-run matches nothing and writes nothing.
 */
public class V3CanonicalizeAnnotationReferenceIds implements Migration {

    private static final Logger logger = LogManager.getLogger();

    static final String FIELD_DATA_SET_IDS = "dataSetIds";
    static final String FIELD_ANNOTATION_IDS = "annotationIds";

    @Override
    public int version() {
        return 3;
    }

    @Override
    public String description() {
        return "canonicalize annotation reference ids to lowercase hex";
    }

    @Override
    public void apply(MongoDatabase database) throws DpException {

        final MongoCollection<Document> annotations =
                database.getCollection(MongoClientBase.COLLECTION_NAME_ANNOTATIONS);

        final Bson filter = Filters.or(
                Filters.exists(FIELD_DATA_SET_IDS), Filters.exists(FIELD_ANNOTATION_IDS));

        long canonicalizedCount = 0;
        try {
            for (Document document : annotations.find(filter)) {
                try {
                    final List<Bson> updates = new ArrayList<>();
                    for (String field : List.of(FIELD_DATA_SET_IDS, FIELD_ANNOTATION_IDS)) {
                        final List<String> storedIds = document.getList(field, String.class);
                        if (storedIds == null || storedIds.isEmpty()) {
                            continue;
                        }
                        final List<String> canonicalIds = new ArrayList<>(storedIds.size());
                        for (String storedId : storedIds) {
                            if (ObjectId.isValid(storedId)) {
                                canonicalIds.add(new ObjectId(storedId).toHexString());
                            } else {
                                logger.warn(
                                        "V3CanonicalizeAnnotationReferenceIds: annotation {} {} entry '{}' "
                                                + "is not an ObjectId; left unchanged",
                                        document.get("_id"), field, storedId);
                                canonicalIds.add(storedId);
                            }
                        }
                        if (!canonicalIds.equals(storedIds)) {
                            updates.add(Updates.set(field, canonicalIds));
                        }
                    }
                    if (updates.isEmpty()) {
                        continue;
                    }
                    annotations.updateOne(
                            Filters.eq("_id", document.getObjectId("_id")), Updates.combine(updates));
                    canonicalizedCount++;
                } catch (MongoException ex) {
                    // MongoException extends RuntimeException; rethrow so a database failure is
                    // classified by the outer catch, not reported as a malformed document
                    throw ex;
                } catch (RuntimeException ex) {
                    // fail closed, but name the document (the corrupt-bucket convention): an
                    // operator repairing a legacy database needs to know which annotation to fix
                    final String errorMsg = "annotation " + document.get("_id")
                            + " has a malformed reference id array: " + ex.getMessage();
                    logger.error("V3CanonicalizeAnnotationReferenceIds: {}", errorMsg, ex);
                    throw new DpException(errorMsg, ex);
                }
            }
        } catch (MongoException ex) {
            logger.error(
                    "V3CanonicalizeAnnotationReferenceIds: mongo exception canonicalizing ids: {}",
                    ex.getMessage(), ex);
            throw new DpException("error canonicalizing annotation reference ids: " + ex.getMessage());
        }

        logger.info(
                "V3CanonicalizeAnnotationReferenceIds: canonicalized reference ids on {} annotation(s)",
                canonicalizedCount);
    }
}
