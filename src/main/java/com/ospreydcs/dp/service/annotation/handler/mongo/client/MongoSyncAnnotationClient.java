package com.ospreydcs.dp.service.annotation.handler.mongo.client;

import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.ospreydcs.dp.grpc.v1.annotation.DeleteSampleStatusesRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryAnnotationsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryConfigurationActivationsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryConfigurationsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryDataSetsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryPvMetadataRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QuerySampleStatusesRequest;
import com.ospreydcs.dp.grpc.v1.annotation.SaveSampleStatusesRequest;
import com.ospreydcs.dp.grpc.v1.common.SampleStatusColumn;
import com.ospreydcs.dp.grpc.v1.common.SampleStatusFrame;
import com.ospreydcs.dp.service.annotation.handler.model.SampleStatusPageToken;
import com.ospreydcs.dp.service.common.bson.BsonConstants;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationActivationDocument;
import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationDocument;
import com.ospreydcs.dp.service.common.bson.pvmetadata.PvMetadataDocument;
import com.ospreydcs.dp.service.common.bson.samplestatus.SampleStatusBucketDocument;
import com.ospreydcs.dp.service.common.bson.samplestatus.SampleStatusDocumentUtility;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.model.AnnotationQueryResult;
import com.ospreydcs.dp.service.common.model.ConfigurationActivationQueryResult;
import com.ospreydcs.dp.service.common.model.ConfigurationQueryResult;
import com.ospreydcs.dp.service.common.model.DataSetQueryResult;
import com.ospreydcs.dp.service.common.model.MongoCountResult;
import com.ospreydcs.dp.service.common.model.MongoDeleteResult;
import com.ospreydcs.dp.service.common.model.MongoInsertOneResult;
import com.ospreydcs.dp.service.common.model.MongoSaveResult;
import com.ospreydcs.dp.service.common.model.PvMetadataQueryResult;
import com.ospreydcs.dp.service.common.model.SampleStatusQueryResult;
import com.ospreydcs.dp.service.common.mongo.MongoQueryFilterBuilder;
import com.ospreydcs.dp.service.common.mongo.MongoSyncClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Indexes.ascending;

public class MongoSyncAnnotationClient extends MongoSyncClient implements MongoAnnotationClientInterface {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    /**
     * Default page size applied by queryDataSets, queryAnnotations, queryPvMetadata,
     * queryConfigurations, and queryConfigurationActivations when the request's limit is 0/unset.
     *
     * <p>Shared by all five so a change to the default cannot land on a subset of them.  It matters
     * most for the match-all queries: since #245 (and #248 Phase 1 for the dataSets/annotations
     * queries) made an empty criteria list match-all, an unset limit would otherwise materialize
     * every matching document into an ArrayList, and because a nextPageToken is only produced when
     * the page is full, the caller could not even detect it.  The default is deliberately
     * unconditional rather than applied only to the match-all case — see #245 plan D1.
     */
    private static final int DEFAULT_QUERY_LIMIT = 100;

    /**
     * Documents for one page of a skip-paged query plus the token for the next page — the carrier
     * returned by {@link #applySkipPaging}.  nextPageToken is empty on the last page.
     */
    private record PagedDocuments<T>(List<T> documents, String nextPageToken) {}

    /**
     * Decodes a skip-based page token produced by {@link #applySkipPaging}.  An unparseable token
     * is ignored (page 0) rather than rejected; Phase 3 of #248 converts these interim tokens to
     * opaque with reject-on-malformed per the proto contract.
     */
    private static int decodePageTokenSkip(String pageToken) {
        if (pageToken == null || pageToken.isBlank()) {
            return 0;
        }
        try {
            return Integer.parseInt(new String(Base64.getDecoder().decode(pageToken), StandardCharsets.UTF_8));
        } catch (Exception ex) {
            logger.warn("invalid page token, ignoring: {}", pageToken);
            return 0;
        }
    }

    /**
     * Applies skip/limit paging to a prepared find (filter and sort already set), fetching one
     * document past the page to detect whether a next page exists without an extra count query.
     * The single implementation serves all five skip-paged queries so their token semantics cannot
     * drift; Phase 3 of #248 replaces the interim Base64 skip tokens with opaque tokens here.
     *
     * <p>The probe guards limit + 1 against overflow: a client may send limit = Integer.MAX_VALUE
     * (proto uint32), where a bare + 1 wraps negative and hands the driver an undefined page.
     * Mongo exceptions propagate to the caller, which owns the per-method error result.
     */
    private static <T> PagedDocuments<T> applySkipPaging(FindIterable<T> query, int skip, int limit) {
        if (skip > 0) {
            query = query.skip(skip);
        }
        final int probeLimit = limit < Integer.MAX_VALUE ? limit + 1 : limit;
        final List<T> documents = new ArrayList<>();
        query.limit(probeLimit).into(documents);
        String nextPageToken = "";
        if (documents.size() > limit) {
            documents.remove(documents.size() - 1);
            // skip is bounded by a prior token and limit by the check above, but their sum can
            // still exceed the int skip the driver accepts; past that point paging simply ends
            final long nextSkip = (long) skip + (long) limit;
            if (nextSkip <= Integer.MAX_VALUE) {
                nextPageToken = Base64.getEncoder().encodeToString(
                        Long.toString(nextSkip).getBytes(StandardCharsets.UTF_8));
            }
        }
        return new PagedDocuments<>(documents, nextPageToken);
    }

    @Override
    public DataSetDocument findDataSet(String dataSetId) {
        // Collapses "absent" and "query failed" to null. Callers that must tell the two apart —
        // saveDataSet, which reports the former as a rejection — use lookupDataSet() instead.
        try {
            return lookupDataSet(dataSetId);
        } catch (DpException ex) {
            // already logged with its stack trace in lookupDataSet()
            return null;
        }
    }

    /**
     * Looks up a DataSetDocument by id, distinguishing an absent document (null) from a failed
     * query (DpException). {@link #findDataSet} cannot make that distinction, so a caller whose
     * response depends on it — a missing document is a business-rule rejection, a failed query is
     * an infrastructure error — must use this method.
     */
    private DataSetDocument lookupDataSet(String dataSetId) throws DpException {
        // TODO: do we need to wrap this in a retry loop?  I'm not adding it now, my reasoning is that if the caller
        // sending request has a dataSetId, it already exists in the database.
        final List<DataSetDocument> matchingDocuments = new ArrayList<>();

        // wrap this in a try/catch because otherwise we take out the thread if mongo throws an exception
        try {
            mongoCollectionDataSets.find(
                    eq(BsonConstants.BSON_KEY_DATA_SET_ID, new ObjectId(dataSetId))).into(matchingDocuments);
        } catch (Exception ex) {
            // log here with the trace: DpException carries only the message onward
            logger.error("lookupDataSet: mongo exception in find(): {}", ex.getMessage(), ex);
            throw new DpException("error querying DataSetDocument by id: " + ex.getMessage());
        }

        return matchingDocuments.isEmpty() ? null : matchingDocuments.get(0);
    }

    @Override
    public MongoSaveResult saveDataSet(DataSetDocument dataSetDocument, String existingDocumentId) {

        logger.debug("saving DataSetDocument existingDocumentId: {}", existingDocumentId);

        // try to fetch existing document
        DataSetDocument existingDocument = null;
        if (!existingDocumentId.isBlank()) {
            try {
                existingDocument = lookupDataSet(existingDocumentId);
            } catch (DpException ex) {
                // Not necessarily a MongoException: an unparseable id throws IllegalArgumentException
                // from the ObjectId constructor. Let the wrapped message carry the specifics.
                final String errorMsg = "error looking up DataSetDocument by id: " + ex.getMessage();
                logger.error("saveDataSet lookup error: {}", ex.getMessage(), ex);
                return MongoSaveResult.error(errorMsg, existingDocumentId, false);
            }
            if (existingDocument == null) {
                // the caller referenced a document that does not exist: a rejection, not an error
                final String rejectMsg = "saveDataSet no DataSetDocument found with id: " + existingDocumentId;
                logger.debug(rejectMsg);
                return MongoSaveResult.reject(rejectMsg, existingDocumentId, false);
            }
        }

        if (existingDocument == null) {
            // create a new document
            dataSetDocument.addCreationTime(); // set creation time
            InsertOneResult insertOneResult = mongoCollectionDataSets.insertOne(dataSetDocument);

            if (!insertOneResult.wasAcknowledged()) {
                final String errorMsg = "insertOne failed for new DataSetDocument, result not acknowledged";
                logger.error(errorMsg);
                return new MongoSaveResult(true, errorMsg, null, true);
            }

            // check if result contains id inserted
            if (insertOneResult.getInsertedId() == null) {
                final String errorMsg = "DataSetDocument insert failed to return document id";
                logger.error(errorMsg);
                return new MongoSaveResult(true, errorMsg, null, true);
            }

            // insert was successful
            return new MongoSaveResult(
                    false,
                    "",
                    insertOneResult.getInsertedId().asObjectId().getValue().toString(),
                    true);

        } else {
            // update existing document

            // use original creation time and add update time
            dataSetDocument.setCreatedAt(existingDocument.getCreatedAt());
            dataSetDocument.addUpdatedTime();

            UpdateResult result = null;
            try {
                // No upsert: the filter is on _id, so an upsert would not re-create this document —
                // it would insert a *different* one under a new id if the document were deleted
                // between the lookup above and this write. Matching zero documents is the honest
                // signal for that race, handled below.
                final Bson idFilter = eq(BsonConstants.BSON_KEY_DATA_SET_ID, new ObjectId(existingDocumentId));
                result = mongoCollectionDataSets.replaceOne(idFilter, dataSetDocument);
            } catch (MongoException ex) {
                final String errorMsg = "MongoException replacing DataSetDocument: " + ex.getMessage();
                logger.error("saveDataSet replace error: {}", ex.getMessage(), ex);
                return MongoSaveResult.error(errorMsg, existingDocumentId, false);
            }

            if (!result.wasAcknowledged()) {
                final String errorMsg = "replaceOne not acknowledged for existing DataSetDocument id: "
                        + existingDocumentId;
                logger.error(errorMsg);
                return MongoSaveResult.error(errorMsg, existingDocumentId, false);
            }

            // Test matchedCount, not modifiedCount. With the upsert removed, matching nothing is the
            // only real failure here: the document was deleted between the lookup above and this
            // write. modifiedCount additionally reports 0 when the stored document is unchanged by
            // the replacement, which is a successful save, not a failure.
            if (result.getMatchedCount() == 0) {
                final String rejectMsg = "saveDataSet no DataSetDocument found with id: " + existingDocumentId
                        + " (deleted concurrently)";
                logger.debug(rejectMsg);
                return MongoSaveResult.reject(rejectMsg, existingDocumentId, false);
            }

            return new MongoSaveResult(false, "", existingDocumentId, false);
        }
    }

    @Override
    public DataSetQueryResult executeQueryDataSets(QueryDataSetsRequest request) {

        // Create query filter from request search criteria.  Phase 1 of #248 (plan D4) preserves the
        // legacy combination semantics verbatim: id / owner criteria AND with everything (global
        // bucket), text / pvName criteria OR with each other (criteria bucket).  Criterion types new
        // in dp-grpc 1.16.0 (name, tags, attributes) have no legacy behavior to preserve and follow
        // the proto contract instead -- criteria list entries AND -- so they join the global bucket.
        // Phase 3 makes the combination all-AND for every criterion type.
        final List<Bson> globalFilterList = new ArrayList<>();
        final List<Bson> criteriaFilterList = new ArrayList<>();

        // Criterion contents are validated in AnnotationServiceImpl.queryDataSets() before the job
        // is enqueued -- blank entries and non-ObjectId ids are rejected there -- so filters are
        // built without re-filtering.  A blank entry silently dropped here would turn the criterion
        // into a match-all (#243), and a malformed ObjectId would throw past the job into
        // QueueHandlerBase, hanging the caller's response stream.
        for (QueryDataSetsRequest.QueryDataSetsCriterion criterion : request.getCriteriaList()) {
            switch (criterion.getCriterionCase()) {

                case IDCRITERION -> {
                    // ids within a criterion are ORed
                    final List<ObjectId> objectIds = criterion.getIdCriterion().getIdsList().stream()
                            .map(ObjectId::new)
                            .toList();
                    globalFilterList.add(Filters.in(BsonConstants.BSON_KEY_DATA_SET_ID, objectIds));
                }

                case OWNERCRITERION -> {
                    globalFilterList.add(Filters.in(
                            BsonConstants.BSON_KEY_DATA_SET_OWNER_ID,
                            criterion.getOwnerCriterion().getOwnerIdsList()));
                }

                case NAMECRITERION -> {
                    final var c = criterion.getNameCriterion();
                    globalFilterList.add(MongoQueryFilterBuilder.nameMatchFilter(
                            BsonConstants.BSON_KEY_DATA_SET_NAME,
                            c.getExactList(), c.getPrefixList(), c.getContainsList()));
                }

                case TEXTCRITERION -> {
                    criteriaFilterList.add(Filters.text(criterion.getTextCriterion().getText()));
                }

                case PVNAMECRITERION -> {
                    // pv names within a criterion are ORed
                    criteriaFilterList.add(Filters.in(
                            BsonConstants.BSON_KEY_DATA_SET_BLOCK_PV_NAMES,
                            criterion.getPvNameCriterion().getNamesList()));
                }

                case TAGSCRITERION -> {
                    globalFilterList.add(MongoQueryFilterBuilder.tagsFilter(
                            criterion.getTagsCriterion().getValuesList()));
                }

                case ATTRIBUTESCRITERION -> {
                    final var c = criterion.getAttributesCriterion();
                    globalFilterList.add(
                            MongoQueryFilterBuilder.attributeFilter(c.getKey(), c.getValuesList()));
                }

                case CRITERION_NOT_SET -> {
                    // rejected by validation before the job runs, but log if one slips through
                    logger.error("executeQueryDataSets unexpected error criterion case not set");
                }
            }
        }

        // An empty criteria list is match-all, not an error -- same contract as the #245 metadata
        // queries, so there is deliberately no emptiness check here.

        // create global filter to be combined with and operator (default matches all DataSets)
        Bson globalFilter = Filters.exists(BsonConstants.BSON_KEY_DATA_SET_ID);
        if (globalFilterList.size() > 0) {
            globalFilter = and(globalFilterList);
        }

        // create criteria filter to be combined with or operator (default matches all DataSets)
        Bson criteriaFilter = Filters.exists(BsonConstants.BSON_KEY_DATA_SET_ID);
        if (criteriaFilterList.size() > 0) {
            criteriaFilter = or(criteriaFilterList);
        }

        // combine global filter with criteria filter using and operator
        final Bson queryFilter = and(globalFilter, criteriaFilter);

        logger.debug("executing queryDataSets filter: {}", queryFilter);

        // The default limit is unconditional (#245): page size must not depend on any other
        // request field.
        final int limit = request.getLimit() > 0 ? request.getLimit() : DEFAULT_QUERY_LIMIT;
        final int skip = decodePageTokenSkip(request.getPageToken());

        final PagedDocuments<DataSetDocument> page;
        try {
            page = applySkipPaging(
                    mongoCollectionDataSets
                            .find(queryFilter)
                            .sort(ascending(BsonConstants.BSON_KEY_DATA_SET_ID)),
                    skip,
                    limit);
        } catch (Exception ex) {
            logger.error("executeQueryDataSets: mongo exception: {}", ex.getMessage(), ex);
            return null;
        }

        return new DataSetQueryResult(page.documents(), page.nextPageToken());
    }

    @Override
    public AnnotationDocument findAnnotation(String annotationId) {
        // Collapses "absent" and "query failed" to null. Callers that must tell the two apart —
        // saveAnnotation, which reports the former as a rejection — use lookupAnnotation() instead.
        try {
            return lookupAnnotation(annotationId);
        } catch (DpException ex) {
            // already logged with its stack trace in lookupAnnotation()
            return null;
        }
     }

    /**
     * Looks up an AnnotationDocument by id, distinguishing an absent document (null) from a failed
     * query (DpException). See {@link #lookupDataSet} for why the distinction matters.
     */
    private AnnotationDocument lookupAnnotation(String annotationId) throws DpException {

        // TODO: do we need to wrap this in a retry loop?  I'm not adding it now, my reasoning is that if the caller
        // sending request has an annotationId, it already exists in the database.
        final List<AnnotationDocument> matchingDocuments = new ArrayList<>();

        // wrap this in a try/catch because otherwise we take out the thread if mongo throws an exception
        try {
            mongoCollectionAnnotations.find(
                    eq(BsonConstants.BSON_KEY_ANNOTATION_ID, new ObjectId(annotationId))).into(matchingDocuments);
        } catch (Exception ex) {
            // log here with the trace: DpException carries only the message onward
            logger.error("lookupAnnotation: mongo exception in find(): {}", ex.getMessage(), ex);
            throw new DpException("error querying AnnotationDocument by id: " + ex.getMessage());
        }

        return matchingDocuments.isEmpty() ? null : matchingDocuments.get(0);
     }

    @Override
    public MongoSaveResult saveAnnotation(AnnotationDocument annotationDocument, String existingDocumentId) {

        logger.debug("saving AnnotationDocument existingDocumentId: {}", existingDocumentId);

        // try to fetch existing document
        AnnotationDocument existingDocument = null;
        if (!existingDocumentId.isBlank()) {
            try {
                existingDocument = lookupAnnotation(existingDocumentId);
            } catch (DpException ex) {
                // Not necessarily a MongoException: an unparseable id throws IllegalArgumentException
                // from the ObjectId constructor. Let the wrapped message carry the specifics.
                final String errorMsg = "error looking up AnnotationDocument by id: " + ex.getMessage();
                logger.error("saveAnnotation lookup error: {}", ex.getMessage(), ex);
                return MongoSaveResult.error(errorMsg, existingDocumentId, false);
            }
            if (existingDocument == null) {
                // the caller referenced a document that does not exist: a rejection, not an error
                final String rejectMsg = "saveAnnotation no AnnotationDocument found with id: " + existingDocumentId;
                logger.debug(rejectMsg);
                return MongoSaveResult.reject(rejectMsg, existingDocumentId, false);
            }
        }

        if (existingDocument == null) {
            // create a new document
            annotationDocument.addCreationTime(); // set creation time
            InsertOneResult insertOneResult = mongoCollectionAnnotations.insertOne(annotationDocument);

            if (!insertOneResult.wasAcknowledged()) {
                final String errorMsg = "insertOne failed for new AnnotationDocument, result not acknowledged";
                logger.error(errorMsg);
                return new MongoSaveResult(true, errorMsg, null, true);
            }

            // check if result contains id inserted
            if (insertOneResult.getInsertedId() == null) {
                final String errorMsg = "AnnotationDocument insert failed to return document id";
                logger.error(errorMsg);
                return new MongoSaveResult(true, errorMsg, null, true);
            }

            // insert was successful
            return new MongoSaveResult(
                    false,
                    "",
                    insertOneResult.getInsertedId().asObjectId().getValue().toString(),
                    true);

        } else {
            // update existing document

            // use original creation time and add update time
            annotationDocument.setCreatedAt(existingDocument.getCreatedAt());
            annotationDocument.addUpdatedTime();

            UpdateResult result = null;
            try {
                // No upsert: the filter is on _id, so an upsert would not re-create this document —
                // it would insert a *different* one under a new id if the document were deleted
                // between the lookup above and this write. Matching zero documents is the honest
                // signal for that race, handled below.
                final Bson idFilter = eq(BsonConstants.BSON_KEY_ANNOTATION_ID, new ObjectId(existingDocumentId));
                result = mongoCollectionAnnotations.replaceOne(idFilter, annotationDocument);
            } catch (MongoException ex) {
                final String errorMsg = "MongoException replacing AnnotationDocument: " + ex.getMessage();
                logger.error("saveAnnotation replace error: {}", ex.getMessage(), ex);
                return MongoSaveResult.error(errorMsg, existingDocumentId, false);
            }

            if (!result.wasAcknowledged()) {
                final String errorMsg = "replaceOne not acknowledged for existing AnnotationDocument id: "
                        + existingDocumentId;
                logger.error(errorMsg);
                return MongoSaveResult.error(errorMsg, existingDocumentId, false);
            }

            // Test matchedCount, not modifiedCount. With the upsert removed, matching nothing is the
            // only real failure here: the document was deleted between the lookup above and this
            // write. modifiedCount additionally reports 0 when the stored document is unchanged by
            // the replacement, which is a successful save, not a failure.
            if (result.getMatchedCount() == 0) {
                final String rejectMsg = "saveAnnotation no AnnotationDocument found with id: " + existingDocumentId
                        + " (deleted concurrently)";
                logger.debug(rejectMsg);
                return MongoSaveResult.reject(rejectMsg, existingDocumentId, false);
            }

            return new MongoSaveResult(false, "", existingDocumentId, false);
        }
    }

    @Override
    public AnnotationQueryResult executeQueryAnnotations(QueryAnnotationsRequest request) {

        // Create query filter from request search criteria.  Phase 1 of #248 (plan D4) preserves the
        // legacy combination semantics verbatim: id / owner / dataSets / text criteria AND with
        // everything (global bucket), annotations / tags / attributes criteria OR with each other
        // (criteria bucket).  The NameCriterion, new in dp-grpc 1.16.0, has no legacy behavior to
        // preserve and follows the proto contract instead -- criteria list entries AND -- so it
        // joins the global bucket.  Phase 3 makes the combination all-AND for every criterion type.
        final List<Bson> globalFilterList = new ArrayList<>();
        final List<Bson> criteriaFilterList = new ArrayList<>();

        // Criterion contents are validated in AnnotationServiceImpl.queryAnnotations() before the
        // job is enqueued -- blank entries and non-ObjectId ids are rejected there -- so filters are
        // built without re-filtering.  A blank entry silently dropped here would turn the criterion
        // into a match-all (#243), and a malformed ObjectId would throw past the job into
        // QueueHandlerBase, hanging the caller's response stream.
        for (QueryAnnotationsRequest.QueryAnnotationsCriterion criterion : request.getCriteriaList()) {
            switch (criterion.getCriterionCase()) {

                case IDCRITERION -> {
                    // ids within a criterion are ORed
                    final List<ObjectId> objectIds = criterion.getIdCriterion().getIdsList().stream()
                            .map(ObjectId::new)
                            .toList();
                    globalFilterList.add(Filters.in(BsonConstants.BSON_KEY_ANNOTATION_ID, objectIds));
                }

                case OWNERCRITERION -> {
                    globalFilterList.add(Filters.in(
                            BsonConstants.BSON_KEY_ANNOTATION_OWNER_ID,
                            criterion.getOwnerCriterion().getOwnerIdsList()));
                }

                case DATASETSCRITERION -> {
                    // associated dataset ids filter, combined with other filters by AND operator
                    globalFilterList.add(Filters.in(
                            BsonConstants.BSON_KEY_ANNOTATION_DATASET_IDS,
                            criterion.getDataSetsCriterion().getDataSetIdsList()));
                }

                case ANNOTATIONSCRITERION -> {
                    // associated annotation ids filter, combined with other filters by OR operator
                    criteriaFilterList.add(Filters.in(
                            BsonConstants.BSON_KEY_ANNOTATION_ANNOTATION_IDS,
                            criterion.getAnnotationsCriterion().getAnnotationIdsList()));
                }

                case NAMECRITERION -> {
                    final var c = criterion.getNameCriterion();
                    globalFilterList.add(MongoQueryFilterBuilder.nameMatchFilter(
                            BsonConstants.BSON_KEY_ANNOTATION_NAME,
                            c.getExactList(), c.getPrefixList(), c.getContainsList()));
                }

                case TEXTCRITERION -> {
                    // full text search filter, combined with other filters by AND operator
                    globalFilterList.add(Filters.text(criterion.getTextCriterion().getText()));
                }

                case TAGSCRITERION -> {
                    // tags filter, combined with other filters by OR operator
                    criteriaFilterList.add(MongoQueryFilterBuilder.tagsFilter(
                            criterion.getTagsCriterion().getValuesList()));
                }

                case ATTRIBUTESCRITERION -> {
                    // attributes filter, combined with other filters by OR operator
                    final var c = criterion.getAttributesCriterion();
                    criteriaFilterList.add(
                            MongoQueryFilterBuilder.attributeFilter(c.getKey(), c.getValuesList()));
                }

                case CRITERION_NOT_SET -> {
                    // rejected by validation before the job runs, but log if one slips through
                    logger.error("executeQueryAnnotations unexpected error criterion case not set");
                }
            }
        }

        // An empty criteria list is match-all, not an error -- same contract as the #245 metadata
        // queries, so there is deliberately no emptiness check here.

        // create global filter to be combined with and operator (default matches all Annotations)
        Bson globalFilter = Filters.exists(BsonConstants.BSON_KEY_ANNOTATION_ID);
        if (globalFilterList.size() > 0) {
            globalFilter = and(globalFilterList);
        }

        // create criteria filter to be combined with or operator (default matches all Annotations)
        Bson criteriaFilter = Filters.exists(BsonConstants.BSON_KEY_ANNOTATION_ID);
        if (criteriaFilterList.size() > 0) {
            criteriaFilter = or(criteriaFilterList);
        }

        // combine global filter with criteria filter using and operator
        final Bson queryFilter = and(globalFilter, criteriaFilter);

        logger.debug("executing queryAnnotations filter: {}", queryFilter);

        // The default limit is unconditional (#245): page size must not depend on any other
        // request field.
        final int limit = request.getLimit() > 0 ? request.getLimit() : DEFAULT_QUERY_LIMIT;
        final int skip = decodePageTokenSkip(request.getPageToken());

        final PagedDocuments<AnnotationDocument> page;
        try {
            page = applySkipPaging(
                    mongoCollectionAnnotations
                            .find(queryFilter)
                            .sort(ascending(BsonConstants.BSON_KEY_ANNOTATION_ID)),
                    skip,
                    limit);
        } catch (Exception ex) {
            logger.error("executeQueryAnnotations: mongo exception: {}", ex.getMessage(), ex);
            return null;
        }

        return new AnnotationQueryResult(page.documents(), page.nextPageToken());
    }

    @Override
    public MongoInsertOneResult insertCalculations(CalculationsDocument calculationsDocument) {

        logger.debug("inserting CalculationsDocument id: {}", calculationsDocument.getId());

        // set createdAt field for document
        calculationsDocument.addCreationTime();

        InsertOneResult result = null;
        boolean isError = false;
        String errorMsg = "";
        try {
            result = mongoCollectionCalculations.insertOne(calculationsDocument);
        } catch (MongoException ex) {
            isError = true;
            errorMsg = "MongoException inserting CalculationsDocument: " + ex.getMessage();
            logger.error(errorMsg);
        }

        return new MongoInsertOneResult(isError, errorMsg, result);
    }

    @Override
    public CalculationsDocument findCalculations(String calculationsId) {

        // TODO: do we need to wrap this in a retry loop?  I'm not adding it now, my reasoning is that if the caller
        // sending request has a calculationsId, it already exists in the database.
        List<CalculationsDocument> matchingDocuments = new ArrayList<>();

        // wrap this in a try/catch because otherwise we take out the thread if mongo throws an exception
        try {
            mongoCollectionCalculations.find(
                    eq(BsonConstants.BSON_KEY_CALCULATIONS_ID, new ObjectId(calculationsId))).into(matchingDocuments);
        } catch (Exception ex) {
            logger.error("findCalculations: mongo exception in find(): {}", ex.getMessage());
            return null;
        }

        if (!matchingDocuments.isEmpty()) {
            return matchingDocuments.get(0);
        } else {
            return null;
        }
    }

    @Override
    public MongoSaveResult savePvMetadata(PvMetadataDocument document) {

        logger.debug("saving PvMetadataDocument pvName: {}", document.getPvName());

        // Look up only by canonical pvName (not alias) to avoid mistakenly copying createdAt
        // from an unrelated document that merely has this pvName as one of its aliases.
        final List<PvMetadataDocument> exactMatches = new ArrayList<>();
        try {
            mongoCollectionPvMetadata.find(
                    eq(BsonConstants.BSON_KEY_PV_METADATA_PV_NAME, document.getPvName())
            ).into(exactMatches);
        } catch (Exception ex) {
            final String errorMsg = "MongoException looking up PvMetadataDocument by pvName: " + ex.getMessage();
            logger.error(errorMsg);
            return new MongoSaveResult(true, errorMsg, null, false);
        }
        final PvMetadataDocument existingDocument = exactMatches.isEmpty() ? null : exactMatches.get(0);

        try {
            if (existingDocument == null) {
                document.addCreationTime();
                InsertOneResult insertOneResult = mongoCollectionPvMetadata.insertOne(document);

                if (!insertOneResult.wasAcknowledged()) {
                    final String errorMsg = "insertOne failed for PvMetadataDocument, result not acknowledged";
                    logger.error(errorMsg);
                    return new MongoSaveResult(true, errorMsg, null, true);
                }
                if (insertOneResult.getInsertedId() == null) {
                    final String errorMsg = "PvMetadataDocument insert failed to return document id";
                    logger.error(errorMsg);
                    return new MongoSaveResult(true, errorMsg, null, true);
                }
                return new MongoSaveResult(false, "", document.getPvName(), true);

            } else {
                document.setCreatedAt(existingDocument.getCreatedAt());
                document.addUpdatedTime();

                final Bson filter = eq(BsonConstants.BSON_KEY_PV_METADATA_PV_NAME, document.getPvName());
                final ReplaceOptions replaceOptions = new ReplaceOptions().upsert(true);
                final UpdateResult result = mongoCollectionPvMetadata.replaceOne(filter, document, replaceOptions);

                if (!result.wasAcknowledged()) {
                    final String errorMsg = "replaceOne not acknowledged for PvMetadataDocument pvName: "
                            + document.getPvName();
                    logger.error(errorMsg);
                    return new MongoSaveResult(true, errorMsg, document.getPvName(), false);
                }
                return new MongoSaveResult(false, "", document.getPvName(), false);
            }
        } catch (MongoException ex) {
            final String errorMsg = "MongoException saving PvMetadataDocument: " + ex.getMessage();
            logger.error(errorMsg);
            return new MongoSaveResult(true, errorMsg, null, false);
        }
    }

    @Override
    public PvMetadataQueryResult executeQueryPvMetadata(QueryPvMetadataRequest request) {

        final List<Bson> filterList = new ArrayList<>();

        for (QueryPvMetadataRequest.QueryPvMetadataCriterion criterion : request.getCriteriaList()) {
            switch (criterion.getCriterionCase()) {

                case PVNAMECRITERION -> {
                    final QueryPvMetadataRequest.QueryPvMetadataCriterion.PvNameCriterion c =
                            criterion.getPvNameCriterion();
                    final Bson f = MongoQueryFilterBuilder.nameMatchFilter(
                            BsonConstants.BSON_KEY_PV_METADATA_PV_NAME,
                            c.getExactList(), c.getPrefixList(), c.getContainsList());
                    if (f != null) {
                        filterList.add(f);
                    }
                }

                case ALIASESCRITERION -> {
                    final QueryPvMetadataRequest.QueryPvMetadataCriterion.AliasesCriterion c =
                            criterion.getAliasesCriterion();
                    final Bson f = MongoQueryFilterBuilder.nameMatchFilter(
                            BsonConstants.BSON_KEY_PV_METADATA_ALIASES,
                            c.getExactList(), c.getPrefixList(), c.getContainsList());
                    if (f != null) {
                        filterList.add(f);
                    }
                }

                case TAGSCRITERION -> {
                    final QueryPvMetadataRequest.QueryPvMetadataCriterion.TagsCriterion c =
                            criterion.getTagsCriterion();
                    filterList.add(MongoQueryFilterBuilder.tagsFilter(c.getValuesList()));
                }

                case ATTRIBUTESCRITERION -> {
                    final QueryPvMetadataRequest.QueryPvMetadataCriterion.AttributesCriterion c =
                            criterion.getAttributesCriterion();
                    filterList.add(MongoQueryFilterBuilder.attributeFilter(c.getKey(), c.getValuesList()));
                }

                default -> {
                    logger.error("executeQueryPvMetadata unexpected criterion case: {}", criterion.getCriterionCase());
                }
            }
        }

        final Bson queryFilter = filterList.isEmpty()
                ? Filters.exists(BsonConstants.BSON_KEY_PV_METADATA_PV_NAME)
                : and(filterList);

        // limit is always positive (DEFAULT_QUERY_LIMIT when unset), so there is no unbounded path.
        final int limit = request.getLimit() > 0 ? request.getLimit() : DEFAULT_QUERY_LIMIT;
        final int skip = decodePageTokenSkip(request.getPageToken());

        final PagedDocuments<PvMetadataDocument> page;
        try {
            page = applySkipPaging(
                    mongoCollectionPvMetadata
                            .find(queryFilter)
                            .sort(ascending(BsonConstants.BSON_KEY_PV_METADATA_PV_NAME)),
                    skip,
                    limit);
        } catch (Exception ex) {
            logger.error("executeQueryPvMetadata: mongo exception: {}", ex.getMessage(), ex);
            return null;
        }

        return new PvMetadataQueryResult(page.documents(), page.nextPageToken());
    }

    /**
     * Looks up a PvMetadataDocument by pvName or alias, distinguishing an absent document (null)
     * from a failed query (DpException). Checked for the same reason as
     * {@link #findConfigurationByName}: an unchecked throw here escaped {@link #deletePvMetadata},
     * which called it without a catch.
     */
    @Override
    public PvMetadataDocument findPvMetadataByNameOrAlias(String pvNameOrAlias) throws DpException {

        final List<PvMetadataDocument> matchingDocuments = new ArrayList<>();

        try {
            final Bson filter = or(
                    eq(BsonConstants.BSON_KEY_PV_METADATA_PV_NAME, pvNameOrAlias),
                    eq(BsonConstants.BSON_KEY_PV_METADATA_ALIASES, pvNameOrAlias));
            mongoCollectionPvMetadata.find(filter).into(matchingDocuments);
        } catch (Exception ex) {
            // log here with the trace: DpException carries only the message onward
            logger.error("findPvMetadataByNameOrAlias: mongo exception in find(): {}", ex.getMessage(), ex);
            throw new DpException("error querying PvMetadataDocument by name or alias: " + ex.getMessage());
        }

        return matchingDocuments.isEmpty() ? null : matchingDocuments.get(0);
    }

    @Override
    public MongoDeleteResult deletePvMetadata(String pvNameOrAlias) {

        final PvMetadataDocument existingDocument;
        try {
            existingDocument = findPvMetadataByNameOrAlias(pvNameOrAlias);
        } catch (DpException ex) {
            // A failed lookup is an infrastructure error, not "no such record": returning the
            // not-found result below would report a database outage as a rejection to the caller.
            final String errorMsg = "error looking up PvMetadata for '" + pvNameOrAlias + "': " + ex.getMessage();
            logger.error("deletePvMetadata lookup error: {}", ex.getMessage(), ex);
            return MongoDeleteResult.error(errorMsg);
        }
        if (existingDocument == null) {
            return new MongoDeleteResult(false, "", null);
        }

        final String canonicalPvName = existingDocument.getPvName();

        try {
            final Bson filter = eq(BsonConstants.BSON_KEY_PV_METADATA_PV_NAME, canonicalPvName);
            final DeleteResult result = mongoCollectionPvMetadata.deleteOne(filter);
            if (!result.wasAcknowledged()) {
                final String errorMsg = "deleteOne not acknowledged for pvName: " + canonicalPvName;
                logger.error(errorMsg);
                return new MongoDeleteResult(true, errorMsg, null);
            }
            return new MongoDeleteResult(false, "", canonicalPvName);
        } catch (MongoException ex) {
            final String errorMsg = "MongoException deleting PvMetadataDocument: " + ex.getMessage();
            logger.error(errorMsg);
            return new MongoDeleteResult(true, errorMsg, null);
        }
    }

    // =========================================================
    // Configuration CRUD
    // =========================================================

    private boolean activationsExistForConfiguration(String configurationName) {
        // No exception catch here — a MongoException propagates to callers so that a transient
        // DB error is not silently treated as "no activations exist", which could allow an unsafe
        // category change or delete to proceed.
        final long count = mongoCollectionConfigurationActivations.countDocuments(
                eq(BsonConstants.BSON_KEY_ACTIVATION_CONFIGURATION_NAME, configurationName));
        return count > 0;
    }

    @Override
    public MongoSaveResult saveConfiguration(ConfigurationDocument document) {

        logger.debug("saving ConfigurationDocument configurationName: {}", document.getConfigurationName());

        final List<ConfigurationDocument> exactMatches = new ArrayList<>();
        try {
            mongoCollectionConfigurations.find(
                    eq(BsonConstants.BSON_KEY_CONFIGURATION_NAME, document.getConfigurationName())
            ).into(exactMatches);
        } catch (Exception ex) {
            final String errorMsg = "MongoException looking up ConfigurationDocument by configurationName: " + ex.getMessage();
            logger.error(errorMsg);
            return new MongoSaveResult(true, errorMsg, null, false);
        }
        final ConfigurationDocument existingDocument = exactMatches.isEmpty() ? null : exactMatches.get(0);

        // reject category change if activations exist
        if (existingDocument != null && !existingDocument.getCategory().equals(document.getCategory())) {
            final boolean hasActivations;
            try {
                hasActivations = activationsExistForConfiguration(document.getConfigurationName());
            } catch (MongoException ex) {
                final String errorMsg = "MongoException checking activations for configurationName '"
                        + document.getConfigurationName() + "': " + ex.getMessage();
                logger.error(errorMsg);
                return new MongoSaveResult(true, errorMsg, null, false);
            }
            if (hasActivations) {
                final String rejectMsg = "cannot change category for configurationName '"
                        + document.getConfigurationName()
                        + "': existing activations must be deleted first";
                return MongoSaveResult.reject(rejectMsg, null, false);
            }
        }
        // NOTE: the activation existence check above and the subsequent replaceOne are not atomic.
        // A concurrent saveConfigurationActivation could slip in between them on a multi-threaded
        // deployment. Full atomicity would require MongoDB transactions (replica set only). This is
        // an accepted limitation for v1; tracked for resolution when transaction support is added.

        try {
            if (existingDocument == null) {
                document.addCreationTime();
                final com.mongodb.client.result.InsertOneResult insertOneResult =
                        mongoCollectionConfigurations.insertOne(document);
                if (!insertOneResult.wasAcknowledged()) {
                    final String errorMsg = "insertOne failed for ConfigurationDocument, result not acknowledged";
                    logger.error(errorMsg);
                    return new MongoSaveResult(true, errorMsg, null, true);
                }
                if (insertOneResult.getInsertedId() == null) {
                    final String errorMsg = "ConfigurationDocument insert failed to return document id";
                    logger.error(errorMsg);
                    return new MongoSaveResult(true, errorMsg, null, true);
                }
                return new MongoSaveResult(false, "", document.getConfigurationName(), true);

            } else {
                document.setCreatedAt(existingDocument.getCreatedAt());
                document.addUpdatedTime();

                final Bson filter = eq(BsonConstants.BSON_KEY_CONFIGURATION_NAME, document.getConfigurationName());
                final ReplaceOptions replaceOptions = new ReplaceOptions().upsert(true);
                final UpdateResult result = mongoCollectionConfigurations.replaceOne(filter, document, replaceOptions);

                if (!result.wasAcknowledged()) {
                    final String errorMsg = "replaceOne not acknowledged for ConfigurationDocument configurationName: "
                            + document.getConfigurationName();
                    logger.error(errorMsg);
                    return new MongoSaveResult(true, errorMsg, document.getConfigurationName(), false);
                }
                return new MongoSaveResult(false, "", document.getConfigurationName(), false);
            }
        } catch (MongoException ex) {
            final String errorMsg = "MongoException saving ConfigurationDocument: " + ex.getMessage();
            logger.error(errorMsg);
            return new MongoSaveResult(true, errorMsg, null, false);
        }
    }

    /**
     * Looks up a ConfigurationDocument by name, distinguishing an absent document (null) from a
     * failed query (DpException). See {@link #lookupDataSet} for why the distinction matters.
     *
     * <p>This throws a checked {@link DpException} rather than an unchecked exception on purpose.
     * It previously wrapped failures in a bare {@code RuntimeException}, which slipped past
     * {@code saveConfigurationActivation}'s {@code catch (MongoException)} and escaped the job
     * entirely — the queue worker logged it and moved on, so the dispatcher never ran and the
     * caller's response stream was left open until it timed out. A checked exception makes the
     * compiler enforce that every caller decides what to do with a query failure.
     */
    @Override
    public ConfigurationDocument findConfigurationByName(String configurationName) throws DpException {
        try {
            return mongoCollectionConfigurations.find(
                    eq(BsonConstants.BSON_KEY_CONFIGURATION_NAME, configurationName)).first();
        } catch (Exception ex) {
            // log here with the trace: DpException carries only the message onward
            logger.error("findConfigurationByName: mongo exception in find(): {}", ex.getMessage(), ex);
            throw new DpException("error querying ConfigurationDocument by name: " + ex.getMessage());
        }
    }

    @Override
    public ConfigurationQueryResult executeQueryConfigurations(QueryConfigurationsRequest request) {

        final List<Bson> filterList = new ArrayList<>();

        for (QueryConfigurationsRequest.QueryConfigurationsCriterion criterion : request.getCriteriaList()) {
            switch (criterion.getCriterionCase()) {

                case NAMECRITERION -> {
                    final var c = criterion.getNameCriterion();
                    final List<Bson> nameFilters = new ArrayList<>();
                    if (!c.getExactList().isEmpty()) {
                        nameFilters.add(Filters.in(BsonConstants.BSON_KEY_CONFIGURATION_NAME, c.getExactList()));
                    }
                    for (String prefix : c.getPrefixList()) {
                        nameFilters.add(Filters.regex(BsonConstants.BSON_KEY_CONFIGURATION_NAME,
                                "^" + java.util.regex.Pattern.quote(prefix)));
                    }
                    for (String contains : c.getContainsList()) {
                        nameFilters.add(Filters.regex(BsonConstants.BSON_KEY_CONFIGURATION_NAME,
                                ".*" + java.util.regex.Pattern.quote(contains) + ".*"));
                    }
                    if (!nameFilters.isEmpty()) {
                        filterList.add(nameFilters.size() == 1 ? nameFilters.get(0) : or(nameFilters));
                    }
                }

                case CATEGORYCRITERION -> {
                    filterList.add(Filters.in(BsonConstants.BSON_KEY_CONFIGURATION_CATEGORY,
                            criterion.getCategoryCriterion().getValuesList()));
                }

                case PARENTCRITERION -> {
                    filterList.add(Filters.in(BsonConstants.BSON_KEY_CONFIGURATION_PARENT_NAME,
                            criterion.getParentCriterion().getValuesList()));
                }

                case TAGSCRITERION -> {
                    filterList.add(Filters.in(BsonConstants.BSON_KEY_TAGS,
                            criterion.getTagsCriterion().getValuesList()));
                }

                case ATTRIBUTESCRITERION -> {
                    final var c = criterion.getAttributesCriterion();
                    final String mapKey = BsonConstants.BSON_KEY_ATTRIBUTES + "." + c.getKey();
                    if (c.getValuesList().isEmpty()) {
                        filterList.add(Filters.exists(mapKey));
                    } else {
                        filterList.add(Filters.in(mapKey, c.getValuesList()));
                    }
                }

                default -> {
                    logger.error("executeQueryConfigurations unexpected criterion case: {}",
                            criterion.getCriterionCase());
                }
            }
        }

        final Bson queryFilter = filterList.isEmpty()
                ? Filters.exists(BsonConstants.BSON_KEY_CONFIGURATION_NAME)
                : and(filterList);

        final int limit = request.getLimit() > 0 ? request.getLimit() : DEFAULT_QUERY_LIMIT;
        final int skip = decodePageTokenSkip(request.getPageToken());

        final PagedDocuments<ConfigurationDocument> page;
        try {
            page = applySkipPaging(
                    mongoCollectionConfigurations
                            .find(queryFilter)
                            .sort(ascending(BsonConstants.BSON_KEY_CONFIGURATION_NAME)),
                    skip,
                    limit);
        } catch (Exception ex) {
            logger.error("executeQueryConfigurations: mongo exception: {}", ex.getMessage(), ex);
            return null;
        }

        return new ConfigurationQueryResult(page.documents(), page.nextPageToken());
    }

    @Override
    public MongoDeleteResult deleteConfiguration(String configurationName) {

        final boolean hasActivations;
        try {
            hasActivations = activationsExistForConfiguration(configurationName);
        } catch (MongoException ex) {
            final String errorMsg = "MongoException checking activations for configurationName '"
                    + configurationName + "': " + ex.getMessage();
            logger.error(errorMsg);
            return new MongoDeleteResult(true, errorMsg, null);
        }
        if (hasActivations) {
            final String rejectMsg = "cannot delete configurationName '" + configurationName
                    + "': existing activations must be deleted first";
            return MongoDeleteResult.reject(rejectMsg);
        }

        try {
            final Bson filter = eq(BsonConstants.BSON_KEY_CONFIGURATION_NAME, configurationName);
            final DeleteResult result = mongoCollectionConfigurations.deleteOne(filter);
            if (!result.wasAcknowledged()) {
                final String errorMsg = "deleteOne not acknowledged for configurationName: " + configurationName;
                logger.error(errorMsg);
                return new MongoDeleteResult(true, errorMsg, null);
            }
            if (result.getDeletedCount() == 0) {
                // not found — signal via null deletedIdentifier
                return new MongoDeleteResult(false, "", null);
            }
            return new MongoDeleteResult(false, "", configurationName);
        } catch (MongoException ex) {
            final String errorMsg = "MongoException deleting ConfigurationDocument: " + ex.getMessage();
            logger.error(errorMsg);
            return new MongoDeleteResult(true, errorMsg, null);
        }
    }

    // ---- Configuration Activation methods ----

    /**
     * Checks if an overlap exists for the given activation parameters.
     * Overlap rules: same configurationName OR same internalCategory, time intervals overlap.
     * excludeClientActivationId: exclude this record from the overlap check (used for updates).
     *
     * Note: no exception catch here — callers must handle MongoException so that a transient DB
     * error is not silently treated as "no overlap", which would allow overlapping activations to
     * be inserted in violation of the API contract.
     *
     * Also note: the overlap check and the subsequent insert/replace are not atomic operations.
     * Concurrent saves on multiple worker threads could both pass this check before either write
     * completes, resulting in overlapping activations being persisted. Full atomicity would require
     * MongoDB transactions (replica set only). This is an accepted limitation for v1; tracked for
     * resolution when transaction support is added.
     */
    private boolean overlapExists(String configurationName, String internalCategory,
                                   Instant startTime, Instant endTime,
                                   String excludeClientActivationId) {
        // endTime filter for the candidate: candidate.endTime > startTime OR candidate.endTime absent
        final Bson candidateEndTimeFilter = or(
                exists(BsonConstants.BSON_KEY_ACTIVATION_END_TIME, false),
                gt(BsonConstants.BSON_KEY_ACTIVATION_END_TIME, startTime)
        );

        // startTime filter for the candidate: candidate.startTime < endTime (skip if endTime null — always overlaps)
        final Bson candidateStartTimeFilter = endTime != null
                ? lt(BsonConstants.BSON_KEY_ACTIVATION_START_TIME, endTime)
                : null;

        final Bson excludeFilter = (excludeClientActivationId != null && !excludeClientActivationId.isBlank())
                ? ne(BsonConstants.BSON_KEY_ACTIVATION_CLIENT_ID, excludeClientActivationId)
                : null;

        // Query 1: same configurationName overlap
        final List<Bson> q1Filters = new ArrayList<>();
        q1Filters.add(eq(BsonConstants.BSON_KEY_ACTIVATION_CONFIGURATION_NAME, configurationName));
        if (excludeFilter != null) q1Filters.add(excludeFilter);
        if (candidateStartTimeFilter != null) q1Filters.add(candidateStartTimeFilter);
        q1Filters.add(candidateEndTimeFilter);
        final long count1 = mongoCollectionConfigurationActivations.countDocuments(and(q1Filters));
        if (count1 > 0) return true;

        // Query 2: same category overlap
        final List<Bson> q2Filters = new ArrayList<>();
        q2Filters.add(eq(BsonConstants.BSON_KEY_ACTIVATION_INTERNAL_CATEGORY, internalCategory));
        if (excludeFilter != null) q2Filters.add(excludeFilter);
        if (candidateStartTimeFilter != null) q2Filters.add(candidateStartTimeFilter);
        q2Filters.add(candidateEndTimeFilter);
        final long count2 = mongoCollectionConfigurationActivations.countDocuments(and(q2Filters));
        return count2 > 0;
    }

    @Override
    public MongoSaveResult saveConfigurationActivation(ConfigurationActivationDocument document) {
        try {
            // look up Configuration to get internalCategory
            final ConfigurationDocument config;
            try {
                config = findConfigurationByName(document.getConfigurationName());
            } catch (DpException ex) {
                // A failed lookup is an infrastructure error, not "no such Configuration": reporting
                // it as the rejection below would invert the caller's retry decision.
                final String errorMsg = "error looking up Configuration for configurationName '"
                        + document.getConfigurationName() + "': " + ex.getMessage();
                logger.error("saveConfigurationActivation lookup error: {}", ex.getMessage(), ex);
                return MongoSaveResult.error(errorMsg, null, false);
            }
            if (config == null) {
                return MongoSaveResult.reject(
                        "no Configuration found for configurationName: '" + document.getConfigurationName() + "'",
                        null, false);
            }
            document.setInternalCategory(config.getCategory());

            // determine excludeId for overlap check (non-blank clientActivationId = potential update)
            final String excludeId = (document.getClientActivationId() != null
                    && !document.getClientActivationId().isBlank())
                    ? document.getClientActivationId() : null;

            // check for overlap
            final boolean overlap;
            try {
                overlap = overlapExists(document.getConfigurationName(), document.getInternalCategory(),
                        document.getStartTime(), document.getEndTime(), excludeId);
            } catch (MongoException ex) {
                final String errorMsg = "MongoException checking activation overlap for configurationName '"
                        + document.getConfigurationName() + "': " + ex.getMessage();
                logger.error(errorMsg);
                return new MongoSaveResult(true, errorMsg, null, false);
            }
            if (overlap) {
                return MongoSaveResult.reject(
                        "overlapping activation exists for configurationName '"
                                + document.getConfigurationName() + "' or category '"
                                + document.getInternalCategory() + "'",
                        null, false);
            }

            // generate server-side UUID if clientActivationId not supplied
            if (document.getClientActivationId() == null || document.getClientActivationId().isBlank()) {
                document.setClientActivationId(UUID.randomUUID().toString());
            }

            // find existing record by clientActivationId
            final ConfigurationActivationDocument existing = findConfigurationActivationById(
                    document.getClientActivationId());

            if (existing == null) {
                // new record
                document.addCreationTime();
                mongoCollectionConfigurationActivations.insertOne(document);
            } else {
                // update existing: preserve createdAt, set updatedAt
                document.setCreatedAt(existing.getCreatedAt());
                document.addUpdatedTime();
                final Bson filter = eq(BsonConstants.BSON_KEY_ACTIVATION_CLIENT_ID,
                        document.getClientActivationId());
                final ReplaceOptions replaceOptions = new ReplaceOptions().upsert(true);
                mongoCollectionConfigurationActivations.replaceOne(filter, document, replaceOptions);
            }

            return new MongoSaveResult(false, "", document.getClientActivationId(), existing == null);

        } catch (MongoException ex) {
            final String errorMsg = "MongoException saving ConfigurationActivationDocument: " + ex.getMessage();
            logger.error(errorMsg);
            return new MongoSaveResult(true, errorMsg, null, false);
        }
    }

    @Override
    public ConfigurationActivationDocument findConfigurationActivationById(String clientActivationId) {
        // Note: MongoException is not caught here — callers must handle it so that a backend failure
        // is surfaced as RESULT_STATUS_ERROR rather than silently reported as "not found".
        return mongoCollectionConfigurationActivations.find(
                eq(BsonConstants.BSON_KEY_ACTIVATION_CLIENT_ID, clientActivationId)).first();
    }

    @Override
    public ConfigurationActivationDocument findConfigurationActivationByCompositeKey(
            String configurationName, Instant startTime) {
        // Note: MongoException is not caught here — callers must handle it so that a backend failure
        // is surfaced as RESULT_STATUS_ERROR rather than silently reported as "not found".
        return mongoCollectionConfigurationActivations.find(
                and(
                        eq(BsonConstants.BSON_KEY_ACTIVATION_CONFIGURATION_NAME, configurationName),
                        eq(BsonConstants.BSON_KEY_ACTIVATION_START_TIME, startTime)
                )).first();
    }

    @Override
    public ConfigurationActivationQueryResult executeQueryConfigurationActivations(
            QueryConfigurationActivationsRequest request) {

        final List<Bson> filterList = new ArrayList<>();

        for (var criterion : request.getCriteriaList()) {
            switch (criterion.getCriterionCase()) {
                case TIMESTAMPCRITERION -> {
                    final Instant ts = com.ospreydcs.dp.service.common.protobuf.TimestampUtility
                            .instantFromTimestamp(criterion.getTimestampCriterion().getTimestamp());
                    filterList.add(MongoQueryFilterBuilder.activationContainsInstantFilter(ts));
                }
                case TIMERANGECRITERION -> {
                    final Instant rangeStart = com.ospreydcs.dp.service.common.protobuf.TimestampUtility
                            .instantFromTimestamp(criterion.getTimeRangeCriterion().getStartTime());
                    final Instant rangeEnd = com.ospreydcs.dp.service.common.protobuf.TimestampUtility
                            .instantFromTimestamp(criterion.getTimeRangeCriterion().getEndTime());
                    filterList.add(MongoQueryFilterBuilder.activationOverlapsRangeFilter(rangeStart, rangeEnd));
                }
                case CONFIGURATIONNAMECRITERION -> {
                    filterList.add(in(BsonConstants.BSON_KEY_ACTIVATION_CONFIGURATION_NAME,
                            criterion.getConfigurationNameCriterion().getValuesList()));
                }
                case CLIENTACTIVATIONIDCRITERION -> {
                    filterList.add(in(BsonConstants.BSON_KEY_ACTIVATION_CLIENT_ID,
                            criterion.getClientActivationIdCriterion().getValuesList()));
                }
                case CATEGORYCRITERION -> {
                    filterList.add(in(BsonConstants.BSON_KEY_ACTIVATION_INTERNAL_CATEGORY,
                            criterion.getCategoryCriterion().getValuesList()));
                }
                case TAGSCRITERION -> {
                    filterList.add(MongoQueryFilterBuilder.tagsFilter(
                            criterion.getTagsCriterion().getValuesList()));
                }
                case ATTRIBUTESCRITERION -> {
                    final var ac = criterion.getAttributesCriterion();
                    filterList.add(MongoQueryFilterBuilder.attributeFilter(ac.getKey(), ac.getValuesList()));
                }
                default -> {
                    // unknown criterion — ignored
                }
            }
        }

        final Bson filter = filterList.isEmpty() ? new org.bson.Document() : and(filterList);

        // pagination
        final int limit = request.getLimit() > 0 ? request.getLimit() : DEFAULT_QUERY_LIMIT;
        final int skip = decodePageTokenSkip(request.getPageToken());

        final PagedDocuments<ConfigurationActivationDocument> page;
        try {
            page = applySkipPaging(
                    mongoCollectionConfigurationActivations
                            .find(filter)
                            .sort(ascending(BsonConstants.BSON_KEY_ACTIVATION_START_TIME)),
                    skip,
                    limit);
        } catch (Exception ex) {
            logger.error("executeQueryConfigurationActivations: mongo exception: {}", ex.getMessage(), ex);
            return null;
        }

        return new ConfigurationActivationQueryResult(page.documents(), page.nextPageToken());
    }

    @Override
    public MongoDeleteResult deleteConfigurationActivation(String clientActivationId) {
        try {
            final Bson filter = eq(BsonConstants.BSON_KEY_ACTIVATION_CLIENT_ID, clientActivationId);
            final DeleteResult result = mongoCollectionConfigurationActivations.deleteOne(filter);
            if (!result.wasAcknowledged()) {
                final String errorMsg = "deleteOne not acknowledged for clientActivationId: " + clientActivationId;
                logger.error(errorMsg);
                return new MongoDeleteResult(true, errorMsg, null);
            }
            if (result.getDeletedCount() == 0) {
                return new MongoDeleteResult(false, "", null);
            }
            return new MongoDeleteResult(false, "", clientActivationId);
        } catch (MongoException ex) {
            final String errorMsg = "MongoException deleting ConfigurationActivationDocument by id: " + ex.getMessage();
            logger.error(errorMsg);
            return new MongoDeleteResult(true, errorMsg, null);
        }
    }

    @Override
    public MongoDeleteResult deleteConfigurationActivationByCompositeKey(
            String configurationName, Instant startTime) {
        try {
            final Bson filter = and(
                    eq(BsonConstants.BSON_KEY_ACTIVATION_CONFIGURATION_NAME, configurationName),
                    eq(BsonConstants.BSON_KEY_ACTIVATION_START_TIME, startTime)
            );
            // Fetch the document first so we can return its actual clientActivationId in the response.
            final ConfigurationActivationDocument existing =
                    mongoCollectionConfigurationActivations.find(filter).first();
            if (existing == null) {
                return new MongoDeleteResult(false, "", null);
            }
            final DeleteResult result = mongoCollectionConfigurationActivations.deleteOne(filter);
            if (!result.wasAcknowledged()) {
                final String errorMsg = "deleteOne not acknowledged for compositeKey configurationName: "
                        + configurationName + ", startTime: " + startTime;
                logger.error(errorMsg);
                return new MongoDeleteResult(true, errorMsg, null);
            }
            if (result.getDeletedCount() == 0) {
                // Concurrent delete between our find and deleteOne — treat as not found.
                return new MongoDeleteResult(false, "", null);
            }
            return new MongoDeleteResult(false, "", existing.getClientActivationId());
        } catch (MongoException ex) {
            final String errorMsg = "MongoException deleting ConfigurationActivationDocument by compositeKey: "
                    + ex.getMessage();
            logger.error(errorMsg);
            return new MongoDeleteResult(true, errorMsg, null);
        }
    }

    @Override
    public ConfigurationActivationQueryResult getActiveConfigurations(Instant timestamp) {
        try {
            final Bson filter = and(
                    lte(BsonConstants.BSON_KEY_ACTIVATION_START_TIME, timestamp),
                    or(
                            exists(BsonConstants.BSON_KEY_ACTIVATION_END_TIME, false),
                            gt(BsonConstants.BSON_KEY_ACTIVATION_END_TIME, timestamp)
                    )
            );

            final List<ConfigurationActivationDocument> documents = new ArrayList<>();
            mongoCollectionConfigurationActivations.find(filter)
                    .sort(ascending(BsonConstants.BSON_KEY_ACTIVATION_START_TIME))
                    .into(documents);

            return new ConfigurationActivationQueryResult(documents, "");
        } catch (MongoException ex) {
            logger.error("getActiveConfigurations: mongo exception: {}", ex.getMessage());
            return null;
        }
    }

    // =========================================================
    // Sample Status
    // =========================================================

    /**
     * Carve-and-insert upsert: for each incoming column, exactly-colliding timestamps are carved
     * out of existing overlapping documents (maintaining the invariant that no two documents
     * assert the same (pvName, timestamp, domain, layer) identity key), then the incoming column
     * is inserted as a new document preserving its axis representation. Existing documents with
     * overlapping spans but no colliding timestamps are left untouched, provenance intact.
     *
     * <p>Carve rewrites happen before the insert, so a mid-write failure can lose replaced
     * statuses but never leaves two documents asserting the same key. Partial persistence on
     * error is documented API behavior; the returned count reflects the statuses persisted before
     * the failure.
     */
    @Override
    public MongoCountResult saveSampleStatuses(SaveSampleStatusesRequest request) {

        final Instant now = Instant.now();
        long savedCount = 0;

        // frames processed in request order so a later frame's write wins on duplicate keys
        for (SampleStatusFrame frame : request.getFramesList()) {

            final List<Long> frameTimestamps =
                    SampleStatusDocumentUtility.timestampNanosList(frame.getDataTimestamps());
            final Set<Long> frameTimestampSet = new HashSet<>(frameTimestamps);
            final long frameFirstNanos = frameTimestamps.get(0);
            final long frameLastNanos = frameTimestamps.get(frameTimestamps.size() - 1);

            for (SampleStatusColumn column : frame.getStatusColumnsList()) {

                // Find existing documents overlapping the incoming span for this identity prefix.
                // A Mongo error here must abort the save rather than be read as "no overlap",
                // which would insert colliding documents and violate the storage invariant.
                final List<SampleStatusBucketDocument> overlappingDocuments = new ArrayList<>();
                try {
                    mongoCollectionSampleStatusBuckets.find(and(
                            eq(BsonConstants.BSON_KEY_SAMPLE_STATUS_PV_NAME, column.getPvName()),
                            eq(BsonConstants.BSON_KEY_SAMPLE_STATUS_DOMAIN, frame.getDomain()),
                            eq(BsonConstants.BSON_KEY_SAMPLE_STATUS_LAYER, frame.getLayer()),
                            lte(BsonConstants.BSON_KEY_SAMPLE_STATUS_FIRST_TIME_NANOS, frameLastNanos),
                            gte(BsonConstants.BSON_KEY_SAMPLE_STATUS_LAST_TIME_NANOS, frameFirstNanos)
                    )).into(overlappingDocuments);
                } catch (MongoException ex) {
                    final String errorMsg = "MongoException querying overlapping sample status documents: "
                            + ex.getMessage();
                    logger.error("saveSampleStatuses overlap query error: {}", ex.getMessage(), ex);
                    return new MongoCountResult(true, errorMsg, savedCount);
                }

                try {
                    for (SampleStatusBucketDocument existingDocument : overlappingDocuments) {
                        final SampleStatusDocumentUtility.RemovalResult removal =
                                SampleStatusDocumentUtility.removeTimestamps(existingDocument, frameTimestampSet);
                        if (removal.removedCount() == 0) {
                            continue;
                        }
                        mongoCollectionSampleStatusBuckets.deleteOne(
                                eq(BsonConstants.BSON_KEY_SAMPLE_STATUS_ID, existingDocument.getId()));
                        for (SampleStatusBucketDocument replacement : removal.replacementDocuments()) {
                            // rewritten documents take the incoming save's provenance and a fresh
                            // updatedTime ("most recent save affecting the bucket")
                            replacement.setSource(request.getSource().isBlank() ? null : request.getSource());
                            replacement.setModifiedBy(
                                    request.getModifiedBy().isBlank() ? null : request.getModifiedBy());
                            replacement.setUpdatedTime(now);
                            mongoCollectionSampleStatusBuckets.insertOne(replacement);
                        }
                    }

                    final SampleStatusBucketDocument newDocument =
                            SampleStatusBucketDocument.fromSampleStatusColumn(
                                    frame.getDomain(),
                                    frame.getLayer(),
                                    frame.getDataTimestamps(),
                                    column,
                                    request.getSource(),
                                    request.getModifiedBy(),
                                    now);
                    mongoCollectionSampleStatusBuckets.insertOne(newDocument);

                } catch (MongoException | DpException ex) {
                    final String errorMsg = "error writing sample status documents for PV: "
                            + column.getPvName() + ": " + ex.getMessage();
                    logger.error("saveSampleStatuses write error: {}", ex.getMessage(), ex);
                    return new MongoCountResult(true, errorMsg, savedCount);
                }

                savedCount += frameTimestamps.size();
            }
        }

        return new MongoCountResult(false, "", savedCount);
    }

    /**
     * Filter resuming a keyset-paged sample status query strictly after the given sort position
     * in (pvName, domain, layer, firstTimeNanos) tuple order.
     */
    private static Bson sampleStatusResumeFilter(SampleStatusPageToken position) {
        return or(
                gt(BsonConstants.BSON_KEY_SAMPLE_STATUS_PV_NAME, position.pvName()),
                and(
                        eq(BsonConstants.BSON_KEY_SAMPLE_STATUS_PV_NAME, position.pvName()),
                        gt(BsonConstants.BSON_KEY_SAMPLE_STATUS_DOMAIN, position.domain())),
                and(
                        eq(BsonConstants.BSON_KEY_SAMPLE_STATUS_PV_NAME, position.pvName()),
                        eq(BsonConstants.BSON_KEY_SAMPLE_STATUS_DOMAIN, position.domain()),
                        gt(BsonConstants.BSON_KEY_SAMPLE_STATUS_LAYER, position.layer())),
                and(
                        eq(BsonConstants.BSON_KEY_SAMPLE_STATUS_PV_NAME, position.pvName()),
                        eq(BsonConstants.BSON_KEY_SAMPLE_STATUS_DOMAIN, position.domain()),
                        eq(BsonConstants.BSON_KEY_SAMPLE_STATUS_LAYER, position.layer()),
                        gt(BsonConstants.BSON_KEY_SAMPLE_STATUS_FIRST_TIME_NANOS, position.firstTimeNanos())));
    }

    @Override
    public SampleStatusQueryResult executeQuerySampleStatuses(
            QuerySampleStatusesRequest request,
            int limit,
            SampleStatusPageToken position
    ) {
        final long beginNanos =
                SampleStatusDocumentUtility.timestampNanos(request.getTimeRange().getBeginTime());
        final long endNanos =
                SampleStatusDocumentUtility.timestampNanos(request.getTimeRange().getEndTime());

        final List<Bson> filters = new ArrayList<>();
        // TimeRange overlap test: firstTime < endTime AND lastTime >= beginTime; boundary
        // documents are returned whole (not trimmed), matching queryBuckets
        filters.add(lt(BsonConstants.BSON_KEY_SAMPLE_STATUS_FIRST_TIME_NANOS, endNanos));
        filters.add(gte(BsonConstants.BSON_KEY_SAMPLE_STATUS_LAST_TIME_NANOS, beginNanos));
        // filter fields combine with AND across fields, OR within a field; empty list = match all
        if (!request.getPvNamesList().isEmpty()) {
            filters.add(in(BsonConstants.BSON_KEY_SAMPLE_STATUS_PV_NAME, request.getPvNamesList()));
        }
        if (!request.getDomainsList().isEmpty()) {
            filters.add(in(BsonConstants.BSON_KEY_SAMPLE_STATUS_DOMAIN, request.getDomainsList()));
        }
        if (!request.getLayersList().isEmpty()) {
            filters.add(in(BsonConstants.BSON_KEY_SAMPLE_STATUS_LAYER, request.getLayersList()));
        }
        if (position != null) {
            filters.add(sampleStatusResumeFilter(position));
        }

        // Fetch limit+1 to detect whether a next page exists without an extra count query.
        final List<SampleStatusBucketDocument> documents = new ArrayList<>();
        try {
            mongoCollectionSampleStatusBuckets.find(and(filters))
                    .sort(ascending(
                            BsonConstants.BSON_KEY_SAMPLE_STATUS_PV_NAME,
                            BsonConstants.BSON_KEY_SAMPLE_STATUS_DOMAIN,
                            BsonConstants.BSON_KEY_SAMPLE_STATUS_LAYER,
                            BsonConstants.BSON_KEY_SAMPLE_STATUS_FIRST_TIME_NANOS))
                    .limit(limit + 1)
                    .into(documents);
        } catch (MongoException ex) {
            logger.error("executeQuerySampleStatuses: mongo exception: {}", ex.getMessage(), ex);
            return null;
        }

        String nextPageToken = "";
        if (documents.size() > limit) {
            documents.remove(documents.size() - 1); // trim the extra probe document
            final SampleStatusBucketDocument lastDocument = documents.get(documents.size() - 1);
            nextPageToken = new SampleStatusPageToken(
                    lastDocument.getPvName(),
                    lastDocument.getDomain(),
                    lastDocument.getLayer(),
                    lastDocument.getFirstTimeNanos()).encode();
        }

        return new SampleStatusQueryResult(documents, nextPageToken);
    }

    /**
     * Deletion is exact at the sample axis: documents fully inside [beginTime, endTime) are
     * removed, boundary documents are trimmed or split via removeRange(). Trimmed survivors keep
     * their original provenance — deletion is not a save. The count accumulates individual
     * statuses removed, not documents.
     */
    @Override
    public MongoCountResult deleteSampleStatuses(DeleteSampleStatusesRequest request) {

        final long beginNanos =
                SampleStatusDocumentUtility.timestampNanos(request.getTimeRange().getBeginTime());
        final long endNanos =
                SampleStatusDocumentUtility.timestampNanos(request.getTimeRange().getEndTime());

        final List<Bson> filters = new ArrayList<>();
        filters.add(eq(BsonConstants.BSON_KEY_SAMPLE_STATUS_DOMAIN, request.getDomain()));
        filters.add(eq(BsonConstants.BSON_KEY_SAMPLE_STATUS_LAYER, request.getLayer()));
        // empty pvNames is a deliberate wildcard deleting the (domain, layer)'s statuses for all PVs
        if (!request.getPvNamesList().isEmpty()) {
            filters.add(in(BsonConstants.BSON_KEY_SAMPLE_STATUS_PV_NAME, request.getPvNamesList()));
        }
        filters.add(lt(BsonConstants.BSON_KEY_SAMPLE_STATUS_FIRST_TIME_NANOS, endNanos));
        filters.add(gte(BsonConstants.BSON_KEY_SAMPLE_STATUS_LAST_TIME_NANOS, beginNanos));

        long deletedCount = 0;

        // Iterate with a cursor rather than materializing the matched set: a wildcard delete
        // (layer retirement) can match many documents. Replacement documents inserted during
        // iteration never re-match the filter (a prefix run ends before beginTime, a suffix run
        // starts at or after endTime), so the loop cannot observe its own writes.
        try (MongoCursor<SampleStatusBucketDocument> cursor =
                     mongoCollectionSampleStatusBuckets.find(and(filters)).iterator()) {
            while (cursor.hasNext()) {
                final SampleStatusBucketDocument document = cursor.next();
                final SampleStatusDocumentUtility.RemovalResult removal =
                        SampleStatusDocumentUtility.removeRange(document, beginNanos, endNanos);
                if (removal.removedCount() == 0) {
                    // span overlaps the range but no individual sample falls inside it
                    continue;
                }
                mongoCollectionSampleStatusBuckets.deleteOne(
                        eq(BsonConstants.BSON_KEY_SAMPLE_STATUS_ID, document.getId()));
                for (SampleStatusBucketDocument replacement : removal.replacementDocuments()) {
                    mongoCollectionSampleStatusBuckets.insertOne(replacement);
                }
                deletedCount += removal.removedCount();
            }
        } catch (MongoException | DpException ex) {
            final String errorMsg = "error deleting sample status documents: " + ex.getMessage();
            logger.error("deleteSampleStatuses error: {}", ex.getMessage(), ex);
            return new MongoCountResult(true, errorMsg, deletedCount);
        }

        return new MongoCountResult(false, "", deletedCount);
    }

}
