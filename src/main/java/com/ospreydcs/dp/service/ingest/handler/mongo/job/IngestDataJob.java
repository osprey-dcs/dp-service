package com.ospreydcs.dp.service.ingest.handler.mongo.job;

import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.telemetry.DpMetrics;
import com.ospreydcs.dp.service.ingest.handler.model.HandlerIngestionRequest;
import com.ospreydcs.dp.service.ingest.handler.model.HandlerIngestionResult;
import com.ospreydcs.dp.service.ingest.handler.mongo.client.MongoIngestionClientInterface;
import com.ospreydcs.dp.service.ingest.handler.mongo.MongoIngestionHandler;
import com.ospreydcs.dp.service.ingest.model.IngestionRequestStatus;
import com.ospreydcs.dp.service.ingest.model.IngestionTaskResult;
import com.ospreydcs.dp.service.ingest.service.IngestionServiceImpl;
import io.opentelemetry.api.common.Attributes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

/**
 * This class is created to service an IngestDataRequest received by one of the data ingestion API methods. The
 * execute() method is dispatched to handleIngestionRequest(), which does the core work of the Ingestion Service.
 */
public class IngestDataJob extends HandlerJob {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final HandlerIngestionRequest request;
    private final MongoIngestionClientInterface mongoClient;
    private final MongoIngestionHandler handler;

    public IngestDataJob(
            HandlerIngestionRequest request,
            MongoIngestionClientInterface mongoClient,
            MongoIngestionHandler handler
    ) {
        this.request = request;
        this.mongoClient = mongoClient;
        this.handler = handler;
    }

    @Override
    public void execute() {
        this.handleIngestionRequest(request);
    }

    /**
     * Handles an IngestDataRequest received by one of the data ingestion API methods.  Checks that specified providerId
     * is valid by database lookup. Generates a batch of BSON BucketDocuments, one for each data column in the request.
     * Inserts the batch of documents to MongoDB, and verifies handling. Inserts a RequestStatusDocument in MongoDB for
     * checking the status of the request asynchronously.  Publishes data columns for subscribed PVs.
     *
     * @param handlerIngestionRequest
     * @return
     */
    public HandlerIngestionResult handleIngestionRequest(HandlerIngestionRequest handlerIngestionRequest) {

        // Telemetry (issue #212, section 2) is recorded in a finally rather than beside the return,
        // because an exception escaping this method is caught and dropped by the QueueHandlerBase
        // worker. Recorded on the return alone, the request the operator most needs to see -- one
        // that failed hard enough to skip its own requestStatus insert -- would be the one request
        // that appeared in no counter at all, and dp.ingest.requests would report a lower rate
        // rather than a higher error rate.
        final IngestionTelemetry telemetry = new IngestionTelemetry(handlerIngestionRequest);
        try {
            return handleIngestionRequest_(handlerIngestionRequest, telemetry);
        } finally {
            telemetry.record();
        }
    }

    private HandlerIngestionResult handleIngestionRequest_(
            HandlerIngestionRequest handlerIngestionRequest,
            IngestionTelemetry telemetry
    ) {
        final IngestDataRequest request = handlerIngestionRequest.request;
        logger.debug("id: {} handling ingestion request providerId: {} requestId: {}",
                this.hashCode(), request.getProviderId(), request.getClientRequestId());

        IngestionRequestStatus status = IngestionRequestStatus.SUCCESS;
        boolean isError = false;
        String errorMsg = "";
        List<String> idsCreated = new ArrayList<>();

        // validate providerId by getting providerName
        String providerName = mongoClient.providerNameForId(request.getProviderId());

        if (handlerIngestionRequest.rejected) {
            // request already rejected, but we want to add details in request status
            isError = true;
            errorMsg = handlerIngestionRequest.rejectMsg;
            status = IngestionRequestStatus.REJECTED;

        } else {

            // flag error for invalid providerId
            if (providerName == null) {
                isError = true;
                errorMsg = "invalid providerId: " + request.getProviderId();
                logger.error(errorMsg);

            } else {

                // generate batch of bucket documents for request
                List<BucketDocument> dataDocumentBatch = null;
                try {
                    dataDocumentBatch = BucketDocument.generateBucketsFromRequest(request, providerName);
                } catch (DpException e) {
                    isError = true;
                    errorMsg = e.getMessage();
                    status = IngestionRequestStatus.ERROR;
                }

                if (dataDocumentBatch != null) {
                    // add the batch to mongo and handle result
                    IngestionTaskResult ingestionTaskResult =
                            mongoClient.insertBatch(request, dataDocumentBatch);

                    if (ingestionTaskResult.isError) {
                        isError = true;
                        errorMsg = ingestionTaskResult.msg;
                        logger.error(errorMsg);

                    } else {

                        InsertManyResult insertManyResult = ingestionTaskResult.insertManyResult;

                        if (!insertManyResult.wasAcknowledged()) {
                            // check mongo insertMany result was acknowledged
                            isError = true;
                            errorMsg = "insertMany result not acknowledged";
                            logger.error(errorMsg);

                        } else {

                            long recordsInsertedCount = insertManyResult.getInsertedIds().size();
                            long recordsExpected = dataDocumentBatch.size();

                            if (recordsInsertedCount != recordsExpected) {
                                // check records inserted matches expected
                                isError = true;
                                errorMsg = "insertMany actual records inserted: "
                                        + recordsInsertedCount + " mismatch expected: " + recordsExpected;
                                logger.error(errorMsg);

                            } else {
                                // get list of ids created
                                for (var entry : insertManyResult.getInsertedIds().entrySet()) {
                                    idsCreated.add(entry.getValue().asString().getValue());
                                }
                                telemetry.recordBucketsInserted(recordsInsertedCount);
                            }
                        }
                    }
                }
            }

            if (isError) {
                status = IngestionRequestStatus.ERROR;

            }
        }
        
        telemetry.setStatus(status);

        // save request status and check result of insert operation
        if (providerName == null) {
            providerName = "";
        }
        RequestStatusDocument statusDocument = new RequestStatusDocument(
                request.getProviderId(),
                providerName,
                request.getClientRequestId(),
                status,
                errorMsg,
                idsCreated);
        InsertOneResult insertRequestStatusResult = mongoClient.insertRequestStatus(statusDocument);
        if (insertRequestStatusResult == null) {
            logger.error("error inserting request status");
        } else {
            if (!insertRequestStatusResult.wasAcknowledged()) {
                logger.error("insertOne not acknowledged inserting request status");
            } else {
                logger.debug("inserted request status id:" + insertRequestStatusResult.getInsertedId());
            }
        }

        // publish request PV data to subscribeData() subscribers
        if (! isError) {
            handler.getSourceMonitorPublisher().publishDataSubscriptions(request, providerName);
        }

        return new HandlerIngestionResult(isError, errorMsg);
    }

    /**
     * Accumulates the telemetry for one handled ingestion request and records it once (issue #212).
     *
     * <p>Kept alongside the job rather than in a shared class because ingestion has exactly one
     * job type; the query service's equivalent ({@code QueryTelemetry}) is separate only because
     * its stages are threaded through three job types and nine dispatchers.
     *
     * <p>Per the D8 cardinality policy the only attribute attached is {@code dp.outcome}. In
     * particular the provider id and the client request id are <em>not</em> attributes: a facility
     * generates an unbounded number of client request ids, and one per time series is how a
     * metrics backend is taken down by the service it monitors. Both are already recorded per
     * request in the {@code requestStatus} collection, which is where a specific request is
     * looked up.
     */
    private static final class IngestionTelemetry {

        private final long arrivalNanos;
        private final long requestBytes;
        private final long sampleCount;

        /**
         * Defaults to {@code error} so that an exception escaping the job -- which never reaches
         * {@code setStatus} -- is counted as the failure it is rather than as a success.
         */
        private String outcome = DpMetrics.OUTCOME_ERROR;
        private long bucketCount = 0;

        IngestionTelemetry(HandlerIngestionRequest handlerIngestionRequest) {
            final IngestDataRequest request = handlerIngestionRequest.request;
            this.arrivalNanos = handlerIngestionRequest.arrivalNanos;
            this.requestBytes = request.getSerializedSize();
            this.sampleCount = (long) IngestionServiceImpl.getNumRequestRows(request)
                    * IngestionServiceImpl.getNumRequestColumns(request);
        }

        /**
         * Records the buckets actually acknowledged by the database, not the size of the generated
         * batch: the two differ exactly when the insert partially failed, and that is the case an
         * operator is trying to see.
         */
        void recordBucketsInserted(long count) {
            this.bucketCount = count;
        }

        void setStatus(IngestionRequestStatus status) {
            this.outcome = switch (status) {
                case SUCCESS -> DpMetrics.OUTCOME_SUCCESS;
                case REJECTED -> DpMetrics.OUTCOME_REJECT;
                case ERROR -> DpMetrics.OUTCOME_ERROR;
            };
        }

        void record() {
            try {
                final long totalNanos = System.nanoTime() - arrivalNanos;
                final Attributes outcomeAttributes = Attributes.of(DpMetrics.ATTR_OUTCOME, outcome);

                DpMetrics.ingestRequests().add(1, outcomeAttributes);
                DpMetrics.ingestDuration()
                        .record(DpMetrics.nanosToSeconds(totalNanos), outcomeAttributes);

                // Counted on every outcome, not just success. A rejected or failed request
                // contributes a zero bucket count but real bytes and samples, so the ratio of
                // dp.ingest.buckets to dp.ingest.samples stays readable as "what fraction of the
                // offered load was actually stored" rather than silently excluding the load that
                // was not.
                DpMetrics.ingestBuckets().add(bucketCount);
                DpMetrics.ingestSamples().add(sampleCount);
                DpMetrics.ingestRequestBytes().add(requestBytes);

            } catch (RuntimeException ex) {
                // This runs in a finally on the job's return path. A failure here must never
                // replace the job's own outcome, or a metrics problem would present as an
                // ingestion problem.
                logger.error("error recording ingestion telemetry: {}", ex.getMessage(), ex);
            }
        }
    }

}
