package com.ospreydcs.dp.service.ingest.handler.mongo.job;

import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.bucket.EventMetadataDocument;
import com.ospreydcs.dp.service.common.grpc.DataTimestampsUtility;
import com.ospreydcs.dp.service.common.grpc.TimestampUtility;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.ingest.handler.model.HandlerIngestionRequest;
import com.ospreydcs.dp.service.ingest.handler.model.HandlerIngestionResult;
import com.ospreydcs.dp.service.ingest.handler.mongo.client.MongoIngestionClientInterface;
import com.ospreydcs.dp.service.ingest.handler.mongo.MongoIngestionHandler;
import com.ospreydcs.dp.service.ingest.model.DpIngestionException;
import com.ospreydcs.dp.service.ingest.model.IngestionRequestStatus;
import com.ospreydcs.dp.service.ingest.model.IngestionTaskResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

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

    public HandlerIngestionResult handleIngestionRequest(HandlerIngestionRequest handlerIngestionRequest) {

        final IngestDataRequest request = handlerIngestionRequest.request;
        logger.debug("id: {} handling ingestion request providerId: {} requestId: {}",
                this.hashCode(), request.getProviderId(), request.getClientRequestId());

        IngestionRequestStatus status = IngestionRequestStatus.SUCCESS;
        boolean isError = false;
        String errorMsg = "";
        List<String> idsCreated = new ArrayList<>();

        if (handlerIngestionRequest.rejected) {
            // request already rejected, but we want to add details in request status
            isError = true;
            errorMsg = handlerIngestionRequest.rejectMsg;
            status = IngestionRequestStatus.REJECTED;

        } else {

            // generate batch of bucket documents for request
            List<BucketDocument> dataDocumentBatch = null;
            try {
                dataDocumentBatch = generateBucketsFromRequest(request);
            } catch (DpIngestionException e) {
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
                        long recordsExpected = request.getIngestionDataFrame().getDataColumnsList().size();
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
                        }
                    }
                }

                if (isError) {
                    status = IngestionRequestStatus.ERROR;
                }
            }
        }

        // save request status and check result of insert operation
        RequestStatusDocument statusDocument = new RequestStatusDocument(
                request.getProviderId(),
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
                logger.trace("inserted request status id:" + insertRequestStatusResult.getInsertedId());
            }
        }

        return new HandlerIngestionResult(isError, errorMsg);
    }

    /**
     * Generates a list of POJO objects, which are written as a batch to mongodb by customizing the codec registry.
     *
     * NOTE: DATABASE CODE LIKE insertMany SILENTLY FAILS IF AN INSTANCE VARIABLE IS ADDED TO TsDataBucket
     * WITHOUT ACCESSOR METHODS!!!  Very hard to troubleshoot.
     *
     * @param request
     * @return
     */
    protected static List<BucketDocument> generateBucketsFromRequest(IngestDataRequest request)
            throws DpIngestionException {

        final List<BucketDocument> bucketList = new ArrayList<>();

        // get timestamp details
        DataTimestampsUtility.DataTimestampsModel timeSpecModel = new DataTimestampsUtility.DataTimestampsModel(request.getIngestionDataFrame().getDataTimestamps());
        final Timestamp firstTimestamp = timeSpecModel.getFirstTimestamp();
        final long firstTimestampSeconds = firstTimestamp.getEpochSeconds();
        final long firstTimestampNanos = firstTimestamp.getNanoseconds();
        final Date firstTimestampDate = TimestampUtility.dateFromTimestamp(firstTimestamp);
        final Timestamp lastTimestamp = timeSpecModel.getLastTimestamp();
        final long lastTimestampSeconds = lastTimestamp.getEpochSeconds();
        final long lastTimestampNanos = lastTimestamp.getNanoseconds();
        final Date lastTimestampDate = TimestampUtility.dateFromTimestamp(lastTimestamp);

        // create BSON document for each column
        final List<DataColumn> columns = request.getIngestionDataFrame().getDataColumnsList();
        for (DataColumn column : columns) {
            final String pvName = column.getName();
            final String documentId = pvName + "-" + firstTimestampSeconds + "-" + firstTimestampNanos;

            BucketDocument bucket = new BucketDocument();

            bucket.setId(documentId);
            bucket.setPvName(pvName);

            bucket.writeDataColumnContent(column);
            final DataValue.ValueCase dataValueCase = column.getDataValues(0).getValueCase();
            bucket.setDataTypeCase(dataValueCase.getNumber());
            bucket.setDataType(dataValueCase.name());

            final DataTimestamps requestDataTimestamps = request.getIngestionDataFrame().getDataTimestamps();
            bucket.writeDataTimestampsContent(requestDataTimestamps);
            final DataTimestamps.ValueCase dataTimestampsCase = requestDataTimestamps.getValueCase();
            bucket.setDataTimestampsCase(dataTimestampsCase.getNumber());
            bucket.setDataTimestampsType(dataTimestampsCase.name());

            bucket.setFirstTime(firstTimestampDate);
            bucket.setFirstSeconds(firstTimestampSeconds);
            bucket.setFirstNanos(firstTimestampNanos);
            bucket.setLastTime(lastTimestampDate);
            bucket.setLastSeconds(lastTimestampSeconds);
            bucket.setLastNanos(lastTimestampNanos);
            bucket.setSamplePeriod(timeSpecModel.getSamplePeriodNanos());
            bucket.setSampleCount(timeSpecModel.getSampleCount());

            // add metadata
            Map<String, String> attributeMap = new TreeMap<>();
            String eventDescription = "";
            long eventStartSeconds = 0;
            long eventStartNanos = 0;
            long eventStopSeconds = 0;
            long eventStopNanos = 0;
            for (Attribute attribute : request.getAttributesList()) {
                attributeMap.put(attribute.getName(), attribute.getValue());
            }
            if (request.hasEventMetadata()) {
                if (request.getEventMetadata().getDescription() != null) {
                    eventDescription = request.getEventMetadata().getDescription();
                }
                if (request.getEventMetadata().hasStartTimestamp()) {
                    eventStartSeconds = request.getEventMetadata().getStartTimestamp().getEpochSeconds();
                    eventStartNanos = request.getEventMetadata().getStartTimestamp().getNanoseconds();
                }
                if (request.getEventMetadata().hasStopTimestamp()) {
                    eventStopSeconds = request.getEventMetadata().getStopTimestamp().getEpochSeconds();
                    eventStopNanos = request.getEventMetadata().getStopTimestamp().getNanoseconds();
                }
            }
            bucket.setAttributeMap(attributeMap);
            EventMetadataDocument eventMetadataDocument = new EventMetadataDocument();
            eventMetadataDocument.setDescription(eventDescription);
            eventMetadataDocument.setStartSeconds(eventStartSeconds);
            eventMetadataDocument.setStartNanos(eventStartNanos);
            eventMetadataDocument.setStopSeconds(eventStopSeconds);
            eventMetadataDocument.setStopNanos(eventStopNanos);
            bucket.setEventMetadata(eventMetadataDocument);
            bucket.setProviderId(request.getProviderId());
            bucket.setClientRequestId(request.getClientRequestId());

            bucketList.add(bucket);
        }

        return bucketList;
    }

 }
