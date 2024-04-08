package com.ospreydcs.dp.service.query.handler.mongo;

import com.ospreydcs.dp.grpc.v1.query.*;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.grpc.GrpcUtility;
import com.ospreydcs.dp.service.common.handler.QueueHandlerBase;
import com.ospreydcs.dp.service.common.model.ValidationResult;
import com.ospreydcs.dp.service.query.handler.QueryHandlerUtility;
import com.ospreydcs.dp.service.query.handler.interfaces.QueryHandlerInterface;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoSyncQueryClient;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.*;
import com.ospreydcs.dp.service.query.handler.mongo.job.QueryMetadataJob;
import com.ospreydcs.dp.service.query.handler.mongo.job.QueryDataJob;
import com.ospreydcs.dp.service.query.handler.mongo.job.QueryTableJob;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MongoQueryHandler extends QueueHandlerBase implements QueryHandlerInterface {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // configuration
    public static final String CFG_KEY_NUM_WORKERS = "QueryHandler.numWorkers";
    public static final int DEFAULT_NUM_WORKERS = 7;

    // instance variables
    private final MongoQueryClientInterface mongoQueryClient;

    public MongoQueryHandler(MongoQueryClientInterface clientInterface) {
        this.mongoQueryClient = clientInterface;
    }

    public static MongoQueryHandler newMongoSyncQueryHandler() {
        return new MongoQueryHandler(new MongoSyncQueryClient());
    }

    protected int getNumWorkers_() {
        return configMgr().getConfigInteger(CFG_KEY_NUM_WORKERS, DEFAULT_NUM_WORKERS);
    }

    @Override
    protected boolean init_() {
        logger.trace("init_");
        if (!mongoQueryClient.init()) {
            logger.error("error in mongoQueryClient.init()");
            return false;
        }
        return true;
    }

    @Override
    protected boolean fini_() {
        if (!mongoQueryClient.fini()) {
            logger.error("error in mongoQueryClient.fini()");
        }
        return true;
    }

    @Override
    public ValidationResult validateQuerySpecData(QueryDataRequest.QuerySpec querySpec) {
        return QueryHandlerUtility.validateQuerySpecData(querySpec);
    }

    public static DataTimestamps dataTimestampsForBucket(BucketDocument document) {

        final DataTimestamps.Builder dataTimestampsBuilder = DataTimestamps.newBuilder();

        // TODO: determine whether to use explicit timestamp list if BucketDocument contains non-empty list,
        // otherwise use SamplingClock as currently implemented...

        final Timestamp startTime = GrpcUtility.timestampFromSeconds(document.getFirstSeconds(), document.getFirstNanos());
        final SamplingClock.Builder samplingClockBuilder = SamplingClock.newBuilder();
        samplingClockBuilder.setStartTime(startTime);
        samplingClockBuilder.setPeriodNanos(document.getSampleFrequency());
        samplingClockBuilder.setCount(document.getNumSamples());
        samplingClockBuilder.build();

        dataTimestampsBuilder.setSamplingClock(samplingClockBuilder);
        return dataTimestampsBuilder.build();
    }

    public static <T> QueryDataResponse.QueryData.DataBucket dataBucketFromDocument(
            BucketDocument<T> document
    ) {
        final QueryDataResponse.QueryData.DataBucket.Builder bucketBuilder =
                QueryDataResponse.QueryData.DataBucket.newBuilder();

        // add data timestamps
        bucketBuilder.setDataTimestamps(dataTimestampsForBucket(document));

        // add data values
        final DataColumn.Builder columnBuilder = DataColumn.newBuilder();
        columnBuilder.setName(document.getColumnName());
//        addBucketDataToColumn(document, columnBuilder);
        for (T dataValue: document.getColumnDataList()) {
            final DataValue.Builder valueBuilder = DataValue.newBuilder();
            document.addColumnDataValue(dataValue, valueBuilder);
            valueBuilder.build();
            columnBuilder.addDataValues(valueBuilder);
        }
        columnBuilder.build();
        bucketBuilder.setDataColumn(columnBuilder);

        // add attributes
        if (document.getAttributeMap() != null) {
            for (var documentAttributeMapEntry : document.getAttributeMap().entrySet()) {
                final String documentAttributeKey = documentAttributeMapEntry.getKey();
                final String documentAttributeValue = documentAttributeMapEntry.getValue();
                final Attribute responseAttribute = Attribute.newBuilder()
                        .setName(documentAttributeKey)
                        .setValue(documentAttributeValue)
                        .build();
                bucketBuilder.addAttributes(responseAttribute);
            }
        }

        // add event metadata
        if (
                (document.getEventDescription() != null && ! document.getEventDescription().isBlank())
                        || (document.getEventSeconds() > 0)
                        || (document.getEventNanos() > 0)
        ) {
            Timestamp responseEventStartTimestamp = Timestamp.newBuilder()
                    .setEpochSeconds(document.getEventSeconds())
                    .setNanoseconds(document.getEventNanos())
                    .build();
            EventMetadata responseEventMetadata = EventMetadata.newBuilder()
                    .setDescription(document.getEventDescription())
                    .setStartTimestamp(responseEventStartTimestamp)
                    .build();
            bucketBuilder.setEventMetadata(responseEventMetadata);
        }

        return bucketBuilder.build();
    }

    @Override
    public void handleQueryDataStream(
            QueryDataRequest.QuerySpec querySpec, StreamObserver<QueryDataResponse> responseObserver) {

        final DataResponseStreamDispatcher dispatcher = new DataResponseStreamDispatcher(responseObserver);
        final QueryDataJob job = new QueryDataJob(querySpec, dispatcher, responseObserver, mongoQueryClient);

        logger.debug("adding queryResponseStream job id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public QueryResultCursor handleQueryDataBidiStream(
            QueryDataRequest.QuerySpec querySpec, StreamObserver<QueryDataResponse> responseObserver) {


        final DataResponseBidiStreamDispatcher dispatcher = new DataResponseBidiStreamDispatcher(responseObserver);
        final QueryDataJob job = new QueryDataJob(querySpec, dispatcher, responseObserver, mongoQueryClient);
        final QueryResultCursor resultCursor = new QueryResultCursor(this, dispatcher);

        logger.debug("adding queryResponseCursor job id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }

        return resultCursor;
    }

    @Override
    public void handleQueryData(
            QueryDataRequest.QuerySpec querySpec, StreamObserver<QueryDataResponse> responseObserver) {

        final DataResponseUnaryDispatcher dispatcher = new DataResponseUnaryDispatcher(responseObserver);
        final QueryDataJob job = new QueryDataJob(querySpec, dispatcher, responseObserver, mongoQueryClient);

        logger.debug("adding queryResponseSingle job id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleQueryDataTable(
            QueryDataRequest.QuerySpec querySpec, StreamObserver<QueryTableResponse> responseObserver) {

        final QueryTableJob job = new QueryTableJob(querySpec, responseObserver, mongoQueryClient);

        logger.debug("adding queryResponseTable job id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void handleQueryMetadata(
            QueryMetadataRequest request, StreamObserver<QueryMetadataResponse> responseObserver
    ) {
        final QueryMetadataJob job =
                new QueryMetadataJob(request, responseObserver, mongoQueryClient);

        logger.debug("adding queryMetadata job id: {} to queue", responseObserver.hashCode());

        try {
            requestQueue.put(job);
        } catch (InterruptedException e) {
            logger.error("InterruptedException waiting for requestQueue.put");
            Thread.currentThread().interrupt();
        }
    }

}
