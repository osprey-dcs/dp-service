package com.ospreydcs.dp.service.ingest.handler.mongo.job;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataResponse;
import com.ospreydcs.dp.service.common.bson.PvMetadataQueryResultDocument;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.ingest.handler.mongo.SourceMonitorSubscriptionManager;
import com.ospreydcs.dp.service.ingest.handler.mongo.client.MongoIngestionClientInterface;
import com.ospreydcs.dp.service.ingest.handler.mongo.dispatch.SubscribeDataDispatcher;
import com.ospreydcs.dp.service.ingest.model.SourceMonitor;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.Set;

public class SubscribeDataJob extends HandlerJob {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final SubscribeDataRequest request;
    private final StreamObserver<SubscribeDataResponse> responseObserver;
    private final SourceMonitor monitor;
    private final SourceMonitorSubscriptionManager subscriptionManager;
    private final MongoIngestionClientInterface mongoIngestionClient;
    private final MongoQueryClientInterface mongoQueryClient;
    private final SubscribeDataDispatcher dispatcher;

    public SubscribeDataJob(
            SubscribeDataRequest request,
            StreamObserver<SubscribeDataResponse> responseObserver,
            SourceMonitor monitor,
            SourceMonitorSubscriptionManager subscriptionManager,
            MongoIngestionClientInterface mongoIngestionClient,
            MongoQueryClientInterface mongoQueryClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.monitor = monitor;
        this.subscriptionManager = subscriptionManager;
        this.mongoIngestionClient = mongoIngestionClient;
        this.mongoQueryClient = mongoQueryClient;
        this.dispatcher = new SubscribeDataDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {
        
        logger.debug("executing SubscribeDataJob id: {}", this.responseObserver.hashCode());
        
        // validate that request PVs exist in archive
        final Set<String> uniquePvNames = new HashSet<>(request.getNewSubscription().getPvNamesList());
        final MongoCursor<PvMetadataQueryResultDocument> pvMetadata = mongoQueryClient.executeQueryPvMetadata(uniquePvNames);

        // check for error executing mongo query
        if (pvMetadata == null ) {
            dispatcher.sendError(
                    "database error looking up metadata for PV names: " + uniquePvNames.toString());
            return;
        }
        
        // check that metadata is returned for each pv (try to remove each metadata from the set,
        // and make sure set ends up empty)
        while (pvMetadata.hasNext()) {
            final PvMetadataQueryResultDocument pvMetadataDocument = pvMetadata.next();
            final String pvName = pvMetadataDocument.getPvName();
            if (pvName != null) {
                uniquePvNames.remove(pvName);
            }
        }

        // we should have removed all the pv names from the set of unique names, e.g., we received metadata for each
        if (! uniquePvNames.isEmpty()) {
            dispatcher.sendReject("PV names not found in archive: " + uniquePvNames.toString());
            return;
        }

        // add SourceMonitor to subscriptionManager
        this.subscriptionManager.addSubscription(monitor);

        // send an ack message in the response stream
        dispatcher.sendAck();
    }
}
