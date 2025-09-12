package com.ospreydcs.dp.service.query.handler.mongo.job;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.query.ProviderMetadata;
import com.ospreydcs.dp.grpc.v1.query.QueryProvidersRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryProvidersResponse;
import com.ospreydcs.dp.service.common.bson.ProviderDocument;
import com.ospreydcs.dp.service.common.bson.ProviderMetadataQueryResultDocument;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.QueryProviderMetadataDispatcher;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.QueryProvidersDispatcher;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class QueryProvidersJob extends HandlerJob {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryProvidersRequest request;
    private final StreamObserver<QueryProvidersResponse> responseObserver;
    private final QueryProvidersDispatcher dispatcher;
    private final MongoQueryClientInterface mongoClient;

    public QueryProvidersJob(
            QueryProvidersRequest request,
            StreamObserver<QueryProvidersResponse> responseObserver,
            MongoQueryClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        dispatcher = new QueryProvidersDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {

        // execute providers query
        logger.debug("executing QueryProvidersJob id: {}", this.responseObserver.hashCode());
        final MongoCursor<ProviderDocument> cursor =
                this.mongoClient.executeQueryProviders(this.request);

        // check query cursor
        // validate cursor
        if (cursor == null) {
            // send error response and close response stream if cursor is null
            final String msg = "providers query returned null cursor";
            logger.error(msg);
            QueryServiceImpl.sendQueryProvidersResponseError(msg, this.responseObserver);
            return;
        }

        // iterate through query result, creating ProviderInfo with embedded ProviderMetadata for each result document
        List<QueryProvidersResponse.ProvidersResult.ProviderInfo> providerInfos = new ArrayList<>();
        while (cursor.hasNext()) {
            final ProviderDocument providerDocument = cursor.next();
            final String responseProviderId = providerDocument.getId().toString();

            // execute provider metadta query for provider
            final MongoCursor<ProviderMetadataQueryResultDocument> metadataCursor =
                    this.mongoClient.executeQueryProviderMetadata(responseProviderId);

            // check metadata query result cursor
            if (metadataCursor == null) {
                // send error response and close response stream if cursor is null
                final String msg =
                        "provider metadata query returned null cursor for id: " + responseProviderId;
                logger.error(msg);
                QueryServiceImpl.sendQueryProvidersResponseError(msg, this.responseObserver);
                return;
            }

            ProviderMetadata providerMetadata = null;
            if (metadataCursor.hasNext()) {
                // get ProviderMetadtaa for metadta document from result cursor
                final ProviderMetadataQueryResultDocument metadataDocument = metadataCursor.next();
                providerMetadata =
                        QueryProviderMetadataDispatcher.providerMetadataFromDocument(metadataDocument);

            } else {
                // no provider metadata found for specified id
                final String msg =
                        "no metadata found for provider id: " + responseProviderId;
                logger.error(msg);
            }

            // create ProviderInfo for each result document
            final QueryProvidersResponse.ProvidersResult.ProviderInfo responseProviderInfo =
                    providerDocument.toProviderInfo(providerMetadata);
            providerInfos.add(responseProviderInfo);
        }

        // dispatch list of ProviderInfos in response stream
        logger.debug("dispatching QueryProvidersJob id: {}", this.responseObserver.hashCode());
        dispatcher.handleResult(providerInfos);
    }
}
