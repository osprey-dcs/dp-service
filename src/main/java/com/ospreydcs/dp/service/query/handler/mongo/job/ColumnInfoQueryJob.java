package com.ospreydcs.dp.service.query.handler.mongo.job;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.query.QueryRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryResponse;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.ColumnInfoDispatcher;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

public class ColumnInfoQueryJob extends HandlerJob {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryRequest.ColumnInfoQuerySpec columnInfoQuerySpec;
    private final StreamObserver<QueryResponse> responseObserver;
    private final ColumnInfoDispatcher dispatcher;
    private final MongoQueryClientInterface mongoClient;

    public ColumnInfoQueryJob(
            QueryRequest.ColumnInfoQuerySpec columnInfoQuerySpec,
            StreamObserver<QueryResponse> responseObserver,
            MongoQueryClientInterface mongoClient
    ) {
        this.columnInfoQuerySpec = columnInfoQuerySpec;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        dispatcher = new ColumnInfoDispatcher(responseObserver, columnInfoQuerySpec);
    }

    @Override
    public void execute() {
        logger.debug("executing ColumnInfoQueryJob id: {}", this.responseObserver.hashCode());
        final MongoCursor<Document> cursor = this.mongoClient.getColumnInfo(this.columnInfoQuerySpec);
        logger.debug("dispatching ColumnInfoQueryJob id: {}", this.responseObserver.hashCode());
        dispatcher.handleResult(cursor);
    }
}
