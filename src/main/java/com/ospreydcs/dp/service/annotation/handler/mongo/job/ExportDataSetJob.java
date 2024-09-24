package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.service.annotation.handler.model.HandlerExportDataSetRequest;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.ExportDataSetDispatcher;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ExportDataSetJob extends HandlerJob {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final HandlerExportDataSetRequest handlerRequest;
    private final ExportDataSetDispatcher dispatcher;
    private final MongoAnnotationClientInterface mongoClient;


    public ExportDataSetJob(HandlerExportDataSetRequest handlerRequest, MongoAnnotationClientInterface mongoClient) {
        this.handlerRequest = handlerRequest;
        this.mongoClient = mongoClient;
        this.dispatcher = new ExportDataSetDispatcher(handlerRequest, mongoClient);
    }

    @Override
    public void execute() {

//        logger.debug("executing QueryAnnotationsJob id: {}", this.responseObserver.hashCode());
//        final MongoCursor<AnnotationDocument> cursor = this.mongoClient.executeQueryAnnotations(this.request);
//
//        logger.debug("dispatching QueryAnnotationsJob id: {}", this.responseObserver.hashCode());
//        dispatcher.handleResult(cursor);
    }

}
