package com.ospreydcs.dp.service.annotation.handler.mongo.dispatch;

import com.ospreydcs.dp.service.annotation.handler.model.ExportConfiguration;
import com.ospreydcs.dp.service.annotation.handler.model.HandlerExportDataSetRequest;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.handler.Dispatcher;

public class ExportDataSetDispatcher extends Dispatcher {

    private final HandlerExportDataSetRequest handlerRequest;
    private final MongoAnnotationClientInterface mongoClient;

    public ExportDataSetDispatcher(
            HandlerExportDataSetRequest handlerRequest,
            MongoAnnotationClientInterface mongoClient
    ) {
        super();
        this.handlerRequest = handlerRequest;
        this.mongoClient = mongoClient;
    }

    public void handleError(String errorMsg) {
        AnnotationServiceImpl.sendExportDataSetResponseError(errorMsg, handlerRequest.responseObserver);
    }

    public void handleResult(ExportConfiguration.ExportFilePaths exportFilePaths) {
        AnnotationServiceImpl.sendExportDataSetResponseSuccess(
                exportFilePaths.shareFilePath,
                exportFilePaths.fileUrl,
                handlerRequest.responseObserver);
    }
}
