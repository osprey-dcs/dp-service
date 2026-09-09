package com.ospreydcs.dp.service.annotation.handler.mongo.dispatch;

import com.ospreydcs.dp.service.annotation.handler.model.ExportConfiguration;
import com.ospreydcs.dp.service.annotation.handler.model.HandlerExportDataRequest;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.handler.Dispatcher;

public class ExportDataDispatcher extends Dispatcher {

    private final HandlerExportDataRequest handlerRequest;
    private final MongoAnnotationClientInterface mongoClient;

    public ExportDataDispatcher(
            HandlerExportDataRequest handlerRequest,
            MongoAnnotationClientInterface mongoClient
    ) {
        super();
        this.handlerRequest = handlerRequest;
        this.mongoClient = mongoClient;
    }

    /**
     * A client mistake — a referenced dataset or calculations id that matches no record, or a
     * filter naming a frame/column the calculations object does not contain — is a rejection,
     * not a service error (#235 classification, applied to export by #248 plan D30). Retrying
     * the identical request is pointless and the condition is correctable by the caller.
     */
    public void handleReject(String msg) {
        AnnotationServiceImpl.sendExportDataResponseReject(msg, handlerRequest.responseObserver);
    }

    public void handleError(String errorMsg) {
        AnnotationServiceImpl.sendExportDataResponseError(errorMsg, handlerRequest.responseObserver);
    }

    public void handleResult(ExportConfiguration.ExportFilePaths exportFilePaths) {
        AnnotationServiceImpl.sendExportDataResponseSuccess(
                exportFilePaths.shareFilePath,
                exportFilePaths.fileUrl,
                handlerRequest.responseObserver);
    }
}
