package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.annotation.GetAnnotationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.GetAnnotationResponse;
import com.ospreydcs.dp.service.annotation.handler.AnnotationValidationUtility;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.GetAnnotationDispatcher;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;

public class GetAnnotationJob extends HandlerJob {

    private static final Logger logger = LogManager.getLogger();

    private final GetAnnotationRequest request;
    private final StreamObserver<GetAnnotationResponse> responseObserver;
    private final MongoAnnotationClientInterface mongoClient;
    private final GetAnnotationDispatcher dispatcher;

    public GetAnnotationJob(
            GetAnnotationRequest request,
            StreamObserver<GetAnnotationResponse> responseObserver,
            MongoAnnotationClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        this.dispatcher = new GetAnnotationDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {
        logger.debug("executing GetAnnotationJob id: {}", responseObserver.hashCode());

        // blank/malformed id handling is shared: see validateRequiredObjectId (#248 plan D11)
        final ResultStatus idStatus = AnnotationValidationUtility.validateRequiredObjectId(
                "GetAnnotationRequest.annotationId", request.getAnnotationId());
        if (idStatus.isError) {
            dispatcher.handleValidationError(idStatus);
            return;
        }

        final AnnotationDocument annotationDocument;
        try {
            annotationDocument = mongoClient.lookupAnnotation(request.getAnnotationId());
        } catch (DpException ex) {
            dispatcher.handleError("error looking up Annotation: " + ex.getMessage());
            return;
        }
        if (annotationDocument == null) {
            dispatcher.handleResult(null, null);
            return;
        }

        // getAnnotation() is the one method that populates Annotation.calculations inline
        // (annotation.proto). A calculationsId that resolves to nothing is corruption — the
        // annotation asserts calculations exist — and must surface as an error, never as
        // silently-empty content (#248 plan D16).
        CalculationsDocument calculationsDocument = null;
        if (annotationDocument.getCalculationsId() != null) {
            try {
                calculationsDocument = mongoClient.lookupCalculations(annotationDocument.getCalculationsId());
            } catch (DpException ex) {
                dispatcher.handleError("error looking up Calculations: " + ex.getMessage());
                return;
            }
            if (calculationsDocument == null) {
                dispatcher.handleError("annotation " + request.getAnnotationId()
                        + " references calculations document " + annotationDocument.getCalculationsId()
                        + " which does not exist");
                return;
            }
        }

        dispatcher.handleResult(annotationDocument, calculationsDocument);
    }
}
