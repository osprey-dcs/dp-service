package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.mongodb.client.result.InsertOneResult;
import com.ospreydcs.dp.grpc.v1.annotation.Calculations;
import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationResponse;
import com.ospreydcs.dp.service.annotation.handler.mongo.MongoAnnotationHandler;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.CreateAnnotationDispatcher;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.model.MongoInsertOneResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CreateAnnotationJob extends HandlerJob {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    protected final CreateAnnotationRequest request;
    protected final StreamObserver<CreateAnnotationResponse> responseObserver;
    protected final MongoAnnotationClientInterface mongoClient;
    protected final MongoAnnotationHandler handler;
    protected CreateAnnotationDispatcher dispatcher;

    public CreateAnnotationJob(
            CreateAnnotationRequest request,
            StreamObserver<CreateAnnotationResponse> responseObserver,
            MongoAnnotationClientInterface mongoClient,
            MongoAnnotationHandler handler
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        this.handler = handler;
        this.dispatcher = new CreateAnnotationDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {

        logger.debug("executing CreateAnnotationJob id: {}", this.responseObserver.hashCode());

        // validate request, e.g., that ids for associated datasets and annotations exist in the database
        final ResultStatus resultStatus = this.handler.validateCreateAnnotationRequest(request);
        if (resultStatus.isError) {
            dispatcher.handleValidationError(resultStatus);
            return;
        }

        // handle calculations, if specified
        String calculationsDocumentId = null;
        if (request.hasCalculations()) {

            // create calculations document
            final Calculations requestCalculations = request.getCalculations();
            final CalculationsDocument calculationsDocument =
                    CalculationsDocument.fromCalculations(requestCalculations);

            // save calculations document to database
            MongoInsertOneResult result = this.mongoClient.insertCalculations(calculationsDocument);

            // check for errors saving document
            if (result.isError) {
                // send error response and close response stream
                final String errorMsg = "exception inserting CalculationsDocument: " + result.message;
                dispatcher.handleError(errorMsg);
                return;
            }

            // Otherwise check to see if the wrapped InsertOneResult indicates an error
            final InsertOneResult insertOneResult = result.insertOneResult;
            if (!insertOneResult.wasAcknowledged()) {
                final String errorMsg = "CalculationsDocument insert failed (insertOne() not acknowledged)";
                dispatcher.handleError(errorMsg);
                return;
            }

            // check if result contains id inserted
            if (insertOneResult.getInsertedId() == null) {
                final String errorMsg = "CalculationsDocument insert failed to return document id";
                dispatcher.handleError(errorMsg);
                return;
            }

            calculationsDocumentId = insertOneResult.getInsertedId().asObjectId().getValue().toString();
        }

        // save annotation document to mongodb
        final AnnotationDocument annotationDocument =
                AnnotationDocument.fromCreateAnnotationRequest(request, calculationsDocumentId);
        final MongoInsertOneResult result = this.mongoClient.insertAnnotation(annotationDocument);

        // dispatch result in API response stream
        logger.debug("dispatching CreateAnnotationJob id: {}", this.responseObserver.hashCode());
        dispatcher.handleResult(result);
    }
}
