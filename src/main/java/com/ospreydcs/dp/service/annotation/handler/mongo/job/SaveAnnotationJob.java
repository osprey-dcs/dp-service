package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.mongodb.client.result.InsertOneResult;
import com.ospreydcs.dp.grpc.v1.annotation.Calculations;
import com.ospreydcs.dp.grpc.v1.annotation.SaveAnnotationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.SaveAnnotationResponse;
import com.ospreydcs.dp.service.annotation.handler.mongo.MongoAnnotationHandler;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.dispatch.SaveAnnotationDispatcher;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.handler.HandlerJob;
import com.ospreydcs.dp.service.common.model.MongoDeleteResult;
import com.ospreydcs.dp.service.common.model.MongoInsertOneResult;
import com.ospreydcs.dp.service.common.model.MongoSaveResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SaveAnnotationJob extends HandlerJob {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    protected final SaveAnnotationRequest request;
    protected final StreamObserver<SaveAnnotationResponse> responseObserver;
    protected final MongoAnnotationClientInterface mongoClient;
    protected final MongoAnnotationHandler handler;
    protected SaveAnnotationDispatcher dispatcher;

    public SaveAnnotationJob(
            SaveAnnotationRequest request,
            StreamObserver<SaveAnnotationResponse> responseObserver,
            MongoAnnotationClientInterface mongoClient,
            MongoAnnotationHandler handler
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
        this.handler = handler;
        this.dispatcher = new SaveAnnotationDispatcher(responseObserver, request);
    }

    @Override
    public void execute() {

        logger.debug("executing SaveAnnotationJob id: {}", this.responseObserver.hashCode());

        // validate request, e.g., that ids for associated datasets and annotations exist in the
        // database. A lookup failure during validation is an infrastructure failure, dispatched as
        // an ERROR — sending it as a rejection would invert the caller's retry decision (#235).
        final ResultStatus resultStatus;
        try {
            resultStatus = this.handler.validateSaveAnnotationRequest(request);
        } catch (DpException ex) {
            dispatcher.handleError("error validating SaveAnnotationRequest: " + ex.getMessage());
            return;
        }
        if (resultStatus.isError) {
            dispatcher.handleValidationError(resultStatus);
            return;
        }

        // handle calculations, if specified
        String calculationsDocumentId = null;
        if (request.hasCalculations()) {

            // create calculations document
            final Calculations requestCalculations = request.getCalculations();
            final CalculationsDocument calculationsDocument;
            try {
                calculationsDocument = CalculationsDocument.fromCalculations(requestCalculations);
            } catch (DpException ex) {
                dispatcher.handleError(
                        "error converting Calculations to document: " + ex.getMessage());
                return;
            }

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
                AnnotationDocument.fromSaveAnnotationRequest(request, calculationsDocumentId);
        final MongoSaveResult result = this.mongoClient.saveAnnotation(annotationDocument, request.getId());
        // (on a successful update, saveAnnotation() itself deletes a replaced or cleared previous
        // calculations document — #248 plan D14)

        // A failed save must not silently orphan the calculations document inserted above. On a
        // rejection the compensating delete is safe: both reject paths in saveAnnotation() (update
        // id not found, and the concurrent-delete race) fire before any annotation write, so
        // nothing can reference the new document. On an error the write state is ambiguous — the
        // annotation may have been stored despite the reported failure — and deleting would risk
        // the dangling-calculationsId corruption getAnnotation treats as an error (plan D16), so
        // log the possibly-orphaned id instead of acting.
        if (result.isError && calculationsDocumentId != null) {
            if (result.isReject) {
                final MongoDeleteResult compensationResult =
                        this.mongoClient.deleteCalculations(calculationsDocumentId);
                if (compensationResult.isError) {
                    logger.error(
                            "saveAnnotation was rejected and deleting its inserted calculations document {} failed: {}",
                            calculationsDocumentId, compensationResult.message);
                }
            } else {
                logger.error(
                        "saveAnnotation failed after inserting calculations document {}; it may be orphaned: {}",
                        calculationsDocumentId, result.message);
            }
        }

        // dispatch result in API response stream
        logger.debug("dispatching SaveAnnotationJob id: {}", this.responseObserver.hashCode());
        dispatcher.handleResult(result, calculationsDocumentId);
    }
}
