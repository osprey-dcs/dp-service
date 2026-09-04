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

        // validate request, e.g., that ids for associated datasets and annotations exist in the database
        final ResultStatus resultStatus = this.handler.validateSaveAnnotationRequest(request);
        if (resultStatus.isError) {
            dispatcher.handleValidationError(resultStatus);
            return;
        }

        // When replacing an existing annotation, capture its current calculationsId up front so the
        // replaced document can be deleted after a successful save (#248 plan D14) — full-replace
        // semantics apply to calculations like every other field, and without this the previous
        // document is orphaned. A lookup failure here is an error, not "no previous calculations":
        // proceeding would silently skip the cleanup this fix exists to perform.
        String previousCalculationsId = null;
        if (!request.getId().isBlank()) {
            final AnnotationDocument previousDocument;
            try {
                previousDocument = this.mongoClient.lookupAnnotation(request.getId());
            } catch (DpException ex) {
                dispatcher.handleError("error looking up existing Annotation: " + ex.getMessage());
                return;
            }
            // A null previousDocument means the id does not exist; let saveAnnotation() produce its
            // usual rejection rather than duplicating that logic here.
            if (previousDocument != null) {
                previousCalculationsId = previousDocument.getCalculationsId();
            }
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
                AnnotationDocument.fromSaveAnnotationRequest(request, calculationsDocumentId);
        final MongoSaveResult result = this.mongoClient.saveAnnotation(annotationDocument, request.getId());

        // The save replaced (or cleared) the annotation's calculations reference, so delete the
        // previous calculations document (#248 plan D14). Only after a successful save — on a
        // rejected or failed save the stored annotation still references it. A cleanup failure is
        // logged with the orphaned id but does not fail the response: the save itself succeeded,
        // a retry cannot remove the orphan, and reporting an error would mislead the caller.
        if (!result.isError && !result.isReject
                && previousCalculationsId != null
                && !previousCalculationsId.equals(calculationsDocumentId)) {
            final MongoDeleteResult cleanupResult = this.mongoClient.deleteCalculations(previousCalculationsId);
            if (cleanupResult.isError) {
                logger.error("saveAnnotation id: {} replaced calculations document {} but deleting it failed: {}",
                        result.documentId, previousCalculationsId, cleanupResult.message);
            }
        }

        // dispatch result in API response stream
        logger.debug("dispatching SaveAnnotationJob id: {}", this.responseObserver.hashCode());
        dispatcher.handleResult(result, calculationsDocumentId);
    }
}