package com.ospreydcs.dp.service.annotation.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.annotation.QueryAnnotationsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryAnnotationsResponse;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class QueryAnnotationsDispatcher extends Dispatcher {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryAnnotationsRequest request;
    private final StreamObserver<QueryAnnotationsResponse> responseObserver;
    private final MongoAnnotationClientInterface mongoClient;

    public QueryAnnotationsDispatcher(
            StreamObserver<QueryAnnotationsResponse> responseObserver,
            QueryAnnotationsRequest request,
            MongoAnnotationClientInterface mongoClient
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
        this.mongoClient = mongoClient;
    }

    public void handleResult(MongoCursor<AnnotationDocument> cursor) {
        
        // validate cursor
        if (cursor == null) {
            // send error response and close response stream if cursor is null
            final String msg = "query returned null cursor";
            logger.debug(msg);
            AnnotationServiceImpl.sendQueryAnnotationsResponseError(msg, this.responseObserver);
            return;
        } else if (!cursor.hasNext()) {
            logger.trace("query matched no data, cursor is empty");
            AnnotationServiceImpl.sendQueryAnnotationsResponseEmpty(this.responseObserver);
            return;
        }

        final QueryAnnotationsResponse.AnnotationsResult.Builder annotationsResultBuilder =
                QueryAnnotationsResponse.AnnotationsResult.newBuilder();

        while (cursor.hasNext()) {

            // add grpc object for each document in cursor
            final AnnotationDocument annotationDocument = cursor.next();

            // retrieve datasets for annotation
            final List<DataSetDocument> dataSetDocuments = new ArrayList<>();
            for (String dataSetId : annotationDocument.getDataSetIds()) {
                final DataSetDocument dataSetDocument = mongoClient.findDataSet(dataSetId);
                if (dataSetDocument == null) {
                    final String msg =
                            "error retrieving dataset for annotation: " + annotationDocument.getId()
                                    + " no DataSetDocument found with id: " + dataSetId;
                    logger.error(msg);
                    AnnotationServiceImpl.sendQueryAnnotationsResponseError(msg, this.responseObserver);
                }
                dataSetDocuments.add(dataSetDocument);
            }

            // retrieve calculations for annotation
            final String calculationsId = annotationDocument.getCalculationsId();
            CalculationsDocument calculationsDocument = null;
            if (calculationsId != null && ! calculationsId.isBlank()) {
                calculationsDocument = mongoClient.findCalculations(calculationsId);
                if (calculationsDocument == null) {
                    final String msg =
                            "error retrieving calculations for annotation: " + annotationDocument.getId()
                                    + " no CalculationsDocument found with id: " + calculationsId;
                    logger.error(msg);
                    AnnotationServiceImpl.sendQueryAnnotationsResponseError(msg, this.responseObserver);
                }
            }

            // build protobuf Annotation from AnnotationDocument and list of DataSetDocuments and add to result
            final QueryAnnotationsResponse.AnnotationsResult.Annotation responseAnnotation;
            try {
                responseAnnotation = annotationDocument.toAnnotation(dataSetDocuments, calculationsDocument);
                annotationsResultBuilder.addAnnotations(responseAnnotation);
            } catch (DpException e) {
                final String msg =
                        "error building result Annotation: " + e.getMessage();
                logger.error(msg);
                AnnotationServiceImpl.sendQueryAnnotationsResponseError(msg, this.responseObserver);
            }
        }

        // send response and close response stream
        final QueryAnnotationsResponse.AnnotationsResult annotationsResult = annotationsResultBuilder.build();
        AnnotationServiceImpl.sendQueryAnnotationsResponse(annotationsResult, this.responseObserver);
    }
    
}
