package com.ospreydcs.dp.service.annotation.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.annotation.QueryAnnotationsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryAnnotationsResponse;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.model.AnnotationQueryResult;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryAnnotationsDispatcher extends Dispatcher {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final QueryAnnotationsRequest request;
    private final StreamObserver<QueryAnnotationsResponse> responseObserver;

    public QueryAnnotationsDispatcher(
            StreamObserver<QueryAnnotationsResponse> responseObserver,
            QueryAnnotationsRequest request
    ) {
        this.request = request;
        this.responseObserver = responseObserver;
    }

    public void handleError(String errorMsg) {
        AnnotationServiceImpl.sendQueryAnnotationsResponseError(errorMsg, responseObserver);
    }

    public void handleResult(AnnotationQueryResult queryResult) {

        final QueryAnnotationsResponse.AnnotationsResult.Builder annotationsResultBuilder =
                QueryAnnotationsResponse.AnnotationsResult.newBuilder();

        // Build a protobuf Annotation per document.  Since dp-grpc #132 the result carries references
        // only -- dataSetIds and calculationsId -- so there are no per-annotation lookups here.  The
        // previous implementation issued one findDataSet() round trip per dataset id, serially and
        // without batching or de-duplication across annotations sharing a dataset, plus one
        // findCalculations() per annotation.  Callers fetch content with queryDataSets() over the ids
        // gathered across the page, or getCalculations().
        for (AnnotationDocument annotationDocument : queryResult.getDocuments()) {
            annotationsResultBuilder.addAnnotations(annotationDocument.toAnnotation());
        }

        annotationsResultBuilder.setNextPageToken(
                queryResult.getNextPageToken() != null ? queryResult.getNextPageToken() : "");

        // send response and close response stream
        AnnotationServiceImpl.sendQueryAnnotationsResponse(
                annotationsResultBuilder.build(), this.responseObserver);
    }

}
