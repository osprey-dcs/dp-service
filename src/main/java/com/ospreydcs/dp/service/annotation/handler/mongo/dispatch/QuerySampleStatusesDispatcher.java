package com.ospreydcs.dp.service.annotation.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.annotation.QuerySampleStatusesRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QuerySampleStatusesResponse;
import com.ospreydcs.dp.grpc.v1.common.SampleStatusBucket;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.bson.samplestatus.SampleStatusBucketDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class QuerySampleStatusesDispatcher extends Dispatcher {

    private static final Logger logger = LogManager.getLogger();

    private final StreamObserver<QuerySampleStatusesResponse> responseObserver;
    private final QuerySampleStatusesRequest request;

    public QuerySampleStatusesDispatcher(
            StreamObserver<QuerySampleStatusesResponse> responseObserver,
            QuerySampleStatusesRequest request
    ) {
        this.responseObserver = responseObserver;
        this.request = request;
    }

    public void handleValidationError(ResultStatus resultStatus) {
        AnnotationServiceImpl.sendQuerySampleStatusesResponseReject(resultStatus.msg, responseObserver);
    }

    public void handleError(String errorMsg) {
        AnnotationServiceImpl.sendQuerySampleStatusesResponseError(errorMsg, responseObserver);
    }

    public void handleResult(List<SampleStatusBucketDocument> documents, String nextPageToken) {

        final List<SampleStatusBucket> buckets = new ArrayList<>();
        for (SampleStatusBucketDocument document : documents) {
            try {
                buckets.add(document.toSampleStatusBucket());
            } catch (DpException ex) {
                // a malformed stored document must produce a reportable error, never an unchecked throw
                final String errorMsg = "error converting SampleStatusBucketDocument to SampleStatusBucket: "
                        + ex.getMessage();
                logger.error("handleResult: {}", ex.getMessage(), ex);
                handleError(errorMsg);
                return;
            }
        }

        final QuerySampleStatusesResponse.QuerySampleStatusesResult result =
                QuerySampleStatusesResponse.QuerySampleStatusesResult.newBuilder()
                        .addAllSampleStatusBuckets(buckets)
                        .setNextPageToken(nextPageToken != null ? nextPageToken : "")
                        .build();

        AnnotationServiceImpl.sendQuerySampleStatusesResponse(result, responseObserver);
    }
}
