package com.ospreydcs.dp.service.annotation.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.annotation.QueryPvMetadataRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryPvMetadataResponse;
import com.ospreydcs.dp.grpc.v1.common.PvMetadata;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.bson.pvmetadata.PvMetadataDocument;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;

public class QueryPvMetadataDispatcher extends Dispatcher {

    private final StreamObserver<QueryPvMetadataResponse> responseObserver;
    private final QueryPvMetadataRequest request;

    public QueryPvMetadataDispatcher(
            StreamObserver<QueryPvMetadataResponse> responseObserver,
            QueryPvMetadataRequest request
    ) {
        this.responseObserver = responseObserver;
        this.request = request;
    }

    public void handleValidationError(ResultStatus resultStatus) {
        AnnotationServiceImpl.sendQueryPvMetadataResponseReject(resultStatus.msg, responseObserver);
    }

    public void handleError(String errorMsg) {
        AnnotationServiceImpl.sendQueryPvMetadataResponseError(errorMsg, responseObserver);
    }

    public void handleResult(List<PvMetadataDocument> documents, String nextPageToken) {
        final List<PvMetadata> pvMetadataList = new ArrayList<>();
        for (PvMetadataDocument document : documents) {
            pvMetadataList.add(document.toPvMetadata());
        }

        final QueryPvMetadataResponse.PvMetadataResult result =
                QueryPvMetadataResponse.PvMetadataResult.newBuilder()
                        .addAllPvMetadata(pvMetadataList)
                        .setNextPageToken(nextPageToken != null ? nextPageToken : "")
                        .build();

        AnnotationServiceImpl.sendQueryPvMetadataResponse(result, responseObserver);
    }
}
