package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationResponse;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import io.grpc.stub.StreamObserver;

public class CreateCommentAnnotationJob extends CreateAnnotationJob {

    public CreateCommentAnnotationJob(
            CreateAnnotationRequest request,
            StreamObserver<CreateAnnotationResponse> responseObserver,
            MongoAnnotationClientInterface mongoClient
    ) {
        super(request, responseObserver, mongoClient);
    }

    @Override
    public void execute() {

    }
}
