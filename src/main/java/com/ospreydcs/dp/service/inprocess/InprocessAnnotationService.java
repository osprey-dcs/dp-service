package com.ospreydcs.dp.service.inprocess;

import com.ospreydcs.dp.service.annotation.handler.interfaces.AnnotationHandlerInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.MongoAnnotationHandler;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import io.grpc.testing.GrpcCleanupRule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.ClassRule;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;

public class InprocessAnnotationService extends InprocessServiceBase<AnnotationServiceImpl> {

    // static variables
    private static final Logger logger = LogManager.getLogger();
    @ClassRule
    public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    @Override
    protected boolean initService() {
        AnnotationHandlerInterface annotationHandler = MongoAnnotationHandler.newMongoSyncAnnotationHandler();
        service = new AnnotationServiceImpl();
        return service.init(annotationHandler);
    }

    @Override
    protected void finiService() {
        service.fini();
    }

    @Override
    protected AnnotationServiceImpl createServiceMock(AnnotationServiceImpl service) {
        return mock(AnnotationServiceImpl.class, delegatesTo(service));
    }

    @Override
    protected GrpcCleanupRule getGrpcCleanupRule() {
        return grpcCleanup;
    }

    @Override
    protected String getServiceName() {
        return "AnnotationServiceImpl";
    }

}
