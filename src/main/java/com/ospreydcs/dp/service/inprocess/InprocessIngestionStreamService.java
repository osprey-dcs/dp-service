package com.ospreydcs.dp.service.inprocess;

import com.ospreydcs.dp.client.mongo.MongoDemoClient;
import com.ospreydcs.dp.service.ingestionstream.handler.IngestionStreamHandler;
import com.ospreydcs.dp.service.ingestionstream.handler.interfaces.IngestionStreamHandlerInterface;
import com.ospreydcs.dp.service.ingestionstream.service.IngestionStreamServiceImpl;
import io.grpc.ManagedChannel;
import io.grpc.testing.GrpcCleanupRule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.ClassRule;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;

public class InprocessIngestionStreamService extends InprocessServiceBase<IngestionStreamServiceImpl> {

    // static variables
    private static final Logger logger = LogManager.getLogger();
    @ClassRule
    public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    // instance variables (common ones inherited from base class)
    private ManagedChannel ingestionChannel;

    public void init(MongoDemoClient mongoClient, ManagedChannel ingestionChannel) {
        this.ingestionChannel = ingestionChannel;
        super.init(mongoClient);
    }

    @Override
    protected boolean initService() {
        IngestionStreamHandler ingestionStreamHandlerInstance = new IngestionStreamHandler(ingestionChannel);
        IngestionStreamHandlerInterface ingestionStreamHandler = ingestionStreamHandlerInstance;
        service = new IngestionStreamServiceImpl();
        return service.init(ingestionStreamHandler);
    }

    @Override
    protected void finiService() {
        service.fini();
    }

    @Override
    protected IngestionStreamServiceImpl createServiceMock(IngestionStreamServiceImpl service) {
        return mock(IngestionStreamServiceImpl.class, delegatesTo(service));
    }

    @Override
    protected GrpcCleanupRule getGrpcCleanupRule() {
        return grpcCleanup;
    }

    @Override
    protected String getServiceName() {
        return "IngestionStreamServiceImpl";
    }

}
