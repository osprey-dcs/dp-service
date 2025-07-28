package com.ospreydcs.dp.service.inprocess;

import com.ospreydcs.dp.service.common.config.ConfigurationManager;
import com.ospreydcs.dp.service.ingest.handler.interfaces.IngestionHandlerInterface;
import com.ospreydcs.dp.service.ingest.handler.mongo.MongoIngestionHandler;
import com.ospreydcs.dp.service.ingest.service.IngestionServiceImpl;
import io.grpc.ManagedChannel;
import io.grpc.testing.GrpcCleanupRule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.ClassRule;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;

public class InprocessIngestionService extends InprocessServiceBase<IngestionServiceImpl> {

    // static variables
    @ClassRule
    public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
    private static final Logger logger = LogManager.getLogger();

    protected static ConfigurationManager configMgr() {
        return ConfigurationManager.getInstance();
    }

    @Override
    protected boolean initService() {
        IngestionHandlerInterface ingestionHandler = MongoIngestionHandler.newMongoSyncIngestionHandler();
        service = new IngestionServiceImpl();
        return service.init(ingestionHandler);
    }

    @Override
    protected void finiService() {
        service.fini();
    }

    @Override
    protected IngestionServiceImpl createServiceMock(IngestionServiceImpl service) {
        return mock(IngestionServiceImpl.class, delegatesTo(service));
    }

    @Override
    protected GrpcCleanupRule getGrpcCleanupRule() {
        return grpcCleanup;
    }

    @Override
    protected String getServiceName() {
        return "IngestionServiceImpl";
    }

    public ManagedChannel getIngestionChannel() {
        return this.channel;
    }

}
