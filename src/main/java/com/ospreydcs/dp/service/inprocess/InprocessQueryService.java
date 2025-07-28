package com.ospreydcs.dp.service.inprocess;

import com.ospreydcs.dp.service.query.handler.interfaces.QueryHandlerInterface;
import com.ospreydcs.dp.service.query.handler.mongo.MongoQueryHandler;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.Channel;
import io.grpc.testing.GrpcCleanupRule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.ClassRule;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;

public class InprocessQueryService extends InprocessServiceBase<QueryServiceImpl>{

    // static variables
    private static final Logger logger = LogManager.getLogger();
    @ClassRule
    public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    // instance variables (common ones inherited from base class)

    public Channel getQueryChannel() {
        return this.channel;
    }

    @Override
    protected boolean initService() {
        QueryHandlerInterface queryHandler = MongoQueryHandler.newMongoSyncQueryHandler();
        service = new QueryServiceImpl();
        return service.init(queryHandler);
    }

    @Override
    protected void finiService() {
        service.fini();
    }

    @Override
    protected QueryServiceImpl createServiceMock(QueryServiceImpl service) {
        return mock(QueryServiceImpl.class, delegatesTo(service));
    }

    @Override
    protected GrpcCleanupRule getGrpcCleanupRule() {
        return grpcCleanup;
    }

    @Override
    protected String getServiceName() {
        return "QueryServiceImpl";
    }

}
