package com.ospreydcs.dp.service.query.server;

import com.ospreydcs.dp.service.common.server.GrpcServerBase;
import com.ospreydcs.dp.service.query.handler.interfaces.QueryHandlerInterface;
import com.ospreydcs.dp.service.query.handler.mongo.MongoQueryHandler;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class QueryGrpcServer extends GrpcServerBase {

    private static final Logger LOGGER = LogManager.getLogger();

    // configuration
    public static final String CFG_KEY_PORT = "QueryServer.port";
    public static final int DEFAULT_PORT = 50052;

    // instance variables
    private final QueryServiceImpl serviceImpl;

    public QueryGrpcServer(QueryServiceImpl serviceImpl) {
        super(serviceImpl);
        this.serviceImpl = serviceImpl;
    }

    @Override
    protected int getPort_() {
        return configMgr().getConfigInteger(CFG_KEY_PORT, DEFAULT_PORT);
    }

    @Override
    protected void initService_() {

        // create and initialize handler
        QueryHandlerInterface handler = MongoQueryHandler.newMongoSyncQueryHandler();
        LOGGER.info("initService_ using handler: " + handler.getClass().getName());

        // create and initialize ingestion service implementation
        if (!serviceImpl.init(handler)) {
            LOGGER.error("initService_ serviceImpl.init failed");
            return;
        }
    }

    @Override
    protected void finiService_() {
        if (serviceImpl != null) {
            serviceImpl.fini();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        // Note that config overrides passed on the command line must be set using "-D" as VM arguments so that they
        // appear on the command line before the main class.  Otherwise, they are passed as arguments in argv to main.
        // Example: "java -Ddp.GrpcServer.port=50052 com.ospreydcs.dp.query.server.QueryGrpcServer".

        QueryServiceImpl serviceImpl = new QueryServiceImpl();
        final QueryGrpcServer server = new QueryGrpcServer(serviceImpl);
        server.start();
        server.blockUntilShutdown();
    }

}
