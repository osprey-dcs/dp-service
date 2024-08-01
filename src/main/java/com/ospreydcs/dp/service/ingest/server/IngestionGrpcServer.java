package com.ospreydcs.dp.service.ingest.server;

import com.ospreydcs.dp.service.common.server.GrpcServerBase;
import com.ospreydcs.dp.service.ingest.handler.interfaces.IngestionHandlerInterface;
import com.ospreydcs.dp.service.ingest.handler.mongo.MongoIngestionHandler;
import com.ospreydcs.dp.service.ingest.service.IngestionServiceImpl;

import java.io.IOException;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class IngestionGrpcServer extends GrpcServerBase {

    // static variables
    private static final Logger LOGGER = LogManager.getLogger();

    // configuration
    public static final String CFG_KEY_PORT = "IngestionServer.port";
    public static final int DEFAULT_PORT = 50051;

    // instance variables
    private final IngestionServiceImpl serviceImpl;

    public IngestionGrpcServer(IngestionServiceImpl serviceImpl) {
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
        IngestionHandlerInterface handler = MongoIngestionHandler.newMongoSyncIngestionHandler();
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
        // Example: "java -Ddp.GrpcServer.port=50052 com.ospreydcs.dp.ingest.server.IngestionGrpcServer".

        IngestionServiceImpl serviceImpl = new IngestionServiceImpl();
        final IngestionGrpcServer server = new IngestionGrpcServer(serviceImpl);
        server.start();
        server.blockUntilShutdown();
    }

}
