package com.ospreydcs.dp.service.annotation.server;

import com.ospreydcs.dp.service.annotation.handler.interfaces.AnnotationHandlerInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.MongoAnnotationHandler;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.server.GrpcServerBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class AnnotationGrpcServer extends GrpcServerBase {

    // static variables
    private static final Logger LOGGER = LogManager.getLogger();

    // configuration
    public static final String CFG_KEY_PORT = "AnnotationServer.port";
    public static final int DEFAULT_PORT = 50053;

    // instance variables
    private final AnnotationServiceImpl serviceImpl;

    public AnnotationGrpcServer(AnnotationServiceImpl serviceImpl) {
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
        AnnotationHandlerInterface handler = MongoAnnotationHandler.newMongoSyncAnnotationHandler();
        LOGGER.info("initService using handler: " + handler.getClass().getName());

        // create and initialize ingestion service implementation
        if (!serviceImpl.init(handler)) {
            LOGGER.error("initService serviceImpl.init failed");
            return;
        }
    }

    @Override
    protected void finiService_() {
        if (serviceImpl != null) {
            serviceImpl.fini();
        }
    }

    /**
     * Main launches the server from the command line.
     */
    public static void main(String[] args) throws IOException, InterruptedException {

        // Note that config overrides passed on the command line must be set using "-D" as VM arguments so that they
        // appear on the command line before the main class.  Otherwise, they are passed as arguments in argv to main.
        // Example: "java -Ddp.GrpcServer.port=50052 com.ospreydcs.dp.query.server.QueryGrpcServer".

        AnnotationServiceImpl serviceImpl = new AnnotationServiceImpl();
        final AnnotationGrpcServer server = new AnnotationGrpcServer(serviceImpl);
        server.start();
        server.blockUntilShutdown();
    }
}
