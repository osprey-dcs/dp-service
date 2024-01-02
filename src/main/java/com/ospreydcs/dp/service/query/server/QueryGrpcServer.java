package com.ospreydcs.dp.service.query.server;

import com.ospreydcs.dp.common.config.ConfigurationManager;
import com.ospreydcs.dp.service.query.handler.interfaces.QueryHandlerInterface;
import com.ospreydcs.dp.service.query.handler.mongo.MongoQueryHandler;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class QueryGrpcServer {

    private static final Logger LOGGER = LogManager.getLogger();

    // constants
    private static final int TIMEOUT_TERMINATION_SECS = 30;

    // configuration
    public static final String CFG_KEY_PORT = "QueryServer.port";
    public static final int DEFAULT_PORT = 50052;

    private Server server;
    private QueryServiceImpl serviceImpl;

    private static ConfigurationManager configMgr() {
        return ConfigurationManager.getInstance();
    }

    protected void start() throws IOException {

        initService();

        int port = configMgr().getConfigInteger(CFG_KEY_PORT, DEFAULT_PORT);
        server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                .addService(serviceImpl)
                .build()
                .start();
        LOGGER.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                    QueryGrpcServer.this.stop();
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
                System.err.println("*** server shut down");
            }
        });
    }

    protected void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(TIMEOUT_TERMINATION_SECS, TimeUnit.SECONDS);
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    protected void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
        finiService();
    }

    private void initService() {

        // create and initialize handler
        QueryHandlerInterface handler = MongoQueryHandler.newMongoSyncQueryHandler();
        LOGGER.info("initService using handler: " + handler.getClass().getName());

        // create and initialize ingestion service implementation
        serviceImpl = new QueryServiceImpl();
        if (!serviceImpl.init(handler)) {
            LOGGER.error("initService serviceImpl.init failed");
            return;
        }
    }

    private void finiService() {
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

        final QueryGrpcServer server = new QueryGrpcServer();
        server.start();
        server.blockUntilShutdown();
    }

}
