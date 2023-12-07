package com.ospreydcs.dp.service.query.handler.mongo;

import com.ospreydcs.dp.service.ingest.handler.mongo.MongoIngestionClientInterface;
import com.ospreydcs.dp.service.ingest.handler.mongo.MongoIngestionHandler;
import com.ospreydcs.dp.service.ingest.handler.mongo.MongoSyncIngestionClient;
import com.ospreydcs.dp.service.query.handler.QueryHandlerBase;
import com.ospreydcs.dp.service.query.handler.QueryHandlerInterface;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MongoQueryHandler extends QueryHandlerBase implements QueryHandlerInterface {

    private static final Logger LOGGER = LogManager.getLogger();

    // constants
    private static final int TIMEOUT_SECONDS = 60;
    protected static final int MAX_QUEUE_SIZE = 1;
    protected static final int POLL_TIMEOUT_SECONDS = 1;

    // configuration
    public static final String CFG_KEY_NUM_WORKERS = "QueryHandler.numWorkers";
    public static final int DEFAULT_NUM_WORKERS = 7;

    private MongoQueryClientInterface queryClient = null;

    public MongoQueryHandler(MongoQueryClientInterface clientInterface) {
        this.queryClient = clientInterface;
    }

    public static MongoQueryHandler newMongoSyncQueryHandler() {
        return new MongoQueryHandler(new MongoSyncQueryClient());
    }

    @Override
    public boolean init() {
        return true;
    }

    @Override
    public boolean fini() {
        return true;
    }

    @Override
    public boolean start() {
        return true;
    }

    @Override
    public boolean stop() {
        return true;
    }
}
