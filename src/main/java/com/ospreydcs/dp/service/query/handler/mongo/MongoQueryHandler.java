package com.ospreydcs.dp.service.query.handler.mongo;

import com.ospreydcs.dp.service.ingest.handler.mongo.MongoIngestionClientInterface;
import com.ospreydcs.dp.service.ingest.handler.mongo.MongoIngestionHandler;
import com.ospreydcs.dp.service.ingest.handler.mongo.MongoSyncIngestionClient;
import com.ospreydcs.dp.service.query.handler.QueryHandlerBase;
import com.ospreydcs.dp.service.query.handler.QueryHandlerInterface;

public class MongoQueryHandler extends QueryHandlerBase implements QueryHandlerInterface {

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
