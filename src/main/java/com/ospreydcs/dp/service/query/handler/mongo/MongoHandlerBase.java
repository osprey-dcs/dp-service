package com.ospreydcs.dp.service.query.handler.mongo;

import com.ospreydcs.dp.service.query.handler.QueryHandlerBase;
import com.ospreydcs.dp.service.query.handler.QueryHandlerInterface;

public abstract class MongoHandlerBase extends QueryHandlerBase implements QueryHandlerInterface {
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
