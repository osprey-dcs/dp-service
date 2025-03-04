package com.ospreydcs.dp.service.query.handler.mongo;

import com.ospreydcs.dp.service.query.handler.interfaces.ResultCursorInterface;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.QueryDataBidiStreamDispatcher;

public class QueryResultCursor implements ResultCursorInterface {
    private final MongoQueryHandler handler;
    private final QueryDataBidiStreamDispatcher dispatcher;
    public QueryResultCursor(MongoQueryHandler handler, QueryDataBidiStreamDispatcher dispatcher) {
        this.handler = handler;
        this.dispatcher = dispatcher;
    }
    public void close() {
        this.dispatcher.close();
    }
    public void next() {
        this.dispatcher.next();
    }
}
