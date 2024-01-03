package com.ospreydcs.dp.service.query.handler.mongo;

import com.ospreydcs.dp.service.query.handler.interfaces.ResultCursorInterface;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.ResponseCursorDispatcher;

public class QueryResultCursor implements ResultCursorInterface {
    private final MongoQueryHandler handler;
    private final ResponseCursorDispatcher dispatcher;
    public QueryResultCursor(MongoQueryHandler handler, ResponseCursorDispatcher dispatcher) {
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
