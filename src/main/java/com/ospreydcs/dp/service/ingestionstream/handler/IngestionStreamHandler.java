package com.ospreydcs.dp.service.ingestionstream.handler;

import com.ospreydcs.dp.service.ingestionstream.handler.interfaces.IngestionStreamHandlerInterface;

public class IngestionStreamHandler implements IngestionStreamHandlerInterface {
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
