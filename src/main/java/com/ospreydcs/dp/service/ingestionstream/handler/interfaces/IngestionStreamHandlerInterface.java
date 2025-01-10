package com.ospreydcs.dp.service.ingestionstream.handler.interfaces;

public interface IngestionStreamHandlerInterface
{
    boolean init();
    boolean fini();
    boolean start();
    boolean stop();
}
