package com.ospreydcs.dp.service.ingestionstream.service;

import com.ospreydcs.dp.grpc.v1.ingestionstream.DpIngestionStreamServiceGrpc;
import com.ospreydcs.dp.service.ingestionstream.handler.interfaces.IngestionStreamHandlerInterface;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IngestionStreamServiceImpl
        extends DpIngestionStreamServiceGrpc.DpIngestionStreamServiceImplBase
{
    private static final Logger logger = LogManager.getLogger();

    private IngestionStreamHandlerInterface handler;

    public boolean init(IngestionStreamHandlerInterface handler) {
        this.handler = handler;
        if (!handler.init()) {
            logger.error("handler.init failed");
            return false;
        }
        if (!handler.start()) {
            logger.error("handler.start failed");
        }
        return true;
    }

    public void fini() {
        if (handler != null) {
            handler.stop();
            handler.fini();
            handler = null;
        }
    }

}
