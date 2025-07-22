package com.ospreydcs.dp.service.ingestionstream.handler;

import com.ospreydcs.dp.service.ingestionstream.handler.monitor.EventMonitor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class EventMonitorManager {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final IngestionStreamHandler handler;
    private final List<EventMonitor> monitors = new ArrayList<>();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    public EventMonitorManager(
            IngestionStreamHandler ingestionStreamHandler
    ) {
        this.handler = ingestionStreamHandler;
    }

    public void addEventMonitor(final EventMonitor eventMonitor) {

        logger.debug("addEventMonitor id: {}", eventMonitor.responseObserver.hashCode());

        // acquire write lock since method will be called from different threads handling grpc requests/responses
        writeLock.lock();
        try {
            monitors.add(eventMonitor);

        } finally {
            // make sure we always unlock by using finally
            writeLock.unlock();
        }
    }

    private void removeEventMonitor(EventMonitor eventMonitor) {

        logger.debug("removeEventMonitor id: {}", eventMonitor.responseObserver.hashCode());

        // acquire write lock since method will be called from different threads handling grpc requests/responses
        writeLock.lock();
        try {
            monitors.remove(eventMonitor);

        } finally {
            // make sure we always unlock by using finally
            writeLock.unlock();
        }
    }

    public void terminateMonitor(final EventMonitor eventMonitor) {

        logger.debug("terminateMonitor id: {}", eventMonitor.responseObserver.hashCode());

        // remove EventMonitor from list of managed monitors
        this.removeEventMonitor(eventMonitor);

        // terminate the EventMonitor
        eventMonitor.requestShutdown();
    }

    public void shutdown() {
        logger.debug("shutdown");

        // Shutdown all EventMonitors which will handle their own buffer cleanup
        readLock.lock();
        try {
            for (EventMonitor eventMonitor : monitors) {
                eventMonitor.requestShutdown();
             }
        } finally {
            readLock.unlock();
        }
    }

}
