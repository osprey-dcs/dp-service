package com.ospreydcs.dp.service.ingestionstream.handler;

import com.ospreydcs.dp.service.ingestionstream.handler.monitor.EventMonitor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class EventMonitorManager {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final IngestionStreamHandler handler;
    private final List<EventMonitor> monitors = new ArrayList<>();
    public final AtomicBoolean shutdownRequested = new AtomicBoolean(false);
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    public EventMonitorManager(
            IngestionStreamHandler ingestionStreamHandler
    ) {
        this.handler = ingestionStreamHandler;
    }

    public void addEventMonitor(final EventMonitor eventMonitor) {

        if (shutdownRequested.get()) {
            return;
        }

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

        if (shutdownRequested.get()) {
            return;
        }

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

        // terminate the EventMonitor
        eventMonitor.requestShutdown();

        // remove EventMonitor from list of managed monitors
        this.removeEventMonitor(eventMonitor);
    }

    public void shutdown() {

        logger.debug("shutdown");

        if (shutdownRequested.compareAndSet(false, true)) {

            // Shutdown all managed monitors, but only acquire readLock to copy the list of monitors.
            // We'll terminate the monitors after releasing the lock.
            List<EventMonitor> monitorsCopy;

            readLock.lock();
            try {

                monitorsCopy = new ArrayList<>(monitors);

            } finally {
                readLock.unlock();
            }

            for (EventMonitor eventMonitor : monitorsCopy) {
                eventMonitor.requestShutdown();
            }
        }
    }

}
