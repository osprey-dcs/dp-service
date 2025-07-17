package com.ospreydcs.dp.service.ingestionstream.handler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class TriggeredEventManager {

    private static final Logger logger = LogManager.getLogger();

    public static class TriggeredEventManagerConfig {
        private final long expirationDelayNanos;
        private final long cleanupIntervalMs;

        public TriggeredEventManagerConfig(long expirationDelayNanos, long cleanupIntervalMs) {
            this.expirationDelayNanos = expirationDelayNanos;
            this.cleanupIntervalMs = cleanupIntervalMs;
        }

        public long getExpirationDelayNanos() { return expirationDelayNanos; }
        public long getCleanupIntervalMs() { return cleanupIntervalMs; }

        // Convenience methods for time conversion
        public static long secondsToNanos(long seconds) {
            return seconds * 1_000_000_000L;
        }
    }

    private final TriggeredEventManagerConfig config;
    private final ConcurrentLinkedDeque<TriggeredEvent> activeEvents = new ConcurrentLinkedDeque<>();
    private final ScheduledExecutorService cleanupScheduler = Executors.newScheduledThreadPool(1);
    private final AtomicLong eventCounter = new AtomicLong(0);
    private volatile boolean isRunning = false;

    public TriggeredEventManager(TriggeredEventManagerConfig config) {
        this.config = config;
    }

    public void start() {
        if (isRunning) {
            logger.warn("TriggeredEventManager is already running");
            return;
        }

        isRunning = true;
        startPeriodicCleanup();
        logger.info("TriggeredEventManager started with expiration delay: {} ns, cleanup interval: {} ms", 
                   config.getExpirationDelayNanos(), config.getCleanupIntervalMs());
    }

    public void shutdown() {
        if (!isRunning) {
            logger.warn("TriggeredEventManager is not running");
            return;
        }

        logger.info("Shutting down TriggeredEventManager");
        isRunning = false;

        cleanupScheduler.shutdown();
        try {
            if (!cleanupScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                cleanupScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            cleanupScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Clear all active events
        int clearedEvents = activeEvents.size();
        activeEvents.clear();
        
        logger.info("TriggeredEventManager shutdown complete, cleared {} active events", clearedEvents);
    }

    public void addTriggeredEvent(TriggeredEvent event) {
        if (!isRunning) {
            logger.warn("Cannot add triggered event - manager is not running");
            return;
        }

        activeEvents.add(event);
        long eventId = eventCounter.incrementAndGet();
        
        logger.debug("Added triggered event #{}, active events count: {}, expiration time: {}", 
                    eventId, activeEvents.size(), event.getExpirationTime());
    }

    public List<TriggeredEvent> getActiveEvents() {
        return new ArrayList<>(activeEvents);
    }

    public int getActiveEventCount() {
        return activeEvents.size();
    }

    private void startPeriodicCleanup() {
        cleanupScheduler.scheduleWithFixedDelay(
            this::cleanupExpiredEvents,
            config.getCleanupIntervalMs(),
            config.getCleanupIntervalMs(),
            TimeUnit.MILLISECONDS
        );
    }

    private void cleanupExpiredEvents() {
        if (!isRunning) {
            return;
        }

        try {
            Instant now = Instant.now();
            int removedCount = 0;
            int totalChecked = 0;

            // Remove expired events from the front of the deque
            // Note: We assume events are generally added in chronological order
            while (!activeEvents.isEmpty()) {
                TriggeredEvent event = activeEvents.peekFirst();
                if (event == null) {
                    break;
                }

                totalChecked++;
                if (event.isExpired()) {
                    activeEvents.removeFirst();
                    removedCount++;
                    logger.debug("Removed expired triggered event, expiration time: {}, current time: {}", 
                               event.getExpirationTime(), now);
                } else {
                    // If the first event hasn't expired, subsequent events likely haven't either
                    // (assuming chronological order), so we can break early
                    break;
                }
            }

            if (removedCount > 0) {
                logger.debug("Cleanup completed: removed {} expired events out of {} checked, {} active events remaining", 
                           removedCount, totalChecked, activeEvents.size());
            }

        } catch (Exception e) {
            logger.error("Error during triggered event cleanup", e);
        }
    }

    public TriggeredEventManagerConfig getConfig() {
        return config;
    }

    public boolean isRunning() {
        return isRunning;
    }
}