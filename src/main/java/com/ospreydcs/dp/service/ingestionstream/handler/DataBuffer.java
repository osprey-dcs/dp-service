package com.ospreydcs.dp.service.ingestionstream.handler;

import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DataBuffer {

    private static final Logger logger = LogManager.getLogger();

    private final String pvName;
    private final DataBufferConfig config;
    private final List<BufferedDataItem> bufferedItems = new ArrayList<>();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();
    
    private long currentBufferSizeBytes = 0;
    private Instant lastFlushTime = Instant.now();

    public static class DataBufferConfig {
        private final long flushIntervalMs;
        private final long maxBufferSizeBytes;
        private final int maxBufferItems;
        private final long maxItemAgeMs;

        public DataBufferConfig(long flushIntervalMs, long maxBufferSizeBytes, int maxBufferItems, long maxItemAgeMs) {
            this.flushIntervalMs = flushIntervalMs;
            this.maxBufferSizeBytes = maxBufferSizeBytes;
            this.maxBufferItems = maxBufferItems;
            this.maxItemAgeMs = maxItemAgeMs;
        }

        public long getFlushIntervalMs() { return flushIntervalMs; }
        public long getMaxBufferSizeBytes() { return maxBufferSizeBytes; }
        public int getMaxBufferItems() { return maxBufferItems; }
        public long getMaxItemAgeMs() { return maxItemAgeMs; }
    }

    private static class BufferedDataItem {
        private final SubscribeDataResponse.SubscribeDataResult result;
        private final Instant timestamp;
        private final long estimatedSizeBytes;

        public BufferedDataItem(SubscribeDataResponse.SubscribeDataResult result, long estimatedSizeBytes) {
            this.result = result;
            this.timestamp = Instant.now();
            this.estimatedSizeBytes = estimatedSizeBytes;
        }

        public SubscribeDataResponse.SubscribeDataResult getResult() { return result; }
        public Instant getTimestamp() { return timestamp; }
        public long getEstimatedSizeBytes() { return estimatedSizeBytes; }
    }

    public DataBuffer(String pvName, DataBufferConfig config) {
        this.pvName = pvName;
        this.config = config;
    }

    public void addData(SubscribeDataResponse.SubscribeDataResult result) {
        writeLock.lock();
        try {
            long estimatedSize = estimateDataSize(result);
            BufferedDataItem item = new BufferedDataItem(result, estimatedSize);
            
            bufferedItems.add(item);
            currentBufferSizeBytes += estimatedSize;
            
            logger.debug("Added data to buffer for PV: {}, buffer size: {} bytes, {} items", 
                        pvName, currentBufferSizeBytes, bufferedItems.size());
        } finally {
            writeLock.unlock();
        }
    }

    public boolean shouldFlush() {
        readLock.lock();
        try {
            if (bufferedItems.isEmpty()) {
                return false;
            }

            Instant now = Instant.now();
            long timeSinceLastFlush = now.toEpochMilli() - lastFlushTime.toEpochMilli();
            
            // Check if any items have exceeded max age
            boolean hasExpiredItems = bufferedItems.stream()
                .anyMatch(item -> {
                    long itemAge = now.toEpochMilli() - item.getTimestamp().toEpochMilli();
                    return itemAge >= config.getMaxItemAgeMs();
                });
            
            return timeSinceLastFlush >= config.getFlushIntervalMs() ||
                   currentBufferSizeBytes >= config.getMaxBufferSizeBytes() ||
                   bufferedItems.size() >= config.getMaxBufferItems() ||
                   hasExpiredItems;
        } finally {
            readLock.unlock();
        }
    }

    public List<SubscribeDataResponse.SubscribeDataResult> flush() {
        writeLock.lock();
        try {
            if (bufferedItems.isEmpty()) {
                return new ArrayList<>();
            }

            Instant now = Instant.now();
            List<SubscribeDataResponse.SubscribeDataResult> results = new ArrayList<>();
            List<BufferedDataItem> itemsToRemove = new ArrayList<>();
            
            // Only flush items that have reached the configured age
            for (BufferedDataItem item : bufferedItems) {
                long itemAge = now.toEpochMilli() - item.getTimestamp().toEpochMilli();
                if (itemAge >= config.getMaxItemAgeMs()) {
                    results.add(item.getResult());
                    itemsToRemove.add(item);
                }
            }

            // Remove flushed items and update buffer size
            for (BufferedDataItem item : itemsToRemove) {
                bufferedItems.remove(item);
                currentBufferSizeBytes -= item.getEstimatedSizeBytes();
            }

            if (!results.isEmpty()) {
                logger.debug("Flushing {} aged items from buffer for PV: {}, {} items remaining, {} bytes remaining", 
                            results.size(), pvName, bufferedItems.size(), currentBufferSizeBytes);
            }

            lastFlushTime = now;
            return results;
        } finally {
            writeLock.unlock();
        }
    }

    public int getBufferedItemCount() {
        readLock.lock();
        try {
            return bufferedItems.size();
        } finally {
            readLock.unlock();
        }
    }

    public long getCurrentBufferSizeBytes() {
        readLock.lock();
        try {
            return currentBufferSizeBytes;
        } finally {
            readLock.unlock();
        }
    }

    private long estimateDataSize(SubscribeDataResponse.SubscribeDataResult result) {
        AtomicLong size = new AtomicLong(100); // Base overhead for timestamps and structure
        
        result.getDataColumnsList().forEach(dataColumn -> {
            size.addAndGet(dataColumn.getName().length() * 2); // String overhead
            size.addAndGet(dataColumn.getDataValuesList().size() * 50); // Base per-value overhead
            
            dataColumn.getDataValuesList().forEach(dataValue -> {
                switch (dataValue.getValueCase()) {
                    case STRINGVALUE:
                        size.addAndGet(dataValue.getStringValue().length() * 2);
                        break;
                    case BYTEARRAYVALUE:
                        size.addAndGet(dataValue.getByteArrayValue().size());
                        break;
                    case ARRAYVALUE:
                        size.addAndGet(dataValue.getArrayValue().getDataValuesCount() * 32);
                        break;
                    case STRUCTUREVALUE:
                        size.addAndGet(dataValue.getStructureValue().getFieldsCount() * 64);
                        break;
                    case IMAGEVALUE:
                        size.addAndGet(dataValue.getImageValue().getImage().size());
                        break;
                    default:
                        size.addAndGet(8); // Primitive types
                        break;
                }
            });
        });
        
        return size.get();
    }

    public List<SubscribeDataResponse.SubscribeDataResult> forceFlushAll() {
        writeLock.lock();
        try {
            if (bufferedItems.isEmpty()) {
                return new ArrayList<>();
            }

            List<SubscribeDataResponse.SubscribeDataResult> results = new ArrayList<>();
            for (BufferedDataItem item : bufferedItems) {
                results.add(item.getResult());
            }

            logger.debug("Force flushing all {} items from buffer for PV: {}, {} bytes", 
                        bufferedItems.size(), pvName, currentBufferSizeBytes);

            bufferedItems.clear();
            currentBufferSizeBytes = 0;
            lastFlushTime = Instant.now();

            return results;
        } finally {
            writeLock.unlock();
        }
    }

    public int getItemsReadyToFlush() {
        readLock.lock();
        try {
            if (bufferedItems.isEmpty()) {
                return 0;
            }

            Instant now = Instant.now();
            return (int) bufferedItems.stream()
                .filter(item -> {
                    long itemAge = now.toEpochMilli() - item.getTimestamp().toEpochMilli();
                    return itemAge >= config.getMaxItemAgeMs();
                })
                .count();
        } finally {
            readLock.unlock();
        }
    }
}