package com.ospreydcs.dp.service.ingestionstream.handler;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.SamplingClock;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataResponse;
import com.ospreydcs.dp.service.ingestionstream.handler.monitor.DataBuffer;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class DataBufferTest {

    private DataBuffer dataBuffer;
    private DataBuffer.DataBufferConfig config;

    @Before
    public void setUp() {
        config = new DataBuffer.DataBufferConfig(
            1000L,   // 1 second flush interval
            1024L,   // 1KB max buffer size
            10,      // 10 max items
            500_000_000L     // 500ms max item age in nanoseconds
        );
        dataBuffer = new DataBuffer("test-pv", config);
    }

    @Test
    public void testAgeBasedFlushing() throws InterruptedException {
        // Create test data
        SubscribeDataResponse.SubscribeDataResult testResult = createTestResult("test-pv", "test-value");
        DataColumn testColumn = testResult.getDataColumns(0);
        DataTimestamps testTimestamps = testResult.getDataTimestamps();
        
        // Add data to buffer
        dataBuffer.addData(testColumn, testTimestamps);
        
        // Initially, item should not be ready to flush (age < maxItemAge)
        assertEquals(0, dataBuffer.getItemsReadyToFlush());
        assertFalse(dataBuffer.shouldFlush());
        
        // Wait for items to age beyond the configured threshold
        Thread.sleep(600); // Wait longer than maxItemAge (500ms)
        
        // Now item should be ready to flush due to age
        assertEquals(1, dataBuffer.getItemsReadyToFlush());
        assertTrue(dataBuffer.shouldFlush());
        
        // Flush and verify aged items are returned
        List<DataBuffer.BufferedData> flushedResults = dataBuffer.flush();
        assertEquals(1, flushedResults.size());
        assertEquals("test-pv", flushedResults.get(0).getDataColumn().getName());
        assertEquals(0, dataBuffer.getBufferedItemCount());
    }

    @Test
    public void testPartialFlushingByAge() throws InterruptedException {
        // Add first item
        SubscribeDataResponse.SubscribeDataResult result1 = createTestResult("test-pv", "value1");
        dataBuffer.addData(result1.getDataColumns(0), result1.getDataTimestamps());
        
        // Wait for first item to age
        Thread.sleep(600);
        
        // Add second item (should not be aged yet)
        SubscribeDataResponse.SubscribeDataResult result2 = createTestResult("test-pv", "value2");
        dataBuffer.addData(result2.getDataColumns(0), result2.getDataTimestamps());
        
        // Should have 1 item ready to flush (the aged one)
        assertEquals(1, dataBuffer.getItemsReadyToFlush());
        assertTrue(dataBuffer.shouldFlush());
        
        // Flush should only return the aged item
        List<DataBuffer.BufferedData> flushedResults = dataBuffer.flush();
        assertEquals(1, flushedResults.size());
        
        // One item should remain in buffer (the non-aged one)
        assertEquals(1, dataBuffer.getBufferedItemCount());
    }

    @Test
    public void testForceFlushAll() {
        long now = System.currentTimeMillis();
        
        // Add multiple items with recent timestamps (both younger than maxItemAge)
        SubscribeDataResponse.SubscribeDataResult result1 = createTestResultWithTimestamp("test-pv", "value1", now - 100);
        SubscribeDataResponse.SubscribeDataResult result2 = createTestResultWithTimestamp("test-pv", "value2", now - 200);
        dataBuffer.addData(result1.getDataColumns(0), result1.getDataTimestamps());
        dataBuffer.addData(result2.getDataColumns(0), result2.getDataTimestamps());
        
        // Force flush all items regardless of age
        List<DataBuffer.BufferedData> flushedResults = dataBuffer.forceFlushAll();
        assertEquals(2, flushedResults.size());
        assertEquals(0, dataBuffer.getBufferedItemCount());
    }

    private SubscribeDataResponse.SubscribeDataResult createTestResult(String pvName, String value) {
        return createTestResultWithTimestamp(pvName, value, System.currentTimeMillis());
    }
    
    private SubscribeDataResponse.SubscribeDataResult createTestResultWithTimestamp(String pvName, String value, long epochMillis) {
        DataValue dataValue = DataValue.newBuilder()
            .setStringValue(value)
            .build();
        
        DataColumn dataColumn = DataColumn.newBuilder()
            .setName(pvName)
            .addDataValues(dataValue)
            .build();
        
        Timestamp timestamp = Timestamp.newBuilder()
            .setEpochSeconds(epochMillis / 1000)
            .setNanoseconds((int)((epochMillis % 1000) * 1_000_000))
            .build();
        
        SamplingClock clock = SamplingClock.newBuilder()
            .setStartTime(timestamp)
            .setPeriodNanos(1000000000L) // 1 second
            .setCount(1)
            .build();
        
        DataTimestamps timestamps = DataTimestamps.newBuilder()
            .setSamplingClock(clock)
            .build();
        
        return SubscribeDataResponse.SubscribeDataResult.newBuilder()
            .setDataTimestamps(timestamps)
            .addDataColumns(dataColumn)
            .build();
    }
}