package com.ospreydcs.dp.service.ingest;

import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.ingestion.*;
import com.ospreydcs.dp.service.common.grpc.GrpcUtility;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;

/**
 * Provides features and utilities for testing of ingestion service by inheritance to derived classes.
 */
public class IngestionTestBase {

    public static enum IngestionDataType {
        STRING,
        DOUBLE,
        INT,
        BYTE_ARRAY,
        BOOLEAN,
        IMAGE,
        STRUCTURE,
        ARRAY_DOUBLE
    }

    /**
     * Encapsulates the parameters for creating an IngestionRequest API object.
     */
    public static class IngestionRequestParams {

        public Integer providerId = null;
        public String requestId = null;
        public boolean setRequestTime = true;
        public Long snapshotStartTimestampSeconds = null;
        public Long snapshotStartTimestampNanos = null;
        public List<Long> timestampsSecondsList = null;
        public List<Long> timestampNanosList = null;
        public Long samplingClockStartSeconds = null;
        public Long samplingClockStartNanos = null;
        public Long samplingClockPeriodNanos = null;
        public Integer samplingClockCount = null;
        public List<String> columnNames = null;
        public IngestionDataType dataType = null;
        public List<List<Object>> values = null;
        public Map<String, String> attributes = null;
        public String eventDescription = null;
        public Long eventStartSeconds = null;
        public Long eventStartNanos = null;
        public Long eventStopSeconds = null;
        public Long eventStopNanos = null;

        public IngestionRequestParams(
                Integer providerId,
                String requestId,
                Long snapshotStartTimestampSeconds,
                Long snapshotStartTimestampNanos,
                List<Long> timestampsSecondsList,
                List<Long> timestampNanosList,
                Long samplingClockStartSeconds,
                Long samplingClockStartNanos,
                Long samplingClockPeriodNanos,
                Integer samplingClockCount,
                List<String> columnNames,
                IngestionDataType dataType,
                List<List<Object>> values) {

            this.providerId = providerId;
            this.requestId = requestId;
            this.snapshotStartTimestampSeconds = snapshotStartTimestampSeconds;
            this.snapshotStartTimestampNanos = snapshotStartTimestampNanos;
            this.timestampsSecondsList = timestampsSecondsList;
            this.timestampNanosList = timestampNanosList;
            this.samplingClockStartSeconds = samplingClockStartSeconds;
            this.samplingClockStartNanos = samplingClockStartNanos;
            this.samplingClockPeriodNanos = samplingClockPeriodNanos;
            this.samplingClockCount = samplingClockCount;
            this.columnNames = columnNames;
            this.dataType = dataType;
            this.values = values;
        }

        public IngestionRequestParams(
                Integer providerId,
                String requestId,
                Long snapshotStartTimestampSeconds,
                Long snapshotStartTimestampNanos,
                List<Long> timestampsSecondsList,
                List<Long> timestampNanosList,
                Long samplingClockStartSeconds,
                Long samplingClockStartNanos,
                Long samplingClockPeriodNanos,
                Integer samplingClockCount,
                List<String> columnNames,
                IngestionDataType dataType,
                List<List<Object>> values,
                Map<String, String> attributes,
                String eventDescription,
                Long eventStartSeconds,
                Long eventStartNanos,
                Long eventStopSeconds,
                Long eventStopNanos) {

            this(
                    providerId,
                    requestId,
                    snapshotStartTimestampSeconds,
                    snapshotStartTimestampNanos,
                    timestampsSecondsList,
                    timestampNanosList,
                    samplingClockStartSeconds,
                    samplingClockStartNanos,
                    samplingClockPeriodNanos,
                    samplingClockCount,
                    columnNames,
                    dataType,
                    values);

            this.attributes = attributes;
            this.eventDescription = eventDescription;
            this.eventStartSeconds = eventStartSeconds;
            this.eventStartNanos = eventStartNanos;
            this.eventStopSeconds = eventStopSeconds;
            this.eventStopNanos = eventStopNanos;
        }

        public void setRequestTime(boolean setRequestTime) {
            this.setRequestTime = setRequestTime;
        }
    }

    public static IngestDataRequest buildIngestionRequest(IngestionRequestParams params) {
        return buildIngestionRequest(params, null);
    }
    /**
     * Builds an IngestionRequest gRPC API object from an IngestionRequestParams object.
     * This utility avoids having code to build API requests scattered around the test methods.
     *
     * @param params
     * @return
     */
    public static IngestDataRequest buildIngestionRequest(IngestionRequestParams params, List<DataColumn> dataColumnList) {

        IngestDataRequest.Builder requestBuilder = IngestDataRequest.newBuilder();

        if (params.providerId != null) {
            requestBuilder.setProviderId(params.providerId);
        }
        if (params.requestId != null) {
            requestBuilder.setClientRequestId(params.requestId);
        }
        if (params.setRequestTime) {
            requestBuilder.setRequestTime(GrpcUtility.getTimestampNow());
        }

        // set event description if snapshotTimestamp specified
        if (params.snapshotStartTimestampSeconds != null) {
            EventMetadata.Builder EventMetadataBuilder = EventMetadata.newBuilder();
            final Timestamp.Builder snapshotTimestampBuilder = Timestamp.newBuilder();
            snapshotTimestampBuilder.setEpochSeconds(params.snapshotStartTimestampSeconds);
            if (params.snapshotStartTimestampNanos != null) snapshotTimestampBuilder.setNanoseconds(params.snapshotStartTimestampNanos);
            snapshotTimestampBuilder.build();
            EventMetadataBuilder.setStartTimestamp(snapshotTimestampBuilder);
            EventMetadataBuilder.build();
            requestBuilder.setEventMetadata(EventMetadataBuilder);
        }

        IngestDataRequest.IngestionDataFrame.Builder dataFrameBuilder
                = IngestDataRequest.IngestionDataFrame.newBuilder();
        DataTimestamps.Builder dataTimestampsBuilder = DataTimestamps.newBuilder();

        // set list of timestamps if specified
        if (params.timestampsSecondsList != null) {
            assertTrue("timestampNanosList must be specified when timestampSecondsList is provided", params.timestampNanosList != null);
            assertTrue("size of timestampSecondsList and timestampNanosList must match", params.timestampsSecondsList.size() == params.timestampNanosList.size());
            TimestampList.Builder timestampListBuilder = TimestampList.newBuilder();
            for (int i = 0; i < params.timestampsSecondsList.size(); i++) {
                long seconds = params.timestampsSecondsList.get(i);
                long nanos = params.timestampNanosList.get(i);
                Timestamp.Builder timestampBuilder = Timestamp.newBuilder();
                timestampBuilder.setEpochSeconds(seconds);
                timestampBuilder.setNanoseconds(nanos);
                timestampBuilder.build();
                timestampListBuilder.addTimestamps(timestampBuilder);
            }
            timestampListBuilder.build();
            dataTimestampsBuilder.setTimestampList(timestampListBuilder);
            dataTimestampsBuilder.build();
            dataFrameBuilder.setDataTimestamps(dataTimestampsBuilder);
        }

        // set timestamp iterator in time spec
        if (params.samplingClockStartSeconds != null) {
            assertTrue(params.samplingClockStartNanos != null);
            assertTrue(params.samplingClockPeriodNanos != null);
            assertTrue(params.samplingClockCount != null);
            Timestamp.Builder startTimeBuilder = Timestamp.newBuilder();
            startTimeBuilder.setEpochSeconds(params.samplingClockStartSeconds);
            startTimeBuilder.setNanoseconds(params.samplingClockStartNanos);
            startTimeBuilder.build();
            SamplingClock.Builder samplingClockBuilder = SamplingClock.newBuilder();
            samplingClockBuilder.setStartTime(startTimeBuilder);
            samplingClockBuilder.setPeriodNanos(params.samplingClockPeriodNanos);
            samplingClockBuilder.setCount(params.samplingClockCount);
            samplingClockBuilder.build();
            dataTimestampsBuilder.setSamplingClock(samplingClockBuilder);
            dataTimestampsBuilder.build();
            dataFrameBuilder.setDataTimestamps(dataTimestampsBuilder);
        }

        // create list of columns if specified
        if (dataColumnList != null) {
            // caller can override building data columns by providing dataColumnList
            for (DataColumn column : dataColumnList) {
                dataFrameBuilder.addDataColumns(column);
            }

        } else if (params.columnNames != null) {
            assertTrue(params.values != null);
            assertTrue(params.columnNames.size() == params.values.size());
            for (int i = 0 ; i < params.columnNames.size() ; i++) {
                DataColumn.Builder dataColumnBuilder = DataColumn.newBuilder();
                dataColumnBuilder.setName(params.columnNames.get(i));
                DataValue dataValue = null;
                for (Object value : params.values.get(i)) {
                    switch (params.dataType) {
                        case STRING -> {
                            dataValue = DataValue.newBuilder().setStringValue((String) value).build();
                        }
                        case DOUBLE -> {
                            dataValue = DataValue.newBuilder().setDoubleValue((Double) value).build();
                        }
                        case INT -> {
                            dataValue = DataValue.newBuilder().setLongValue((Long) value).build();
                        }
                        case BYTE_ARRAY -> {
                        }
                        case BOOLEAN -> {
                            dataValue = DataValue.newBuilder().setBooleanValue((Boolean) value).build();
                        }
                        case IMAGE -> {
                        }
                        case STRUCTURE -> {
                        }
                        case ARRAY_DOUBLE -> {
                            List<Double> doubleList = (List<Double>) value;
                            Array.Builder arrayBuilder = Array.newBuilder();
                            for (Double doubleValue : doubleList) {
                                arrayBuilder.addDataValues(DataValue.newBuilder().setDoubleValue(doubleValue).build());
                            }
                            arrayBuilder.build();
                            dataValue = DataValue.newBuilder().setArrayValue(arrayBuilder).build();
                        }
                    }

                    dataColumnBuilder.addDataValues(dataValue);
                }
                dataColumnBuilder.build();
                dataFrameBuilder.addDataColumns(dataColumnBuilder);
            }
        }

        // add attributes if specified
        if (params.attributes != null) {
            for (var attributeEntry : params.attributes.entrySet()) {
                String attributeKey = attributeEntry.getKey();
                String attributeValue = attributeEntry.getValue();
                final Attribute.Builder attributeBuilder = Attribute.newBuilder();
                attributeBuilder.setName(attributeKey);
                attributeBuilder.setValue(attributeValue);
                attributeBuilder.build();
                requestBuilder.addAttributes(attributeBuilder);
            }
        }

        // set event metadata if specified
        if (params.eventDescription != null ||  params.eventStartSeconds != null || params.eventStartNanos != null) {

            EventMetadata.Builder eventMetadataBuilder = EventMetadata.newBuilder();

            if (params.eventDescription != null) {
                eventMetadataBuilder.setDescription(params.eventDescription);
            }

            if (params.eventStartSeconds != null || params.eventStartNanos != null) {
                Timestamp.Builder eventTimeBuilder = Timestamp.newBuilder();
                if (params.eventStartSeconds != null) {
                    eventTimeBuilder.setEpochSeconds(params.eventStartSeconds);
                }
                if (params.eventStartNanos != null) {
                    eventTimeBuilder.setNanoseconds(params.eventStartNanos);
                }
                eventTimeBuilder.build();
                eventMetadataBuilder.setStartTimestamp(eventTimeBuilder);
            }

            eventMetadataBuilder.build();
            requestBuilder.setEventMetadata(eventMetadataBuilder);
        }

        dataFrameBuilder.build();
        requestBuilder.setIngestionDataFrame(dataFrameBuilder);
        return requestBuilder.build();
    }

    /**
     * This class implements the StreamObserver interface for IngestionResponse objects for testing
     * IngestionHandler.handleStreamingIngestionRequest().  The constructor specifies the number of
     * IngestionResponse messages expected by the observer.  A CountDownLatch of the specified size is created
     * and decremented for each message received.  The user can use await() to know when all responses have been
     * received.
     */
    public static class IngestionResponseObserver implements StreamObserver<IngestDataResponse> {

        // instance variables
        CountDownLatch finishLatch = null;
        private final List<IngestDataResponse> responseList = Collections.synchronizedList(new ArrayList<>());
        private final AtomicBoolean isError = new AtomicBoolean(false);

        public IngestionResponseObserver(int expectedResponseCount) {
            this.finishLatch = new CountDownLatch(expectedResponseCount);
        }

        public void await() {
            try {
                finishLatch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                System.err.println("InterruptedException waiting for finishLatch");
                isError.set(true);
            }
        }

        public List<IngestDataResponse> getResponseList() {
            return responseList;
        }

        public boolean isError() { return isError.get(); }

        @Override
        public void onNext(IngestDataResponse ingestionResponse) {
            responseList.add(ingestionResponse);
            finishLatch.countDown();
        }

        @Override
        public void onError(Throwable t) {
            Status status = Status.fromThrowable(t);
            System.err.println("IngestionResponseObserver error: " + status);
            isError.set(true);
        }

        @Override
        public void onCompleted() {
        }
    }

}
