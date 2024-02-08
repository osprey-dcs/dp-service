package com.ospreydcs.dp.service.ingest;

import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.ingestion.*;
import com.ospreydcs.dp.service.common.grpc.GrpcUtility;
import com.ospreydcs.dp.service.ingest.service.IngestionServiceImpl;
import io.grpc.Status;
import io.grpc.internal.GrpcUtil;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Provides features and utilities for testing of ingestion service by inheritance to derived classes.
 */
public class IngestionTestBase {

    public static enum IngestionDataType {
        STRING,
        FLOAT,
        INT,
        BYTE_ARRAY,
        BOOLEAN,
        IMAGE,
        STRUCTURE,
        ARRAY_FLOAT
    }

    /**
     * Encapsulates the parameters for creating an IngestionRequest API object.
     */
    public static class IngestionRequestParams {

        public Integer providerId = null;
        public String requestId = null;
        public boolean setRequestTime = true;
        public Long snapshotTimestampSeconds = null;
        public Long snapshotTimestampNanos = null;
        public List<Long> timestampsSecondsList = null;
        public List<Long> timestampNanosList = null;
        public Long timeSpecIteratorStartSeconds = null;
        public Long timeSpecIteratorStartNanos = null;
        public Long timeSpecIteratorSampleIntervalNanos = null;
        public Integer timeSpecIteratorNumSamples = null;
        public List<String> columnNames = null;
        public IngestionDataType dataType = null;
        public List<List<Object>> values = null;
        public Map<String, String> attributes = null;
        public String eventDescription = null;
        public Long eventSeconds = null;
        public Long eventNanos = null;

        public IngestionRequestParams(
                Integer providerId,
                String requestId,
                Long snapshotTimestampSeconds,
                Long snapshotTimestampNanos,
                List<Long> timestampsSecondsList,
                List<Long> timestampNanosList,
                Long timeSpecIteratorStartSeconds,
                Long timeSpecIteratorStartNanos,
                Long timeSpecIteratorSampleIntervalNanos,
                Integer timeSpecIteratorNumSamples,
                List<String> columnNames,
                IngestionDataType dataType,
                List<List<Object>> values) {

            this.providerId = providerId;
            this.requestId = requestId;
            this.snapshotTimestampSeconds = snapshotTimestampSeconds;
            this.snapshotTimestampNanos = snapshotTimestampNanos;
            this.timestampsSecondsList = timestampsSecondsList;
            this.timestampNanosList = timestampNanosList;
            this.timeSpecIteratorStartSeconds = timeSpecIteratorStartSeconds;
            this.timeSpecIteratorStartNanos = timeSpecIteratorStartNanos;
            this.timeSpecIteratorSampleIntervalNanos = timeSpecIteratorSampleIntervalNanos;
            this.timeSpecIteratorNumSamples = timeSpecIteratorNumSamples;
            this.columnNames = columnNames;
            this.dataType = dataType;
            this.values = values;
        }

        public IngestionRequestParams(
                Integer providerId,
                String requestId,
                Long snapshotTimestampSeconds,
                Long snapshotTimestampNanos,
                List<Long> timestampsSecondsList,
                List<Long> timestampNanosList,
                Long timeSpecIteratorStartSeconds,
                Long timeSpecIteratorStartNanos,
                Long timeSpecIteratorSampleIntervalNanos,
                Integer timeSpecIteratorNumSamples,
                List<String> columnNames,
                IngestionDataType dataType,
                List<List<Object>> values,
                Map<String, String> attributes,
                String eventDescription,
                Long eventSeconds,
                Long eventNanos) {

            this(
                    providerId,
                    requestId,
                    snapshotTimestampSeconds,
                    snapshotTimestampNanos,
                    timestampsSecondsList,
                    timestampNanosList,
                    timeSpecIteratorStartSeconds,
                    timeSpecIteratorStartNanos,
                    timeSpecIteratorSampleIntervalNanos,
                    timeSpecIteratorNumSamples,
                    columnNames,
                    dataType,
                    values);

            this.attributes = attributes;
            this.eventDescription = eventDescription;
            this.eventSeconds = eventSeconds;
            this.eventNanos = eventNanos;
        }

        public void setRequestTime(boolean setRequestTime) {
            this.setRequestTime = setRequestTime;
        }
    }

    public static IngestionRequest buildIngestionRequest(IngestionRequestParams params) {
        return buildIngestionRequest(params, null);
    }
    /**
     * Builds an IngestionRequest gRPC API object from an IngestionRequestParams object.
     * This utility avoids having code to build API requests scattered around the test methods.
     * @param params
     * @return
     */
    public static IngestionRequest buildIngestionRequest(IngestionRequestParams params, List<DataColumn> dataColumnList) {

        IngestionRequest.Builder requestBuilder = IngestionRequest.newBuilder();

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
        if (params.snapshotTimestampSeconds != null) {
            EventMetadata.Builder EventMetadataBuilder = EventMetadata.newBuilder();
            final Timestamp.Builder snapshotTimestampBuilder = Timestamp.newBuilder();
            snapshotTimestampBuilder.setEpochSeconds(params.snapshotTimestampSeconds);
            if (params.snapshotTimestampNanos != null) snapshotTimestampBuilder.setNanoseconds(params.snapshotTimestampNanos);
            snapshotTimestampBuilder.build();
            EventMetadataBuilder.setEventTimestamp(snapshotTimestampBuilder);
            EventMetadataBuilder.build();
            requestBuilder.setEventMetadata(EventMetadataBuilder);
        }

        DataTable.Builder dataTableBuilder = DataTable.newBuilder();
        DataTimeSpec.Builder timeSpecBuilder = DataTimeSpec.newBuilder();

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
            timeSpecBuilder.setTimestampList(timestampListBuilder);
            timeSpecBuilder.build();
            dataTableBuilder.setDataTimeSpec(timeSpecBuilder);
        }

        // set timestamp iterator in time spec
        if (params.timeSpecIteratorStartSeconds != null) {
            assertTrue("timeSpecIteratorStartNanos must be specified", params.timeSpecIteratorStartNanos != null);
            assertTrue("timeSpecIteratorSampleIntervalNanos must be specified", params.timeSpecIteratorSampleIntervalNanos != null);
            assertTrue("timeSpecIteratorNumSamples must be specified", params.timeSpecIteratorNumSamples != null);
            Timestamp.Builder startTimeBuilder = Timestamp.newBuilder();
            startTimeBuilder.setEpochSeconds(params.timeSpecIteratorStartSeconds);
            startTimeBuilder.setNanoseconds(params.timeSpecIteratorStartNanos);
            startTimeBuilder.build();
            FixedIntervalTimestampSpec.Builder fixedIntervalSpecBuilder = FixedIntervalTimestampSpec.newBuilder();
            fixedIntervalSpecBuilder.setStartTime(startTimeBuilder);
            fixedIntervalSpecBuilder.setSampleIntervalNanos(params.timeSpecIteratorSampleIntervalNanos);
            fixedIntervalSpecBuilder.setNumSamples(params.timeSpecIteratorNumSamples);
            fixedIntervalSpecBuilder.build();
            timeSpecBuilder.setFixedIntervalTimestampSpec(fixedIntervalSpecBuilder);
            timeSpecBuilder.build();
            dataTableBuilder.setDataTimeSpec(timeSpecBuilder);
        }

        // create list of columns if specified
        if (dataColumnList != null) {
            // caller can override building data columns by providing dataColumnList
            for (DataColumn column : dataColumnList) {
                dataTableBuilder.addDataColumns(column);
            }

        } else if (params.columnNames != null) {
            assertTrue("values list must be specified when columnNames list is provided", params.values != null);
            assertTrue("size of columnNames and values lists must match", params.columnNames.size() == params.values.size());
            for (int i = 0 ; i < params.columnNames.size() ; i++) {
                DataColumn.Builder dataColumnBuilder = DataColumn.newBuilder();
                dataColumnBuilder.setName(params.columnNames.get(i));
                DataValue dataValue = null;
                for (Object value : params.values.get(i)) {
                    switch (params.dataType) {
                        case STRING -> {
                            dataValue = DataValue.newBuilder().setStringValue((String) value).build();
                        }
                        case FLOAT -> {
                            dataValue = DataValue.newBuilder().setFloatValue((Double) value).build();
                        }
                        case INT -> {
                            dataValue = DataValue.newBuilder().setIntValue((Long) value).build();
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
                        case ARRAY_FLOAT -> {
                            List<Double> doubleList = (List<Double>) value;
                            Array.Builder arrayBuilder = Array.newBuilder();
                            for (Double doubleValue : doubleList) {
                                arrayBuilder.addDataValues(DataValue.newBuilder().setFloatValue(doubleValue).build());
                            }
                            arrayBuilder.build();
                            dataValue = DataValue.newBuilder().setArrayValue(arrayBuilder).build();
                        }
                    }

                    dataColumnBuilder.addDataValues(dataValue);
                }
                dataColumnBuilder.build();
                dataTableBuilder.addDataColumns(dataColumnBuilder);
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
        if (params.eventDescription != null ||  params.eventSeconds != null || params.eventNanos != null) {

            EventMetadata.Builder eventMetadataBuilder = EventMetadata.newBuilder();

            if (params.eventDescription != null) {
                eventMetadataBuilder.setEventDescription(params.eventDescription);
            }

            if (params.eventSeconds != null || params.eventNanos != null) {
                Timestamp.Builder eventTimeBuilder = Timestamp.newBuilder();
                if (params.eventSeconds != null) {
                    eventTimeBuilder.setEpochSeconds(params.eventSeconds);
                }
                if (params.eventNanos != null) {
                    eventTimeBuilder.setNanoseconds(params.eventNanos);
                }
                eventTimeBuilder.build();
                eventMetadataBuilder.setEventTimestamp(eventTimeBuilder);
            }

            eventMetadataBuilder.build();
            requestBuilder.setEventMetadata(eventMetadataBuilder);
        }

        dataTableBuilder.build();
        requestBuilder.setDataTable(dataTableBuilder);
        return requestBuilder.build();
    }

    /**
     * This class implements the StreamObserver interface for IngestionResponse objects for testing
     * IngestionHandler.handleStreamingIngestionRequest().  The constructor specifies the number of
     * IngestionResponse messages expected by the observer.  A CountDownLatch of the specified size is created
     * and decremented for each message received.  The user can use await() to know when all responses have been
     * received.
     */
    public static class IngestionResponseObserver implements StreamObserver<IngestionResponse> {

        // instance variables
        CountDownLatch finishLatch = null;
        private final List<IngestionResponse> responseList = Collections.synchronizedList(new ArrayList<>());
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

        public List<IngestionResponse> getResponseList() {
            return responseList;
        }

        public boolean isError() { return isError.get(); }

        @Override
        public void onNext(IngestionResponse ingestionResponse) {
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
