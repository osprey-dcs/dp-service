package com.ospreydcs.dp.client;

import com.ospreydcs.dp.client.result.ApiResultStatus;
import com.ospreydcs.dp.client.result.IngestDataApiResult;
import com.ospreydcs.dp.client.result.RegisterProviderApiResult;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.ingestion.*;
import com.ospreydcs.dp.service.common.protobuf.AttributesUtility;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class IngestionClient extends ServiceApiClientBase {

    public static class RegisterProviderRequestParams {

        public final String name;
        public final String description;
        public final List<String> tags;
        public final Map<String, String> attributes;

        public RegisterProviderRequestParams(String name, Map<String, String> attributes) {
            this.name = name;
            this.description = null;
            this.tags = null;
            this.attributes = attributes;
        }

        public RegisterProviderRequestParams(
                String name,
                String description,
                List<String> tags,
                Map<String, String> attributes
        ) {
            this.name = name;
            this.description = description;
            this.tags = tags;
            this.attributes = attributes;
        }
    }

    public static class RegisterProviderResponseObserver
            extends ApiResponseObserverBase<RegisterProviderResponse> {

        private final List<RegisterProviderResponse> responseList =
                Collections.synchronizedList(new ArrayList<>());

        @Override
        protected boolean hasExceptionalResult(RegisterProviderResponse response) {
            return response.hasExceptionalResult();
        }

        @Override
        protected ExceptionalResult getExceptionalResult(RegisterProviderResponse response) {
            return response.getExceptionalResult();
        }

        @Override
        protected boolean handleResult(RegisterProviderResponse response) {

            if (!response.hasRegistrationResult()) {
                recordFailure(observerName() + " response does not contain RegistrationResult");
                return false;
            }

            responseList.add(response);
            return true;
        }

        public List<RegisterProviderResponse> getResponseList() {
            return responseList;
        }
    }

    public static enum IngestionDataType {
        STRING,
        BOOLEAN,
        UINT,
        ULONG,
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        BYTE_ARRAY,
        ARRAY,
        ARRAY_DOUBLE,
        IMAGE,
        STRUCTURE,
        TIMESTAMP
    }

    public static class IngestionRequestParams {

        public String providerId = null;
        public String requestId = null;
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
        public List<List<DataValue.ValueStatus>> valuesStatus = null;

        /*
         * Optional column-level metadata (provenance, tags, attributes) applied to each DataColumn
         * that buildIngestionRequest() adds to the request's DataFrame.  The other column types
         * defined by DataFrame also carry a metadata field, but buildIngestionRequest() does not
         * build them, so this field does not apply to them.  A column that already carries its own
         * metadata keeps it; this field is only applied to columns without one.  Left null when no
         * metadata is supplied, in which case the columns are built without a metadata field.
         */
        public ColumnMetadata columnMetadata = null;

        public IngestionRequestParams(
                String providerId,
                String requestId
        ) {
            this.providerId = providerId;
            this.requestId = requestId;
        }

        /*
         * Sets the column metadata applied to each column in the request, and returns this params
         * object so the call can be chained to a constructor invocation.  The supplied message is
         * used as-is with no normalization; unlike the convenience overload below, blank provenance
         * fields are not omitted, and an empty message is not treated as "no metadata".  Use
         * clearColumnMetadata() rather than passing null, which is ambiguous between this method
         * and the overload below and so requires an explicit cast at the call site.
         */
        public IngestionRequestParams setColumnMetadata(ColumnMetadata columnMetadata) {
            this.columnMetadata = columnMetadata;
            return this;
        }

        /*
         * Clears any column metadata previously set, so that the request's columns are built
         * without a metadata field.  Returns this params object so the call can be chained to a
         * constructor invocation.
         */
        public IngestionRequestParams clearColumnMetadata() {
            this.columnMetadata = null;
            return this;
        }

        /*
         * Convenience method building ColumnMetadata from its constituent parts, applied to each
         * column in the request.  The provenance message is only set if at least one of
         * source/process is supplied, and a blank argument is treated the same as null.  Note that
         * ColumnProvenance.source and .process are plain proto3 strings with no field presence, so
         * an unsupplied one is indistinguishable from an empty one on the wire and reads back as
         * the empty string; the blank checking here determines whether the enclosing provenance
         * message, which does have presence, is set at all.  If none of the arguments supply a
         * value, columnMetadata is left null rather than set to an empty message, so that the
         * columns are built without a metadata field instead of carrying an empty one.  Attribute
         * entries with a null name or value are skipped.  Returns this params object so the call
         * can be chained to a constructor invocation.
         */
        public IngestionRequestParams setColumnMetadata(
                String provenanceSource,
                String provenanceProcess,
                List<String> tags,
                Map<String, String> attributes
        ) {
            final ColumnMetadata.Builder metadataBuilder = ColumnMetadata.newBuilder();

            final boolean hasSource = provenanceSource != null && !provenanceSource.isBlank();
            final boolean hasProcess = provenanceProcess != null && !provenanceProcess.isBlank();
            if (hasSource || hasProcess) {
                final ColumnProvenance.Builder provenanceBuilder = ColumnProvenance.newBuilder();
                if (hasSource) {
                    provenanceBuilder.setSource(provenanceSource);
                }
                if (hasProcess) {
                    provenanceBuilder.setProcess(provenanceProcess);
                }
                metadataBuilder.setProvenance(provenanceBuilder.build());
            }

            if (tags != null && !tags.isEmpty()) {
                metadataBuilder.addAllTags(tags);
            }

            if (attributes != null && !attributes.isEmpty()) {
                for (Map.Entry<String, String> entry : attributes.entrySet()) {
                    // skip incomplete entries, since the protobuf setters reject null
                    if (entry.getKey() == null || entry.getValue() == null) {
                        continue;
                    }
                    metadataBuilder.addAttributes(
                            Attribute.newBuilder()
                                    .setName(entry.getKey())
                                    .setValue(entry.getValue())
                                    .build());
                }
            }

            final ColumnMetadata metadata = metadataBuilder.build();

            // leave columnMetadata null when nothing was actually supplied, so that the columns are
            // built without a metadata field rather than carrying an empty one
            this.columnMetadata =
                    metadata.equals(ColumnMetadata.getDefaultInstance()) ? null : metadata;

            return this;
        }

        public IngestionRequestParams(
                String providerId,
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
                List<List<Object>> values
        ) {
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
                    values,
                    null);
        }

        public IngestionRequestParams(
                String providerId,
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
                List<List<DataValue.ValueStatus>> valuesStatus
        ) {
            this(providerId, requestId);

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
            this.valuesStatus = valuesStatus;

            // buildIngestionRequest() indexes valuesStatus by the values dimensions, so a mismatch
            // would otherwise surface as an IndexOutOfBoundsException from inside the builder,
            // far from the call site that supplied the mismatched lists
            if (valuesStatus != null && values != null) {
                if (valuesStatus.size() != values.size()) {
                    throw new IllegalArgumentException(
                            "valuesStatus size does not match values size: "
                                    + "valuesStatus: " + valuesStatus.size()
                                    + ", values: " + values.size());
                }
                for (int i = 0; i < values.size(); i++) {
                    final List<DataValue.ValueStatus> columnStatus = valuesStatus.get(i);
                    if (columnStatus == null || columnStatus.size() != values.get(i).size()) {
                        throw new IllegalArgumentException(
                                "valuesStatus[" + i + "] size does not match values[" + i + "] size: "
                                        + "valuesStatus: "
                                        + (columnStatus == null ? "null" : columnStatus.size())
                                        + ", values: " + values.get(i).size());
                    }
                }
            }
        }
    }

    public static class IngestDataResponseObserver implements StreamObserver<IngestDataResponse> {

        // instance variables
        CountDownLatch finishLatch = null;
        private final List<IngestDataResponse> responseList = Collections.synchronizedList(new ArrayList<>());
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        // categorizes a failure so callers can distinguish a service rejection from a service
        // error.  Defaults to NONE, which ApiResultBase maps to LOCAL_FAILURE when a failure was
        // recorded without a status-bearing response -- a transport error, an await timeout, a
        // malformed response sequence.  Defaulting to NONE rather than LOCAL_FAILURE keeps this
        // getter honest for a call that succeeded.  Only the first status recorded is kept, so
        // that the status and the message returned by getErrorMessage() describe the same failure.
        private final AtomicReference<ApiResultStatus> apiResultStatus =
                new AtomicReference<>(ApiResultStatus.NONE);

        private final int expectedResponseCount;

        public IngestDataResponseObserver(int expectedResponseCount) {
            this.expectedResponseCount = expectedResponseCount;
            this.finishLatch = new CountDownLatch(expectedResponseCount);
        }

        public void await() {
            try {
                // this latch counts down once per expected response, so an expired await means
                // fewer responses arrived than were requested; report that as an error rather
                // than letting the caller treat a truncated responseList as complete
                if (!finishLatch.await(1, TimeUnit.MINUTES)) {
                    final String errorMsg = "timed out waiting for finishLatch, received "
                            + responseList.size() + " of " + expectedResponseCount + " responses";
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);
                }
            } catch (InterruptedException e) {
                final String errorMsg = "InterruptedException waiting for finishLatch";
                System.err.println(errorMsg);
                isError.set(true);
                errorMessageList.add(errorMsg);
                // restore the interrupt flag so callers up the stack can still observe it
                Thread.currentThread().interrupt();
            }
        }

        public List<IngestDataResponse> getResponseList() {
            return responseList;
        }

        public boolean isError() { return isError.get(); }

        public String getErrorMessage() {
            if (!errorMessageList.isEmpty()) {
                return errorMessageList.get(0);
            } else {
                return "";
            }
        }

        public ApiResultStatus getApiResultStatus() { return apiResultStatus.get(); }

        @Override
        public void onNext(IngestDataResponse response) {

            if (response.hasExceptionalResult()) {
                // the service's message is recorded verbatim for the caller; this observer's
                // identity goes on the console line only.  See the message contract on
                // ApiResponseObserverBase, which this observer cannot extend because it accepts
                // more than one response.
                final String errorMsg = response.getExceptionalResult().getMessage();
                System.err.println(
                        "IngestDataResponseObserver onNext received exceptional response: "
                                + errorMsg);
                apiResultStatus.compareAndSet(
                        ApiResultStatus.NONE,
                        ApiResultStatus.fromProto(response.getExceptionalResult().getExceptionalResultStatus()));
                isError.set(true);
                errorMessageList.add(errorMsg);
                finishLatch.countDown();
                return;
            }

            responseList.add(response);
            finishLatch.countDown();
        }

        @Override
        public void onError(Throwable t) {
            final Status status = Status.fromThrowable(t);
            final String errorMsg = "IngestDataResponseObserver error: " + status;
            System.err.println(errorMsg);
            isError.set(true);
            errorMessageList.add(errorMsg);
            // this latch is initialized to expectedResponseCount, so a stream that fails after
            // some responses have arrived still has counts outstanding.  Release all of them,
            // otherwise await() expires and reports a timeout instead of the gRPC status.
            while (finishLatch.getCount() > 0) {
                finishLatch.countDown();
            }
        }

        @Override
        public void onCompleted() {
        }
    }

    // static variables
    private static final Logger logger = LogManager.getLogger();

    public IngestionClient(ManagedChannel channel) {
        super(channel);
    }

    public static RegisterProviderRequest buildRegisterProviderRequest(RegisterProviderRequestParams params) {

        RegisterProviderRequest.Builder builder = RegisterProviderRequest.newBuilder();

        if (params.name != null) {
            builder.setProviderName(params.name);
        }

        if (params.description != null) {
            builder.setDescription(params.description);
        }

        if (params.tags != null) {
            builder.addAllTags(params.tags);
        }

        if (params.attributes != null) {
            builder.addAllAttributes(AttributesUtility.attributeListFromMap(params.attributes));
        }

        return builder.build();
    }

    public RegisterProviderApiResult sendRegisterProvider(
            RegisterProviderRequest request
    ) {
        final DpIngestionServiceGrpc.DpIngestionServiceStub asyncStub =
                DpIngestionServiceGrpc.newStub(channel);

        final RegisterProviderResponseObserver responseObserver =
                new RegisterProviderResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.registerProvider(request, responseObserver);
        }).start();

        responseObserver.await();

        if (responseObserver.isError()) {
            return new RegisterProviderApiResult(
                    true, responseObserver.getErrorMessage(), responseObserver.getApiResultStatus());
        } else {
            return new RegisterProviderApiResult(responseObserver.getResponseList().get(0));
        }
    }

    public RegisterProviderApiResult registerProvider(RegisterProviderRequestParams params) {
        // build request
        final RegisterProviderRequest request = buildRegisterProviderRequest(params);
        return sendRegisterProvider(request);
    }

    /**
     * Builds an IngestionRequest gRPC API object from an IngestionRequestParams object.
     * This utility avoids having code to build API requests scattered around the test methods.
     *
     * @param params
     * @return
     */
    public static IngestDataRequest buildIngestionRequest(
            IngestionRequestParams params,
            List<Timestamp> timestamps,
            List<DataColumn> dataColumns
    ) {
        IngestDataRequest.Builder requestBuilder = IngestDataRequest.newBuilder();

        if (params.providerId != null) {
            requestBuilder.setProviderId(params.providerId);
        }
        if (params.requestId != null) {
            requestBuilder.setClientRequestId(params.requestId);
        }

        final DataFrame.Builder dataFrameBuilder = DataFrame.newBuilder();
        final DataTimestamps.Builder dataTimestampsBuilder = DataTimestamps.newBuilder();

        // set DataTimestamps for request
        if (timestamps != null) {
            final TimestampList timestampList = TimestampList.newBuilder().addAllTimestamps(timestamps).build();
            dataTimestampsBuilder.setTimestampList(timestampList);
            dataTimestampsBuilder.build();
            dataFrameBuilder.setDataTimestamps(dataTimestampsBuilder);

        } else if (params.timestampsSecondsList != null) {
            // use explicit timestamp list in DataTimestamps if specified in params

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

        } else if (params.samplingClockStartSeconds != null) {
            // otherwise use Samplingclock for DataTimestamps

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

        // create list of columns
        final List<DataColumn> frameColumns = new ArrayList<>();

        if (dataColumns != null) {
            // use list of columns if provided by caller
            frameColumns.addAll(dataColumns);

        } else if (params.columnNames != null) {
            // otherwise create columns from params

            for (int i = 0 ; i < params.columnNames.size() ; i++) {
                DataColumn.Builder dataColumnBuilder = DataColumn.newBuilder();
                dataColumnBuilder.setName(params.columnNames.get(i));
                DataValue.Builder dataValueBuilder = null;
                int valueIndex = 0;
                for (Object value : params.values.get(i)) {
                    switch (params.dataType) {
                        case STRING -> {
                            dataValueBuilder = DataValue.newBuilder().setStringValue((String) value);
                        }
                        case DOUBLE -> {
                            dataValueBuilder = DataValue.newBuilder().setDoubleValue((Double) value);
                        }
                        case INT -> {
                            dataValueBuilder = DataValue.newBuilder().setIntValue((Integer) value);
                        }
                        case BYTE_ARRAY -> {
                        }
                        case BOOLEAN -> {
                            dataValueBuilder = DataValue.newBuilder().setBooleanValue((Boolean) value);
                        }
                        case IMAGE -> {
                        }
                        case STRUCTURE -> {
                        }
                        case ARRAY_DOUBLE -> {
                            List<?> valueList = null;
                            if (value instanceof List) {
                                valueList = (List<?>) value;
                            } else {
                                logger.error("unexpected value list type: " + value.getClass().getName());
                            }
                            Array.Builder arrayBuilder = Array.newBuilder();
                            for (var listElement : valueList) {
                                if (!(listElement instanceof Double)) {
                                    logger.error("unexpected value list element type: " + listElement.getClass().getName());
                                }
                                arrayBuilder.addDataValues(
                                        DataValue.newBuilder()
                                                .setDoubleValue((Double) listElement)
                                                .build());
                            }
                            arrayBuilder.build();
                            dataValueBuilder = DataValue.newBuilder().setArrayValue(arrayBuilder);
                        }
                    }

                    if (params.valuesStatus != null) {
                        DataValue.ValueStatus valueStatus = params.valuesStatus.get(i).get(valueIndex);
                        dataValueBuilder.setValueStatus(valueStatus);
                    }

                    dataColumnBuilder.addDataValues(dataValueBuilder.build());
                    valueIndex++;
                }

                frameColumns.add(dataColumnBuilder.build());
            }
        }

        // apply optional column metadata to each column in the frame, covering both the
        // caller-supplied column list and the columns built from params above.  A column that
        // already carries its own metadata keeps it, since per-column metadata is the more specific
        // intent than the request-wide default in params.
        final List<DataColumn> metadataColumns;
        if (params.columnMetadata != null) {
            metadataColumns = new ArrayList<>(frameColumns.size());
            for (DataColumn frameColumn : frameColumns) {
                if (frameColumn.hasMetadata()) {
                    metadataColumns.add(frameColumn);
                } else {
                    metadataColumns.add(
                            frameColumn.toBuilder().setMetadata(params.columnMetadata).build());
                }
            }
        } else {
            metadataColumns = frameColumns;
        }

        // add regular DataColumns
        dataFrameBuilder.addAllDataColumns(metadataColumns);

        dataFrameBuilder.build();
        requestBuilder.setIngestionDataFrame(dataFrameBuilder);
        return requestBuilder.build();
    }

    public IngestDataApiResult sendIngestData(IngestDataRequest request) {


        final DpIngestionServiceGrpc.DpIngestionServiceStub asyncStub =
                DpIngestionServiceGrpc.newStub(channel);

        final IngestDataResponseObserver responseObserver = new IngestDataResponseObserver(1);

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.ingestData(request, responseObserver);
        }).start();

        responseObserver.await();

        if (responseObserver.isError()) {
            return new IngestDataApiResult(
                    true, responseObserver.getErrorMessage(), responseObserver.getApiResultStatus());
        } else {
            return new IngestDataApiResult(responseObserver.getResponseList().get(0));
        }
    }

    public IngestDataApiResult ingestData(
            IngestionRequestParams params,
            List<Timestamp> timestamps,
            List<DataColumn> dataColumns
    ) {
        final IngestDataRequest request = buildIngestionRequest(params, timestamps, dataColumns);
        return sendIngestData(request);
    }

}
