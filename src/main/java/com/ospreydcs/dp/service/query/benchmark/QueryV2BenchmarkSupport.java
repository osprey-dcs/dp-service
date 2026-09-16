package com.ospreydcs.dp.service.query.benchmark;

import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.SerializedDataColumn;
import com.ospreydcs.dp.grpc.v1.common.TimeRange;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.query.ColumnTable;
import com.ospreydcs.dp.grpc.v1.query.ExecutionOptions;
import com.ospreydcs.dp.grpc.v1.query.PvNameList;
import com.ospreydcs.dp.grpc.v1.query.PvSelector;
import com.ospreydcs.dp.grpc.v1.query.QueryBucketsRequest;
import com.ospreydcs.dp.grpc.v1.query.QuerySamplesRequest;
import com.ospreydcs.dp.grpc.v1.query.QuerySpec;
import com.ospreydcs.dp.grpc.v1.query.QueryTableRequest;

/**
 * Request builders and value counters shared by the Query API V2 and queryTable benchmark clients
 * (issue #275). Counting mirrors {@link QueryDataResponseObserver}: data values are the scalar
 * samples delivered, data bytes are those values at eight bytes each, gRPC bytes are the serialized
 * response messages -- so the printed rates are comparable across every query method.
 */
final class QueryV2BenchmarkSupport {

    private QueryV2BenchmarkSupport() {
    }

    static QuerySpec querySpec(QueryBenchmarkBase.QueryTaskParams params) {
        return QuerySpec.newBuilder()
                .setTimeRange(TimeRange.newBuilder()
                        .setBeginTime(Timestamp.newBuilder().setEpochSeconds(params.startSeconds()))
                        .setEndTime(Timestamp.newBuilder().setEpochSeconds(params.startSeconds() + params.numSeconds())))
                .setPvSelector(PvSelector.newBuilder()
                        .setPvNameList(PvNameList.newBuilder().addAllPvNames(params.columnNames())))
                .build();
    }

    static QuerySamplesRequest samplesRequest(QueryBenchmarkBase.QueryTaskParams params, String pageToken) {
        final ExecutionOptions.Builder options = ExecutionOptions.newBuilder();
        if (pageToken != null && !pageToken.isEmpty()) {
            options.setPageToken(pageToken);
        }
        return QuerySamplesRequest.newBuilder()
                .setQuerySpec(querySpec(params))
                .setExecutionOptions(options)
                .build();
    }

    static QueryBucketsRequest bucketsRequest(QueryBenchmarkBase.QueryTaskParams params, String pageToken) {
        final ExecutionOptions.Builder options = ExecutionOptions.newBuilder();
        if (pageToken != null && !pageToken.isEmpty()) {
            options.setPageToken(pageToken);
        }
        return QueryBucketsRequest.newBuilder()
                .setQuerySpec(querySpec(params))
                .setExecutionOptions(options)
                .build();
    }

    static QueryTableRequest tableRequest(QueryBenchmarkBase.QueryTaskParams params) {
        return QueryTableRequest.newBuilder()
                .setFormat(QueryTableRequest.TableResultFormat.TABLE_FORMAT_COLUMN)
                .setPvNameList(PvNameList.newBuilder().addAllPvNames(params.columnNames()))
                .setBeginTime(Timestamp.newBuilder().setEpochSeconds(params.startSeconds()))
                .setEndTime(Timestamp.newBuilder().setEpochSeconds(params.startSeconds() + params.numSeconds()))
                .build();
    }

    /** Scalar values in a V2 column table: every column's values (serialized columns by their payload's count). */
    static long countValues(ColumnTable table) {
        long values = 0;
        for (DataColumn column : table.getDataColumnsList()) {
            values += column.getDataValuesCount();
        }
        for (SerializedDataColumn column : table.getSerializedDataColumnsList()) {
            try {
                values += DataColumn.parseFrom(column.getPayload()).getDataValuesCount();
            } catch (com.google.protobuf.InvalidProtocolBufferException ex) {
                // counted as zero; the benchmark fixture never carries serialized columns
            }
        }
        return values;
    }

    /** Scalar values in a bucket, as {@link QueryDataResponseObserver} counts them. */
    static long countValues(DataBucket bucket) {
        if (bucket.getDataValues().hasDataColumn()) {
            return bucket.getDataValues().getDataColumn().getDataValuesCount();
        }
        if (bucket.getDataTimestamps().hasSamplingClock()) {
            return bucket.getDataTimestamps().getSamplingClock().getCount();
        }
        if (bucket.getDataTimestamps().hasTimestampList()) {
            return bucket.getDataTimestamps().getTimestampList().getTimestampsCount();
        }
        return 0;
    }
}
