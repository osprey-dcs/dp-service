package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.query.ColumnTable;
import com.ospreydcs.dp.grpc.v1.query.QuerySamplesResponse;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.exception.NonScalarColumnException;
import com.ospreydcs.dp.service.common.model.TimestampDataMap;
import com.ospreydcs.dp.service.common.utility.TabularDataUtility;
import com.ospreydcs.dp.service.query.handler.model.ResolvedQuery;
import com.ospreydcs.dp.service.query.handler.mongo.MongoQueryHandler;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Server-streaming {@code querySamplesStream} formatter (Q4/Q5/Q8/Q9). Fire-and-consume: assembles
 * the complete union table for the whole window <em>once</em> (like V1 queryTable), then emits it in
 * row-chunks of {@code limit} timestamps — sidestepping the unary path's window-sizing/token
 * machinery entirely (there is no resumable boundary).
 *
 * <p>Column seeding (Q9) is computed once, so the column set and order are trivially stable across
 * streamed chunks. Each chunk also respects the outgoing message-size budget: a chunk is flushed
 * before adding a row that would exceed the wire limit, even if it holds fewer than {@code limit}
 * rows. {@code nextPageToken} is empty on every message (the stream signals completion via
 * {@code onCompleted}). An empty result emits a single empty message. A non-scalar PV is rejected
 * (Q4), same as the unary path.
 *
 * <p><b>Memory note:</b> this materializes the full table server-side (bounded by heap, not the
 * per-message byte limit); for very large ranges the bounded-memory, resumable unary
 * {@code querySamples} is the intended path.
 */
public class QuerySamplesStreamDispatcher extends AbstractQuerySamplesDispatcher {

    private static final Logger logger = LogManager.getLogger();

    private final StreamObserver<QuerySamplesResponse> responseObserver;

    public QuerySamplesStreamDispatcher(StreamObserver<QuerySamplesResponse> responseObserver) {
        this(responseObserver, MongoQueryHandler.getOutgoingMessageSizeLimitBytes());
    }

    /** Package/test constructor allowing the outgoing message-size budget to be injected. */
    public QuerySamplesStreamDispatcher(StreamObserver<QuerySamplesResponse> responseObserver, long byteBudget) {
        super(byteBudget);
        this.responseObserver = responseObserver;
    }

    @Override
    public void executeAndDispatch(ResolvedQuery resolvedQuery, MongoQueryClientInterface mongoClient) {

        if (resolvedQuery.isEmptyResult()) {
            emitEmptyChunkAndComplete();
            return;
        }

        final long[] window = computeWindow(resolvedQuery);

        final MongoCursor<BucketDocument> cursor =
                mongoClient.executeQuerySamplesV2(resolvedQuery, window[0], window[1]);

        if (cursor == null) {
            emitEmptyChunkAndComplete();
            return;
        }

        final TimestampDataMap tableValueMap = seededTable(resolvedQuery);

        try (cursor) {
            // Assemble the full window once. No sizeLimit: streaming materializes the whole table
            // (memory-bounded, per the class note); the byte budget bounds each emitted chunk, below.
            // Trimming uses every resolved fragment rather than the collapsed window (#207).
            TabularDataUtility.addBucketsToTable(
                    tableValueMap, cursor, 0, null,
                    retentionIntervals(resolvedQuery, window[0], window[1]));
        } catch (NonScalarColumnException e) {
            final String msg = "querySamples supports scalar PVs only: PV '" + e.getPvName()
                    + "' has non-scalar column type " + e.getColumnType() + "; use queryBuckets";
            logger.debug(msg);
            QueryServiceImpl.sendQuerySamplesResponseReject(msg, responseObserver);
            return;
        } catch (DpException e) {
            final String msg = "exception building sample result: " + e.getMessage();
            logger.error(msg, e);
            QueryServiceImpl.sendQuerySamplesResponseError(msg, responseObserver);
            return;
        }

        final List<long[]> timestamps = collectTimestamps(tableValueMap);
        if (timestamps.isEmpty()) {
            emitEmptyChunkAndComplete();
            return;
        }

        final int chunkRowLimit = resolvedQuery.getPageSize();
        final List<String> columnNames = tableValueMap.getColumnNameList();
        final int columnCount = columnNames.size();
        final boolean useSerialized = resolvedQuery.isUseSerializedColumns();
        // fixed per-column overhead (column name + repeated-field framing), counted once per row so
        // the estimate stays a conservative upper bound of the emitted ColumnTable size.
        final long perColumnOverhead = columnOverhead(columnNames, useSerialized);

        int chunkStart = 0;
        long chunkBytes = 0;

        for (int rowIndex = 0; rowIndex < timestamps.size(); rowIndex++) {
            final long rowBytes = rowByteEstimate(
                    tableValueMap, timestamps, rowIndex, columnCount, perColumnOverhead);
            final int rowsInChunk = rowIndex - chunkStart;

            // indivisible-oversized guard (mirrors the buckets streaming dispatcher): a single row
            // larger than the whole budget cannot be chunked. Error out naming the timestamp rather
            // than emit an over-limit message that gRPC would abort the whole stream on.
            if (rowsInChunk == 0 && rowBytes > byteBudget) {
                final String msg = "single querySamples row at timestamp "
                        + timestamps.get(rowIndex)[0] + "." + timestamps.get(rowIndex)[1]
                        + " exceeds the outgoing message size limit (" + rowBytes + " > "
                        + byteBudget + " bytes); narrow the PV set or time range";
                logger.error(msg);
                QueryServiceImpl.sendQuerySamplesResponseError(msg, responseObserver);
                return;
            }

            // byte flush: adding this row would overflow the budget and the chunk already has >= 1 row
            if (rowsInChunk >= 1 && chunkBytes + rowBytes > byteBudget) {
                emitChunk(tableValueMap, timestamps, chunkStart, rowIndex, useSerialized);
                chunkStart = rowIndex;
                chunkBytes = 0;
            }

            chunkBytes += rowBytes;

            // count flush: the chunk reached the per-message row limit (inclusive of this row)
            if (rowIndex - chunkStart + 1 >= chunkRowLimit) {
                emitChunk(tableValueMap, timestamps, chunkStart, rowIndex + 1, useSerialized);
                chunkStart = rowIndex + 1;
                chunkBytes = 0;
            }
        }

        // flush the trailing partial chunk
        if (chunkStart < timestamps.size()) {
            emitChunk(tableValueMap, timestamps, chunkStart, timestamps.size(), useSerialized);
        }

        responseObserver.onCompleted();
    }

    /**
     * Conservative <b>upper bound</b> on the serialized size one table row contributes to the emitted
     * {@link ColumnTable} — the timestamp entry plus, for every column, the value's serialized size
     * (or the unset-value framing) and this row's share of the per-column name/framing overhead
     * ({@code perColumnOverhead}, precomputed by {@link #columnOverhead}). It must not under-count:
     * the chunk boundary relies on it to keep each emitted message under the gRPC wire limit.
     */
    private static long rowByteEstimate(
            TimestampDataMap tableValueMap, List<long[]> timestamps, int rowIndex, int columnCount,
            long perColumnOverhead) {
        final long second = timestamps.get(rowIndex)[0];
        final long nano = timestamps.get(rowIndex)[1];
        // Timestamp: two int64 fields (up to 10 bytes varint each) + field tags/length framing.
        long bytes = 24;
        // ORDERING (#199): buildColumnTable drains each row it emits, so this must run before the
        // row is emitted. The chunk loop guarantees that -- a row is always estimated at index
        // rowIndex before any emitChunk whose range reaches it -- but a future change that emits
        // ahead of estimating would read a drained row here. Fail loudly rather than under-count:
        // this estimate bounds the chunk against the gRPC wire limit, so silently treating a
        // missing row as zero bytes would produce an over-limit message that aborts the stream.
        final Map<Integer, DataValue> rowValues = tableValueMap.get(second, nano);
        if (rowValues == null) {
            throw new IllegalStateException(
                    "querySamples row at timestamp " + second + "." + nano
                            + " was drained before its size was estimated; rowByteEstimate must run"
                            + " before the emitChunk that consumes the row (#199)");
        }
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            final DataValue value = rowValues.get(columnIndex);
            // value payload + repeated DataValue tag+length framing (a few bytes); unset value is one
            // empty DataValue message which still costs its framing.
            bytes += ((value != null) ? value.getSerializedSize() : 0) + 6;
        }
        return bytes + perColumnOverhead;
    }

    /**
     * Per-row share of the fixed per-column overhead: each emitted column carries its UTF-8 name and
     * repeated-field framing regardless of how many rows it holds. Amortizing the whole overhead onto
     * every row keeps {@link #rowByteEstimate} a strict upper bound (a chunk of N rows is charged N×
     * the overhead, never less than the single copy actually emitted). Under
     * {@code useSerializedColumns} each column is additionally wrapped in a {@code SerializedDataColumn}
     * (name + payload framing), so the per-column overhead is larger.
     */
    private static long columnOverhead(List<String> columnNames, boolean useSerialized) {
        long overhead = 0;
        for (String name : columnNames) {
            final long nameBytes = name.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            // column name field (tag+len+bytes) + repeated-column tag/length framing; serialized
            // columns pay it twice (inner DataColumn name + outer SerializedDataColumn name/payload).
            overhead += nameBytes + 8;
            if (useSerialized) {
                overhead += nameBytes + 8;
            }
        }
        return overhead;
    }

    private void emitChunk(
            TimestampDataMap tableValueMap, List<long[]> timestamps,
            int fromRow, int toRow, boolean useSerialized) {
        final ColumnTable columnTable = buildColumnTable(tableValueMap, timestamps, fromRow, toRow, useSerialized);
        emit(columnTable);
    }

    private void emitEmptyChunkAndComplete() {
        emit(ColumnTable.getDefaultInstance());
        responseObserver.onCompleted();
    }

    private void emit(ColumnTable columnTable) {
        final QuerySamplesResponse.SampleQueryResult result =
                QuerySamplesResponse.SampleQueryResult.newBuilder()
                        .setColumnTable(columnTable)
                        .setNextPageToken("") // stream signals completion; token always empty
                        .build();
        responseObserver.onNext(QueryServiceImpl.querySamplesResponse(result));
    }
}
