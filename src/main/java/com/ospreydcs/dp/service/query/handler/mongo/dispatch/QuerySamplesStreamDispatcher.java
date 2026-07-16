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
            TabularDataUtility.addBucketsToTable(
                    tableValueMap, cursor, 0, null, window[0], window[1], window[2], window[3]);
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
        final int columnCount = tableValueMap.getColumnNameList().size();
        final boolean useSerialized = resolvedQuery.isUseSerializedColumns();

        int chunkStart = 0;
        long chunkBytes = 0;

        for (int rowIndex = 0; rowIndex < timestamps.size(); rowIndex++) {
            final long rowBytes = rowByteEstimate(tableValueMap, timestamps, rowIndex, columnCount);
            final int rowsInChunk = rowIndex - chunkStart;

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

    /** Approximate serialized size contributed by one table row (timestamp + per-column values). */
    private static long rowByteEstimate(
            TimestampDataMap tableValueMap, List<long[]> timestamps, int rowIndex, int columnCount) {
        final long second = timestamps.get(rowIndex)[0];
        final long nano = timestamps.get(rowIndex)[1];
        long bytes = 12; // rough per-row Timestamp overhead (two varints + framing)
        final Map<Integer, DataValue> rowValues = tableValueMap.get(second, nano);
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            final DataValue value = rowValues.get(columnIndex);
            bytes += (value != null) ? value.getSerializedSize() : 1; // unset value ~ 1 byte of framing
        }
        return bytes;
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
