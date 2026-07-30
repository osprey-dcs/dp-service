package com.ospreydcs.dp.service.common.bson.bucket;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.SamplingClock;
import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
import com.ospreydcs.dp.service.common.bson.DataTimestampsDocument;
import com.ospreydcs.dp.service.common.bson.column.DataColumnDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Covers deserialization of malformed bucket documents.
 *
 * <p>A stored bucket missing its data column or timestamps must surface as {@link DpException}, not
 * as an unchecked exception. The query dispatchers catch only DpException; anything else escapes the
 * dispatch loop and terminates the response stream, leaving the client unable to tell a
 * deserialization failure from an empty result.
 */
public class BucketDocumentDeserializationTest {

    private static final String PV_NAME = "test_pv";

    private static BucketDocument newBucket(boolean withTimestamps, boolean withColumn) {

        final BucketDocument bucket = new BucketDocument();
        bucket.setId(PV_NAME + "-1700000000-0");
        bucket.setPvName(PV_NAME);

        if (withTimestamps) {
            final SamplingClock samplingClock = SamplingClock.newBuilder()
                    .setStartTime(TimestampUtility.timestampFromSeconds(1_700_000_000L, 0L))
                    .setPeriodNanos(1_000_000_000L)
                    .setCount(2)
                    .build();
            bucket.setDataTimestamps(DataTimestampsDocument.fromDataTimestamps(
                    DataTimestamps.newBuilder().setSamplingClock(samplingClock).build()));
        }

        if (withColumn) {
            final DataColumn.Builder columnBuilder = DataColumn.newBuilder().setName(PV_NAME);
            columnBuilder.addDataValues(DataValue.newBuilder().setDoubleValue(1.0).build());
            columnBuilder.addDataValues(DataValue.newBuilder().setDoubleValue(2.0).build());
            bucket.setDataColumn(DataColumnDocument.fromDataColumn(columnBuilder.build()));
        }

        return bucket;
    }

    private static QueryDataRequest.QuerySpec emptyQuerySpec() {
        return QueryDataRequest.QuerySpec.newBuilder().build();
    }

    @Test
    public void testWellFormedBucketDeserializes() throws Exception {
        assertNotNull(BucketDocument.dataBucketFromDocument(
                newBucket(true, true), emptyQuerySpec()));
        assertNotNull(BucketDocument.dataBucketFromDocumentV2(
                newBucket(true, true), false, false));
    }

    @Test
    public void testMissingDataColumnThrowsDpException() {
        try {
            BucketDocument.dataBucketFromDocument(newBucket(true, false), emptyQuerySpec());
            fail("expected DpException for bucket with no data column");
        } catch (DpException ex) {
            assertTrue(ex.getMessage().contains("has no dataColumn"));
            assertTrue(ex.getMessage().contains(PV_NAME));
        }
    }

    @Test
    public void testMissingDataTimestampsThrowsDpException() {
        try {
            BucketDocument.dataBucketFromDocument(newBucket(false, true), emptyQuerySpec());
            fail("expected DpException for bucket with no data timestamps");
        } catch (DpException ex) {
            assertTrue(ex.getMessage().contains("has no dataTimestamps"));
        }
    }

    @Test
    public void testMissingDataColumnThrowsDpExceptionV2() {
        try {
            BucketDocument.dataBucketFromDocumentV2(newBucket(true, false), false, false);
            fail("expected DpException for bucket with no data column");
        } catch (DpException ex) {
            assertTrue(ex.getMessage().contains("has no dataColumn"));
        }
    }

    @Test
    public void testMissingDataTimestampsThrowsDpExceptionV2() {
        try {
            BucketDocument.dataBucketFromDocumentV2(newBucket(false, true), false, false);
            fail("expected DpException for bucket with no data timestamps");
        } catch (DpException ex) {
            assertTrue(ex.getMessage().contains("has no dataTimestamps"));
        }
    }

    @Test
    public void testNullDocumentThrowsDpException() {
        try {
            BucketDocument.dataBucketFromDocument(null, emptyQuerySpec());
            fail("expected DpException for null document");
        } catch (DpException ex) {
            assertTrue(ex.getMessage().contains("null BucketDocument"));
        }
    }
}
