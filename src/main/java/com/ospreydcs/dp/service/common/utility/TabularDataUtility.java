package com.ospreydcs.dp.service.common.utility;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.service.common.bson.column.ColumnDocumentBase;
import com.ospreydcs.dp.service.common.bson.column.DataColumnDocument;
import com.ospreydcs.dp.service.common.bson.column.ScalarColumnDocumentBase;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDataFrameDocument;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.exception.NonScalarColumnException;
import com.ospreydcs.dp.service.common.protobuf.DataTimestampsUtility;
import com.ospreydcs.dp.service.common.model.TimestampDataMap;

import java.time.Instant;
import java.util.*;

public class TabularDataUtility {

    public static record TimestampDataMapSizeStats(int currentDataSize, boolean sizeLimitExceeded) {}

    /**
     * A half-open {@code [begin, end)} sample-retention window, expressed as epoch seconds + nanos.
     *
     * <p>Deliberately a local value type rather than the query package's {@code TimeInterval}: this
     * utility is shared by the query service, the annotation export job, and the integration test
     * wrapper, and must not depend on Query API V2 model classes. Callers holding a {@code TimeInterval}
     * convert at the call site.
     */
    public static record RetentionInterval(
            long beginSeconds, long beginNanos, long endSeconds, long endNanos) {}

    /**
     * Per-sample status filter for Query API V2 {@code querySamples} with a
     * {@code sampleStatusSelector}: {@code matchingTimestampsByPv} holds, per PV, the epoch-nanos
     * timestamps of statuses matching the selector. A sample is "labeled" only by a status at its
     * exact timestamp (nanosecond equality). INCLUDE mode retains a sample iff labeled; EXCLUDE
     * mode drops it iff labeled.
     *
     * <p>Composes with the fragment {@link RetentionInterval} test by intersection: both must pass
     * for a sample to be retained, so a status attached to a sample outside the retrieval
     * fragments has no effect. Like RetentionInterval, this is a local value type so the shared
     * utility does not depend on Query API V2 model classes.
     */
    public static record SampleStatusFilter(
            boolean includeMode, Map<String, Set<Long>> matchingTimestampsByPv) {

        public boolean retains(String pvName, long second, long nano) {
            final Set<Long> matchingTimestamps = matchingTimestampsByPv.get(pvName);
            final boolean labeled = matchingTimestamps != null
                    && matchingTimestamps.contains(second * 1_000_000_000L + nano);
            return labeled == includeMode;
        }
    }

    public static TimestampDataMapSizeStats addBucketsToTable(
            TimestampDataMap tableValueMap,
            MongoCursor<BucketDocument> cursor,
            int previousDataSize,
            Integer sizeLimit, // if null, no limit is applied
            long beginSeconds,
            long beginNanos,
            long endSeconds,
            long endNanos
    ) throws DpException {

        return addBucketsToTable(
                tableValueMap,
                cursor,
                previousDataSize,
                sizeLimit,
                List.of(new RetentionInterval(beginSeconds, beginNanos, endSeconds, endNanos)));
    }

    /**
     * Multi-interval form: a sample is retained when it falls inside <em>any</em> of the given
     * half-open intervals.
     *
     * <p>Exists for Query API V2 {@code querySamples} with a {@code ConfigurationSelector}, which
     * resolves to a set of <b>disjoint</b> retrieval intervals (issue #207). The MongoDB query filters
     * those fragments only at <em>bucket</em> granularity, so a single bucket spanning the gap between
     * two fragments passes the database filter with its in-gap samples intact. Trimming here against
     * the full fragment list — rather than a single collapsed {@code [min begin, max end)} window — is
     * what keeps gap samples out of the result. Single-interval callers ({@code queryTable}, the export
     * job) are unaffected and reach this through the overload above.
     */
    public static TimestampDataMapSizeStats addBucketsToTable(
            TimestampDataMap tableValueMap,
            MongoCursor<BucketDocument> cursor,
            int previousDataSize,
            Integer sizeLimit, // if null, no limit is applied
            List<RetentionInterval> retentionIntervals
    ) throws DpException {
        return addBucketsToTable(
                tableValueMap, cursor, previousDataSize, sizeLimit, retentionIntervals, null);
    }

    /**
     * Full form adding an optional per-sample {@link SampleStatusFilter} (null = no status
     * filtering). A sample must pass both the fragment retention test and the status test — the
     * selectors compose by intersection. A filtered-out sample is simply never inserted, so it
     * surfaces as a missing value (unset DataValue) where other PVs keep the row, and a timestamp
     * at which every PV is filtered out is omitted from the result entirely.
     */
    public static TimestampDataMapSizeStats addBucketsToTable(
            TimestampDataMap tableValueMap,
            MongoCursor<BucketDocument> cursor,
            int previousDataSize,
            Integer sizeLimit, // if null, no limit is applied
            List<RetentionInterval> retentionIntervals,
            SampleStatusFilter statusFilter // if null, no status filtering is applied
    ) throws DpException {

        int currentDataSize = previousDataSize;
        while (cursor.hasNext()) {
            // add buckets to table data structure
            final BucketDocument bucket = cursor.next();
            // Register the bucket's PV name. getColumnIndex() is a mutator (it appends unseen names to
            // the map's column list) and the returned index is deliberately unused here: the call is
            // made for the registration alone, so a PV whose buckets contribute no in-range samples
            // still gets a column slot -- an all-empty column rather than a missing one. The column is
            // normally also registered under the same name inside addColumnsToTable below; this keeps
            // the slot even if a bucket's PV name and its column name ever diverge.
            tableValueMap.getColumnIndex(bucket.getPvName());
            int bucketDataSize = addBucketToTable(bucket, tableValueMap, retentionIntervals, statusFilter);
            currentDataSize = currentDataSize + bucketDataSize;
            // Size accounting is per-BUCKET, not per-timestamp: a whole bucket is added to the table
            // before the limit is checked, so currentDataSize can overshoot sizeLimit by up to one
            // bucket's worth of data. This is intentional and benign for the callers that pass a
            // sizeLimit as a HARD ceiling (queryTable and the export job both discard the result and
            // return an error when sizeLimitExceeded() is true — overshooting by one bucket vs. one
            // byte yields the same error outcome). The V2 querySamples unary dispatcher instead uses
            // the flag as a PAGING boundary (drain-then-truncate) and owns the zero-progress guard for
            // the case where the overshoot collapses to a single indivisible timestamp. Do NOT tighten
            // this to per-timestamp granularity without revisiting those callers — for the hard-ceiling
            // callers it would change nothing observable, and V2's boundary handling already accounts
            // for the overshoot.
            if (sizeLimit != null && currentDataSize > sizeLimit) {
                cursor.close();
                return new TimestampDataMapSizeStats(currentDataSize, true);
            }
        }
        cursor.close();

        return new TimestampDataMapSizeStats(currentDataSize, false);
    }

    private static int addBucketToTable(
            BucketDocument bucket,
            TimestampDataMap tableValueMap,
            List<RetentionInterval> retentionIntervals,
            SampleStatusFilter statusFilter
    ) throws DpException {

        final DataTimestamps bucketDataTimestamps = bucket.getDataTimestamps().toDataTimestamps();

        // Only scalar column types and DataColumnDocument can be converted to DataColumn for tabular export
        final ColumnDocumentBase columnDocument = bucket.getDataColumn();
        final DataColumn bucketColumn;
        
        if (columnDocument instanceof ScalarColumnDocumentBase) {
            // Scalar columns can be converted to legacy DataColumn format
            bucketColumn = ((ScalarColumnDocumentBase<?>) columnDocument).toDataColumn();
        } else if (columnDocument instanceof DataColumnDocument) {
            // Legacy DataColumn documents can be converted directly
            bucketColumn = ((DataColumnDocument) columnDocument).toDataColumn();
        } else {
            // Non-scalar columns (arrays, images, structs) have no tabular (row/column) representation.
            // Throw a neutral, PV-named exception; each caller phrases its own guidance (the export
            // framework and querySamples both catch this and translate for their context, Q4).
            throw new NonScalarColumnException(bucket.getPvName(), columnDocument.getClass().getSimpleName());
        }

        return addColumnsToTable(
                bucketDataTimestamps,
                List.of(bucketColumn),
                tableValueMap,
                retentionIntervals,
                statusFilter);
    }

    /**
     * True when {@code (second, nano)} falls inside any half-open interval in {@code intervals}.
     * A sample exactly at an interval's end belongs to the next interval, not this one.
     */
    private static boolean isRetained(long second, long nano, List<RetentionInterval> intervals) {
        for (RetentionInterval interval : intervals) {
            if (second < interval.beginSeconds() || second > interval.endSeconds()) {
                continue;
            }
            if ((second == interval.beginSeconds() && nano < interval.beginNanos())
                    || (second == interval.endSeconds() && nano >= interval.endNanos())) {
                continue;
            }
            return true;
        }
        return false;
    }

    private static int addColumnsToTable(
            DataTimestamps dataTimestamps,
            List<DataColumn> dataColumns,
            TimestampDataMap tableValueMap,
            long beginSeconds,
            long beginNanos,
            long endSeconds,
            long endNanos
    ) throws DpException {

        return addColumnsToTable(
                dataTimestamps,
                dataColumns,
                tableValueMap,
                List.of(new RetentionInterval(beginSeconds, beginNanos, endSeconds, endNanos)),
                null);
    }

    private static int addColumnsToTable(
            DataTimestamps dataTimestamps,
            List<DataColumn> dataColumns,
            TimestampDataMap tableValueMap,
            List<RetentionInterval> retentionIntervals,
            SampleStatusFilter statusFilter
    ) throws DpException {

        int dataValueSize = 0;
        final DataTimestampsUtility.DataTimestampsIterator dataTimestampsIterator =
                DataTimestampsUtility.dataTimestampsIterator(dataTimestamps);


        // Register every column up front. getColumnIndex() is a mutator -- it appends unseen names to
        // the map's column name list, which is what determines the exported/emitted column set. The
        // pre-#207 code got this incidentally, by calling it inside the per-column loop before the
        // range test skipped a value, so a column whose samples all fall outside the range still got
        // a slot (an all-empty column, not a missing one). Hoisting the range test above that loop
        // would silently drop such columns, so the registration is now explicit rather than a side
        // effect of the skipped path.
        for (DataColumn dataColumn : dataColumns) {
            tableValueMap.getColumnIndex(dataColumn.getName());
        }

        // derserialize DataColumn content from document and the iterate DataValues in column
        int valueIndex = 0;
        while (dataTimestampsIterator.hasNext()) {

            final Timestamp timestamp = dataTimestampsIterator.next();
            final long second = timestamp.getEpochSeconds();
            final long nano = timestamp.getNanoseconds();

            // skip values outside every retention interval (hoisted: the test depends only on the
            // timestamp, so it is the same verdict for every column at this row)
            if (!isRetained(second, nano, retentionIntervals)) {
                valueIndex = valueIndex + 1;
                continue;
            }

            // add next value for each column to tableValueMap
            for (DataColumn dataColumn : dataColumns) {

                // per-sample status filter (per-PV, so evaluated inside the column loop, unlike the
                // column-independent interval test above): a filtered-out sample is never inserted,
                // becoming a missing value at this (PV, timestamp) position
                if (statusFilter != null && !statusFilter.retains(dataColumn.getName(), second, nano)) {
                    continue;
                }

                final DataValue dataValue = dataColumn.getDataValues(valueIndex);
                final int columnIndex = tableValueMap.getColumnIndex(dataColumn.getName());

                // keep track of data size
                dataValueSize = dataValueSize + dataValue.getSerializedSize();

                // add value to tableValueMap
                Map<Integer, DataValue> nanoValueMap = tableValueMap.get(second, nano);
                if (nanoValueMap == null) {
                    nanoValueMap = new TreeMap<>();
                    tableValueMap.put(second, nano, nanoValueMap);
                }
                nanoValueMap.put(columnIndex, dataValue);
            }

            valueIndex = valueIndex + 1;
        }

        return dataValueSize;
    }

    public static TimestampDataMapSizeStats addCalculationsToTable(
            TimestampDataMap tableValueMap,
            CalculationsDocument calculationsDocument,
            Map<String, CalculationsSpec.ColumnNameList> frameColumnNamesMap,
            Instant exportBeginInstant,
            Instant exportEndInstant,
            int previousDataSize,
            Integer sizeLimit // if null, no limit is applied
    ) throws DpException {

        int currentDataSize = previousDataSize;

        // add columns for each CalculationsDataFrame to table
        for (CalculationsDataFrameDocument frameDocument : calculationsDocument.getDataFrames()) {

            final String frameName = frameDocument.getName();

            // create a model for accessing frame's begin/end times
            final DataTimestamps frameDataTimestamps = frameDocument.getDataTimestamps().toDataTimestamps();
            final DataTimestampsUtility.DataTimestampsModel frameTimestampsModel =
                    new DataTimestampsUtility.DataTimestampsModel(frameDataTimestamps);

            // Determine time range for truncating values.
            // We include all values in the frame if exportBeginInstant and exportEndInstant are not specified,
            // otherwise we truncate values outside that range (e.g., if we are exporting a dataset with calculations).
            long beginSeconds;
            long beginNanos;
            long endSeconds;
            long endNanos;
            if (exportBeginInstant == null || exportEndInstant == null) {
                beginSeconds = frameTimestampsModel.getFirstTimestamp().getEpochSeconds();
                beginNanos = frameTimestampsModel.getFirstTimestamp().getNanoseconds();
                endSeconds = frameTimestampsModel.getLastTimestamp().getEpochSeconds();
                endNanos = frameTimestampsModel.getLastTimestamp().getNanoseconds() + 1; // we add one so last value is not truncated
            } else {
                beginSeconds = exportBeginInstant.getEpochSecond();
                beginNanos = exportBeginInstant.getNano();
                endSeconds = exportEndInstant.getEpochSecond();
                endNanos = exportEndInstant.getNano();
            }

            // make list of columns for frame
            List<DataColumn> frameColumns = new ArrayList<>();
            for (DataColumnDocument frameColumnDocument : frameDocument.getDataColumns()) {
                if (frameColumnNamesMap != null) {
                    // only include columns specified in map if one is provided
                    final CalculationsSpec.ColumnNameList frameColumnNamesList = frameColumnNamesMap.get(frameName);
                    if (frameColumnNamesList != null &&
                            frameColumnNamesList.getColumnNamesList().contains(frameColumnDocument.getName())) {
                        frameColumns.add(frameColumnDocument.toDataColumn());
                    }
                } else {
                    frameColumns.add(frameColumnDocument.toDataColumn());
                }
            }

            // add list of columns to tableValueMap
            int frameDataSize = addColumnsToTable(
                    frameDataTimestamps, frameColumns, tableValueMap, beginSeconds, beginNanos, endSeconds, endNanos);

            // update and check export data size against limit
            currentDataSize = currentDataSize + frameDataSize;
            if (sizeLimit != null && currentDataSize > sizeLimit) {
                return new TimestampDataMapSizeStats(currentDataSize, true);
            }
        }

        return new TimestampDataMapSizeStats(currentDataSize, false);
    }

}
