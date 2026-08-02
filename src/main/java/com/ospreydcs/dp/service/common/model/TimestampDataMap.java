package com.ospreydcs.dp.service.common.model;

import com.ospreydcs.dp.grpc.v1.common.DataValue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TimestampDataMap extends TimestampMap<Map<Integer, DataValue>> {

    public record DataRow(long seconds, long nanos, List<DataValue> dataValues) {};

    public class DataRowIterator implements Iterator<DataRow> {

        // instance variables
        private final Iterator<Map.Entry<Long, Map<Long, Map<Integer, DataValue>>>> currentSecondEntryIterator;
        private Long currentSecond = null;
        private Iterator<Map.Entry<Long, Map<Integer, DataValue>>> currentNanoEntryIterator = null;
        private final boolean draining;
        /**
         * When draining, true while the second-level entry most recently returned by
         * {@code currentSecondEntryIterator} is still present in the backing map. Drives a single
         * remove-once-drained rule in {@link #hasNext()} that applies uniformly to every second --
         * the last one, and any that is empty when reached -- rather than special-casing the tail.
         */
        private boolean currentSecondPending = false;

        public DataRowIterator() {
            this(false);
        }

        /**
         * @param draining when true, each row is removed from the backing map as it is returned, so
         *                 the map shrinks while the caller's output representation grows (#199).
         *                 The map is unusable for anything else afterward.
         */
        public DataRowIterator(boolean draining) {
            this.draining = draining;
            this.currentSecondEntryIterator = timestampMap.entrySet().iterator();
            if (this.currentSecondEntryIterator.hasNext()) {
                updateCurrentSecond();
            }
        }

        private void updateCurrentSecond() {
            final Map.Entry<Long, Map<Long, Map<Integer, DataValue>>> currentSecondEntry =
                    this.currentSecondEntryIterator.next();
            this.currentSecond = currentSecondEntry.getKey();
            this.currentNanoEntryIterator = currentSecondEntry.getValue().entrySet().iterator();
            this.currentSecondPending = draining;
        }

        @Override
        public boolean hasNext() {
            if (currentNanoEntryIterator == null) {
                return false;
            }
            // Single rule when draining: a second whose nano entries are exhausted is dropped, then
            // we advance. currentSecondPending makes the removal idempotent, so this is safe both
            // for the final second (no advance follows, and hasNext() may be called repeatedly after
            // exhaustion) and for a second that was already empty when reached. Removal always
            // targets the entry currentSecondEntryIterator last returned, satisfying its contract.
            while (!this.currentNanoEntryIterator.hasNext()) {
                if (this.currentSecondPending) {
                    this.currentSecondEntryIterator.remove();
                    this.currentSecondPending = false;
                }
                if (!this.currentSecondEntryIterator.hasNext()) {
                    return false;
                }
                updateCurrentSecond();
            }
            return true;
        }

        @Override
        public DataRow next() {

            final long rowSecond = currentSecond;
            final Map.Entry<Long, Map<Integer, DataValue>> currentNanoEntry = this.currentNanoEntryIterator.next();
            final long rowNano = currentNanoEntry.getKey();
            final Map<Integer, DataValue> rowColumnValueMap = currentNanoEntry.getValue();
            final List<DataValue> rowDataValues = new ArrayList<>();

            // add values to row for each column data value
            int columnIndex = 0;
            for (String columnName : getColumnNameList()) {
                DataValue columnDataValue = rowColumnValueMap.get(columnIndex);
                if (columnDataValue == null) {
                    columnDataValue = DataValue.newBuilder().build();
                }
                rowDataValues.add(columnDataValue);

                columnIndex = columnIndex + 1;
            }

            if (draining) {
                // Release this row's cell map now that its values are held by the returned DataRow.
                // Removal goes through the iterator rather than the map so the in-progress traversal
                // stays valid; the enclosing per-second map is dropped by hasNext() once drained.
                this.currentNanoEntryIterator.remove();
            }

            return new DataRow(rowSecond, rowNano, rowDataValues);
        }
    }

    final private List<String> columnNameList = new ArrayList<>();

    public int getColumnIndex(String pvName) {
        int columnIndex = columnNameList.indexOf(pvName);
        if (columnIndex == -1) {
            // add column to list and get index
            columnNameList.add(pvName);
            columnIndex = columnNameList.size() - 1;
        }
        return columnIndex;
    }

    public List<String> getColumnNameList() {
        return columnNameList;
    }

    public DataRowIterator dataRowIterator() {
        return new DataRowIterator();
    }

    /**
     * Row iterator that removes each row from this map as it is returned (#199), so a consumer
     * building another representation does not hold both at full size. This map is left empty and
     * must not be used afterward.
     */
    public DataRowIterator drainingDataRowIterator() {
        return new DataRowIterator(true);
    }

}
