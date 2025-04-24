package com.ospreydcs.dp.service.common.protobuf;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.common.model.TimestampMap;

import java.util.Iterator;
import java.util.List;

public class DataColumnUtility {

    public static DataColumn dataColumnWithDoubleValues(
            String name,
            List<Double> dataValues
    ) {
        final DataColumn.Builder dataColumnBuilder = DataColumn.newBuilder();
        dataColumnBuilder.setName(name);
        for (Double value : dataValues) {
            final DataValue dataValue = DataValue.newBuilder().setDoubleValue(value).build();
            dataColumnBuilder.addDataValues(dataValue);
        }
        return dataColumnBuilder.build();
    }

    public static TimestampMap<Double> toTimestampMapDouble(
            DataColumn dataColumn,
            DataTimestamps dataTimestamps
    ) {
        final DataTimestampsUtility.DataTimestampsIterator timestampsIterator =
                DataTimestampsUtility.dataTimestampsIterator(dataTimestamps);
        final TimestampMap<Double> timestampMap = new TimestampMap<>();
        final Iterator<DataValue> valueIterator = dataColumn.getDataValuesList().iterator();
        while (timestampsIterator.hasNext() && valueIterator.hasNext()) {
            final Timestamp timestamp = timestampsIterator.next();
            final DataValue value = valueIterator.next();
            timestampMap.put(timestamp.getEpochSeconds(), timestamp.getNanoseconds(), value.getDoubleValue());
        }
        return timestampMap;
    }
}
