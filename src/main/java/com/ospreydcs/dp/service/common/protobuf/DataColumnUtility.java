package com.ospreydcs.dp.service.common.protobuf;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataValue;

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
}
