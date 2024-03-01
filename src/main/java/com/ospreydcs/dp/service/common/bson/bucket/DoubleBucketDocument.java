package com.ospreydcs.dp.service.common.bson.bucket;

import com.ospreydcs.dp.grpc.v1.common.DataValue;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

@BsonDiscriminator(key = "dataType", value = "DOUBLE")
public class DoubleBucketDocument extends BucketDocument<Double> {
    public DoubleBucketDocument() {}

    @Override
    public void addColumnDataValue(Double dataValue, DataValue.Builder valueBuilder) {
        valueBuilder.setDoubleValue(dataValue);
    }
}
