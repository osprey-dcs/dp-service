package com.ospreydcs.dp.service.common.bson.bucket;

import com.ospreydcs.dp.grpc.v1.common.DataValue;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

@BsonDiscriminator(key = "dataType", value = "BOOLEAN")
public class BooleanBucketDocument extends BucketDocument<Boolean> {
    public BooleanBucketDocument() {}

    @Override
    public void addColumnDataValue(Boolean dataValue, DataValue.Builder valueBuilder) {
        valueBuilder.setBooleanValue(dataValue);
    }
}
