package com.ospreydcs.dp.service.common.bson.bucket;

import com.ospreydcs.dp.grpc.v1.common.DataValue;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

@BsonDiscriminator(key = "dataType", value = "LONG")
public class LongBucketDocument extends BucketDocument<Long> {
    public LongBucketDocument() {}

    @Override
    public void addColumnDataValue(Long dataValue, DataValue.Builder valueBuilder) {
        valueBuilder.setLongValue(dataValue);
    }
}
