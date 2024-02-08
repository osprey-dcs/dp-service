package com.ospreydcs.dp.service.common.bson;

import com.ospreydcs.dp.grpc.v1.common.DataValue;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

@BsonDiscriminator(key = "dataType", value = "STRING")
public class StringBucketDocument extends BucketDocument<String> {
    public StringBucketDocument() {}

    @Override
    public void addColumnDataValue(String dataValue, DataValue.Builder valueBuilder) {
        valueBuilder.setStringValue(dataValue);
    }
}
