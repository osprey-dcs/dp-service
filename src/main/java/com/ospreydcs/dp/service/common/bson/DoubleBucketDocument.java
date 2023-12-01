package com.ospreydcs.dp.service.common.bson;

import org.bson.codecs.pojo.annotations.BsonDiscriminator;

@BsonDiscriminator(key = "dataType", value = "DOUBLE")
public class DoubleBucketDocument extends BucketDocument<Double> {
    public DoubleBucketDocument() {}
}
