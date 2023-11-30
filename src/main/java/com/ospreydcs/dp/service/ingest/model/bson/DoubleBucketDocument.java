package com.ospreydcs.dp.service.ingest.model.bson;

import org.bson.codecs.pojo.annotations.BsonDiscriminator;

@BsonDiscriminator(key = "dataType", value = "DOUBLE")
public class DoubleBucketDocument extends BucketDocument<Double> {
    public DoubleBucketDocument() {}
}
