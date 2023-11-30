package com.ospreydcs.dp.service.ingest.model.bson;

import org.bson.codecs.pojo.annotations.BsonDiscriminator;

@BsonDiscriminator(key = "dataType", value = "LONG")
public class LongBucketDocument extends BucketDocument<Long> {
    public LongBucketDocument() {}
}
