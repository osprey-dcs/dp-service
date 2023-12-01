package com.ospreydcs.dp.service.common.bson;

import org.bson.codecs.pojo.annotations.BsonDiscriminator;

@BsonDiscriminator(key = "dataType", value = "LONG")
public class LongBucketDocument extends BucketDocument<Long> {
    public LongBucketDocument() {}
}
