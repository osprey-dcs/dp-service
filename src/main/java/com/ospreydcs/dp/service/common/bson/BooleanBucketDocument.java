package com.ospreydcs.dp.service.common.bson;

import org.bson.codecs.pojo.annotations.BsonDiscriminator;

@BsonDiscriminator(key = "dataType", value = "BOOLEAN")
public class BooleanBucketDocument extends BucketDocument<Boolean> {
    public BooleanBucketDocument() {}
}
