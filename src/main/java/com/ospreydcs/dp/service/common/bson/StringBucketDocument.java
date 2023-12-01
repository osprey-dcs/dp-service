package com.ospreydcs.dp.service.common.bson;

import org.bson.codecs.pojo.annotations.BsonDiscriminator;

@BsonDiscriminator(key = "dataType", value = "STRING")
public class StringBucketDocument extends BucketDocument<String> {
    public StringBucketDocument() {}
}
