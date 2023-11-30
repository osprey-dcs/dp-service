package com.ospreydcs.dp.service.ingest.model.bson;

import org.bson.codecs.pojo.annotations.BsonDiscriminator;

@BsonDiscriminator(key = "dataType", value = "STRING")
public class StringBucketDocument extends BucketDocument<String> {
    public StringBucketDocument() {}
}
