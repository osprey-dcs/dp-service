package com.ospreydcs.dp.service.annotation.handler.mongo.export;

import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;

public interface BucketedDataExportFileInterface {
    void writeBucketData(BucketDocument bucketDocument);
    void close();
}
