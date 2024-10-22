package com.ospreydcs.dp.service.annotation.handler.mongo.export;

import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.exception.DpException;

public interface BucketedDataExportFileInterface {
    void writeBucketData(BucketDocument bucketDocument) throws DpException;
    void close() throws DpException;
}
