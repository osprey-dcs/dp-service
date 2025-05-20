package com.ospreydcs.dp.service.annotation.handler.mongo.export;

import com.ospreydcs.dp.grpc.v1.common.CalculationsSpec;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.exception.DpException;

import java.util.Map;

public interface BucketedDataExportFileInterface {
    void writeDataSet(DataSetDocument dataSet);
    void writeBucket(BucketDocument bucketDocument) throws DpException;
    void writeCalculations(CalculationsDocument calculationsDocument,
                           Map<String, CalculationsSpec.ColumnNameList> frameColumnNamesMap) throws DpException;
    void close() throws DpException;
}
