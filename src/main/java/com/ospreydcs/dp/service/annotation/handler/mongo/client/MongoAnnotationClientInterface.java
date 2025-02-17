package com.ospreydcs.dp.service.annotation.handler.mongo.client;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.annotation.QueryAnnotationsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryDataSetsRequest;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.model.MongoInsertOneResult;

public interface MongoAnnotationClientInterface {

    boolean init();
    boolean fini();

    DataSetDocument findDataSet(String dataSetId);

    MongoInsertOneResult insertDataSet(DataSetDocument dataSetDocument);

    MongoCursor<DataSetDocument> executeQueryDataSets(QueryDataSetsRequest request);

    AnnotationDocument findAnnotation(String annotationId);

    MongoInsertOneResult insertAnnotation(AnnotationDocument annotationDocument);

    MongoCursor<AnnotationDocument> executeQueryAnnotations(QueryAnnotationsRequest request);

    MongoInsertOneResult insertCalculations(CalculationsDocument calculationsDocument);
}
