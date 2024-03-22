package com.ospreydcs.dp.service.annotation.handler.mongo.client;

import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.model.MongoInsertOneResult;

public interface MongoAnnotationClientInterface {

    boolean init();
    boolean fini();

    MongoInsertOneResult insertDataSet(DataSetDocument dataSetDocument);

    MongoInsertOneResult insertAnnotation(AnnotationDocument annotationDocument);

}
