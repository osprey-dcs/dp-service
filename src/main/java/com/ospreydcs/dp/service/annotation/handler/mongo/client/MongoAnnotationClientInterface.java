package com.ospreydcs.dp.service.annotation.handler.mongo.client;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.annotation.QueryAnnotationsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryDataSetsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryPvMetadataRequest;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.bson.pvmetadata.PvMetadataDocument;
import com.ospreydcs.dp.service.common.model.MongoDeleteResult;
import com.ospreydcs.dp.service.common.model.MongoInsertOneResult;
import com.ospreydcs.dp.service.common.model.MongoSaveResult;

public interface MongoAnnotationClientInterface {

    boolean init();
    boolean fini();

    DataSetDocument findDataSet(String dataSetId);

    MongoSaveResult saveDataSet(DataSetDocument dataSetDocument, String existingDocumentId);

    MongoCursor<DataSetDocument> executeQueryDataSets(QueryDataSetsRequest request);

    AnnotationDocument findAnnotation(String annotationId);

    MongoSaveResult saveAnnotation(AnnotationDocument annotationDocument, String id);

    MongoCursor<AnnotationDocument> executeQueryAnnotations(QueryAnnotationsRequest request);

    MongoInsertOneResult insertCalculations(CalculationsDocument calculationsDocument);

    CalculationsDocument findCalculations(String calculationsId);

    MongoSaveResult savePvMetadata(PvMetadataDocument document);

    MongoCursor<PvMetadataDocument> executeQueryPvMetadata(QueryPvMetadataRequest request);

    String getQueryPvMetadataNextPageToken(QueryPvMetadataRequest request);

    PvMetadataDocument findPvMetadataByNameOrAlias(String pvNameOrAlias);

    MongoDeleteResult deletePvMetadata(String pvNameOrAlias);
}
