package com.ospreydcs.dp.service.annotation.handler.mongo.client;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.annotation.DeleteSampleStatusesRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryAnnotationsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryConfigurationActivationsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryConfigurationsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryDataSetsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryPvMetadataRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QuerySampleStatusesRequest;
import com.ospreydcs.dp.grpc.v1.annotation.SaveSampleStatusesRequest;
import com.ospreydcs.dp.service.annotation.handler.model.SampleStatusPageToken;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationActivationDocument;
import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.bson.pvmetadata.PvMetadataDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.model.AnnotationQueryResult;
import com.ospreydcs.dp.service.common.model.ConfigurationActivationQueryResult;
import com.ospreydcs.dp.service.common.model.ConfigurationQueryResult;
import com.ospreydcs.dp.service.common.model.DataSetQueryResult;
import com.ospreydcs.dp.service.common.model.MongoCountResult;
import com.ospreydcs.dp.service.common.model.MongoDeleteResult;
import com.ospreydcs.dp.service.common.model.MongoInsertOneResult;
import com.ospreydcs.dp.service.common.model.MongoSaveResult;
import com.ospreydcs.dp.service.common.model.PvMetadataQueryResult;
import com.ospreydcs.dp.service.common.model.SampleStatusQueryResult;

import java.time.Instant;

public interface MongoAnnotationClientInterface {

    boolean init();
    boolean fini();

    DataSetDocument findDataSet(String dataSetId);

    MongoSaveResult saveDataSet(DataSetDocument dataSetDocument, String existingDocumentId);

    DataSetQueryResult executeQueryDataSets(QueryDataSetsRequest request);

    AnnotationDocument findAnnotation(String annotationId);

    MongoSaveResult saveAnnotation(AnnotationDocument annotationDocument, String id);

    AnnotationQueryResult executeQueryAnnotations(QueryAnnotationsRequest request);

    MongoInsertOneResult insertCalculations(CalculationsDocument calculationsDocument);

    CalculationsDocument findCalculations(String calculationsId);

    MongoSaveResult savePvMetadata(PvMetadataDocument document);

    PvMetadataQueryResult executeQueryPvMetadata(QueryPvMetadataRequest request);

    /**
     * Returns null if no record matches; throws if the query itself failed. Callers must
     * distinguish the two — see the implementation for why this is checked.
     */
    PvMetadataDocument findPvMetadataByNameOrAlias(String pvNameOrAlias) throws DpException;

    MongoDeleteResult deletePvMetadata(String pvNameOrAlias);

    MongoSaveResult saveConfiguration(ConfigurationDocument document);

    /**
     * Returns null if no Configuration has this name; throws if the query itself failed. Callers
     * must distinguish the two — see the implementation for why this is checked.
     */
    ConfigurationDocument findConfigurationByName(String configurationName) throws DpException;

    ConfigurationQueryResult executeQueryConfigurations(QueryConfigurationsRequest request);

    MongoDeleteResult deleteConfiguration(String configurationName);

    MongoSaveResult saveConfigurationActivation(ConfigurationActivationDocument document);

    ConfigurationActivationDocument findConfigurationActivationById(String clientActivationId);

    ConfigurationActivationDocument findConfigurationActivationByCompositeKey(String configurationName, Instant startTime);

    ConfigurationActivationQueryResult executeQueryConfigurationActivations(QueryConfigurationActivationsRequest request);

    MongoDeleteResult deleteConfigurationActivation(String clientActivationId);

    MongoDeleteResult deleteConfigurationActivationByCompositeKey(String configurationName, Instant startTime);

    ConfigurationActivationQueryResult getActiveConfigurations(Instant timestamp);

    MongoCountResult saveSampleStatuses(SaveSampleStatusesRequest request);

    SampleStatusQueryResult executeQuerySampleStatuses(
            QuerySampleStatusesRequest request, int limit, SampleStatusPageToken position);

    MongoCountResult deleteSampleStatuses(DeleteSampleStatusesRequest request);
}
