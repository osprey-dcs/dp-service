package com.ospreydcs.dp.service.annotation.handler.mongo.job;

import com.ospreydcs.dp.service.annotation.handler.model.ExportConfiguration;
import com.ospreydcs.dp.service.annotation.handler.model.HandlerExportDataSetRequest;
import com.ospreydcs.dp.service.annotation.handler.mongo.client.MongoAnnotationClientInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.export.DatasetExportHdf5File;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;

import java.io.IOException;

public class Hdf5ExportJob extends BucketedDataExportJob {

    public Hdf5ExportJob(
            HandlerExportDataSetRequest handlerRequest,
            MongoAnnotationClientInterface mongoAnnotationClient,
            MongoQueryClientInterface mongoQueryClient
    ) {
        super(handlerRequest, mongoAnnotationClient, mongoQueryClient);
    }

    protected String getFileExtension_() {
        return ExportConfiguration.FILE_EXTENSION_HDF5;
    }

    protected DatasetExportHdf5File createExportFile_(
            DataSetDocument dataset, String serverFilePath) throws IOException {

        return new DatasetExportHdf5File(dataset, serverFilePath);
    }

}
