package com.ospreydcs.dp.service.ingest.handler;

import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.ingestion.*;
import com.ospreydcs.dp.service.common.model.ValidationResult;
import com.ospreydcs.dp.service.ingest.service.IngestionServiceImpl;

public class IngestionValidationUtility {

    public static ValidationResult validateIngestionRequest(IngestDataRequest request) {

        boolean isError = false;
        String statusMsg = "";

        String providerId = request.getProviderId();
        String requestId = request.getClientRequestId();

        int numRequestColumns = request.getIngestionDataFrame().getDataColumnsCount()
                + request.getIngestionDataFrame().getSerializedDataColumnsCount();
        int numRequestRows = IngestionServiceImpl.getNumRequestRows(request);

        // validate request
        if (providerId.isBlank()) {
            // check that provider is specified
            isError = true;
            statusMsg = "providerId must be specified";

        } else if (requestId == null || requestId.isEmpty()) {
            // check that requestId is provided
            isError = true;
            statusMsg = "clientRequestId must be specified";

        } else if (numRequestRows == 0) {
            // check that time spec specifies number of rows for request
            isError = true;
            statusMsg = "IngestDataRequest.ingestionDataFrame.dataTimestamps.value "
                    + "must specify SamplingClock or list of timestamps";

        } else if (numRequestColumns == 0) {
            // check that columns are provided
            isError = true;
            statusMsg = "columns list cannot be empty";

        } else {

            // validate DataColumns
            for (DataColumn column : request.getIngestionDataFrame().getDataColumnsList()) {
                if (column.getDataValuesList().size() != numRequestRows) {
                    // validate column sizes
                    isError = true;
                    statusMsg = "column: " + column.getName() + " size: " + column.getDataValuesList().size()
                            + " mismatch numValues: " + numRequestRows;
                } else if (column.getName() == null || column.getName().isEmpty()) {
                    // check that column name is provided
                    isError = true;
                    statusMsg = "name must be specified for all DataColumns";
                }
            }

            // validate SerializedDatacolumns
            for (SerializedDataColumn serializedDataColumn : request.getIngestionDataFrame().getSerializedDataColumnsList()) {
                if (serializedDataColumn.getName() == null || serializedDataColumn.getName().isEmpty()) {
                    // check that column name is provided
                    isError = true;
                    statusMsg = "name must be specified for all SerializedDataColumns";
                }
            }
        }

        return new ValidationResult(isError, statusMsg);
    }

}
