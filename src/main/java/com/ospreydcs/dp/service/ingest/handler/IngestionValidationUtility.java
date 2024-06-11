package com.ospreydcs.dp.service.ingest.handler;

import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.ingestion.*;
import com.ospreydcs.dp.service.common.model.ValidationResult;
import com.ospreydcs.dp.service.ingest.service.IngestionServiceImpl;

public class IngestionValidationUtility {

    public static ValidationResult validateIngestionRequest(IngestDataRequest request) {

        boolean isError = false;
        String statusMsg = "";

        int providerId = request.getProviderId();
        String requestId = request.getClientRequestId();
        Timestamp requestTimestamp = request.getRequestTime();
        int numRequestColumns = request.getIngestionDataFrame().getDataColumnsList().size();
        int numRequestRows = IngestionServiceImpl.getNumRequestRows(request);

        // validate request
        if (requestTimestamp == null || requestTimestamp.getEpochSeconds() == 0) {
            // check that request timestamp is provided
            isError = true;
            statusMsg = "requestTime must be specified";

        } else if (providerId == 0) {
            // check that provider is specified
            isError = true;
            statusMsg = "providerId must be specified";

        } else if (requestId == null || requestId.isEmpty()) {
            // check that requestId is provided
            isError = true;
            statusMsg = "clientRequestId must be specified";

        } else if (!request.getIngestionDataFrame().getDataTimestamps().hasSamplingClock()) {
            // currently only SamplingClock is supported for time spec
            isError = true;
            statusMsg = "only SamplingClock is currently supported for "
                            + "IngestDataRequest.ingestionDataFrame.dataTimestamps.value";

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
            for (DataColumn column : request.getIngestionDataFrame().getDataColumnsList()) {
                if (column.getDataValuesList().size() != numRequestRows) {
                    // validate column sizes
                    isError = true;
                    statusMsg = "column: " + column.getName() + " size: " + column.getDataValuesList().size()
                            + " mismatch numValues: " + numRequestRows;
                } else if (column.getName() == null || column.getName().isEmpty()) {
                    // check that column name is provided
                    isError = true;
                    statusMsg = "name must be specified for all data columns";
                }
                break;
            }
        }

        return new ValidationResult(isError, statusMsg);
    }

}
