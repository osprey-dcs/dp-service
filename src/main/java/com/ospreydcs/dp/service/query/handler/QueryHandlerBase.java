package com.ospreydcs.dp.service.query.handler;

import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.query.QueryDataByTimeRequest;
import com.ospreydcs.dp.service.common.model.ValidationResult;

public abstract class QueryHandlerBase {

    public ValidationResult validateQueryDataByTimeRequest(QueryDataByTimeRequest request) {

        boolean isError = false;
        String statusMsg = "";

        String columnName = request.getColumnName();
        Timestamp startTime = request.getStartTime();
        Timestamp endTime = request.getEndTime();

        if (columnName == null || columnName.isEmpty()) {
            // check that columnName is specified
            isError = true;
            statusMsg = "columnName must be specified";

        } else if (startTime == null || startTime.getEpochSeconds() == 0) {
            // check that startTime is specified
            isError = true;
            statusMsg = "startTime must be specified";

        } else if (endTime == null || endTime.getEpochSeconds() == 0) {
            // check that endTime is specified
            isError = true;
            statusMsg = "endTime must be specified";

        } else {

            // validate start and end times

            final long startSeconds = startTime.getEpochSeconds();
            final long startNanos = startTime.getNanoseconds();
            final long endSeconds = endTime.getEpochSeconds();
            final long endNanos = endTime.getNanoseconds();

            if (endSeconds < startSeconds) {
                // check that endSeconds >= startSeconds
                isError = true;
                statusMsg = "endTime seconds must be >= startTime seconds";
            } else if (endSeconds == startSeconds && endNanos <= startNanos) {
                // check that endNanos > startNanos when seconds match
                isError = true;
                statusMsg = "endTime nanos must be > startTime nanos when seconds match";
            }

            // TODO: check that time range doesn't exceed configured maximum
        }

        return new ValidationResult(isError, statusMsg);

    }

}
