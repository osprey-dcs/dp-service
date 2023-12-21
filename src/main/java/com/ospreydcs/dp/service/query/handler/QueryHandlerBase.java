package com.ospreydcs.dp.service.query.handler;

import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.query.QueryRequest;
import com.ospreydcs.dp.service.common.model.ValidationResult;

import java.util.List;
import java.util.stream.Collectors;

public abstract class QueryHandlerBase {

    public ValidationResult validateQueryRequest(QueryRequest request) {

        final List<String> columnNames = request.getColumnNamesList();
        final Timestamp startTime = request.getStartTime();
        final Timestamp endTime = request.getEndTime();
        final long startSeconds = startTime.getEpochSeconds();
        final long startNanos = startTime.getNanoseconds();
        final long endSeconds = endTime.getEpochSeconds();
        final long endNanos = endTime.getNanoseconds();

        // check that columnNames list is specified
        if (columnNames == null || columnNames.isEmpty()) {
            return new ValidationResult(true, "columnName must be specified");
        }

        // check that all columnNames are non-empty strings
        List<String> emptyColumnNames = columnNames.stream()
                .filter(name -> name.isBlank())
                .collect(Collectors.toList());
        if (!emptyColumnNames.isEmpty()) {
            return new ValidationResult(true, "columnNamesList contains empty string");
        }

        // check that startTime is specified
        if (startTime == null || startTime.getEpochSeconds() == 0) {
            return new ValidationResult(true, "startTime must be specified");
        }

        // check that endTime is specified
        if (endTime == null || endTime.getEpochSeconds() == 0) {
            return new ValidationResult(true, "endTime must be specified");
        }

        // validate start and end times
        if (endSeconds < startSeconds) {
            // check that endSeconds >= startSeconds
            return new ValidationResult(true, "endTime seconds must be >= startTime seconds");
        } else if (endSeconds == startSeconds && endNanos <= startNanos) {
            // check that endNanos > startNanos when seconds match
            return new ValidationResult(true, "endTime nanos must be > startTime nanos when seconds match");
        }

        // TODO: check that time range doesn't exceed configured maximum

        return new ValidationResult(false, "");

    }

}
