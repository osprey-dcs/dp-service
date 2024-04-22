package com.ospreydcs.dp.service.query.handler;

import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryTableRequest;
import com.ospreydcs.dp.service.common.model.ValidationResult;

import java.util.List;
import java.util.stream.Collectors;

public class QueryHandlerUtility {

    private static ValidationResult validateDataQueryTimeRange(Timestamp beginTime, Timestamp endTime) {

        final long startSeconds = beginTime.getEpochSeconds();
        final long startNanos = beginTime.getNanoseconds();
        final long endSeconds = endTime.getEpochSeconds();
        final long endNanos = endTime.getNanoseconds();

        // check that startTime is specified
        if (beginTime == null || beginTime.getEpochSeconds() == 0) {
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

        return new ValidationResult(false, "");
    }

    public static ValidationResult validateQuerySpecData(QueryDataRequest.QuerySpec querySpec) {

        final List<String> pvNamesList = querySpec.getPvNamesList();

        // check that columnNames list is specified
        if (pvNamesList == null || pvNamesList.isEmpty()) {
            return new ValidationResult(true, "columnName must be specified");
        }

        // check that all columnNames are non-empty strings
        List<String> emptyColumnNames = pvNamesList.stream()
                .filter(name -> name.isBlank())
                .collect(Collectors.toList());
        if (!emptyColumnNames.isEmpty()) {
            return new ValidationResult(true, "columnNamesList contains empty string");
        }

        // TODO: check that time range doesn't exceed configured maximum

        return validateDataQueryTimeRange(querySpec.getBeginTime(), querySpec.getEndTime());
    }

    public static ValidationResult validateQueryTableRequest(QueryTableRequest request) {

        // validate pvNameList or pvNamePattern
        if (request.hasPvNameList()) {
            if (request.getPvNameList().getPvNamesCount() == 0) {
                final String errorMsg = "QueryTableRequest.pvNameList.pvNames must not be empty";
                return new ValidationResult(true, errorMsg);
            }

        } else if (request.hasPvNamePattern()) {
            if (request.getPvNamePattern().getPattern().isBlank()) {
                final String errorMsg = "QueryTableRequest.pvNamePattern.pattern must not be empty";
                return new ValidationResult(true, errorMsg);
            }
        } else {
            final String errorMsg = "QueryTableRequest must specify either pvNameList or pvNamePattern";
            return new ValidationResult(true, errorMsg);
        }

        return validateDataQueryTimeRange(request.getBeginTime(), request.getEndTime());
    }

}
