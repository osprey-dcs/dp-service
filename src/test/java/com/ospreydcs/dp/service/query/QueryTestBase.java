package com.ospreydcs.dp.service.query;

import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.query.QueryDataByTimeRequest;

public class QueryTestBase {

    public class QueryDataByTimeRequestParams {

        public String columnName = null;
        public Long startTimeSeconds = null;
        public Long startTimeNanos = null;
        public Long endTimeSeconds = null;
        public Long endTimeNanos = null;

        public QueryDataByTimeRequestParams(
                String columnName,
                Long startTimeSeconds,
                Long startTimeNanos,
                Long endTimeSeconds,
                Long endTimeNanos) {

            this.columnName = columnName;
            this.startTimeSeconds = startTimeSeconds;
            this.startTimeNanos = startTimeNanos;
            this.endTimeSeconds = endTimeSeconds;
            this.endTimeNanos = endTimeNanos;
        }
    }
    
    public QueryDataByTimeRequest buildQueryDataByTimeRequest(QueryDataByTimeRequestParams params) {
        
        // build API query request from params
        QueryDataByTimeRequest.Builder requestBuilder = QueryDataByTimeRequest.newBuilder();
        
        if (params.columnName != null) {
            requestBuilder.setColumnName(params.columnName);
        }
        
        if (params.startTimeSeconds != null) {
            final Timestamp.Builder startTimeBuilder = Timestamp.newBuilder();
            startTimeBuilder.setEpochSeconds(params.startTimeSeconds);
            if (params.startTimeNanos != null) startTimeBuilder.setNanoseconds(params.startTimeNanos);
            startTimeBuilder.build();
            requestBuilder.setStartTime(startTimeBuilder);
        }
        
        if (params.endTimeSeconds != null) {
            final Timestamp.Builder endTimeBuilder = Timestamp.newBuilder();
            endTimeBuilder.setEpochSeconds(params.endTimeSeconds);
            if (params.endTimeNanos != null) endTimeBuilder.setNanoseconds(params.endTimeNanos);
            endTimeBuilder.build();
            requestBuilder.setEndTime(endTimeBuilder);
        }

        return requestBuilder.build();
    }
}
