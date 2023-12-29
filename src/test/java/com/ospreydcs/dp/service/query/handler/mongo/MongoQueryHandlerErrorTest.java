package com.ospreydcs.dp.service.query.handler.mongo;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.ResponseType;
import com.ospreydcs.dp.grpc.v1.query.QueryRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryResponse;
import com.ospreydcs.dp.service.common.bson.BucketDocument;
import com.ospreydcs.dp.service.query.QueryTestBase;
import com.ospreydcs.dp.service.query.handler.model.HandlerQueryRequest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class MongoQueryHandlerErrorTest extends MongoQueryHandlerTestBase {

    protected static class ErrorTestClient extends MongoSyncQueryClient implements TestClientInterface {

        @Override
        protected String getCollectionNameBuckets() {
            // THIS TEST WILL create an empty mongo collection, unfortunately.
            return getTestCollectionNameBuckets();
        }

        @Override
        protected String getCollectionNameRequestStatus() {
            return getTestCollectionNameRequestStatus();
        }

        public int insertBucketDocuments(List<BucketDocument> documentList) {
            // DOESN'T ACTUALLY INSERT DATA to mongo, we don't need it for these tests.
            return documentList.size();
        }

        @Override
        public MongoCursor<BucketDocument> executeQuery(HandlerQueryRequest handlerQueryRequest) {
            // THIS IS KEY FOR THE TEST CASE.
            // THE NULL CURSOR returned by this method triggers the error response condition from the handler!
            return null;
        }
    }

    @BeforeClass
    public static void setUp() throws Exception {
        ErrorTestClient testClient = new ErrorTestClient();
        MongoQueryHandler handler = new MongoQueryHandler(testClient);
        setUp(handler, testClient);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        MongoQueryHandlerTestBase.tearDown();
    }

    @Test
    public void testProcessQueryRequestCursorError() {

        // assemble query request
        String col1Name = columnNameBase + "1";
        String col2Name = columnNameBase + "2";
        List<String> columnNames = List.of(col1Name, col2Name);
        QueryTestBase.QueryRequestParams params = new QueryTestBase.QueryRequestParams(
                columnNames,
                startSeconds,
                0L,
                startSeconds + 5,
                0L);
        QueryRequest request = buildQueryRequest(params);

        // send request
        final int numResponesesExpected = 1;

        // WE EXPECT processQueryRequest to trigger an error response from the handler since
        // ErrorTestClient.executeQuery RETURNS A NULL CURSOR.
        List<QueryResponse> responseList = processQueryRequest(request, numResponesesExpected);

        // examine response
        assertEquals(numResponesesExpected, responseList.size());
        QueryResponse summaryResponse = responseList.get(0);
        assertEquals(ResponseType.ERROR_RESPONSE, summaryResponse.getResponseType());
        assertTrue(summaryResponse.hasQueryReport());
        assertTrue(summaryResponse.getQueryReport().hasQueryStatus());
        QueryResponse.QueryReport.QueryStatus status = summaryResponse.getQueryReport().getQueryStatus();
        assertEquals(QueryResponse.QueryReport.QueryStatus.QueryStatusType.QUERY_STATUS_ERROR, status.getQueryStatusType());
        assertEquals("executeQuery returned null cursor", status.getStatusMessage());
    }

}
