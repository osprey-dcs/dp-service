package com.ospreydcs.dp.service.query.handler.mongo.job;

import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import com.ospreydcs.dp.grpc.v1.query.QueryTableRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryTableResponse;
import com.ospreydcs.dp.service.common.telemetry.DpMetrics;
import com.ospreydcs.dp.service.query.handler.QueryTelemetry;
import com.ospreydcs.dp.service.query.handler.mongo.client.MongoQueryClientInterface;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.QueryDataAbstractDispatcher;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.QueryV2Dispatcher;
import io.grpc.stub.StreamObserver;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A throwable escaping a query job must be recorded as an error, not as the telemetry's default
 * outcome of success (CLAUDE.md, "An unchecked throw out of a job must be classified before
 * {@code complete()} runs").
 *
 * <p>The {@code Error} cases are #314's: once the worker survives a non-fatal {@code Error}, the
 * same one can recur on every request, and a catch of {@code RuntimeException} alone counted each
 * occurrence as a success. {@code StackOverflowError} is the non-fatal {@code Error} the worker is
 * designed to survive.
 */
@RunWith(JUnit4.class)
public class QueryJobThrowableClassificationTest {

    private static void assertClassifiedAsError(Throwable toThrow, QueryTelemetry telemetry, Runnable job) {
        try {
            job.run();
            fail("expected " + toThrow.getClass().getSimpleName() + " to propagate to the worker");
        } catch (Throwable t) {
            assertSame("the job must rethrow, so the worker can decide whether to exit", toThrow, t);
        }
        assertEquals(DpMetrics.OUTCOME_ERROR, telemetry.getOutcome());
    }

    private static void runQueryV2Job(Throwable toThrow) {
        final QueryTelemetry telemetry = new QueryTelemetry("queryV2JobTest");
        final QueryV2Dispatcher dispatcher = mock(QueryV2Dispatcher.class);
        doThrow(toThrow).when(dispatcher).executeAndDispatch(any(), any());
        final QueryV2Job job =
                new QueryV2Job(null, dispatcher, mock(MongoQueryClientInterface.class), telemetry);
        assertClassifiedAsError(toThrow, telemetry, job::execute);
    }

    @SuppressWarnings("unchecked")
    private static void runQueryDataJob(Throwable toThrow) {
        final QueryTelemetry telemetry = new QueryTelemetry("queryDataJobTest");
        final MongoQueryClientInterface client = mock(MongoQueryClientInterface.class);
        when(client.executeQueryData(any())).thenThrow(toThrow);
        final QueryDataJob job = new QueryDataJob(
                QueryDataRequest.QuerySpec.getDefaultInstance(),
                mock(QueryDataAbstractDispatcher.class),
                (StreamObserver<QueryDataResponse>) mock(StreamObserver.class),
                client,
                telemetry);
        assertClassifiedAsError(toThrow, telemetry, job::execute);
    }

    @SuppressWarnings("unchecked")
    private static void runQueryTableJob(Throwable toThrow) {
        final QueryTelemetry telemetry = new QueryTelemetry("queryTableJobTest");
        final MongoQueryClientInterface client = mock(MongoQueryClientInterface.class);
        when(client.executeQueryTable(any())).thenThrow(toThrow);
        final QueryTableJob job = new QueryTableJob(
                QueryTableRequest.getDefaultInstance(),
                (StreamObserver<QueryTableResponse>) mock(StreamObserver.class),
                client,
                telemetry);
        assertClassifiedAsError(toThrow, telemetry, job::execute);
    }

    @Test
    public void testQueryV2JobRuntimeExceptionIsAnError() {
        runQueryV2Job(new IllegalStateException("simulated"));
    }

    @Test
    public void testQueryV2JobErrorIsAnError() {
        runQueryV2Job(new StackOverflowError("simulated"));
    }

    @Test
    public void testQueryDataJobRuntimeExceptionIsAnError() {
        runQueryDataJob(new IllegalStateException("simulated"));
    }

    @Test
    public void testQueryDataJobErrorIsAnError() {
        runQueryDataJob(new StackOverflowError("simulated"));
    }

    @Test
    public void testQueryTableJobRuntimeExceptionIsAnError() {
        runQueryTableJob(new IllegalStateException("simulated"));
    }

    @Test
    public void testQueryTableJobErrorIsAnError() {
        runQueryTableJob(new StackOverflowError("simulated"));
    }
}
