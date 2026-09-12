package com.ospreydcs.dp.service.ingest.handler.mongo.client;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import com.ospreydcs.dp.service.common.bson.BsonConstants;
import com.ospreydcs.dp.service.common.bson.pvstats.PvStatsDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.mongo.MongoTestClient;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Unit test for {@link PvStatsMaxSpanUpdater} against the real test database (#232).
 *
 * <p>The updater has two halves and the test pins both. The stored half is the {@code $max}
 * upsert on {@code pvStats}: first sight of a PV creates its document, a larger span raises the
 * stored value, and a smaller span never lowers it, not even one stored by another process. The
 * in-process half is the high-watermark cache that gates the write: a span the cache already
 * covers issues no write at all, and the cache advances only after the bulk write is known to
 * have succeeded, so a failed or unacknowledged write is retried by the next batch rather than
 * skipped for the life of the process (plan D5).
 *
 * <p>Writes are observed through a Mockito mock of {@link MongoCollection} that delegates every
 * call to the real dp-test collection: the database state stays real, while the test can count
 * {@code bulkWrite} calls, inspect the write models, and make one call throw or come back
 * unacknowledged. A collection handle on a closed client would also fail, but with a driver state
 * exception rather than the {@link MongoException} the updater classifies, and it could not
 * produce the unacknowledged case.
 */
public class PvStatsMaxSpanUpdaterTest {

    private static final CodecRegistry CODEC_REGISTRY = MongoClientSettings.getDefaultCodecRegistry();

    /**
     * Exposes the protected pvStats collection handle. {@code MongoTestClient.init()} drops and
     * recreates the test database, so constructing and initializing this client is the whole setup.
     */
    private static class TestClient extends MongoTestClient {
        MongoCollection<Document> pvStatsCollection() {
            return mongoCollectionPvStats.withDocumentClass(Document.class);
        }
    }

    private static TestClient client;
    private static MongoCollection<Document> realCollection;

    // per test: a fresh delegating mock and a fresh updater, so no watermark leaks between tests
    private MongoCollection<Document> collection;
    private PvStatsMaxSpanUpdater updater;

    @BeforeClass
    public static void setUp() {
        client = new TestClient();
        assertTrue("test client init failed", client.init());
        realCollection = client.pvStatsCollection();
    }

    @AfterClass
    public static void tearDown() {
        client.fini();
    }

    @Before
    public void clearPvStats() {
        realCollection.deleteMany(new Document());
        collection = delegatingMock(realCollection);
        updater = new PvStatsMaxSpanUpdater(collection);
    }

    @SuppressWarnings("unchecked")
    private static MongoCollection<Document> delegatingMock(MongoCollection<Document> real) {
        return mock(MongoCollection.class, delegatesTo(real));
    }

    @Test
    public void testFirstSightUpsertsDocument() throws DpException {

        final String pvName = "updater_first";
        assertNull("fixture must start without a pvStats document", storedSpan(pvName));

        updater.recordSpan(List.of(pvName), 42L);

        final ArgumentCaptor<List<WriteModel<Document>>> writes = ArgumentCaptor.captor();
        final ArgumentCaptor<BulkWriteOptions> options = ArgumentCaptor.captor();
        verify(collection, times(1)).bulkWrite(writes.capture(), options.capture());
        assertEquals("first sight of a PV is one upsert", Set.of(pvName), assertMaxUpserts(writes.getValue(), 42L));
        assertTrue("stats bulk must be unordered", !options.getValue().isOrdered());

        assertEquals(Long.valueOf(42L), storedSpan(pvName));
    }

    @Test
    public void testLargerSpanRaisesStoredValue() throws DpException {

        final String pvName = "updater_raise";

        updater.recordSpan(List.of(pvName), 10L);
        assertEquals(Long.valueOf(10L), storedSpan(pvName));

        updater.recordSpan(List.of(pvName), 25L);
        assertEquals("a larger span must raise the stored value", Long.valueOf(25L), storedSpan(pvName));

        verify(collection, times(2)).bulkWrite(anyList(), any(BulkWriteOptions.class));
    }

    @Test
    public void testSmallerOrEqualSpanIssuesNoWrite() throws DpException {

        final String pvName = "updater_covered";

        updater.recordSpan(List.of(pvName), 25L);
        verify(collection, times(1)).bulkWrite(anyList(), any(BulkWriteOptions.class));

        // equal and smaller spans are covered by the watermark: no write at all, not a no-op write
        updater.recordSpan(List.of(pvName), 25L);
        updater.recordSpan(List.of(pvName), 10L);
        updater.recordSpan(List.of(pvName), 0L);
        verify(collection, times(1)).bulkWrite(anyList(), any(BulkWriteOptions.class));

        assertEquals(Long.valueOf(25L), storedSpan(pvName));
    }

    @Test
    public void testSmallerSpanNeverLowersValueStoredByAnotherProcess() throws DpException {

        // a document this process has never written, standing in for a peer's larger observation
        final String pvName = "updater_peer";
        final PvStatsDocument peerDocument = new PvStatsDocument();
        peerDocument.setPvName(pvName);
        peerDocument.setMaxBucketSpanSeconds(100L);
        client.insertPvStatsDocument(peerDocument);

        // no cache entry, so the write is issued -- and $max must leave the larger stored value alone
        updater.recordSpan(List.of(pvName), 50L);
        verify(collection, times(1)).bulkWrite(anyList(), any(BulkWriteOptions.class));
        assertEquals("$max must not lower a larger stored value", Long.valueOf(100L), storedSpan(pvName));

        // the watermark records what this process wrote, not what is stored, so the same span is
        // skipped next time and a larger one still goes through
        updater.recordSpan(List.of(pvName), 50L);
        verify(collection, times(1)).bulkWrite(anyList(), any(BulkWriteOptions.class));
        updater.recordSpan(List.of(pvName), 150L);
        verify(collection, times(2)).bulkWrite(anyList(), any(BulkWriteOptions.class));
        assertEquals(Long.valueOf(150L), storedSpan(pvName));
    }

    @Test
    public void testManyNamesShareOneBulkAndDuplicatesCollapse() throws DpException {

        final String pvA = "updater_bulk_a";
        final String pvB = "updater_bulk_b";

        updater.recordSpan(List.of(pvA, pvB, pvA), 7L);

        final ArgumentCaptor<List<WriteModel<Document>>> writes = ArgumentCaptor.captor();
        verify(collection, times(1)).bulkWrite(writes.capture(), any(BulkWriteOptions.class));
        assertEquals("one bulk carries one upsert per distinct PV",
                Set.of(pvA, pvB), assertMaxUpserts(writes.getValue(), 7L));
        assertEquals(2, writes.getValue().size());

        assertEquals(Long.valueOf(7L), storedSpan(pvA));
        assertEquals(Long.valueOf(7L), storedSpan(pvB));

        // a later batch naming both plus a new PV writes only the new one
        final String pvC = "updater_bulk_c";
        updater.recordSpan(List.of(pvA, pvB, pvC), 7L);
        verify(collection, times(2)).bulkWrite(writes.capture(), any(BulkWriteOptions.class));
        assertEquals(Set.of(pvC), assertMaxUpserts(writes.getValue(), 7L));
    }

    @Test
    public void testWatermarkNotAdvancedWhenBulkWriteThrows() throws DpException {

        final String pvName = "updater_throws";
        doThrow(new MongoException("simulated connection failure"))
                .when(collection).bulkWrite(anyList(), any(BulkWriteOptions.class));

        final DpException thrown =
                assertThrows(DpException.class, () -> updater.recordSpan(List.of(pvName), 30L));
        assertTrue("message should carry the driver failure, was: " + thrown.getMessage(),
                thrown.getMessage().contains("simulated connection failure"));
        assertNull("nothing reached the database", storedSpan(pvName));

        // with the collection healthy again the same span must be written, not skipped as covered
        doAnswer(delegatesTo(realCollection))
                .when(collection).bulkWrite(anyList(), any(BulkWriteOptions.class));
        updater.recordSpan(List.of(pvName), 30L);
        verify(collection, times(2)).bulkWrite(anyList(), any(BulkWriteOptions.class));
        assertEquals(Long.valueOf(30L), storedSpan(pvName));
    }

    @Test
    public void testWatermarkNotAdvancedWhenBulkWriteUnacknowledged() throws DpException {

        final String pvName = "updater_unacked";
        doReturn(BulkWriteResult.unacknowledged())
                .when(collection).bulkWrite(anyList(), any(BulkWriteOptions.class));

        final DpException thrown =
                assertThrows(DpException.class, () -> updater.recordSpan(List.of(pvName), 30L));
        assertTrue("message should say the write was not acknowledged, was: " + thrown.getMessage(),
                thrown.getMessage().contains("not acknowledged"));
        assertNull("an unacknowledged write is not known to be stored", storedSpan(pvName));

        doAnswer(delegatesTo(realCollection))
                .when(collection).bulkWrite(anyList(), any(BulkWriteOptions.class));
        updater.recordSpan(List.of(pvName), 30L);
        verify(collection, times(2)).bulkWrite(anyList(), any(BulkWriteOptions.class));
        assertEquals(Long.valueOf(30L), storedSpan(pvName));
    }

    @Test
    public void testNegativeSpanIsRejectedBeforeAnyWrite() {

        assertThrows(IllegalArgumentException.class,
                () -> updater.recordSpan(List.of("updater_negative"), -1L));
        verify(collection, never()).bulkWrite(anyList(), any(BulkWriteOptions.class));
    }

    /**
     * Asserts every model in a captured bulk is an upsert of {@code $max maxBucketSpanSeconds}
     * to the expected span, keyed by PV name, with no PV repeated; returns the PV names written.
     */
    private static Set<String> assertMaxUpserts(List<WriteModel<Document>> writes, long expectedSpanSeconds) {
        final Set<String> pvNames = new HashSet<>();
        for (WriteModel<Document> write : writes) {
            assertTrue("every stats write must be an UpdateOneModel, was: " + write,
                    write instanceof UpdateOneModel);
            final UpdateOneModel<Document> update = (UpdateOneModel<Document>) write;
            assertTrue("stats write must upsert", update.getOptions().isUpsert());
            final BsonDocument filter = update.getFilter().toBsonDocument(BsonDocument.class, CODEC_REGISTRY);
            final BsonDocument operators = update.getUpdate().toBsonDocument(BsonDocument.class, CODEC_REGISTRY);
            assertEquals("stats write must use $max and nothing else, was: " + operators,
                    Set.of("$max"), operators.keySet());
            assertEquals(expectedSpanSeconds, operators.getDocument("$max")
                    .getInt64(BsonConstants.BSON_KEY_PV_STATS_MAX_BUCKET_SPAN_SECONDS).getValue());
            final String pvName = filter.getString(BsonConstants.BSON_KEY_PV_STATS_PV_NAME).getValue();
            assertTrue("one bulk must not carry two upserts for " + pvName, pvNames.add(pvName));
        }
        return pvNames;
    }

    private static Long storedSpan(String pvName) {
        final PvStatsDocument document = client.findPvStatsNoRetry(pvName);
        return document == null ? null : document.getMaxBucketSpanSeconds();
    }

}
