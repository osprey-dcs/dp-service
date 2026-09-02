package com.ospreydcs.dp.service.common.mongo.migration;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.mongo.MongoTestClient;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Covers the marker document, and in particular the claim protocol that elects exactly one process
 * to migrate. The documented deployment starts three service processes against one database, so a
 * claim that could be held twice would allow two concurrent migrations.
 */
public class SchemaVersionMarkerTest {

    private MarkerTestClient testClient;
    private MongoDatabase database;
    private MongoCollection<Document> markerCollection;

    private static class MarkerTestClient extends MongoTestClient {
        MongoDatabase database() {
            return mongoDatabase;
        }
    }

    @Before
    public void setUp() {
        testClient = new MarkerTestClient();
        testClient.init();
        database = testClient.database();
        markerCollection = SchemaVersionMarker.collection(database);
        markerCollection.deleteMany(new Document());
    }

    @After
    public void tearDown() {
        markerCollection.deleteMany(new Document());
        testClient.fini();
    }

    @Test
    public void testAbsentMarkerIsReportedAsAbsentNotAsAVersion() throws DpException {

        final SchemaVersionMarker.MarkerState state = SchemaVersionMarker.read(database);

        assertFalse(
                "an absent marker must be distinguishable from a marker at version 0; the caller "
                        + "decides between fresh install and legacy database and cannot do so if "
                        + "absence is reported as a version",
                state.present());
    }

    @Test
    public void testStampFreshCreatesMarkerAndSecondCallIsRefused() throws DpException {

        assertTrue(SchemaVersionMarker.stampFresh(database, 3));

        final SchemaVersionMarker.MarkerState state = SchemaVersionMarker.read(database);
        assertTrue(state.present());
        assertEquals(3, state.version());
        assertFalse(state.migrating());

        assertFalse(
                "a second stamp must not overwrite an existing marker; a concurrent process may "
                        + "have written the authoritative one",
                SchemaVersionMarker.stampFresh(database, 99));
        assertEquals(3, SchemaVersionMarker.read(database).version());
        assertEquals(1, markerCollection.countDocuments());
    }

    @Test
    public void testClaimSucceedsOnceAndIsRefusedToASecondCaller() throws DpException {

        SchemaVersionMarker.createUnmanagedMarker(database);

        assertTrue(
                SchemaVersionMarker.claimForMigration(
                        database, SchemaVersionMarker.VERSION_UNMANAGED, "first-process"));
        assertFalse(
                "exactly one process may hold the claim, or two migrations run concurrently",
                SchemaVersionMarker.claimForMigration(
                        database, SchemaVersionMarker.VERSION_UNMANAGED, "second-process"));

        final SchemaVersionMarker.MarkerState state = SchemaVersionMarker.read(database);
        assertTrue(state.migrating());
        assertEquals("first-process", state.migratingHost());
        assertNotNull(
                "migratingSince lets an operator tell a stuck migration from a slow one",
                state.migratingSince());
    }

    @Test
    public void testClaimIsRefusedWhenTheObservedVersionIsStale() throws DpException {

        SchemaVersionMarker.stampFresh(database, 2);

        assertFalse(
                "a claim built on a version another process has already advanced past must fail, "
                        + "or the claimant would re-run migrations already applied",
                SchemaVersionMarker.claimForMigration(database, 1, "stale-process"));
    }

    @Test
    public void testReleaseClearsTheClaimAndAllowsAnother() throws DpException {

        SchemaVersionMarker.createUnmanagedMarker(database);
        assertTrue(SchemaVersionMarker.claimForMigration(
                database, SchemaVersionMarker.VERSION_UNMANAGED, "first-process"));

        SchemaVersionMarker.releaseClaim(database);

        final SchemaVersionMarker.MarkerState state = SchemaVersionMarker.read(database);
        assertFalse(state.migrating());

        assertTrue(SchemaVersionMarker.claimForMigration(
                database, SchemaVersionMarker.VERSION_UNMANAGED, "second-process"));
    }

    @Test
    public void testRecordAppliedAdvancesVersionAndAppendsToAuditList() throws DpException {

        SchemaVersionMarker.createUnmanagedMarker(database);

        SchemaVersionMarker.recordApplied(database, 1, "first change");
        SchemaVersionMarker.recordApplied(database, 2, "second change");

        assertEquals(2, SchemaVersionMarker.read(database).version());
        assertEquals(
                List.of(1, 2), SchemaVersionMarker.readAppliedVersions(database));
    }

    @Test
    public void testCorruptMarkerIsAnErrorRatherThanAnAssumedVersion() {

        markerCollection.insertOne(
                new Document("_id", SchemaVersionMarker.MARKER_ID).append("somethingElse", 1));

        try {
            SchemaVersionMarker.read(database);
            fail("a marker with no version field must not be read as any particular version");
        } catch (DpException ex) {
            assertTrue(ex.getMessage().contains("corrupt"));
        }
    }
}
