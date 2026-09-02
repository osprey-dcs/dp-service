package com.ospreydcs.dp.service.common.mongo.migration;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.ospreydcs.dp.service.common.bson.bucket.BucketSpanVerifier;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.mongo.MongoClientBase;
import com.ospreydcs.dp.service.common.mongo.MongoTestClient;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Covers the migration runner's decision table. The scenarios that matter are the ones where a wrong
 * decision is silent: stamping a populated legacy database as already-migrated, or replaying
 * migrations against a fresh install.
 */
public class SchemaMigrationRunnerTest {

    private RunnerTestClient testClient;
    private MongoDatabase database;
    private MongoCollection<Document> markerCollection;

    /** Exposes the raw database handle, which is what the runner operates on. */
    private static class RunnerTestClient extends MongoTestClient {
        MongoDatabase database() {
            return mongoDatabase;
        }
    }

    /** Records that it ran, so tests can assert whether the runner invoked it. */
    private static class RecordingMigration implements Migration {

        final int version;
        final AtomicInteger applyCount = new AtomicInteger();

        RecordingMigration(int version) {
            this.version = version;
        }

        @Override
        public int version() {
            return version;
        }

        @Override
        public String description() {
            return "recording migration v" + version;
        }

        @Override
        public void apply(MongoDatabase database) {
            applyCount.incrementAndGet();
        }
    }

    /** Fails, to exercise the claim-retained-on-failure path. */
    private static class FailingMigration implements Migration {

        @Override
        public int version() {
            return 1;
        }

        @Override
        public String description() {
            return "failing migration";
        }

        @Override
        public void apply(MongoDatabase database) throws DpException {
            throw new DpException("deliberate test failure");
        }
    }

    @Before
    public void setUp() {
        testClient = new RunnerTestClient();
        testClient.init();
        database = testClient.database();
        markerCollection = SchemaVersionMarker.collection(database);
        clearAll();
    }

    /**
     * Clearing the marker here is load-bearing, not tidiness. Several tests below deliberately leave
     * a retained `migrating: true` claim, which is exactly the state that blocks startup — so a
     * marker left behind would be picked up by the next test's client init. {@code MongoTestClient}
     * suppresses migrations for its pre-drop init so that cannot hang the suite, but leaving the
     * claim would still be a surprise waiting for whoever changes that.
     */
    @After
    public void tearDown() {
        clearAll();
        testClient.fini();
    }

    private void clearAll() {
        markerCollection.deleteMany(new Document());
        for (String name : SchemaMigrationRunner.MANAGED_COLLECTION_NAMES) {
            database.getCollection(name).deleteMany(new Document());
        }
    }

    private void seedData() {
        database.getCollection(MongoClientBase.COLLECTION_NAME_ANNOTATIONS)
                .insertOne(new Document("name", "seeded"));
    }

    // ------------------------------------------------------------------
    // fresh vs. legacy: the distinction the mechanism depends on
    // ------------------------------------------------------------------

    @Test
    public void testFreshDatabaseIsStampedWithoutRunningMigrations() throws DpException {

        final RecordingMigration migration = new RecordingMigration(1);
        new SchemaMigrationRunner(database, List.of(migration), 1).run();

        assertEquals(
                "a fresh install must not replay migrations against empty collections",
                0, migration.applyCount.get());

        final SchemaVersionMarker.MarkerState state = SchemaVersionMarker.read(database);
        assertTrue(state.present());
        assertEquals(1, state.version());
        assertFalse(state.migrating());
    }

    @Test
    public void testPopulatedDatabaseWithNoMarkerRunsAllMigrations() throws DpException {

        seedData();

        final RecordingMigration first = new RecordingMigration(1);
        final RecordingMigration second = new RecordingMigration(2);
        new SchemaMigrationRunner(database, List.of(first, second), 2).run();

        assertEquals(
                "a database with data but no marker predates the mechanism and must be migrated",
                1, first.applyCount.get());
        assertEquals(1, second.applyCount.get());
        assertEquals(2, SchemaVersionMarker.read(database).version());
    }

    @Test
    public void testDataInAnyManagedCollectionCountsAsPopulated() throws DpException {

        // Not the annotations collection: the probe must consult every managed collection, since a
        // collection it skips makes a populated database look fresh and silently skips migrations.
        database.getCollection(MongoClientBase.COLLECTION_NAME_SAMPLE_STATUS_BUCKETS)
                .insertOne(new Document("pvName", "seeded"));

        final RecordingMigration migration = new RecordingMigration(1);
        new SchemaMigrationRunner(database, List.of(migration), 1).run();

        assertEquals(1, migration.applyCount.get());
    }

    @Test
    public void testBucketSpanVerificationMarkerAloneCountsAsPopulated() throws DpException {

        // A previous deployment's bucket-span marker is evidence the database has been served
        // before, even with every data collection emptied by a purge, a retention wipe, or a
        // partial restore. Verified against MongoDB 8.0 that omitting this collection from the
        // probe made exactly this database report applyCount=0 — stamped as migrated with the
        // migrations silently skipped, which is the failure the mechanism exists to prevent.
        //
        // The constant lives on BucketSpanVerifier rather than MongoClientBase, which is why the
        // list-coverage test below scans both classes.
        database.getCollection(BucketSpanVerifier.COLLECTION_NAME_BUCKET_SPAN_VERIFICATION)
                .insertOne(new Document("verifiedLimitSeconds", 86400L));

        final RecordingMigration migration = new RecordingMigration(1);
        new SchemaMigrationRunner(database, List.of(migration), 1).run();

        assertEquals(
                "a database carrying a prior deployment's bucket-span marker is not a fresh install",
                1, migration.applyCount.get());
    }

    // ------------------------------------------------------------------
    // version comparison
    // ------------------------------------------------------------------

    @Test
    public void testAlreadyCurrentIsNoOp() throws DpException {

        seedData();
        SchemaVersionMarker.stampFresh(database, 1);

        final RecordingMigration migration = new RecordingMigration(1);
        new SchemaMigrationRunner(database, List.of(migration), 1).run();

        assertEquals(0, migration.applyCount.get());
        assertEquals(1, SchemaVersionMarker.read(database).version());
    }

    @Test
    public void testOnlyPendingMigrationsAreApplied() throws DpException {

        seedData();
        SchemaVersionMarker.stampFresh(database, 1);

        final RecordingMigration first = new RecordingMigration(1);
        final RecordingMigration second = new RecordingMigration(2);
        new SchemaMigrationRunner(database, List.of(first, second), 2).run();

        assertEquals("already-applied migration must not re-run", 0, first.applyCount.get());
        assertEquals(1, second.applyCount.get());
        assertEquals(2, SchemaVersionMarker.read(database).version());
    }

    @Test
    public void testDatabaseNewerThanBinaryRefusesToStart() throws DpException {

        seedData();
        SchemaVersionMarker.stampFresh(database, 5);

        try {
            new SchemaMigrationRunner(database, List.of(new RecordingMigration(1)), 1).run();
            fail("expected refusal on a database written by a newer service build");
        } catch (DpException ex) {
            assertTrue(
                    "message should name both versions so an operator can act: " + ex.getMessage(),
                    ex.getMessage().contains("5") && ex.getMessage().contains("newer"));
        }
    }

    // ------------------------------------------------------------------
    // failure handling
    // ------------------------------------------------------------------

    @Test
    public void testFailedMigrationRetainsClaimAndDoesNotAdvanceVersion() throws DpException {

        seedData();

        try {
            new SchemaMigrationRunner(database, List.of(new FailingMigration()), 1).run();
            fail("expected the migration failure to propagate");
        } catch (DpException ex) {
            assertTrue(ex.getMessage().contains("deliberate test failure"));
        }

        final SchemaVersionMarker.MarkerState state = SchemaVersionMarker.read(database);
        assertEquals(
                "version must not advance past a migration that failed",
                SchemaVersionMarker.VERSION_UNMANAGED, state.version());
        assertTrue(
                "claim must be retained so the next startup blocks rather than serving an "
                        + "unknown schema",
                state.migrating());
    }

    @Test
    public void testStuckClaimBlocksStartup() throws DpException {

        seedData();
        SchemaVersionMarker.createUnmanagedMarker(database);
        // Simulate a process that crashed while holding the claim.
        assertTrue(SchemaVersionMarker.claimForMigration(
                database, SchemaVersionMarker.VERSION_UNMANAGED, "crashed-host"));

        final SchemaMigrationRunner runner =
                new ShortWaitRunner(database, List.of(new RecordingMigration(1)), 1);

        try {
            runner.run();
            fail("expected a stuck claim to block startup rather than proceed");
        } catch (DpException ex) {
            assertTrue(
                    "message should identify the claim holder: " + ex.getMessage(),
                    ex.getMessage().contains("crashed-host"));
        }
    }

    /** Overrides the wait so the stuck-claim test does not take five minutes. */
    private static class ShortWaitRunner extends SchemaMigrationRunner {

        ShortWaitRunner(MongoDatabase database, List<Migration> migrations, int targetVersion) {
            super(database, migrations, targetVersion, 1_500L, 250L);
        }
    }

    // ------------------------------------------------------------------
    // declarations that would otherwise only fail against a real database
    // ------------------------------------------------------------------

    @Test
    public void testMigrationListIsContiguousFromOne() {

        final List<Migration> migrations = SchemaMigrationRunner.MIGRATIONS;

        assertEquals(
                "migration list length must equal SCHEMA_VERSION",
                SchemaMigrationRunner.SCHEMA_VERSION, migrations.size());

        for (int i = 0; i < migrations.size(); i++) {
            assertEquals(
                    "migration versions must be contiguous from 1 with no gaps or duplicates",
                    i + 1, migrations.get(i).version());
            assertNotNull(migrations.get(i).description());
            assertFalse(migrations.get(i).description().isBlank());
        }
    }

    @Test
    public void testManagedCollectionListCoversEveryDeclaredCollection() throws Exception {

        // A collection declared anywhere but omitted from MANAGED_COLLECTION_NAMES makes a populated
        // database look fresh, which stamps it as migrated and silently skips every migration. Pin
        // the list against the declared constants rather than trusting a hand copy.
        //
        // Both declaring classes are consulted. bucketSpanVerification lives on BucketSpanVerifier
        // rather than MongoClientBase, so scanning only the latter would leave it out of scope of a
        // test whose whole promise is that a new collection cannot be forgotten.
        final List<String> declared = new ArrayList<>();
        for (Class<?> declaringClass : List.of(MongoClientBase.class, BucketSpanVerifier.class)) {
            for (Field field : declaringClass.getDeclaredFields()) {
                if (!Modifier.isStatic(field.getModifiers())
                        || !field.getName().startsWith("COLLECTION_NAME_")
                        || field.getType() != String.class) {
                    continue;
                }
                // serviceMetadata holds the marker itself, not service data; including it would make
                // every database look populated the moment a marker is written.
                if (field.getName().equals("COLLECTION_NAME_SERVICE_METADATA")) {
                    continue;
                }
                field.setAccessible(true);
                declared.add((String) field.get(null));
            }
        }

        assertFalse("expected to find COLLECTION_NAME_* constants", declared.isEmpty());

        for (String name : declared) {
            assertTrue(
                    "collection '" + name + "' is declared on MongoClientBase but missing from "
                            + "SchemaMigrationRunner.MANAGED_COLLECTION_NAMES; a populated database "
                            + "would look fresh and skip migrations",
                    SchemaMigrationRunner.MANAGED_COLLECTION_NAMES.contains(name));
        }

        assertEquals(
                "MANAGED_COLLECTION_NAMES has entries not declared on MongoClientBase",
                declared.size(), SchemaMigrationRunner.MANAGED_COLLECTION_NAMES.size());
    }
}
