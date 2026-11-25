package com.ospreydcs.dp.service.common.mongo.migration;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.mongo.MongoClientBase;
import org.bson.BsonValue;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Pins that every operation a migration issues carries the claim's comment (issue #292).
 *
 * <p>A process about to take over an abandoned claim first looks, by that comment, for an operation
 * of the previous holder still running on the server, and waits for it. An operation without the
 * comment is invisible to the check, so a migration that forgets it puts back the failure the
 * check exists for: a second full scan started beside one that is still running, once per kill.
 * Nothing else would notice the omission.
 *
 * <p><b>The test names what is exempt, not what is checked.</b> Every command a migration starts
 * must carry the comment unless its name is in {@link #EXEMPT_COMMANDS}. A list of checked commands
 * would pass a future migration issuing a {@code delete}, a {@code findAndModify}, a
 * {@code distinct} or a {@code createIndexes}, and the last is the long-running case the check
 * matters most for. Adding a name to the exempt set is a decision to make against "what the check
 * cannot see" on {@link SchemaVersionMarker#findOrphanedOperations}: it is safe only for a command
 * that finishes at once.
 */
public class MigrationOperationCommentTest {

    /** Commands that cannot carry a comment in driver 5.4.0 and finish at once. */
    private static final Set<String> EXEMPT_COMMANDS = Set.of("dropIndexes", "drop");

    private static final MigrationContext CONTEXT =
            MigrationContext.forClaimToken("comment-coverage-test");

    /**
     * The commands each shipped migration must be seen to issue, so that a fixture which stops
     * driving one of a migration's operations fails here instead of quietly checking less.
     */
    private static final Map<Integer, Set<String>> EXPECTED_COMMANDS = Map.of(
            1, Set.of("listIndexes", "dropIndexes", "aggregate", "update"),
            2, Set.of("find", "update"),
            3, Set.of("find", "update"),
            4, Set.of("update"),
            5, Set.of("aggregate", "drop"));

    private LeaseTestSupport.TestClient testClient;
    private LeaseTestSupport.RecordingClient recording;
    private MongoDatabase database;
    private MongoDatabase recordedDatabase;

    @Before
    public void setUp() {
        testClient = new LeaseTestSupport.TestClient();
        testClient.init();
        database = testClient.database();
        recording = new LeaseTestSupport.RecordingClient();
        recordedDatabase = recording.database(database.getName());
        clearAll();
    }

    @After
    public void tearDown() {
        clearAll();
        recording.close();
        testClient.fini();
    }

    private void clearAll() {
        for (String name : SchemaMigrationRunner.MANAGED_COLLECTION_NAMES) {
            database.getCollection(name).deleteMany(new Document());
        }
        dropTextIndexes();
    }

    private void dropTextIndexes() {
        final MongoCollection<Document> annotations =
                database.getCollection(MongoClientBase.COLLECTION_NAME_ANNOTATIONS);
        for (Document index : annotations.listIndexes()) {
            if (index.get("weights", Document.class) != null) {
                annotations.dropIndex(index.getString("name"));
            }
        }
    }

    /** Data that makes every shipped migration issue each of its operations. */
    private void seedFixture() {

        final MongoCollection<Document> annotations =
                database.getCollection(MongoClientBase.COLLECTION_NAME_ANNOTATIONS);
        // v1: a stale text index to drop and a field to rename
        dropTextIndexes();
        annotations.createIndex(Indexes.compoundIndex(
                Indexes.text("name"), Indexes.text("comment"), Indexes.ascending("ownerId")));
        // v1 renames; v2 has tags to normalize; v3 has reference ids to canonicalize
        annotations.insertOne(new Document("name", "legacy")
                .append("comment", "an old comment")
                .append("tags", List.of("Beam Loss", "OUTAGE"))
                .append("dataSetIds", List.of("66A1B2C3D4E5F60718293A4B")));

        // v4: a bucket column and a calculations column without a discriminator; v5: a span to seed
        database.getCollection(MongoClientBase.COLLECTION_NAME_BUCKETS).insertOne(
                new Document("_id", "pv-100-0")
                        .append("pvName", "pv")
                        .append("firstTime", new Document("seconds", 100L).append("nanos", 0L))
                        .append("lastTime", new Document("seconds", 160L).append("nanos", 0L))
                        .append("dataColumn", new Document("name", "pv")));
        database.getCollection(MongoClientBase.COLLECTION_NAME_CALCULATIONS).insertOne(
                new Document("dataFrames", List.of(new Document("name", "frame")
                        .append("dataColumns", List.of(new Document("name", "column"))))));
        // v5 drops this
        database.getCollection(MongoClientBase.COLLECTION_NAME_BUCKET_SPAN_VERIFICATION_LEGACY)
                .insertOne(new Document("verifiedLimitSeconds", 86400L));
    }

    /** Runs {@code migration} through the recording client and returns the commands it started. */
    private List<LeaseTestSupport.StartedCommand> commandsOf(Migration migration) throws DpException {
        recording.started.clear();
        migration.apply(recordedDatabase, CONTEXT);
        final List<LeaseTestSupport.StartedCommand> commands = new ArrayList<>();
        for (LeaseTestSupport.StartedCommand command : recording.started) {
            // the client's own traffic (session and connection upkeep) goes to admin
            if (command.databaseName().equals(database.getName())) {
                commands.add(command);
            }
        }
        return commands;
    }

    private static List<String> uncommented(List<LeaseTestSupport.StartedCommand> commands) {
        final List<String> offenders = new ArrayList<>();
        for (LeaseTestSupport.StartedCommand command : commands) {
            if (EXEMPT_COMMANDS.contains(command.name())) {
                continue;
            }
            final BsonValue comment = command.command().get("comment");
            if (comment == null || !comment.isString()
                    || !comment.asString().getValue().equals(CONTEXT.operationComment())) {
                offenders.add(command.name());
            }
        }
        return offenders;
    }

    @Test
    public void testEveryOperationOfEveryShippedMigrationCarriesTheClaimComment() throws DpException {

        seedFixture();

        for (Migration migration : SchemaMigrationRunner.MIGRATIONS) {

            final Set<String> expected = EXPECTED_COMMANDS.get(migration.version());
            assertNotNull(
                    "migration v" + migration.version() + " has no entry in EXPECTED_COMMANDS: add "
                            + "one, and data to seedFixture() that makes it issue each operation",
                    expected);

            final List<LeaseTestSupport.StartedCommand> commands = commandsOf(migration);

            final Set<String> issued = commands.stream()
                    .map(LeaseTestSupport.StartedCommand::name).collect(Collectors.toSet());
            assertTrue(
                    "the fixture no longer makes migration v" + migration.version() + " issue "
                            + expected + "; it issued " + issued,
                    issued.containsAll(expected));

            assertEquals(
                    "migration v" + migration.version() + " issued commands without the claim "
                            + "comment; a takeover cannot see them still running on the server",
                    List.of(), uncommented(commands));
        }
    }

    @Test
    public void testAnUncommentedCommandOutsideTheExemptSetIsCaught() throws DpException {

        // The check must catch a command nobody thought to list. A delete is one no shipped
        // migration issues.
        final Migration forgetful = new Migration() {
            @Override
            public int version() {
                return 99;
            }

            @Override
            public String description() {
                return "issues a delete without the comment";
            }

            @Override
            public void apply(MongoDatabase database, MigrationContext context) {
                database.getCollection(MongoClientBase.COLLECTION_NAME_ANNOTATIONS)
                        .deleteMany(new Document("name", "nothing"));
            }
        };

        assertEquals(List.of("delete"), uncommented(commandsOf(forgetful)));
    }
}
