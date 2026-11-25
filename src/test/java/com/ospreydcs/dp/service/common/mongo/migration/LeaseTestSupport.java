package com.ospreydcs.dp.service.common.mongo.migration;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.ospreydcs.dp.service.common.mongo.MongoClientBase;
import com.ospreydcs.dp.service.common.mongo.MongoTestClient;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/** Fixtures shared by the migration lease tests (issue #292). */
final class LeaseTestSupport {

    private LeaseTestSupport() {
    }

    /** Exposes the raw handles the runner operates on. */
    static class TestClient extends MongoTestClient {

        MongoDatabase database() {
            return mongoDatabase;
        }

        MongoDatabase adminDatabase() {
            return mongoClient.getDatabase(MongoClientBase.ADMIN_DATABASE_NAME);
        }
    }

    /** One command the driver started: its name, database, and full body. */
    record StartedCommand(String name, String databaseName, BsonDocument command) {

        boolean isCurrentOp() {
            return name.equals("aggregate")
                    && command.containsKey("pipeline")
                    && !command.getArray("pipeline").isEmpty()
                    && command.getArray("pipeline").get(0).asDocument().containsKey("$currentOp");
        }
    }

    /** A second client to the test server that records every command it starts. */
    static class RecordingClient implements AutoCloseable {

        final List<StartedCommand> started = new CopyOnWriteArrayList<>();
        private final MongoClient client;

        RecordingClient() {
            client = MongoClients.create(MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(MongoClientBase.getMongoConnectString()))
                    .addCommandListener(new CommandListener() {
                        @Override
                        public void commandStarted(CommandStartedEvent event) {
                            // the event's document is reused by the driver; keep a copy
                            started.add(new StartedCommand(
                                    event.getCommandName(),
                                    event.getDatabaseName(),
                                    event.getCommand().clone()));
                        }
                    })
                    .build());
        }

        MongoDatabase database(String name) {
            return client.getDatabase(name);
        }

        @Override
        public void close() {
            client.close();
        }
    }

    /** Called before a marker write is sent; may change the marker first, or throw. */
    interface MarkerWriteHook {
        void beforeWrite(BsonDocument filter, BsonDocument update);
    }

    /**
     * A database that behaves as {@code real}, except that {@code hook} sees every update of the
     * marker collection before it is sent. This is how a test puts an event between a waiter's
     * read and its write, or makes one particular marker write fail.
     */
    @SuppressWarnings("unchecked")
    static MongoDatabase interceptingMarkerWrites(MongoDatabase real, MarkerWriteHook hook) {

        final MongoCollection<Document> realMarker = SchemaVersionMarker.collection(real);
        final MongoCollection<Document> marker = mock(MongoCollection.class, delegatesTo(realMarker));

        doAnswer(invocation -> {
            hook.beforeWrite(render(invocation.getArgument(0)), render(invocation.getArgument(1)));
            return realMarker.updateOne(
                    invocation.<Bson>getArgument(0), invocation.<Bson>getArgument(1));
        }).when(marker).updateOne(any(Bson.class), any(Bson.class));

        doAnswer(invocation -> {
            hook.beforeWrite(render(invocation.getArgument(0)), render(invocation.getArgument(1)));
            return realMarker.findOneAndUpdate(
                    invocation.<Bson>getArgument(0), invocation.<Bson>getArgument(1),
                    invocation.<FindOneAndUpdateOptions>getArgument(2));
        }).when(marker).findOneAndUpdate(
                any(Bson.class), any(Bson.class), any(FindOneAndUpdateOptions.class));

        final MongoDatabase database = mock(MongoDatabase.class, delegatesTo(real));
        doReturn(marker).when(database)
                .getCollection(SchemaVersionMarker.COLLECTION_NAME_SERVICE_METADATA);
        return database;
    }

    static BsonDocument render(Bson bson) {
        return bson.toBsonDocument(Document.class, MongoClientSettings.getDefaultCodecRegistry());
    }

    static Document rawMarker(MongoDatabase database) {
        return SchemaVersionMarker.collection(database)
                .find(new Document("_id", SchemaVersionMarker.MARKER_ID)).first();
    }

    static void setOnMarker(MongoDatabase database, Document fields) {
        SchemaVersionMarker.collection(database).updateOne(
                new Document("_id", SchemaVersionMarker.MARKER_ID), new Document("$set", fields));
    }
}
