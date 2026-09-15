package com.ospreydcs.dp.service.common.mongo;

import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import com.ospreydcs.dp.service.common.telemetry.DpMetrics;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.concurrent.TimeUnit;

/**
 * Records {@code db.client.operation.duration} for every MongoDB command this process issues
 * (issue #212, D4).
 *
 * <p>This is the only view of server round-trips — {@code find} against {@code getMore} against
 * {@code aggregate} against {@code insert} — and it covers every collection and every service
 * without a line of per-call code, including the ingestion writes and every annotation-service
 * query. The instrument is named for the OpenTelemetry database semantic conventions rather than
 * with the {@code dp.} prefix so a dashboard written against the convention works here unchanged.
 *
 * <p><b>What this does and does not cover.</b> A command event pair is emitted only for a command
 * that actually reached a server. A failure during server selection — the whole replica set
 * unreachable, or a connection that cannot be established — throws {@code MongoTimeoutException}
 * to the caller having emitted <em>no</em> command events at all (verified against MongoDB 8.0).
 * So a total database outage reads here as the metric falling silent, not as a rise in
 * {@code error.type}; alerting on "database round-trips stopped" needs the absence of data, and an
 * {@code error.type} rate is a signal about commands the server answered with an error.
 *
 * <p>{@code error.type} is the class of the throwable on the <em>event</em>, which is the
 * command-level failure and not always what the caller catches: a bad index hint surfaces here
 * as {@code MongoCommandException} while the caller sees the driver's
 * {@code MongoQueryException} wrapper. An alert matching on the caller-facing class name would
 * never fire.
 *
 * <p><b>Redaction.</b> Only the collection name is read out of the command document, never the
 * filter. A query filter carries PV names, and both the cardinality policy (D8) and basic
 * discretion about what lands in a metrics backend forbid that. The collection and command names
 * are small bounded sets, so the series count here is bounded by their product.
 */
public class DpMongoCommandListener implements CommandListener {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    /**
     * Field naming the collection on a {@code getMore}, whose first field is the cursor id rather
     * than a collection name.
     */
    private static final String COMMAND_FIELD_COLLECTION = "collection";

    /**
     * Collection name captured at {@code commandStarted}, read back at {@code commandSucceeded} or
     * {@code commandFailed}.
     *
     * <p>A {@code ThreadLocal} rather than a map keyed by request id, because the driver invokes
     * the listener <em>synchronously on the thread that issued the command</em> and delivers that
     * command's end event on the same thread — verified across 3364 commands on 25 threads with
     * deliberate failures mixed in: zero request-id collisions, zero end events on another thread.
     * A map would need an eviction policy for the end event that never arrives, and would grow
     * without bound if that policy were ever wrong. A single-slot {@code ThreadLocal} cannot leak
     * more than one string per driver thread no matter what the driver does.
     *
     * <p>The value is the collection name, or the empty string for a command with no collection
     * (such as {@code hello}, {@code dropDatabase} or {@code endSessions}); null means no start
     * event was seen on this thread, which is why {@link #collectionOf} distinguishes the two.
     */
    private static final ThreadLocal<String> startedCollectionName = new ThreadLocal<>();

    @Override
    public void commandStarted(CommandStartedEvent event) {
        // Unconditional set, never a set-only-on-success: if a start event were ever delivered
        // without the matching end event that clears the slot, the next command on this thread
        // would otherwise inherit the previous command's collection name and be recorded against
        // the wrong collection — a plausible-looking wrong number rather than an error.
        try {
            startedCollectionName.set(collectionNameFromCommand(event.getCommand()));
        } catch (Exception ex) {
            // Never let instrumentation disturb the operation being measured. The driver happens to
            // swallow a listener exception today (verified), but relying on that would make this
            // class's correctness depend on an undocumented driver behavior, and a swallowed
            // exception is the failure mode CLAUDE.md warns about throughout.
            startedCollectionName.remove();
            logger.error(
                    "commandStarted instrumentation error for command: {} exception: {}",
                    event.getCommandName(), ex.getMessage(), ex);
        }
    }

    @Override
    public void commandSucceeded(CommandSucceededEvent event) {
        record(
                event.getCommandName(),
                event.getDatabaseName(),
                event.getElapsedTime(TimeUnit.NANOSECONDS),
                null);
    }

    @Override
    public void commandFailed(CommandFailedEvent event) {
        record(
                event.getCommandName(),
                event.getDatabaseName(),
                event.getElapsedTime(TimeUnit.NANOSECONDS),
                event.getThrowable());
    }

    /**
     * Records one command's duration and clears the per-thread collection name.
     *
     * <p>The clear is in a {@code finally} so that a thread cannot carry a stale collection name
     * into the next command it issues — which would silently attribute one collection's latency to
     * another, a wrong answer rather than an error.
     */
    private void record(String commandName, String databaseName, long elapsedNanos, Throwable error) {

        try {
            final AttributesBuilder attributes = Attributes.builder()
                    .put(DpMetrics.ATTR_DB_OPERATION_NAME, commandName)
                    .put(DpMetrics.ATTR_DB_NAMESPACE, databaseName);

            final String collectionName = collectionOf();
            if (collectionName != null && !collectionName.isEmpty()) {
                attributes.put(DpMetrics.ATTR_DB_COLLECTION_NAME, collectionName);
            }

            if (error != null) {
                attributes.put(DpMetrics.ATTR_ERROR_TYPE, error.getClass().getSimpleName());
            }

            DpMetrics.dbOperationDuration()
                    .record(DpMetrics.nanosToSeconds(elapsedNanos), attributes.build());

        } catch (Exception ex) {
            logger.error(
                    "command instrumentation error for command: {} exception: {}",
                    commandName, ex.getMessage(), ex);
        } finally {
            startedCollectionName.remove();
        }
    }

    private static String collectionOf() {
        return startedCollectionName.get();
    }

    /**
     * Collection name a command applies to, or the empty string when it names none.
     *
     * <p>The shapes, verified against MongoDB 8.0 with the 5.4.0 driver: {@code find},
     * {@code insert}, {@code update}, {@code delete}, {@code aggregate}, {@code count},
     * {@code distinct}, {@code createIndexes} and {@code killCursors} all carry the collection as
     * the value of their first field; {@code getMore} carries the cursor id there and the
     * collection in a separate {@code collection} field; {@code hello}, {@code dropDatabase} and
     * {@code endSessions} name no collection at all.
     *
     * <p>Note that {@code countDocuments()} issues an {@code aggregate}, not a {@code count} —
     * only {@code estimatedDocumentCount()} issues {@code count} — so a dashboard panel filtering
     * on {@code db.operation.name="count"} would miss nearly every count this codebase performs.
     */
    static String collectionNameFromCommand(BsonDocument command) {

        if (command == null || command.isEmpty()) {
            return "";
        }

        final BsonValue firstValue = command.get(command.getFirstKey());
        if (firstValue != null && firstValue.isString()) {
            return firstValue.asString().getValue();
        }

        // getMore, whose first field is the cursor id
        final BsonValue collectionValue = command.get(COMMAND_FIELD_COLLECTION);
        if (collectionValue != null && collectionValue.isString()) {
            return collectionValue.asString().getValue();
        }

        return "";
    }

    /** Clears this thread's captured state; for tests that assert the listener leaves none. */
    static void clearThreadStateForTest() {
        startedCollectionName.remove();
    }

}
