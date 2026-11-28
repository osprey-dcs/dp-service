package com.ospreydcs.dp.service.common.server;

/**
 * What this process's initialization is doing about the schema, for the readiness gate's
 * rejection message and the initialization gauges (issue #293).
 *
 * <p>The schema migration runner knows whether this process is migrating or waiting, and it is
 * four constructors below the server that needs to say so. It sets the value here; the gate and
 * the gauges read it.
 *
 * <p><b>This is static state, deliberately.</b> {@code DpTelemetry} avoids process-wide state
 * because the integration tests build many servers in one JVM. It is acceptable here because the
 * value describes the database, not a server: every service in one JVM reads the same marker. A
 * stale value is harmless once a gate is open, since nothing reads the description then.
 */
public final class InitializationStatus {

    public enum Phase {
        /** Initializing, with no migration seen (also the state once the schema is settled). */
        STARTING,
        /** This process holds the migration claim and is applying a migration. */
        MIGRATING,
        /** Another process holds the migration claim and this one is waiting for it. */
        WAITING
    }

    /**
     * @param migrationVersion the version being applied, meaningful only when {@code MIGRATING}
     * @param targetVersion the version this binary migrates to, meaningful only when
     *     {@code MIGRATING}
     * @param phaseSinceNanos {@code System.nanoTime()} when the phase began
     * @param takeoverEnabled whether the startup orphaned-operation check could be run: null
     *     until it has been tried
     */
    public record Snapshot(
            Phase phase,
            int migrationVersion,
            int targetVersion,
            long phaseSinceNanos,
            Boolean takeoverEnabled) {
    }

    private static volatile Snapshot current = initial();

    private InitializationStatus() {
    }

    private static Snapshot initial() {
        return new Snapshot(Phase.STARTING, 0, 0, System.nanoTime(), null);
    }

    public static Snapshot current() {
        return current;
    }

    public static synchronized void setStarting() {
        if (current.phase() != Phase.STARTING) {
            current = new Snapshot(Phase.STARTING, 0, 0, System.nanoTime(), current.takeoverEnabled());
        }
    }

    public static synchronized void setMigrating(int migrationVersion, int targetVersion) {
        current = new Snapshot(
                Phase.MIGRATING, migrationVersion, targetVersion, System.nanoTime(),
                current.takeoverEnabled());
    }

    public static synchronized void setWaiting() {
        if (current.phase() != Phase.WAITING) {
            current = new Snapshot(Phase.WAITING, 0, 0, System.nanoTime(), current.takeoverEnabled());
        }
    }

    /** Records whether the startup orphaned-operation check could be run. */
    public static synchronized void setTakeoverEnabled(boolean enabled) {
        final Snapshot now = current;
        current = new Snapshot(
                now.phase(), now.migrationVersion(), now.targetVersion(), now.phaseSinceNanos(),
                enabled);
    }

    /**
     * The reason a closed gate gives. It goes to every client, so it does not name the process
     * that holds a migration: that is a pid and a host name, and it is in the log.
     */
    public static String describe() {
        final Snapshot now = current;
        return switch (now.phase()) {
            case MIGRATING -> "service is initializing: applying schema migration version "
                    + now.migrationVersion() + " (target " + now.targetVersion() + ")";
            case WAITING -> "service is initializing: waiting for a schema migration running in "
                    + "another process";
            case STARTING -> "service is initializing";
        };
    }

    public static synchronized void resetForTest() {
        current = initial();
    }
}
