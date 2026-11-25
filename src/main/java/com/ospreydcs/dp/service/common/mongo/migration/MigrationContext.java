package com.ospreydcs.dp.service.common.mongo.migration;

/**
 * What the runner hands a {@link Migration} beyond the database (issue #292).
 *
 * @param operationComment the comment every operation the migration issues must carry, on every
 *     driver call that accepts one. It names the claim the migration runs under, and it is how a
 *     process about to take over a claim sees that an operation of a previous holder is still
 *     running on the server. An operation without it is invisible to that check.
 */
public record MigrationContext(String operationComment) {

    /** Every migration operation's comment begins with this; the claim token follows. */
    public static final String OPERATION_COMMENT_PREFIX = "dp-schema-migration:";

    public static MigrationContext forClaimToken(String claimToken) {
        return new MigrationContext(OPERATION_COMMENT_PREFIX + claimToken);
    }
}
