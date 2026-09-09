package com.ospreydcs.dp.service.common.mongo.migration.migrations;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.mongo.MongoClientBase;
import com.ospreydcs.dp.service.common.mongo.migration.Migration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.List;

/**
 * Stamps the {@code _t: "dataColumn"} class discriminator on every embedded legacy column document
 * that lacks it — bucket {@code dataColumn} subdocuments and calculations frame {@code dataColumns}
 * array entries alike (#248 Phase 4, plan D27).
 *
 * <p>Columns written before rel-1.13.0 predate the {@code @BsonDiscriminator} that #173 added, and
 * #173 shipped no migration because the mechanism did not exist until #254 — this is that missing
 * migration. The POJO codec decodes a discriminator-less entry only under a <i>concrete</i> declared
 * field type; both storage sites now declare the abstract {@code ColumnDocumentBase}
 * ({@code BucketDocument.dataColumn} since #173, {@code CalculationsDataFrameDocument.dataColumns}
 * since the #248 Phase 4 retype), so an unstamped entry throws {@code CodecConfigurationException}
 * mid-decode. For buckets that failure is fully silent today: it escapes the query dispatchers'
 * {@code DpException}-only catch upstream of {@code dataBucketFromDocument()}, and the client
 * receives zero buckets with no error. The pre-1.13 subdocument is field-identical to the current
 * shape ({@code name}/{@code valueCase}/{@code valueType}/{@code bytes}), so stamping the
 * discriminator is sufficient — no field rewrite is needed.
 *
 * <p>On a large archive the buckets update is a one-time full collection scan (there is no index on
 * {@code dataColumn._t}); expect minutes to perhaps an hour at reference-archive scale. Waiting
 * service processes time out after five minutes with the held-claim message and must simply be
 * restarted — see the operator guidance in {@code doc/schema-migration.md}.
 *
 * <p><b>Idempotency.</b> Both updates filter on entries whose {@code _t} key is absent and set
 * exactly that key, so a stamped entry can never match again: a re-run matches nothing and writes
 * nothing. The filters also naturally skip documents that never held an embedded column document —
 * v1-shaped buckets without a {@code dataColumn} subdocument, and typed columns stamped by 1.13+
 * builds (whatever their {@code _t} value) are all untouched.
 */
public class V4StampColumnDiscriminators implements Migration {

    private static final Logger logger = LogManager.getLogger();

    static final String FIELD_DATA_COLUMN = "dataColumn";
    static final String FIELD_DATA_FRAMES = "dataFrames";
    static final String FIELD_DATA_COLUMNS = "dataColumns";
    static final String DISCRIMINATOR_KEY = "_t";
    static final String DISCRIMINATOR_LEGACY_VALUE = "dataColumn";

    @Override
    public int version() {
        return 4;
    }

    @Override
    public String description() {
        return "stamp _t discriminator on legacy bucket and calculations columns";
    }

    @Override
    public void apply(MongoDatabase database) throws DpException {

        try {
            // buckets: one embedded subdocument per document
            final MongoCollection<Document> buckets =
                    database.getCollection(MongoClientBase.COLLECTION_NAME_BUCKETS);
            final Bson bucketFilter = Filters.and(
                    Filters.exists(FIELD_DATA_COLUMN),
                    Filters.exists(FIELD_DATA_COLUMN + "." + DISCRIMINATOR_KEY, false));
            final UpdateResult bucketResult = buckets.updateMany(
                    bucketFilter,
                    Updates.set(
                            FIELD_DATA_COLUMN + "." + DISCRIMINATOR_KEY, DISCRIMINATOR_LEGACY_VALUE));
            logger.info(
                    "V4StampColumnDiscriminators: stamped _t on {} bucket document(s)",
                    bucketResult.getModifiedCount());

            // calculations: per entry of each frame's dataColumns array. The "frame" array filter
            // restricts traversal to frames that actually carry a dataColumns array, so the
            // positional operator cannot fault on a frame without one; the "col" filter selects
            // exactly the entries missing the discriminator.
            final MongoCollection<Document> calculations =
                    database.getCollection(MongoClientBase.COLLECTION_NAME_CALCULATIONS);
            final Bson calculationsFilter = Filters.elemMatch(
                    FIELD_DATA_FRAMES,
                    Filters.elemMatch(
                            FIELD_DATA_COLUMNS, Filters.exists(DISCRIMINATOR_KEY, false)));
            final UpdateResult calculationsResult = calculations.updateMany(
                    calculationsFilter,
                    Updates.set(
                            FIELD_DATA_FRAMES + ".$[frame]." + FIELD_DATA_COLUMNS + ".$[col]."
                                    + DISCRIMINATOR_KEY,
                            DISCRIMINATOR_LEGACY_VALUE),
                    new UpdateOptions().arrayFilters(List.of(
                            Filters.exists("frame." + FIELD_DATA_COLUMNS),
                            Filters.exists("col." + DISCRIMINATOR_KEY, false))));
            logger.info(
                    "V4StampColumnDiscriminators: stamped _t on column entries in {} calculations document(s)",
                    calculationsResult.getModifiedCount());

        } catch (MongoException ex) {
            logger.error(
                    "V4StampColumnDiscriminators: mongo exception stamping discriminators: {}",
                    ex.getMessage(), ex);
            throw new DpException("error stamping column discriminators: " + ex.getMessage(), ex);
        }
    }
}
