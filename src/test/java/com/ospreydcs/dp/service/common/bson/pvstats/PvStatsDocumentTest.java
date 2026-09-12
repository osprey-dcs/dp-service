package com.ospreydcs.dp.service.common.bson.pvstats;

import com.ospreydcs.dp.service.common.bson.BsonConstants;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.junit.Test;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Pins the BSON shape of {@link PvStatsDocument} (#232): the PV name is stored under {@code _id}
 * via {@code @BsonId} — the first document class in this codebase keyed by a string rather than an
 * {@code ObjectId id} property — and the statistic field is written under the key the query side
 * reads. Both are silent failures if wrong: the POJO codec drops a field it cannot map without
 * error, and a misnamed key would make every {@code $max} upsert and {@code $in} read miss.
 */
public class PvStatsDocumentTest {

    private static final CodecRegistry REGISTRY = fromRegistries(
            getDefaultCodecRegistry(),
            fromProviders(PojoCodecProvider.builder().register(PvStatsDocument.class).build()));

    @Test
    public void testEncodesPvNameAsIdAndSpanUnderQueryKey() {
        final PvStatsDocument document = new PvStatsDocument();
        document.setPvName("pv-1");
        document.setMaxBucketSpanSeconds(300L);

        final BsonDocument encoded = BsonDocumentWrapper.asBsonDocument(document, REGISTRY);

        assertEquals("pv-1", encoded.getString(BsonConstants.BSON_KEY_PV_STATS_PV_NAME).getValue());
        assertEquals(
                300L,
                encoded.getInt64(BsonConstants.BSON_KEY_PV_STATS_MAX_BUCKET_SPAN_SECONDS).getValue());
        assertEquals(2, encoded.size());
        assertTrue("no pvName key must be written beside _id", !encoded.containsKey("pvName"));
    }

    @Test
    public void testDecodesRoundTrip() {
        final PvStatsDocument document = new PvStatsDocument();
        document.setPvName("pv-2");
        document.setMaxBucketSpanSeconds(86400L);

        final BsonDocument encoded = BsonDocumentWrapper.asBsonDocument(document, REGISTRY);
        final PvStatsDocument decoded = REGISTRY.get(PvStatsDocument.class)
                .decode(encoded.asBsonReader(), DecoderContext.builder().build());

        assertEquals("pv-2", decoded.getPvName());
        assertEquals(86400L, decoded.getMaxBucketSpanSeconds());
    }
}
