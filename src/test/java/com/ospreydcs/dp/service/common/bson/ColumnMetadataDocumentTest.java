package com.ospreydcs.dp.service.common.bson;

import com.ospreydcs.dp.grpc.v1.common.Attribute;
import com.ospreydcs.dp.grpc.v1.common.ColumnMetadata;
import com.ospreydcs.dp.grpc.v1.common.ColumnProvenance;
import com.ospreydcs.dp.grpc.v1.common.TimeRange;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class ColumnMetadataDocumentTest {

    // -----------------------------------------------------------------------
    // fromColumnMetadata tests
    // -----------------------------------------------------------------------

    @Test
    public void testFromColumnMetadata_fullMetadata() {
        ColumnMetadata proto = ColumnMetadata.newBuilder()
                .setProvenance(ColumnProvenance.newBuilder()
                        .setSource("archiver")
                        .setProcess("epics-bridge")
                        .build())
                .addTags("fast")
                .addTags("critical")
                .addAttributes(Attribute.newBuilder().setName("units").setValue("mm").build())
                .addAttributes(Attribute.newBuilder().setName("site").setValue("SLAC").build())
                .build();

        ColumnMetadataDocument doc = ColumnMetadataDocument.fromColumnMetadata(proto);

        assertNotNull(doc.getProvenance());
        assertEquals("archiver", doc.getProvenance().getSource());
        assertEquals("epics-bridge", doc.getProvenance().getProcess());
        assertEquals(Arrays.asList("fast", "critical"), doc.getTags());
        assertNotNull(doc.getAttributes());
        assertEquals("mm", doc.getAttributes().get("units"));
        assertEquals("SLAC", doc.getAttributes().get("site"));
    }

    @Test
    public void testFromColumnMetadata_provenanceOnly() {
        ColumnMetadata proto = ColumnMetadata.newBuilder()
                .setProvenance(ColumnProvenance.newBuilder()
                        .setSource("src")
                        .setProcess("proc")
                        .build())
                .build();

        ColumnMetadataDocument doc = ColumnMetadataDocument.fromColumnMetadata(proto);

        assertNotNull(doc.getProvenance());
        assertEquals("src", doc.getProvenance().getSource());
        assertEquals("proc", doc.getProvenance().getProcess());
        assertNull(doc.getTags());
        assertNull(doc.getAttributes());
    }

    @Test
    public void testFromColumnMetadata_tagsOnly() {
        ColumnMetadata proto = ColumnMetadata.newBuilder()
                .addTags("tag1")
                .addTags("tag2")
                .build();

        ColumnMetadataDocument doc = ColumnMetadataDocument.fromColumnMetadata(proto);

        assertNull(doc.getProvenance());
        assertEquals(Arrays.asList("tag1", "tag2"), doc.getTags());
        assertNull(doc.getAttributes());
    }

    @Test
    public void testFromColumnMetadata_attributesOnly() {
        ColumnMetadata proto = ColumnMetadata.newBuilder()
                .addAttributes(Attribute.newBuilder().setName("k").setValue("v").build())
                .build();

        ColumnMetadataDocument doc = ColumnMetadataDocument.fromColumnMetadata(proto);

        assertNull(doc.getProvenance());
        assertNull(doc.getTags());
        assertNotNull(doc.getAttributes());
        assertEquals("v", doc.getAttributes().get("k"));
    }

    @Test
    public void testFromColumnMetadata_emptyMetadata() {
        ColumnMetadata proto = ColumnMetadata.newBuilder().build();

        ColumnMetadataDocument doc = ColumnMetadataDocument.fromColumnMetadata(proto);

        assertNull(doc.getProvenance());
        assertNull(doc.getTags());
        assertNull(doc.getAttributes());
    }

    // -----------------------------------------------------------------------
    // toColumnMetadata round-trip tests
    // -----------------------------------------------------------------------

    @Test
    public void testRoundTrip_fullMetadata() {
        ColumnMetadata original = ColumnMetadata.newBuilder()
                .setProvenance(ColumnProvenance.newBuilder()
                        .setSource("archiver")
                        .setProcess("bridge")
                        .build())
                .addTags("fast")
                .addTags("critical")
                .addAttributes(Attribute.newBuilder().setName("units").setValue("mm").build())
                .build();

        ColumnMetadataDocument doc = ColumnMetadataDocument.fromColumnMetadata(original);
        ColumnMetadata restored = doc.toColumnMetadata();

        assertEquals("archiver", restored.getProvenance().getSource());
        assertEquals("bridge", restored.getProvenance().getProcess());
        assertEquals(2, restored.getTagsCount());
        assertTrue(restored.getTagsList().contains("fast"));
        assertTrue(restored.getTagsList().contains("critical"));
        assertEquals(1, restored.getAttributesCount());
        assertEquals("units", restored.getAttributes(0).getName());
        assertEquals("mm", restored.getAttributes(0).getValue());
    }

    @Test
    public void testRoundTrip_emptyDocument() {
        ColumnMetadataDocument doc = new ColumnMetadataDocument();
        ColumnMetadata restored = doc.toColumnMetadata();

        assertFalse(restored.hasProvenance());
        assertEquals(0, restored.getTagsCount());
        assertEquals(0, restored.getAttributesCount());
    }

    // -----------------------------------------------------------------------
    // ColumnProvenanceDocument tests
    // -----------------------------------------------------------------------

    @Test
    public void testColumnProvenanceDocument_roundTrip() {
        ColumnProvenance proto = ColumnProvenance.newBuilder()
                .setSource("data-store")
                .setProcess("normalizer")
                .build();

        ColumnProvenanceDocument doc = ColumnProvenanceDocument.fromColumnProvenance(proto);
        assertEquals("data-store", doc.getSource());
        assertEquals("normalizer", doc.getProcess());

        ColumnProvenance restored = doc.toColumnProvenance();
        assertEquals("data-store", restored.getSource());
        assertEquals("normalizer", restored.getProcess());
    }

    @Test
    public void testColumnProvenanceDocument_nullSafety() {
        ColumnProvenanceDocument doc = new ColumnProvenanceDocument();
        // source and process are null — toColumnProvenance() should not throw
        ColumnProvenance restored = doc.toColumnProvenance();
        assertEquals("", restored.getSource());
        assertEquals("", restored.getProcess());
    }

    // -----------------------------------------------------------------------
    // derivedFrom provenance link tests
    // -----------------------------------------------------------------------

    @Test
    public void testDerivedFrom_pvNameArmWithTimeRange_roundTrip() {
        ColumnProvenance original = ColumnProvenance.newBuilder()
                .setSource("calc-engine")
                .addDerivedFrom(ColumnProvenance.ColumnSource.newBuilder()
                        .setPvName("pv:input:1")
                        .setTimeRange(TimeRange.newBuilder()
                                .setBeginTime(Timestamp.newBuilder().setEpochSeconds(100).setNanoseconds(1))
                                .setEndTime(Timestamp.newBuilder().setEpochSeconds(200).setNanoseconds(2))))
                .build();

        ColumnProvenanceDocument doc = ColumnProvenanceDocument.fromColumnProvenance(original);

        assertNotNull(doc.getDerivedFrom());
        assertEquals(1, doc.getDerivedFrom().size());
        ColumnSourceDocument sourceDoc = doc.getDerivedFrom().get(0);
        assertEquals("pv:input:1", sourceDoc.getPvName());
        assertNull(sourceDoc.getCalculationsColumn());
        assertEquals(100, sourceDoc.getTimeRangeBegin().getSeconds());
        assertEquals(1, sourceDoc.getTimeRangeBegin().getNanos());
        assertEquals(200, sourceDoc.getTimeRangeEnd().getSeconds());

        assertEquals("derivedFrom must round-trip exactly", original, doc.toColumnProvenance());
    }

    @Test
    public void testDerivedFrom_calculationsColumnArm_roundTrip() {
        ColumnProvenance original = ColumnProvenance.newBuilder()
                .addDerivedFrom(ColumnProvenance.ColumnSource.newBuilder()
                        .setCalculationsColumn(ColumnProvenance.CalculationsColumn.newBuilder()
                                .setCalculationsId("66a1b2c3d4e5f60718293a4b")
                                .setFrameName("frame-1")
                                .setColumnName("mean")))
                .build();

        ColumnProvenanceDocument doc = ColumnProvenanceDocument.fromColumnProvenance(original);

        assertNotNull(doc.getDerivedFrom());
        ColumnSourceDocument sourceDoc = doc.getDerivedFrom().get(0);
        assertNull(sourceDoc.getPvName());
        assertEquals("66a1b2c3d4e5f60718293a4b", sourceDoc.getCalculationsColumn().getCalculationsId());
        assertEquals("frame-1", sourceDoc.getCalculationsColumn().getFrameName());
        assertEquals("mean", sourceDoc.getCalculationsColumn().getColumnName());
        assertNull("no timeRange supplied", sourceDoc.getTimeRangeBegin());
        assertNull("no timeRange supplied", sourceDoc.getTimeRangeEnd());

        assertEquals("derivedFrom must round-trip exactly", original, doc.toColumnProvenance());
    }

    @Test
    public void testDerivedFrom_multipleSources_roundTrip() {
        ColumnProvenance original = ColumnProvenance.newBuilder()
                .setSource("diff-calc")
                .setProcess("subtract")
                .addDerivedFrom(ColumnProvenance.ColumnSource.newBuilder().setPvName("pv:a"))
                .addDerivedFrom(ColumnProvenance.ColumnSource.newBuilder().setPvName("pv:b"))
                .build();

        ColumnProvenanceDocument doc = ColumnProvenanceDocument.fromColumnProvenance(original);
        assertEquals(2, doc.getDerivedFrom().size());
        assertEquals("derivedFrom list order and content must round-trip", original, doc.toColumnProvenance());
    }

    @Test
    public void testDerivedFrom_absent_storedAsNull() {
        ColumnProvenance original = ColumnProvenance.newBuilder().setSource("src").build();
        ColumnProvenanceDocument doc = ColumnProvenanceDocument.fromColumnProvenance(original);
        assertNull("empty derivedFrom list must be stored as null, not an empty list", doc.getDerivedFrom());
        assertEquals(0, doc.toColumnProvenance().getDerivedFromCount());
    }

    @Test
    public void testDerivedFrom_originNotSet_tolerated() {
        // an unset origin oneof is stored as supplied and round-trips as an empty ColumnSource
        ColumnProvenance original = ColumnProvenance.newBuilder()
                .addDerivedFrom(ColumnProvenance.ColumnSource.newBuilder()
                        .setTimeRange(TimeRange.newBuilder()
                                .setBeginTime(Timestamp.newBuilder().setEpochSeconds(5))))
                .build();

        ColumnProvenanceDocument doc = ColumnProvenanceDocument.fromColumnProvenance(original);
        ColumnSourceDocument sourceDoc = doc.getDerivedFrom().get(0);
        assertNull(sourceDoc.getPvName());
        assertNull(sourceDoc.getCalculationsColumn());
        assertEquals(5, sourceDoc.getTimeRangeBegin().getSeconds());
        assertEquals(original, doc.toColumnProvenance());
    }
}
