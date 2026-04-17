package com.ospreydcs.dp.service.common.bson;

import com.ospreydcs.dp.grpc.v1.common.Attribute;
import com.ospreydcs.dp.grpc.v1.common.ColumnMetadata;
import com.ospreydcs.dp.grpc.v1.common.ColumnProvenance;
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
}
