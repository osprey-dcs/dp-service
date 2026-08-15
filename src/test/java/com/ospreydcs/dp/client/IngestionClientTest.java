package com.ospreydcs.dp.client;

import com.ospreydcs.dp.client.IngestionClient.IngestionDataType;
import com.ospreydcs.dp.client.IngestionClient.IngestionRequestParams;
import com.ospreydcs.dp.grpc.v1.common.Attribute;
import com.ospreydcs.dp.grpc.v1.common.ColumnMetadata;
import com.ospreydcs.dp.grpc.v1.common.ColumnProvenance;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Provides test coverage for IngestionClient.IngestionRequestParams and the column metadata and
 * valuesStatus handling in IngestionClient.buildIngestionRequest().
 */
public class IngestionClientTest {

    private static final String PROVIDER_ID = "1";
    private static final String REQUEST_ID = "request-1";
    private static final List<String> COLUMN_NAMES = Arrays.asList("pv_01", "pv_02");
    private static final List<List<Object>> VALUES =
            Arrays.asList(Arrays.asList(12.34, 42.00), Arrays.asList(56.78, 90.12));

    /*
     * Builds params using the 13 argument constructor, which does not accept valuesStatus.
     */
    private static IngestionRequestParams buildParams() {
        return buildParams(null);
    }

    /*
     * Builds params using the 14 argument constructor, which accepts valuesStatus explicitly.
     */
    private static IngestionRequestParams buildParams(
            List<List<DataValue.ValueStatus>> valuesStatus
    ) {
        final Instant instantNow = Instant.now();
        return new IngestionRequestParams(
                PROVIDER_ID,
                REQUEST_ID,
                null,
                null,
                null,
                null,
                instantNow.getEpochSecond(),
                0L,
                1_000_000L,
                2,
                COLUMN_NAMES,
                IngestionDataType.DOUBLE,
                VALUES,
                valuesStatus);
    }

    private static List<DataColumn> buildRequestColumns(IngestionRequestParams params) {
        final IngestDataRequest request = IngestionClient.buildIngestionRequest(params, null, null);
        return request.getIngestionDataFrame().getDataColumnsList();
    }

    /**
     * Verifies that valuesStatus supplied to the 14 argument constructor reaches the built request.
     * Regression coverage for the self assignment in the 13 argument constructor, which left the
     * field permanently null and made the valuesStatus handling in buildIngestionRequest()
     * unreachable.
     */
    @Test
    public void testValuesStatusReachesBuiltRequest() {

        final DataValue.ValueStatus statusOne = DataValue.ValueStatus.newBuilder()
                .setMessage("status-one")
                .build();
        final DataValue.ValueStatus statusTwo = DataValue.ValueStatus.newBuilder()
                .setMessage("status-two")
                .build();

        final List<List<DataValue.ValueStatus>> valuesStatus = Arrays.asList(
                Arrays.asList(statusOne, statusTwo),
                Arrays.asList(statusTwo, statusOne));

        final IngestionRequestParams params = buildParams(valuesStatus);
        assertEquals(valuesStatus, params.valuesStatus);

        final List<DataColumn> columns = buildRequestColumns(params);
        assertEquals(COLUMN_NAMES.size(), columns.size());

        // each value carries the status supplied for its column and row position
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            final List<DataValue> dataValues = columns.get(columnIndex).getDataValuesList();
            assertEquals(VALUES.get(columnIndex).size(), dataValues.size());
            for (int valueIndex = 0; valueIndex < dataValues.size(); valueIndex++) {
                assertEquals(
                        valuesStatus.get(columnIndex).get(valueIndex),
                        dataValues.get(valueIndex).getValueStatus());
            }
        }
    }

    /**
     * Verifies that the 13 argument constructor still builds a request, leaving valuesStatus unset.
     */
    @Test
    public void testThirteenArgumentConstructorLeavesValuesStatusUnset() {

        final IngestionRequestParams params = buildParams();
        assertNull(params.valuesStatus);

        final List<DataColumn> columns = buildRequestColumns(params);
        assertEquals(COLUMN_NAMES.size(), columns.size());

        for (DataColumn column : columns) {
            for (DataValue dataValue : column.getDataValuesList()) {
                assertFalse(dataValue.hasValueStatus());
            }
        }
    }

    /**
     * Verifies that column metadata is applied to columns built from the params columnNames and
     * values, and that it coexists with valuesStatus.
     */
    @Test
    public void testColumnMetadataAppliedToGeneratedColumns() {

        final DataValue.ValueStatus status = DataValue.ValueStatus.newBuilder()
                .setMessage("status")
                .build();
        final List<List<DataValue.ValueStatus>> valuesStatus = Arrays.asList(
                Arrays.asList(status, status),
                Arrays.asList(status, status));

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attribute-name", "attribute-value");

        final IngestionRequestParams params = buildParams(valuesStatus)
                .setColumnMetadata(
                        "test-source",
                        "test-process",
                        List.of("tag-one", "tag-two"),
                        attributes);

        final List<DataColumn> columns = buildRequestColumns(params);
        assertEquals(COLUMN_NAMES.size(), columns.size());

        // metadata is applied uniformly to every generated column
        for (DataColumn column : columns) {
            assertTrue(column.hasMetadata());
            final ColumnMetadata metadata = column.getMetadata();
            assertEquals("test-source", metadata.getProvenance().getSource());
            assertEquals("test-process", metadata.getProvenance().getProcess());
            assertEquals(List.of("tag-one", "tag-two"), metadata.getTagsList());
            assertEquals(1, metadata.getAttributesCount());
            assertEquals("attribute-name", metadata.getAttributes(0).getName());
            assertEquals("attribute-value", metadata.getAttributes(0).getValue());

            // metadata application preserves the per value status set on the generated values
            for (DataValue dataValue : column.getDataValuesList()) {
                assertEquals(status, dataValue.getValueStatus());
            }
        }
    }

    /**
     * Verifies that column metadata is applied to a caller supplied column list, the path used by
     * the desktop app's Excel import.
     */
    @Test
    public void testColumnMetadataAppliedToCallerSuppliedColumns() {

        final DataColumn callerColumn = DataColumn.newBuilder()
                .setName("pv_caller")
                .addDataValues(DataValue.newBuilder().setDoubleValue(1.0).build())
                .build();

        final IngestionRequestParams params = buildParams()
                .setColumnMetadata("caller-source", null, null, null);

        final IngestDataRequest request =
                IngestionClient.buildIngestionRequest(params, null, List.of(callerColumn));
        final List<DataColumn> columns = request.getIngestionDataFrame().getDataColumnsList();

        // the caller supplied column replaces the columns that would be generated from params
        assertEquals(1, columns.size());
        assertEquals("pv_caller", columns.get(0).getName());
        assertTrue(columns.get(0).hasMetadata());
        assertEquals("caller-source", columns.get(0).getMetadata().getProvenance().getSource());

        // source and process are plain proto3 strings with no field presence, so an omitted field
        // is indistinguishable from an empty one on the wire and reads back as the empty string
        assertEquals("", columns.get(0).getMetadata().getProvenance().getProcess());
    }

    /**
     * Verifies that a column already carrying its own metadata keeps it, since per column metadata
     * is more specific than the request wide default in params.
     */
    @Test
    public void testExistingColumnMetadataIsPreserved() {

        final ColumnMetadata columnOwnMetadata = ColumnMetadata.newBuilder()
                .setProvenance(ColumnProvenance.newBuilder().setSource("column-source").build())
                .build();

        final DataColumn withMetadata = DataColumn.newBuilder()
                .setName("pv_with_metadata")
                .addDataValues(DataValue.newBuilder().setDoubleValue(1.0).build())
                .setMetadata(columnOwnMetadata)
                .build();

        final DataColumn withoutMetadata = DataColumn.newBuilder()
                .setName("pv_without_metadata")
                .addDataValues(DataValue.newBuilder().setDoubleValue(2.0).build())
                .build();

        final IngestionRequestParams params = buildParams()
                .setColumnMetadata("params-source", null, null, null);

        final IngestDataRequest request = IngestionClient.buildIngestionRequest(
                params, null, List.of(withMetadata, withoutMetadata));
        final List<DataColumn> columns = request.getIngestionDataFrame().getDataColumnsList();

        assertEquals(2, columns.size());

        // the column with its own metadata keeps it
        assertEquals("column-source", columns.get(0).getMetadata().getProvenance().getSource());

        // the column without metadata receives the params metadata
        assertEquals("params-source", columns.get(1).getMetadata().getProvenance().getSource());
    }

    /**
     * Verifies handling of partially supplied provenance.  ColumnProvenance.source and .process are
     * plain proto3 strings with no field presence, so an unsupplied one cannot be distinguished
     * from an empty one and reads back as the empty string.  What the blank checking controls is
     * whether the enclosing provenance message, which does have presence, is set at all.
     */
    @Test
    public void testPartiallySuppliedProvenance() {

        final IngestionRequestParams processOnly = buildParams()
                .setColumnMetadata(null, "only-process", null, null);

        final ColumnProvenance provenance = processOnly.columnMetadata.getProvenance();
        assertTrue(processOnly.columnMetadata.hasProvenance());
        assertEquals("only-process", provenance.getProcess());
        assertEquals("", provenance.getSource());

        // blank strings are treated the same as null
        final IngestionRequestParams blankSource = buildParams()
                .setColumnMetadata("   ", "only-process", null, null);
        assertEquals("", blankSource.columnMetadata.getProvenance().getSource());

        // provenance is left unset entirely when neither source nor process is supplied
        final IngestionRequestParams tagsOnly = buildParams()
                .setColumnMetadata(null, null, List.of("tag"), null);
        assertFalse(tagsOnly.columnMetadata.hasProvenance());
        assertEquals(List.of("tag"), tagsOnly.columnMetadata.getTagsList());
    }

    /**
     * Verifies that the convenience overload leaves columnMetadata null when no argument supplies a
     * value, so that the built columns carry no metadata field rather than an empty one.
     */
    @Test
    public void testEmptyColumnMetadataLeavesFieldNull() {

        final IngestionRequestParams allNull = buildParams()
                .setColumnMetadata(null, null, null, null);
        assertNull(allNull.columnMetadata);

        final IngestionRequestParams allBlankOrEmpty = buildParams()
                .setColumnMetadata("  ", "", List.of(), Map.of());
        assertNull(allBlankOrEmpty.columnMetadata);

        // the built columns carry no metadata field at all
        for (DataColumn column : buildRequestColumns(allBlankOrEmpty)) {
            assertFalse(column.hasMetadata());
        }
    }

    /**
     * Verifies that attribute entries with a null name or value are skipped rather than raising a
     * NullPointerException from the protobuf setters.
     */
    @Test
    public void testAttributeEntriesWithNullsAreSkipped() {

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("good-name", "good-value");
        attributes.put("null-value-name", null);
        attributes.put(null, "null-name-value");

        final IngestionRequestParams params = buildParams()
                .setColumnMetadata(null, null, null, attributes);

        final List<Attribute> attributeList = params.columnMetadata.getAttributesList();
        assertEquals(1, attributeList.size());
        assertEquals("good-name", attributeList.get(0).getName());
        assertEquals("good-value", attributeList.get(0).getValue());
    }

    /**
     * Verifies that the raw setter overload applies the supplied message as is, with no
     * normalization, and that passing null clears any metadata previously set.
     */
    @Test
    public void testRawSetterAppliesMessageAsIs() {

        final ColumnMetadata empty = ColumnMetadata.getDefaultInstance();

        final IngestionRequestParams params = buildParams().setColumnMetadata(empty);
        assertEquals(empty, params.columnMetadata);

        // an empty message supplied directly is applied, unlike the convenience overload
        for (DataColumn column : buildRequestColumns(params)) {
            assertTrue(column.hasMetadata());
        }

        // passing null clears it
        params.setColumnMetadata((ColumnMetadata) null);
        assertNull(params.columnMetadata);
        for (DataColumn column : buildRequestColumns(params)) {
            assertFalse(column.hasMetadata());
        }
    }

    /**
     * Verifies that columns are built without a metadata field when no column metadata is supplied.
     */
    @Test
    public void testNoColumnMetadataByDefault() {

        final IngestionRequestParams params = buildParams();
        assertNull(params.columnMetadata);

        for (DataColumn column : buildRequestColumns(params)) {
            assertFalse(column.hasMetadata());
        }
    }
}
