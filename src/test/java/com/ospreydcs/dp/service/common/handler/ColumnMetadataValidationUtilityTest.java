package com.ospreydcs.dp.service.common.handler;

import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for the shared column-metadata validator, including the field-path prefix
 * parameterization and the derivedFrom length limits (length checks only — links are stored as
 * supplied and may dangle, so there are no existence or ObjectId-format checks).
 */
public class ColumnMetadataValidationUtilityTest {

    private static final String LONG_STRING =
            "x".repeat(ColumnMetadataValidationUtility.MAX_STRING_LENGTH + 1);

    private static ColumnMetadata metadataWithDerivedFrom(ColumnProvenance.ColumnSource source) {
        return ColumnMetadata.newBuilder()
                .setProvenance(ColumnProvenance.newBuilder()
                        .setSource("src")
                        .addDerivedFrom(source))
                .build();
    }

    @Test
    public void testValidMetadataWithDerivedFrom_passes() {
        ColumnMetadata metadata = ColumnMetadata.newBuilder()
                .setProvenance(ColumnProvenance.newBuilder()
                        .setSource("archiver")
                        .setProcess("bridge")
                        .addDerivedFrom(ColumnProvenance.ColumnSource.newBuilder()
                                .setPvName("pv:input")
                                .setTimeRange(TimeRange.newBuilder()
                                        .setBeginTime(Timestamp.newBuilder().setEpochSeconds(1))
                                        .setEndTime(Timestamp.newBuilder().setEpochSeconds(2))))
                        .addDerivedFrom(ColumnProvenance.ColumnSource.newBuilder()
                                .setCalculationsColumn(ColumnProvenance.CalculationsColumn.newBuilder()
                                        .setCalculationsId("66a1b2c3d4e5f60718293a4b")
                                        .setFrameName("f").setColumnName("c"))))
                .addTags("tag")
                .addAttributes(Attribute.newBuilder().setName("k").setValue("v"))
                .build();

        ResultStatus result = ColumnMetadataValidationUtility.validateColumnMetadata(metadata, "path");
        assertFalse(result.msg, result.isError);
    }

    @Test
    public void testDerivedFrom_calculationsIdNotObjectIdShaped_passes() {
        // stored as supplied: a link may point at records not yet created, so no ObjectId parse
        ColumnMetadata metadata = metadataWithDerivedFrom(ColumnProvenance.ColumnSource.newBuilder()
                .setCalculationsColumn(ColumnProvenance.CalculationsColumn.newBuilder()
                        .setCalculationsId("not-an-object-id"))
                .build());
        ResultStatus result = ColumnMetadataValidationUtility.validateColumnMetadata(metadata, "path");
        assertFalse(result.msg, result.isError);
    }

    @Test
    public void testDerivedFrom_originNotSet_passes() {
        ColumnMetadata metadata = metadataWithDerivedFrom(
                ColumnProvenance.ColumnSource.newBuilder().build());
        ResultStatus result = ColumnMetadataValidationUtility.validateColumnMetadata(metadata, "path");
        assertFalse(result.msg, result.isError);
    }

    @Test
    public void testDerivedFrom_pvNameTooLong_rejected() {
        ColumnMetadata metadata = metadataWithDerivedFrom(ColumnProvenance.ColumnSource.newBuilder()
                .setPvName(LONG_STRING)
                .build());
        ResultStatus result = ColumnMetadataValidationUtility.validateColumnMetadata(metadata, "myPrefix");
        assertTrue(result.isError);
        assertTrue(result.msg, result.msg.contains(
                "myPrefix.metadata.provenance.derivedFrom[0].pvName length exceeds maximum"));
    }

    @Test
    public void testDerivedFrom_calculationsColumnFieldsTooLong_rejected() {
        String[] expectedFields = {"calculationsId", "frameName", "columnName"};
        ColumnProvenance.CalculationsColumn[] columns = {
                ColumnProvenance.CalculationsColumn.newBuilder().setCalculationsId(LONG_STRING).build(),
                ColumnProvenance.CalculationsColumn.newBuilder().setFrameName(LONG_STRING).build(),
                ColumnProvenance.CalculationsColumn.newBuilder().setColumnName(LONG_STRING).build(),
        };
        for (int i = 0; i < columns.length; i++) {
            ColumnMetadata metadata = metadataWithDerivedFrom(ColumnProvenance.ColumnSource.newBuilder()
                    .setCalculationsColumn(columns[i])
                    .build());
            ResultStatus result = ColumnMetadataValidationUtility.validateColumnMetadata(metadata, "p");
            assertTrue(expectedFields[i], result.isError);
            assertTrue(result.msg, result.msg.contains(
                    "p.metadata.provenance.derivedFrom[0].calculationsColumn." + expectedFields[i]
                            + " length exceeds maximum"));
        }
    }

    @Test
    public void testProvenanceSourceTooLong_rejectedWithPrefix() {
        ColumnMetadata metadata = ColumnMetadata.newBuilder()
                .setProvenance(ColumnProvenance.newBuilder().setSource(LONG_STRING))
                .build();
        ResultStatus result = ColumnMetadataValidationUtility.validateColumnMetadata(
                metadata, "calculations.dataFrames[2]");
        assertTrue(result.isError);
        assertTrue(result.msg, result.msg.contains(
                "calculations.dataFrames[2].metadata.provenance.source length exceeds maximum"));
    }

    @Test
    public void testValidateAllColumnMetadata_prefixNamesColumnListAndIndex() {
        ColumnMetadata bad = ColumnMetadata.newBuilder()
                .setProvenance(ColumnProvenance.newBuilder().setProcess(LONG_STRING))
                .build();
        DataFrame frame = DataFrame.newBuilder()
                .addDoubleColumns(DoubleColumn.newBuilder().setName("ok").addValues(1.0))
                .addDoubleColumns(DoubleColumn.newBuilder().setName("bad").addValues(2.0).setMetadata(bad))
                .build();

        ResultStatus result = ColumnMetadataValidationUtility.validateAllColumnMetadata(frame, "somePrefix");
        assertTrue(result.isError);
        assertTrue(result.msg, result.msg.contains(
                "somePrefix.doubleColumns[1].metadata.provenance.process length exceeds maximum"));
    }

    @Test
    public void testValidateAllColumnMetadata_cleanFrame_passes() {
        DataFrame frame = DataFrame.newBuilder()
                .addDoubleColumns(DoubleColumn.newBuilder().setName("pv").addValues(1.0))
                .addStringColumns(StringColumn.newBuilder().setName("pv2").addValues("v"))
                .build();
        ResultStatus result = ColumnMetadataValidationUtility.validateAllColumnMetadata(frame, "x");
        assertFalse(result.msg, result.isError);
    }
}
