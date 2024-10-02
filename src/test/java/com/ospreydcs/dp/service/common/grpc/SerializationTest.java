package com.ospreydcs.dp.service.common.grpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.ospreydcs.dp.grpc.v1.common.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class SerializationTest {

    // constants
    private final static String FILE_PATH = "src/test/resources/serializationTest/";
    private final static String FILE_EXTENSION = ".binpb";
    private final static String TYPE_STRING = "string";
    private final static String TYPE_BOOLEAN = "boolean";
    private final static String TYPE_UINT = "uint";
    private final static String TYPE_ULONG = "ulong";
    private final static String TYPE_INT = "int";
    private final static String TYPE_LONG = "long";
    private final static String TYPE_FLOAT = "float";
    private final static String TYPE_DOUBLE = "double";
    private final static String TYPE_BYTE_ARRAY = "byteArray";
    private final static String TYPE_ARRAY = "array";
    private final static String TYPE_STRUCTURE = "structure";
    private final static String TYPE_IMAGE = "image";
    private final static String TYPE_TIMESTAMP = "timestamp";
    private final static long SECONDS = 1698767462L;
    private final static long NANOS = 100_000_000L;

    // static variables
    private static ByteString imageByteString = null;

    @Test
    public void dataValueCaseTest() {
        DataValue dv = DataValue.newBuilder().setDoubleValue(12.34).build();
        DataValue.ValueCase vc = dv.getValueCase();
        assertEquals(8, vc.getNumber());
        assertEquals("DOUBLEVALUE", vc.name());

        vc = DataValue.ValueCase.ARRAYVALUE;
        assertEquals(10, vc.getNumber());
        assertEquals("ARRAYVALUE", vc.name());
    }

    @Test
    public void serializationTest() {
        verifySerialization(stringDataColumn());
        verifySerialization(booleanDataColumn());
        verifySerialization(uintDataColumn());
        verifySerialization(ulongDataColumn());
        verifySerialization(intDataColumn());
        verifySerialization(longDataColumn());
        verifySerialization(floatDataColumn());
        verifySerialization(doubleDataColumn());
        verifySerialization(byteArrayDataColumn());
        verifySerialization(arrayDataColumn());
        verifySerialization(structureDataColumn());
        verifySerialization(imageDataColumn());
        verifySerialization(timestampDataColumn());
    }

    private static File fileNameForType(String typeName) {
        return new File(FILE_PATH + typeName + FILE_EXTENSION);
    }

    private static DataColumn stringDataColumn() {
        final String pvName = TYPE_STRING;
        final List<DataValue> dataValueList = new ArrayList<>();
        for (int valueIndex = 0; valueIndex < 5; ++valueIndex) {
            final DataValue dataValue = DataValue.newBuilder().setStringValue("junk").build();
            dataValueList.add(dataValue);
        }
        return dataColumnWithValueList(pvName, dataValueList);
    }

    private static DataColumn booleanDataColumn() {
        final String pvName = TYPE_BOOLEAN;
        final List<DataValue> dataValueList = new ArrayList<>();
        for (int valueIndex = 0; valueIndex < 5; ++valueIndex) {
            final DataValue dataValue = DataValue.newBuilder().setBooleanValue(true).build();
            dataValueList.add(dataValue);
        }
        return dataColumnWithValueList(pvName, dataValueList);
    }

    private static DataColumn uintDataColumn() {
        final String pvName = TYPE_UINT;
        final List<DataValue> dataValueList = new ArrayList<>();
        for (int valueIndex = 0; valueIndex < 5; ++valueIndex) {
            final DataValue dataValue = DataValue.newBuilder().setUintValue(42).build();
            dataValueList.add(dataValue);
        }
        return dataColumnWithValueList(pvName, dataValueList);
    }

    private static DataColumn ulongDataColumn() {
        final String pvName = TYPE_ULONG;
        final List<DataValue> dataValueList = new ArrayList<>();
        for (int valueIndex = 0; valueIndex < 5; ++valueIndex) {
            final DataValue dataValue = DataValue.newBuilder().setUlongValue(42L).build();
            dataValueList.add(dataValue);
        }
        return dataColumnWithValueList(pvName, dataValueList);
    }

    private static DataColumn intDataColumn() {
        final String pvName = TYPE_INT;
        final List<DataValue> dataValueList = new ArrayList<>();
        for (int valueIndex = 0; valueIndex < 5; ++valueIndex) {
            final DataValue dataValue = DataValue.newBuilder().setIntValue(-42).build();
            dataValueList.add(dataValue);
        }
        return dataColumnWithValueList(pvName, dataValueList);
    }

    private static DataColumn longDataColumn() {
        final String pvName = TYPE_LONG;
        final List<DataValue> dataValueList = new ArrayList<>();
        for (int valueIndex = 0; valueIndex < 5; ++valueIndex) {
            final DataValue dataValue = DataValue.newBuilder().setLongValue(-42L).build();
            dataValueList.add(dataValue);
        }
        return dataColumnWithValueList(pvName, dataValueList);
    }

    private static DataColumn floatDataColumn() {
        final String pvName = TYPE_FLOAT;
        final List<DataValue> dataValueList = new ArrayList<>();
        for (int valueIndex = 0; valueIndex < 5; ++valueIndex) {
            final DataValue dataValue = DataValue.newBuilder().setFloatValue(3.14F).build();
            dataValueList.add(dataValue);
        }
        return dataColumnWithValueList(pvName, dataValueList);
    }

    private static DataColumn doubleDataColumn() {
        final String pvName = TYPE_DOUBLE;
        final List<DataValue> dataValueList = new ArrayList<>();
        for (int valueIndex = 0; valueIndex < 5; ++valueIndex) {
            final DataValue dataValue = DataValue.newBuilder().setDoubleValue(3.14159).build();
            dataValueList.add(dataValue);
        }
        return dataColumnWithValueList(pvName, dataValueList);
    }

    private static DataColumn byteArrayDataColumn() {
        final String pvName = TYPE_BYTE_ARRAY;
        final List<DataValue> dataValueList = new ArrayList<>();
        for (int valueIndex = 0; valueIndex < 5; ++valueIndex) {
            final byte[] byteArray = {0xa, 0x2, 0xf};
            final DataValue dataValue =
                    DataValue.newBuilder().setByteArrayValue(ByteString.copyFrom(byteArray)).build();
            dataValueList.add(dataValue);
        }
        return dataColumnWithValueList(pvName, dataValueList);
    }

    private static DataValue arrayDataValue() {
        final DataValue.Builder outerArrayValueBuilder = DataValue.newBuilder();
        final Array.Builder outerArrayBuilder = Array.newBuilder();

        for (int arrayValueIndex = 0 ; arrayValueIndex < 5 ; ++arrayValueIndex) {
            final DataValue.Builder innerArrayValueBuilder = DataValue.newBuilder();
            final Array.Builder innerArrayBuilder = Array.newBuilder();

            final List<String> array1D = new ArrayList<>();

            for (int arrayElementIndex = 0 ; arrayElementIndex < 5 ; ++arrayElementIndex) {
                final String arrayElementValueString = arrayValueIndex + ":" + arrayElementIndex;
                final DataValue arrayElementValue =
                        DataValue.newBuilder().setStringValue(arrayElementValueString).build();
                innerArrayBuilder.addDataValues(arrayElementValue);
                array1D.add(arrayElementValueString);
            }

            innerArrayBuilder.build();
            innerArrayValueBuilder.setArrayValue(innerArrayBuilder);
            innerArrayValueBuilder.build();
            outerArrayBuilder.addDataValues(innerArrayValueBuilder);
        }

        outerArrayBuilder.build();
        outerArrayValueBuilder.setArrayValue(outerArrayBuilder);
        return outerArrayValueBuilder.build();
    }

    private static DataColumn arrayDataColumn() {
        final String pvName = TYPE_ARRAY;

        // build 2d array values
        final List<DataValue> dataValueList = new ArrayList<>();
        for (int valueIndex = 0 ; valueIndex < 5 ; ++valueIndex) {
            dataValueList.add(arrayDataValue());
        }
        return dataColumnWithValueList(pvName, dataValueList);
    }

    private static DataColumn structureDataColumn() {

        final String pvName = TYPE_STRUCTURE;

        final String ARRAY_MEMBER = "array2D";
        final String STRING_MEMBER = "string";
        final String STRING_VALUE = "junk";
        final String BOOLEAN_MEMBER = "boolean";
        final boolean BOOLEAN_VALUE = false;
        final String DOUBLE_MEMBER = "double";
        final double DOUBLE_VALUE = 3.14;
        final String IMAGE_MEMBER = "image";
        final String INTEGER_MEMBER = "integer";
        final int INTEGER_VALUE = 42;
        final String TIMESTAMP_MEMBER = "timestamp";
        final String STRUCTURE_MEMBER = "structure";

        final List<DataValue> dataValueList = new ArrayList<>();
        for (int valueIndex = 0; valueIndex < 5; ++valueIndex) {

            // create structure for row
            final DataValue.Builder structureValueBuilder = DataValue.newBuilder();
            final Structure.Builder structureBuilder = Structure.newBuilder();

            // add some scalars string, bool, double

            final DataValue stringDataValue = DataValue.newBuilder().setStringValue(STRING_VALUE).build();
            final Structure.Field stringField =
                    Structure.Field.newBuilder().setName(STRING_MEMBER).setValue(stringDataValue).build();
            structureBuilder.addFields(stringField);

            final DataValue booleanDataValue = DataValue.newBuilder().setBooleanValue(BOOLEAN_VALUE).build();
            final Structure.Field booleanField =
                    Structure.Field.newBuilder().setName(BOOLEAN_MEMBER).setValue(booleanDataValue).build();
            structureBuilder.addFields(booleanField);

            final DataValue doubleDataValue = DataValue.newBuilder().setDoubleValue(DOUBLE_VALUE).build();
            final Structure.Field doubleField =
                    Structure.Field.newBuilder().setName(DOUBLE_MEMBER).setValue(doubleDataValue).build();
            structureBuilder.addFields(doubleField);

            // add image
            final Image image =
                    Image.newBuilder().setFileType(Image.FileType.BMP).setImage(getImageByteString()).build();
            final DataValue imageDataValue = DataValue.newBuilder().setImageValue(image).build();
            final Structure.Field imageField =
                    Structure.Field.newBuilder().setName(IMAGE_MEMBER).setValue(imageDataValue).build();
            structureBuilder.addFields(imageField);

            // add nested structure with integer and timestamp

            final DataValue.Builder nestedStructureValueBuilder = DataValue.newBuilder();
            final Structure.Builder nestedStructureBuilder = Structure.newBuilder();

            final DataValue integerDataValue = DataValue.newBuilder().setIntValue(INTEGER_VALUE).build();
            final Structure.Field integerField =
                    Structure.Field.newBuilder().setName(INTEGER_MEMBER).setValue(integerDataValue).build();
            nestedStructureBuilder.addFields(integerField);

            final Timestamp timestamp = Timestamp.newBuilder()
                    .setEpochSeconds(SECONDS)
                    .setNanoseconds(NANOS)
                    .build();
            final DataValue timestampDataValue = DataValue.newBuilder().setTimestampValue(timestamp).build();
            final Structure.Field timestampField =
                    Structure.Field.newBuilder().setName(TIMESTAMP_MEMBER).setValue(timestampDataValue).build();
            nestedStructureBuilder.addFields(timestampField);

            nestedStructureBuilder.build();
            nestedStructureValueBuilder.setStructureValue(nestedStructureBuilder);
            nestedStructureValueBuilder.build();

            final Structure.Field nestedStructureField = Structure.Field.newBuilder()
                    .setName(STRUCTURE_MEMBER)
                    .setValue(nestedStructureValueBuilder)
                    .build();
            structureBuilder.addFields(nestedStructureField);

            // get array from array ingestion DataColumn list and add to structure
            DataValue arrayDataValue = arrayDataValue();
            assertTrue(arrayDataValue.hasArrayValue());
            Structure.Field arrayField =
                    Structure.Field.newBuilder().setName(ARRAY_MEMBER).setValue(arrayDataValue).build();
            structureBuilder.addFields(arrayField);

            structureBuilder.build();
            structureValueBuilder.setStructureValue(structureBuilder);
            DataValue dataValue = structureValueBuilder.build();
            dataValueList.add(dataValue);

        }
        return dataColumnWithValueList(pvName, dataValueList);
    }

    private static DataColumn imageDataColumn() {
        final String pvName = TYPE_IMAGE;
        final List<DataValue> dataValueList = new ArrayList<>();

        // read test image
        for (int valueIndex = 0 ; valueIndex < 5 ; ++valueIndex) {
            final DataValue.Builder imageDataValueBuilder = DataValue.newBuilder();
            final Image image =
                    Image.newBuilder().setFileType(Image.FileType.BMP).setImage(getImageByteString()).build();
            imageDataValueBuilder.setImageValue(image);
            DataValue dataValue = imageDataValueBuilder.build();
            dataValueList.add(dataValue);
        }
        return dataColumnWithValueList(pvName, dataValueList);
    }

    private static DataColumn timestampDataColumn() {
        final String pvName = TYPE_TIMESTAMP;
        final List<DataValue> dataValueList = new ArrayList<>();
        for (int valueIndex = 0; valueIndex < 5; ++valueIndex) {
            Timestamp timestamp =
                    Timestamp.newBuilder().setEpochSeconds(SECONDS).setNanoseconds(NANOS).build();
            final DataValue dataValue = DataValue.newBuilder().setTimestampValue(timestamp).build();
            dataValueList.add(dataValue);
        }
        return dataColumnWithValueList(pvName, dataValueList);
    }

    private static ByteString getImageByteString() {
        if (imageByteString == null) {
            try {
                InputStream inputStream =
                        SerializationTest.class.getClassLoader().getResourceAsStream("test-image.bmp");
                imageByteString = ByteString.readFrom(inputStream);
            } catch (IOException ex) {
                fail("error loading test-image.bmp: " + ex.getMessage());
            }
        }
        return imageByteString;
    }

    private static DataColumn dataColumnWithValueList(String pvName, List<DataValue> dataValueList) {
        // build DataColumn from list of DataValues
        final DataColumn.Builder dataColumnBuilder = DataColumn.newBuilder();
        dataColumnBuilder.setName(pvName);
        dataColumnBuilder.addAllDataValues(dataValueList);
        return dataColumnBuilder.build();
    }

    private void verifySerialization(DataColumn dataColumn) {

        // serialize DataColumn contents and create a new DataColumn by parsing
        final byte[] dataColumnBytes = dataColumn.toByteArray();
        DataColumn dataColumnParsed = null;
        try {
            dataColumnParsed = DataColumn.parseFrom(dataColumnBytes);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
        assertEquals(dataColumn, dataColumnParsed);

        // check to see if file containing serialized DataColumn exists
        final File binPbFile = fileNameForType(dataColumn.getName());

        if ( ! binPbFile.exists()) {
            fail("binpb file not found: " + binPbFile);

        } else {
            // compare serialized DataColumn content to existing file
            System.out.println("reading existing binpbFile: " + binPbFile);
            byte[] binPbFileContent = null;
            try {
                final InputStream inputStream = new FileInputStream(binPbFile);
                binPbFileContent = inputStream.readAllBytes();
                inputStream.close();
            } catch (IOException e) {
                fail("error reading binpb file: " + binPbFile);
            }

            DataColumn dataColumnFromFile = null;
            assertArrayEquals(dataColumn.toByteArray(), binPbFileContent);
            try {
                dataColumnFromFile = DataColumn.parseFrom(binPbFileContent);
            } catch (InvalidProtocolBufferException ex) {
                fail("exception parsing DataColumn from file content: " + ex.getMessage());
            }
            assertEquals(dataColumn, dataColumnFromFile);

            System.out.println("verified type: " + dataColumn.getName());
        }
    }

    private static void initTestDataFile(DataColumn dataColumn) {

        final File binPbFile = fileNameForType(dataColumn.getName());

        if (binPbFile.exists()) {
            fail("binpb file already exists: " + binPbFile);

        } else {
            // create data file with serialized content
            System.out.println("creating binpbFile: " + binPbFile);
            try {
                final FileOutputStream outputStream = new FileOutputStream(binPbFile);
                outputStream.write(dataColumn.toByteArray());
                outputStream.flush();
                outputStream.close();
            } catch (IOException e) {
                fail("error creating binpb file: " + binPbFile);;
            }
        }
    }

    public static void main(final String[] args) {

        // check if output directory exists
        File outputDirectory = new File(FILE_PATH);
        if ( ! outputDirectory.exists()) {
            assertTrue(outputDirectory.mkdir());
        }

        initTestDataFile(stringDataColumn());
        initTestDataFile(booleanDataColumn());
        initTestDataFile(uintDataColumn());
        initTestDataFile(ulongDataColumn());
        initTestDataFile(intDataColumn());
        initTestDataFile(longDataColumn());
        initTestDataFile(floatDataColumn());
        initTestDataFile(doubleDataColumn());
        initTestDataFile(byteArrayDataColumn());
        initTestDataFile(arrayDataColumn());
        initTestDataFile(structureDataColumn());
        initTestDataFile(imageDataColumn());
        initTestDataFile(timestampDataColumn());
    }

}
