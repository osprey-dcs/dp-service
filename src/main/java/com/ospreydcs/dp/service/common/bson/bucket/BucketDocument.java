package com.ospreydcs.dp.service.common.bson.bucket;

import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
import com.ospreydcs.dp.service.common.bson.column.*;
import com.ospreydcs.dp.service.common.bson.DataTimestampsDocument;
import com.ospreydcs.dp.service.common.bson.DpBsonDocumentBase;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * This POJO is for writing time series data to mongodb by customizing the code registry.
 *
 * NOTE: DATABASE CODE LIKE insertMany SILENTLY FAILS IF AN INSTANCE VARIABLE IS ADDED WITHOUT ACCESSOR METHODS!!!
 */
public class BucketDocument extends DpBsonDocumentBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private String id;
    private String pvName;
    private ColumnDocumentBase dataColumn;
    private DataTimestampsDocument dataTimestamps;
    private String providerId;
    private String providerName;
    private String clientRequestId;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPvName() {
        return pvName;
    }

    public void setPvName(String pvName) {
        this.pvName = pvName;
    }

    public ColumnDocumentBase getDataColumn() {
        return dataColumn;
    }

    public void setDataColumn(ColumnDocumentBase dataColumn) {
        this.dataColumn = dataColumn;
    }

    public DataTimestampsDocument getDataTimestamps() {
        return dataTimestamps;
    }

    public void setDataTimestamps(DataTimestampsDocument dataTimestamps) {
        this.dataTimestamps = dataTimestamps;
    }

    public String getProviderId() {
        return providerId;
    }

    public void setProviderId(String providerId) {
        this.providerId = providerId;
    }

    public String getProviderName() {
        return providerName;
    }

    public void setProviderName(String providerName) {
        this.providerName = providerName;
    }

    public String getClientRequestId() {
        return clientRequestId;
    }

    public void setClientRequestId(String clientRequestId) {
        this.clientRequestId = clientRequestId;
    }

    private static BucketDocument columnBucketDocument(
            String pvName,
            IngestDataRequest request,
            ColumnDocumentBase dataColumnDocument,
            String providerName
    ) {
        final BucketDocument bucket = new BucketDocument();

        // create DataTimestampsDocument for the request
        final DataTimestampsDocument requestDataTimestampsDocument =
                DataTimestampsDocument.fromDataTimestamps(request.getIngestionDataFrame().getDataTimestamps());

        // get PV name and generate id for BucketDocument
        final String documentId = pvName + "-"
                + requestDataTimestampsDocument.getFirstTime().getSeconds() + "-"
                + requestDataTimestampsDocument.getFirstTime().getNanos();
        bucket.setId(documentId);
        bucket.setPvName(pvName);
        bucket.setProviderId(request.getProviderId());
        bucket.setProviderName(providerName);
        bucket.setClientRequestId(request.getClientRequestId());

        bucket.setDataColumn(dataColumnDocument);

        // embed requestDataTimesetampsDocument within each BucketDocument
        bucket.setDataTimestamps(requestDataTimestampsDocument);

        return bucket;
    }

    /**
     * Generates a list of POJO objects, which are written as a batch to mongodb by customizing the codec registry.
     * <p>
     * NOTE: DATABASE CODE LIKE insertMany SILENTLY FAILS IF AN INSTANCE VARIABLE IS ADDED TO TsDataBucket
     * WITHOUT ACCESSOR METHODS!!!  Very hard to troubleshoot.
     *
     * @param request
     * @param providerName
     * @return
     */
    public static List<BucketDocument> generateBucketsFromRequest(IngestDataRequest request, String providerName)
            throws DpException {

        final List<BucketDocument> bucketList = new ArrayList<>();

        // create BucketDocument for each DataColumn
        for (DataColumn column : request.getIngestionDataFrame().getDataColumnsList()) {
            ColumnDocumentBase columnDocument = DataColumnDocument.fromDataColumn(column);
            bucketList.add(columnBucketDocument(column.getName(), request, columnDocument, providerName));
        }

        // create BucketDocument for each SerializedDataColumn
        for (SerializedDataColumn column : request.getIngestionDataFrame().getSerializedDataColumnsList()) {
            ColumnDocumentBase columnDocument = SerializedDataColumnDocument.fromSerializedDataColumn(column);
            bucketList.add(columnBucketDocument(column.getName(), request, columnDocument, providerName));
        }

        // create BucketDocument for each DoubleColumn
        for (DoubleColumn column : request.getIngestionDataFrame().getDoubleColumnsList()) {
            ColumnDocumentBase columnDocument = DoubleColumnDocument.fromDoubleColumn(column);
            bucketList.add(columnBucketDocument(column.getName(), request, columnDocument, providerName));
        }

        // create BucketDocument for each FloatColumn
        for (FloatColumn column : request.getIngestionDataFrame().getFloatColumnsList()) {
            ColumnDocumentBase columnDocument = FloatColumnDocument.fromFloatColumn(column);
            bucketList.add(columnBucketDocument(column.getName(), request, columnDocument, providerName));
        }

        // create BucketDocument for each Int64Column
        for (Int64Column column : request.getIngestionDataFrame().getInt64ColumnsList()) {
            ColumnDocumentBase columnDocument = Int64ColumnDocument.fromInt64Column(column);
            bucketList.add(columnBucketDocument(column.getName(), request, columnDocument, providerName));
        }

        // create BucketDocument for each Int32Column
        for (Int32Column column : request.getIngestionDataFrame().getInt32ColumnsList()) {
            ColumnDocumentBase columnDocument = Int32ColumnDocument.fromInt32Column(column);
            bucketList.add(columnBucketDocument(column.getName(), request, columnDocument, providerName));
        }

        // create BucketDocument for each BoolColumn
        for (BoolColumn column : request.getIngestionDataFrame().getBoolColumnsList()) {
            ColumnDocumentBase columnDocument = BoolColumnDocument.fromBoolColumn(column);
            bucketList.add(columnBucketDocument(column.getName(), request, columnDocument, providerName));
        }

        // create BucketDocument for each StringColumn
        for (StringColumn column : request.getIngestionDataFrame().getStringColumnsList()) {
            ColumnDocumentBase columnDocument = StringColumnDocument.fromStringColumn(column);
            bucketList.add(columnBucketDocument(column.getName(), request, columnDocument, providerName));
        }

        // create BucketDocument for each EnumColumn
        for (EnumColumn column : request.getIngestionDataFrame().getEnumColumnsList()) {
            ColumnDocumentBase columnDocument = EnumColumnDocument.fromEnumColumn(column);
            bucketList.add(columnBucketDocument(column.getName(), request, columnDocument, providerName));
        }

        // create BucketDocument for each DoubleArrayColumn
        for (DoubleArrayColumn column : request.getIngestionDataFrame().getDoubleArrayColumnsList()) {
            ColumnDocumentBase columnDocument = DoubleArrayColumnDocument.fromDoubleArrayColumn(column);
            bucketList.add(columnBucketDocument(column.getName(), request, columnDocument, providerName));
        }

        // create BucketDocument for each FloatArrayColumn
        for (FloatArrayColumn column : request.getIngestionDataFrame().getFloatArrayColumnsList()) {
            ColumnDocumentBase columnDocument = FloatArrayColumnDocument.fromFloatArrayColumn(column);
            bucketList.add(columnBucketDocument(column.getName(), request, columnDocument, providerName));
        }

        // create BucketDocument for each Int32ArrayColumn
        for (Int32ArrayColumn column : request.getIngestionDataFrame().getInt32ArrayColumnsList()) {
            ColumnDocumentBase columnDocument = Int32ArrayColumnDocument.fromInt32ArrayColumn(column);
            bucketList.add(columnBucketDocument(column.getName(), request, columnDocument, providerName));
        }

        // create BucketDocument for each Int64ArrayColumn
        for (Int64ArrayColumn column : request.getIngestionDataFrame().getInt64ArrayColumnsList()) {
            ColumnDocumentBase columnDocument = Int64ArrayColumnDocument.fromInt64ArrayColumn(column);
            bucketList.add(columnBucketDocument(column.getName(), request, columnDocument, providerName));
        }

        // create BucketDocument for each BoolArrayColumn
        for (BoolArrayColumn column : request.getIngestionDataFrame().getBoolArrayColumnsList()) {
            ColumnDocumentBase columnDocument = BoolArrayColumnDocument.fromBoolArrayColumn(column);
            bucketList.add(columnBucketDocument(column.getName(), request, columnDocument, providerName));
        }

        // create BucketDocument for each StructColumn
        for (StructColumn column : request.getIngestionDataFrame().getStructColumnsList()) {
            ColumnDocumentBase columnDocument = StructColumnDocument.fromStructColumn(column);
            bucketList.add(columnBucketDocument(column.getName(), request, columnDocument, providerName));
        }

        // create BucketDocument for each ImageColumn
        for (ImageColumn column : request.getIngestionDataFrame().getImageColumnsList()) {
            ColumnDocumentBase columnDocument = ImageColumnDocument.fromImageColumn(column);
            bucketList.add(columnBucketDocument(column.getName(), request, columnDocument, providerName));
        }

        return bucketList;
    }

    public static DataBucket dataBucketFromDocument(
            BucketDocument document,
            QueryDataRequest.QuerySpec querySpec
    ) throws DpException {

        requireDeserializableBucket(document);

        try {
            final DataBucket.Builder bucketBuilder = DataBucket.newBuilder();

            // set name
            bucketBuilder.setPvName(document.getPvName());

            // add data timestamps
            DataTimestamps dataTimestamps = document.getDataTimestamps().toDataTimestamps();
            bucketBuilder.setDataTimestamps(dataTimestamps);

            // add data values
            document.getDataColumn().addColumnToBucket(bucketBuilder);

            // add provider details
            if (document.getProviderId() != null) {
                bucketBuilder.setProviderId(document.getProviderId());
            }
            if (document.getProviderName() != null) {
                bucketBuilder.setProviderName(document.getProviderName());
            }

            return bucketBuilder.build();

        } catch (RuntimeException ex) {
            throw deserializationFailure(document, ex);
        }
    }

    /**
     * Rejects a stored bucket that is missing the fields deserialization dereferences, so the caller
     * sees a {@link DpException} it can turn into an error response.
     *
     * <p>Without this, a malformed or partially-written document produces a NullPointerException,
     * which the query dispatchers do not catch (they handle only DpException). That escapes the
     * dispatcher loop, terminates the response stream, and leaves the client unable to distinguish
     * a deserialization failure from an empty result — a silent wrong answer rather than an error.
     */
    private static void requireDeserializableBucket(BucketDocument document) throws DpException {

        if (document == null) {
            throw new DpException("cannot build DataBucket from null BucketDocument");
        }
        if (document.getDataTimestamps() == null) {
            throw new DpException(
                    "BucketDocument id: " + document.getId() + " pvName: " + document.getPvName()
                            + " has no dataTimestamps");
        }
        if (document.getDataColumn() == null) {
            throw new DpException(
                    "BucketDocument id: " + document.getId() + " pvName: " + document.getPvName()
                            + " has no dataColumn");
        }
    }

    /**
     * Wraps an unexpected runtime failure during deserialization as a {@link DpException}, for the
     * same reason as {@link #requireDeserializableBucket}: the dispatchers can report a DpException
     * to the client, whereas an escaping RuntimeException silently truncates the response.
     */
    private static DpException deserializationFailure(BucketDocument document, RuntimeException ex) {
        final String message =
                "error deserializing BucketDocument id: " + document.getId()
                        + " pvName: " + document.getPvName()
                        + " exception: " + ex;
        logger.error(message, ex);
        return new DpException(message);
    }

    /**
     * Builds a Query API V2 {@link DataBucket} from a stored bucket document, honoring the V2
     * representation flags.
     *
     * <p><b>excludeColumnMetadata</b> (Q8): the default (false) includes column metadata, which
     * {@code addColumnToBucket}/{@code applyMetadataToProto} already restores; when true, the
     * {@code metadata} field is cleared on the emitted column.
     *
     * <p><b>useSerializedColumns</b> (Q5, pass-through-only per plan 11.2.4(ii)): columns that were
     * stored serialized are emitted as {@code SerializedDataColumn} (already the behavior of
     * {@code addColumnToBucket} for {@code SerializedDataColumnDocument}); typed/scalar-stored columns
     * are emitted in their typed form regardless of this flag — no typed-to-serialized conversion and
     * no fabricated {@code encoding} contract in this phase. The flag is accepted for API symmetry and
     * has no effect on typed columns.
     */
    public static DataBucket dataBucketFromDocumentV2(
            BucketDocument document,
            boolean useSerializedColumns,
            boolean excludeColumnMetadata
    ) throws DpException {

        requireDeserializableBucket(document);

        try {
            final DataBucket.Builder bucketBuilder = DataBucket.newBuilder();

            bucketBuilder.setPvName(document.getPvName());
            bucketBuilder.setDataTimestamps(document.getDataTimestamps().toDataTimestamps());

            // add data values (polymorphic: serialized-stored columns pass through as SerializedDataColumn,
            // typed/legacy columns emit their typed form) — useSerializedColumns is pass-through-only here.
            document.getDataColumn().addColumnToBucket(bucketBuilder);

            if (excludeColumnMetadata && bucketBuilder.hasDataValues()) {
                bucketBuilder.setDataValues(clearColumnMetadata(bucketBuilder.getDataValues()));
            }

            if (document.getProviderId() != null) {
                bucketBuilder.setProviderId(document.getProviderId());
            }
            if (document.getProviderName() != null) {
                bucketBuilder.setProviderName(document.getProviderName());
            }

            return bucketBuilder.build();

        } catch (RuntimeException ex) {
            throw deserializationFailure(document, ex);
        }
    }

    /**
     * Returns a copy of the given {@link DataValues} with the {@code metadata} field cleared on
     * whichever column type is set in the oneof. Uses reflection to invoke {@code clearMetadata()} on
     * the set column's builder, mirroring the reflective approach used to set metadata on ingest, so
     * all column types are handled uniformly without an 18-arm switch.
     */
    private static DataValues clearColumnMetadata(DataValues dataValues) {
        final DataValues.ValuesCase valuesCase = dataValues.getValuesCase();
        if (valuesCase == DataValues.ValuesCase.VALUES_NOT_SET) {
            return dataValues;
        }
        try {
            final DataValues.Builder dataValuesBuilder = dataValues.toBuilder();
            // find the getter for the set column, clear metadata on it, set it back
            final com.google.protobuf.Descriptors.FieldDescriptor fieldDescriptor =
                    dataValues.getDescriptorForType().findFieldByNumber(valuesCase.getNumber());
            final com.google.protobuf.Message columnMessage =
                    (com.google.protobuf.Message) dataValues.getField(fieldDescriptor);
            final com.google.protobuf.Message.Builder columnBuilder = columnMessage.toBuilder();
            final com.google.protobuf.Descriptors.FieldDescriptor metadataField =
                    columnBuilder.getDescriptorForType().findFieldByName("metadata");
            if (metadataField != null && columnBuilder.hasField(metadataField)) {
                columnBuilder.clearField(metadataField);
                dataValuesBuilder.setField(fieldDescriptor, columnBuilder.build());
                return dataValuesBuilder.build();
            }
            return dataValues;
        } catch (Exception ex) {
            // metadata suppression is best-effort formatting, not correctness — never fail the query.
            return dataValues;
        }
    }

}
