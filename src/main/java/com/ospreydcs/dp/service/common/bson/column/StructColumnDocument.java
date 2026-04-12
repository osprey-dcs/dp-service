package com.ospreydcs.dp.service.common.bson.column;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataValues;
import com.ospreydcs.dp.grpc.v1.common.StructColumn;
import com.ospreydcs.dp.service.common.bson.ColumnMetadataDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

/**
 * BSON document class for StructColumn storage in MongoDB.
 * Serializes the entire StructColumn proto to binary storage using protobuf's standard
 * wire format, consistent with the DataColumnDocument approach.
 */
@BsonDiscriminator(key = "_t", value = "structColumn")
public class StructColumnDocument extends BinaryColumnDocumentBase {

    private String schemaId;

    public String getSchemaId() {
        return schemaId;
    }

    public void setSchemaId(String schemaId) {
        this.schemaId = schemaId;
    }

    public static StructColumnDocument fromStructColumn(StructColumn requestColumn) throws DpException {
        StructColumnDocument document = new StructColumnDocument();
        document.setName(requestColumn.getName());
        document.setSchemaId(requestColumn.getSchemaId());
        document.setBinaryData(requestColumn.toByteArray());
        if (requestColumn.hasMetadata()) {
            document.setColumnMetadata(ColumnMetadataDocument.fromColumnMetadata(requestColumn.getMetadata()));
        }
        return document;
    }

    @Override
    protected Message deserializeToProtobufColumn() throws DpException {
        try {
            return StructColumn.parseFrom(getBinaryData());
        } catch (InvalidProtocolBufferException e) {
            throw new DpException("Failed to deserialize StructColumn: " + e.getMessage());
        }
    }

    @Override
    public void addColumnToBucket(DataBucket.Builder bucketBuilder) throws DpException {
        StructColumn structColumn = (StructColumn) toProtobufColumn();
        DataValues dataValues = DataValues.newBuilder().setStructColumn(structColumn).build();
        bucketBuilder.setDataValues(dataValues);
    }
}
