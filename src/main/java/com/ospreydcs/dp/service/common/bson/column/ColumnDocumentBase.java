package com.ospreydcs.dp.service.common.bson.column;

import com.google.protobuf.Message;
import com.ospreydcs.dp.grpc.v1.common.ColumnMetadata;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.service.common.bson.ColumnMetadataDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

@BsonDiscriminator
public abstract class ColumnDocumentBase {

    // instance variables
    private String name;
    private ColumnMetadataDocument columnMetadata;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ColumnMetadataDocument getColumnMetadata() {
        return columnMetadata;
    }

    public void setColumnMetadata(ColumnMetadataDocument columnMetadata) {
        this.columnMetadata = columnMetadata;
    }

    /**
     * Converts this document to its corresponding protobuf column message.
     * Each branch of the hierarchy implements this differently:
     * - Scalar columns use incremental builder pattern
     * - Binary columns use direct deserialization pattern
     */
    public abstract Message toProtobufColumn();

    /**
     * If this document has columnMetadata, sets it on the provided proto message by rebuilding
     * via the proto builder's setMetadata() method.  Returns the original message unchanged when
     * no metadata is stored (the common case — zero overhead).
     */
    protected Message applyMetadataToProto(Message proto) {
        if (columnMetadata == null) {
            return proto;
        }
        try {
            ColumnMetadata metaProto = columnMetadata.toColumnMetadata();
            Message.Builder builder = proto.toBuilder();
            builder.getClass().getMethod("setMetadata", ColumnMetadata.class).invoke(builder, metaProto);
            return builder.build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to apply columnMetadata to proto column", e);
        }
    }

    public byte[] toByteArray() {
        return toProtobufColumn().toByteArray();
    }

    /**
     * Adds the column to the supplied DataBucket.Builder for use in query result.
     *
     * @param bucketBuilder
     * @throws DpException
     */
    public abstract void addColumnToBucket(DataBucket.Builder bucketBuilder) throws DpException;
}
