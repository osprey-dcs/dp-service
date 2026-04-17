package com.ospreydcs.dp.service.common.bson.column;

import com.google.protobuf.Message;
import com.ospreydcs.dp.grpc.v1.common.ColumnMetadata;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.service.common.bson.ColumnMetadataDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

@BsonDiscriminator
public abstract class ColumnDocumentBase {

    private static final ConcurrentHashMap<Class<?>, Method> SET_METADATA_METHOD_CACHE =
            new ConcurrentHashMap<>();

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
     * The resolved setMetadata Method is cached per builder class to avoid repeated reflection lookups.
     */
    protected Message applyMetadataToProto(Message proto) {
        if (columnMetadata == null) {
            return proto;
        }
        try {
            ColumnMetadata metaProto = columnMetadata.toColumnMetadata();
            Message.Builder builder = proto.toBuilder();
            Class<?> builderClass = builder.getClass();
            Method setMetadata = SET_METADATA_METHOD_CACHE.computeIfAbsent(builderClass, cls -> {
                try {
                    return cls.getMethod("setMetadata", ColumnMetadata.class);
                } catch (NoSuchMethodException e) {
                    throw new RuntimeException("No setMetadata(ColumnMetadata) method on " + cls.getName(), e);
                }
            });
            setMetadata.invoke(builder, metaProto);
            return builder.build();
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to apply columnMetadata to proto column of type " + proto.getClass().getName(), e);
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
