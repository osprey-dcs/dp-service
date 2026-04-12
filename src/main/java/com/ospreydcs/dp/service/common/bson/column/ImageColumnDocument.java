package com.ospreydcs.dp.service.common.bson.column;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataValues;
import com.ospreydcs.dp.grpc.v1.common.ImageColumn;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

/**
 * BSON document class for ImageColumn storage in MongoDB.
 * Serializes the entire ImageColumn proto to binary storage using protobuf's standard
 * wire format, consistent with the DataColumnDocument approach.
 */
@BsonDiscriminator(key = "_t", value = "imageColumn")
public class ImageColumnDocument extends BinaryColumnDocumentBase {

    private ImageDescriptorDocument imageDescriptor;

    public ImageDescriptorDocument getImageDescriptor() {
        return imageDescriptor;
    }

    public void setImageDescriptor(ImageDescriptorDocument imageDescriptor) {
        this.imageDescriptor = imageDescriptor;
    }

    public static ImageColumnDocument fromImageColumn(ImageColumn requestColumn) throws DpException {
        ImageColumnDocument document = new ImageColumnDocument();
        document.setName(requestColumn.getName());
        document.setImageDescriptor(ImageDescriptorDocument.fromImageDescriptor(requestColumn.getImageDescriptor()));
        document.setBinaryData(requestColumn.toByteArray());
        return document;
    }

    @Override
    protected Message deserializeToProtobufColumn() throws DpException {
        try {
            return ImageColumn.parseFrom(getBinaryData());
        } catch (InvalidProtocolBufferException e) {
            throw new DpException("Failed to deserialize ImageColumn: " + e.getMessage());
        }
    }

    @Override
    public void addColumnToBucket(DataBucket.Builder bucketBuilder) throws DpException {
        ImageColumn imageColumn = (ImageColumn) toProtobufColumn();
        DataValues dataValues = DataValues.newBuilder().setImageColumn(imageColumn).build();
        bucketBuilder.setDataValues(dataValues);
    }
}
