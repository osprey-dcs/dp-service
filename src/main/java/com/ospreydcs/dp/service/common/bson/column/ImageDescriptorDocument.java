package com.ospreydcs.dp.service.common.bson.column;

import com.ospreydcs.dp.grpc.v1.common.ImageDescriptor;

/**
 * BSON document class for storing ImageDescriptor as an embedded subdocument within ImageColumnDocument.
 */
public class ImageDescriptorDocument {

    private int width;
    private int height;
    private int channels;
    private String encoding;

    public int getWidth() {
        return width;
    }

    public void setWidth(int width) {
        this.width = width;
    }

    public int getHeight() {
        return height;
    }

    public void setHeight(int height) {
        this.height = height;
    }

    public int getChannels() {
        return channels;
    }

    public void setChannels(int channels) {
        this.channels = channels;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public static ImageDescriptorDocument fromImageDescriptor(ImageDescriptor descriptor) {
        ImageDescriptorDocument doc = new ImageDescriptorDocument();
        doc.setWidth((int) descriptor.getWidth());
        doc.setHeight((int) descriptor.getHeight());
        doc.setChannels((int) descriptor.getChannels());
        doc.setEncoding(descriptor.getEncoding());
        return doc;
    }

    public ImageDescriptor toImageDescriptor() {
        return ImageDescriptor.newBuilder()
                .setWidth(width)
                .setHeight(height)
                .setChannels(channels)
                .setEncoding(encoding != null ? encoding : "")
                .build();
    }
}
