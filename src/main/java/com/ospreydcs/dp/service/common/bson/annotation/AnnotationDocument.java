package com.ospreydcs.dp.service.common.bson.annotation;

import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationRequest;
import com.ospreydcs.dp.grpc.v1.common.Attribute;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@BsonDiscriminator(key="type")
public abstract class AnnotationDocument {

    private String type;
    private Integer authorId;
    private List<String> tags;
    private Map<String, String> attributeMap;
    private DataSet dataSet;

    /*
     * NOTE: This method was renamed from setFieldsFromRequest(), which made the mongo codec thrown an exception
     * because there was no property "fieldsFromRequest".  I changed the method to not use the bean property getter
     * method naming convention, and that solved the problem.
     */
    public void applyRequestFieldValues(CreateAnnotationRequest request) {
        setAuthorId(request.getAuthorId());
        setTags(request.getTagsList());
        final Map<String, String> attributeMap = new TreeMap<>();
        for (Attribute attribute : request.getAttributesList()) {
            attributeMap.put(attribute.getName(), attribute.getValue());
        }
        setAttributeMap(attributeMap);
        final List<DataBlock> dataBlocks = new ArrayList<>();
        for (com.ospreydcs.dp.grpc.v1.annotation.DataBlock dataBlock : request.getDataSet().getDataBlocksList()) {
            final Timestamp blockBeginTime =  dataBlock.getBeginTime();
            final Timestamp blockEndtime = dataBlock.getEndTime();
            final long blockBeginSeconds = blockBeginTime.getEpochSeconds();
            final long blockBeginNanos = blockBeginTime.getNanoseconds();
            final long blockEndSeconds = blockEndtime.getEpochSeconds();
            final long blockEndNanos = blockEndtime.getNanoseconds();
            List<String> blockPvNames = dataBlock.getPvNamesList();
            DataBlock documentBlock = new DataBlock(
                    blockBeginSeconds,
                    blockBeginNanos,
                    blockEndSeconds,
                    blockEndNanos,
                    blockPvNames);
            dataBlocks.add(documentBlock);
        }
        DataSet documentDataSet = new DataSet();
        documentDataSet.setDataBlocks(dataBlocks);
        setDataSet(documentDataSet);
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Integer getAuthorId() {
        return authorId;
    }

    public void setAuthorId(Integer authorId) {
        this.authorId = authorId;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public Map<String, String> getAttributeMap() {
        return attributeMap;
    }

    public void setAttributeMap(Map<String, String> attributeMap) {
        this.attributeMap = attributeMap;
    }

    public DataSet getDataSet() {
        return dataSet;
    }

    public void setDataSet(DataSet dataSet) {
        this.dataSet = dataSet;
    }
}
