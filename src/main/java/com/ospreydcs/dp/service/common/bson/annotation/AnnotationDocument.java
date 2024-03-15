package com.ospreydcs.dp.service.common.bson.annotation;

import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationRequest;
import com.ospreydcs.dp.grpc.v1.common.Attribute;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

import java.util.*;

@BsonDiscriminator(key="type")
public abstract class AnnotationDocument {

    // instance variables
    private String type;
    private Integer authorId;
    private Set<String> tags;
    private Map<String, String> attributeMap;
    private DataSet dataSet;

    // abstract methods
    abstract List<String> diffRequestDetails(CreateAnnotationRequest request);

    /*
     * NOTE: This method was renamed from setFieldsFromRequest(), which made the mongo codec thrown an exception
     * because there was no property "fieldsFromRequest".  I changed the method to not use the bean property getter
     * method naming convention, and that solved the problem.
     */
    public void applyRequestFieldValues(CreateAnnotationRequest request) {
        setAuthorId(request.getAuthorId());
        setTags(new TreeSet(request.getTagsList()));
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

    public List<String> diffRequest(CreateAnnotationRequest request) {

        // get diff for details for specific annotation type
        final List<String> diffs = diffRequestDetails(request);

        // diff authorId
        if (request.getAuthorId() != this.getAuthorId()) {
            final String msg = "author mismatch: " + this.getAuthorId()
                    + " expected: " + request.getAuthorId();
            diffs.add(msg);
        }

        // diff tags
        final Set<String> requestTagSet = new TreeSet<>(request.getTagsList());
        if (requestTagSet.size() != this.getTags().size() || ! Objects.equals(requestTagSet, this.getTags())) {
            final String msg = "tags mismatch: " + this.getTags().toString()
                    + " expected: " + requestTagSet.toString();
            diffs.add(msg);
        }

        // diff attributes
        if (request.getAttributesList().size() != getAttributeMap().size()) {
            final String msg = "attributes list size mismatch: " + getAttributeMap().size()
                    + " expected: " + request.getAttributesList().size();
            diffs.add(msg);
        }
        for (Attribute requestAttribute : request.getAttributesList()) {
            final String attributeValue = this.getAttributeMap().get(requestAttribute.getName());
            if (attributeValue == null || ! attributeValue.equals(requestAttribute.getValue())) {
                final String msg = "attribute key: " + requestAttribute.getName()
                        + " value mismatch: " + attributeValue
                        + " expected: " + requestAttribute.getValue();
                diffs.add(msg);
            }
        }

        // diff DataSet
        if (request.getDataSet().getDataBlocksList().size() != getDataSet().getDataBlocks().size()) {
            final String msg = "DataSet DataBlocks list size mismatch: " + getDataSet().getDataBlocks().size()
                    + " expected: " + request.getDataSet().getDataBlocksList().size();
            diffs.add(msg);
        }
        for (int blockIndex = 0 ; blockIndex < request.getDataSet().getDataBlocksList().size() ; ++blockIndex) {

            final com.ospreydcs.dp.grpc.v1.annotation.DataBlock requestDataBlock =
                    request.getDataSet().getDataBlocksList().get(blockIndex);
            final Timestamp requestBlockBeginTime =  requestDataBlock.getBeginTime();
            final Timestamp requestBlockEndTime = requestDataBlock.getEndTime();
            final long requestBlockBeginSeconds = requestBlockBeginTime.getEpochSeconds();
            final long requestBlockBeginNanos = requestBlockBeginTime.getNanoseconds();
            final long requestBlockEndSeconds = requestBlockEndTime.getEpochSeconds();
            final long requestBlockEndNanos = requestBlockEndTime.getNanoseconds();
            Set<String> requestPvNames = new TreeSet<>(requestDataBlock.getPvNamesList());

            final DataBlock dataBlock = this.getDataSet().getDataBlocks().get(blockIndex);
            if (requestBlockBeginSeconds != dataBlock.getBeginTimeSeconds()) {
                final String msg = "block beginTime seconds mistmatch: " + dataBlock.getBeginTimeSeconds() 
                        + " expected: " + requestBlockBeginSeconds;
                diffs.add(msg);
            }
            if (requestBlockBeginNanos != dataBlock.getBeginTimeNanos()) {
                final String msg = "block beginTime nanos mistmatch: " + dataBlock.getBeginTimeNanos()
                        + " expected: " + requestBlockBeginNanos;
                diffs.add(msg);
            }
            if (requestBlockEndSeconds != dataBlock.getEndTimeSeconds()) {
                final String msg = "block endTime seconds mistmatch: " + dataBlock.getEndTimeSeconds()
                        + " expected: " + requestBlockEndSeconds;
                diffs.add(msg);
            }
            if (requestBlockEndNanos != dataBlock.getEndTimeNanos()) {
                final String msg = "block endTime nanos mistmatch: " + dataBlock.getEndTimeNanos()
                        + " expected: " + requestBlockEndNanos;
                diffs.add(msg);
            }
            if (requestPvNames.size() != dataBlock.getPvNames().size()) {
                final String msg = "block pvNames list size mismatch: " + dataBlock.getPvNames().size()
                        + " expected: " + requestPvNames.size();
                diffs.add(msg);
            }
            if (!Objects.equals(requestPvNames, dataBlock.getPvNames())) {
                final String msg = "block pvNames list mismatch: " + dataBlock.getPvNames().toString()
                        + " expected: " + requestPvNames.toString();
                diffs.add(msg);
            }

        }

        return diffs;
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

    public Set<String> getTags() {
        return tags;
    }

    public void setTags(Set<String> tags) {
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
