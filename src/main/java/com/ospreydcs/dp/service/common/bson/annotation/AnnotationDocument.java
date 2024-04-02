package com.ospreydcs.dp.service.common.bson.annotation;

import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationRequest;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.annotation.QueryAnnotationsResponse;
import com.ospreydcs.dp.service.common.bson.dataset.DocumentDataBlock;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;
import org.bson.types.ObjectId;

import java.util.*;

@BsonDiscriminator(key="type")
public abstract class AnnotationDocument {

    // constants
    public static final String ANNOTATION_TYPE_COMMENT = "COMMENT";

    // instance variables
    private ObjectId id;
    private String type;
    private String ownerId;
    private String dataSetId;

    // abstract methods
    abstract protected List<String> diffRequestDetails(CreateAnnotationRequest request);
    abstract protected void addAnnotationDetails(
            QueryAnnotationsResponse.AnnotationsResult.Annotation.Builder responseAnnotation);

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getOwnerId() {
        return ownerId;
    }

    public void setOwnerId(String ownerId) {
        this.ownerId = ownerId;
    }

    public String getDataSetId() {
        return dataSetId;
    }

    public void setDataSetId(String dataSetId) {
        this.dataSetId = dataSetId;
    }

    /*
     * NOTE: This method was renamed from setFieldsFromRequest(), which made the mongo codec thrown an exception
     * because there was no property "fieldsFromRequest".  I changed the method to not use the bean property getter
     * method naming convention, and that solved the problem.
     */
    public void applyRequestFieldValues(CreateAnnotationRequest request) {
        setOwnerId(request.getOwnerId());
        setDataSetId(request.getDataSetId());
    }

    public List<String> diffRequest(CreateAnnotationRequest request) {

        // get diff for details for specific annotation type
        final List<String> diffs = diffRequestDetails(request);

        // diff authorId
        if (! Objects.equals(request.getOwnerId(), this.getOwnerId())) {
            final String msg = "ownerId mismatch: " + this.getOwnerId()
                    + " expected: " + request.getOwnerId();
            diffs.add(msg);
        }

        // diff dataSetId
        if (! Objects.equals(request.getDataSetId(), this.getDataSetId())) {
            final String msg = "dataSetId mismatch: " + this.getDataSetId()
                    + " expected: " + request.getDataSetId();
            diffs.add(msg);
        }

        return diffs;
    }

    public QueryAnnotationsResponse.AnnotationsResult.Annotation buildAnnotation(DataSetDocument dataSetDocument) {

        final QueryAnnotationsResponse.AnnotationsResult.Annotation.Builder annotationBuilder =
                QueryAnnotationsResponse.AnnotationsResult.Annotation.newBuilder();

        // add base annotation fields to response object
        annotationBuilder.setAnnotationId(this.getId().toString());
        annotationBuilder.setOwnerId(this.getOwnerId());

        // add dataset content to response object
        com.ospreydcs.dp.grpc.v1.annotation.DataSet.Builder dataSetBuilder =
                com.ospreydcs.dp.grpc.v1.annotation.DataSet.newBuilder();
        for (DocumentDataBlock documentDataBlock : dataSetDocument.getDataBlocks()) {
            Timestamp blockBeginTime = Timestamp.newBuilder()
                    .setEpochSeconds(documentDataBlock.getBeginTimeSeconds())
                    .setNanoseconds(documentDataBlock.getBeginTimeNanos())
                    .build();
            Timestamp blockEndTime = Timestamp.newBuilder()
                    .setEpochSeconds(documentDataBlock.getEndTimeSeconds())
                    .setNanoseconds(documentDataBlock.getEndTimeNanos())
                    .build();
            com.ospreydcs.dp.grpc.v1.annotation.DataBlock responseBlock =
                    com.ospreydcs.dp.grpc.v1.annotation.DataBlock.newBuilder()
                            .setBeginTime(blockBeginTime)
                            .setEndTime(blockEndTime)
                            .addAllPvNames(documentDataBlock.getPvNames())
                            .build();
            dataSetBuilder.addDataBlocks(responseBlock);
        }
        annotationBuilder.setDataSet(dataSetBuilder);

        // add annotation-type-specific details to response
        addAnnotationDetails(annotationBuilder);

        return annotationBuilder.build();
    }

}
