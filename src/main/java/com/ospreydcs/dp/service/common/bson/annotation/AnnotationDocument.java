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
    private DataSetDocument dataSet;

    // abstract methods
    abstract protected List<String> diffRequestDetails(CreateAnnotationRequest request);
    abstract protected void addAnnotationDetails(
            QueryAnnotationsResponse.AnnotationsResult.Annotation.Builder responseAnnotation);

    /*
     * NOTE: This method was renamed from setFieldsFromRequest(), which made the mongo codec thrown an exception
     * because there was no property "fieldsFromRequest".  I changed the method to not use the bean property getter
     * method naming convention, and that solved the problem.
     */
    public void applyRequestFieldValues(CreateAnnotationRequest request) {
        setOwnerId(request.getOwnerId());
        final List<DocumentDataBlock> dataBlocks = new ArrayList<>();
//        for (com.ospreydcs.dp.grpc.v1.annotation.DataBlock dataBlock : request.getDataSet().getDataBlocksList()) {
//            final Timestamp blockBeginTime =  dataBlock.getBeginTime();
//            final Timestamp blockEndtime = dataBlock.getEndTime();
//            final long blockBeginSeconds = blockBeginTime.getEpochSeconds();
//            final long blockBeginNanos = blockBeginTime.getNanoseconds();
//            final long blockEndSeconds = blockEndtime.getEpochSeconds();
//            final long blockEndNanos = blockEndtime.getNanoseconds();
//            List<String> blockPvNames = dataBlock.getPvNamesList();
//            DataBlock documentBlock = new DataBlock(
//                    blockBeginSeconds,
//                    blockBeginNanos,
//                    blockEndSeconds,
//                    blockEndNanos,
//                    blockPvNames);
//            dataBlocks.add(documentBlock);
//        }
//        DataSet documentDataSet = new DataSet();
//        documentDataSet.setDataBlocks(dataBlocks);
//        setDataSet(documentDataSet);
    }

    public List<String> diffRequest(CreateAnnotationRequest request) {

        // get diff for details for specific annotation type
        final List<String> diffs = diffRequestDetails(request);

        // diff authorId
        if (request.getOwnerId() != this.getOwnerId()) {
            final String msg = "owner mismatch: " + this.getOwnerId()
                    + " expected: " + request.getOwnerId();
            diffs.add(msg);
        }

//        // diff DataSet
//        if (request.getDataSet().getDataBlocksList().size() != getDataSet().getDataBlocks().size()) {
//            final String msg = "DataSet DataBlocks list size mismatch: " + getDataSet().getDataBlocks().size()
//                    + " expected: " + request.getDataSet().getDataBlocksList().size();
//            diffs.add(msg);
//        }
//        for (int blockIndex = 0 ; blockIndex < request.getDataSet().getDataBlocksList().size() ; ++blockIndex) {
//
//            final com.ospreydcs.dp.grpc.v1.common.DataBlock requestDataBlock =
//                    request.getDataSet().getDataBlocksList().get(blockIndex);
//            final Timestamp requestBlockBeginTime =  requestDataBlock.getBeginTime();
//            final Timestamp requestBlockEndTime = requestDataBlock.getEndTime();
//            final long requestBlockBeginSeconds = requestBlockBeginTime.getEpochSeconds();
//            final long requestBlockBeginNanos = requestBlockBeginTime.getNanoseconds();
//            final long requestBlockEndSeconds = requestBlockEndTime.getEpochSeconds();
//            final long requestBlockEndNanos = requestBlockEndTime.getNanoseconds();
//            Set<String> requestPvNames = new TreeSet<>(requestDataBlock.getPvNamesList());
//
//            final DataBlock dataBlock = this.getDataSet().getDataBlocks().get(blockIndex);
//            if (requestBlockBeginSeconds != dataBlock.getBeginTimeSeconds()) {
//                final String msg = "block beginTime seconds mistmatch: " + dataBlock.getBeginTimeSeconds()
//                        + " expected: " + requestBlockBeginSeconds;
//                diffs.add(msg);
//            }
//            if (requestBlockBeginNanos != dataBlock.getBeginTimeNanos()) {
//                final String msg = "block beginTime nanos mistmatch: " + dataBlock.getBeginTimeNanos()
//                        + " expected: " + requestBlockBeginNanos;
//                diffs.add(msg);
//            }
//            if (requestBlockEndSeconds != dataBlock.getEndTimeSeconds()) {
//                final String msg = "block endTime seconds mistmatch: " + dataBlock.getEndTimeSeconds()
//                        + " expected: " + requestBlockEndSeconds;
//                diffs.add(msg);
//            }
//            if (requestBlockEndNanos != dataBlock.getEndTimeNanos()) {
//                final String msg = "block endTime nanos mistmatch: " + dataBlock.getEndTimeNanos()
//                        + " expected: " + requestBlockEndNanos;
//                diffs.add(msg);
//            }
//            if (requestPvNames.size() != dataBlock.getPvNames().size()) {
//                final String msg = "block pvNames list size mismatch: " + dataBlock.getPvNames().size()
//                        + " expected: " + requestPvNames.size();
//                diffs.add(msg);
//            }
//            if (!Objects.equals(requestPvNames, dataBlock.getPvNames())) {
//                final String msg = "block pvNames list mismatch: " + dataBlock.getPvNames().toString()
//                        + " expected: " + requestPvNames.toString();
//                diffs.add(msg);
//            }
//
//        }

        return diffs;
    }
    
    public QueryAnnotationsResponse.AnnotationsResult.Annotation buildAnnotation() {

        final QueryAnnotationsResponse.AnnotationsResult.Annotation.Builder annotationBuilder =
                QueryAnnotationsResponse.AnnotationsResult.Annotation.newBuilder();

        // add base annotation fields to response object
        annotationBuilder.setAnnotationId(this.getId().toString());
        annotationBuilder.setOwnerId(this.getOwnerId());
        com.ospreydcs.dp.grpc.v1.annotation.DataSet.Builder dataSetBuilder =
                com.ospreydcs.dp.grpc.v1.annotation.DataSet.newBuilder();
        for (DocumentDataBlock documentDataBlock :
                this.getDataSet().getDataBlocks()
        ) {
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

        // add annotation-type-specific details to response
        addAnnotationDetails(annotationBuilder);

        return annotationBuilder.build();
    }

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

    public DataSetDocument getDataSet() {
        return dataSet;
    }

    public void setDataSet(DataSetDocument dataSet) {
        this.dataSet = dataSet;
    }

}
