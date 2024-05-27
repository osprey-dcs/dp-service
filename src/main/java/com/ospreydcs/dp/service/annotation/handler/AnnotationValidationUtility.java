package com.ospreydcs.dp.service.annotation.handler;

import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.CommentAnnotation;
import com.ospreydcs.dp.grpc.v1.annotation.DataBlock;
import com.ospreydcs.dp.grpc.v1.annotation.DataSet;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.common.model.ValidationResult;

import java.util.List;

public class AnnotationValidationUtility {

    public static ValidationResult validateDataSet(DataSet dataSet) {

        // DataSet must include ownerId
        if (dataSet.getOwnerId() == null || dataSet.getOwnerId().isBlank()) {
            final String errorMsg = "DataSet ownerId must be specified";
            return new ValidationResult(true, errorMsg);
        }

        // DataSet must include description
        if (dataSet.getDescription() == null || dataSet.getDescription().isBlank()) {
            final String errorMsg = "DataSet description must be specified";
            return new ValidationResult(true, errorMsg);
        }

        // DataSet must contain one or more DataBlocks
        final List<DataBlock> requestDataBlocks = dataSet.getDataBlocksList();
        if (requestDataBlocks.isEmpty()) {
            final String errorMsg = "DataSet must include one or more data blocks";
            return new ValidationResult(true, errorMsg);
        }

        // validate each DataBlock
        for (DataBlock dataBlock : requestDataBlocks) {

            // validate beginTime
            final Timestamp blockBeginTime = dataBlock.getBeginTime();
            if (blockBeginTime.getEpochSeconds() < 1) {
                final String errorMsg = "DataSet.DataBlock.beginTime must be non-zero";
                return new ValidationResult(true, errorMsg);
            }

            // validate endTime
            final Timestamp blockEndTime = dataBlock.getEndTime();
            if (blockEndTime.getEpochSeconds() < 1) {
                final String errorMsg = "DataSet.DataBlock.endTime must be non-zero";
                return new ValidationResult(true, errorMsg);
            }

            // validate pvNames list not empty
            final List<String> blockPvNames = dataBlock.getPvNamesList();
            if (blockPvNames.isEmpty()) {
                final String errorMsg = "DataSet.DataBlock.pvNames must not be empty";
                return new ValidationResult(true, errorMsg);
            }
        }

        // validation successful
        return new ValidationResult(false, "");
    }

    public static ValidationResult validateCreateAnnotationRequestCommon(CreateAnnotationRequest request) {

        // owner must be specified
        final String requestOwnerId = request.getOwnerId();
        if (requestOwnerId.isBlank()) {
            final String errorMsg = "CreateAnnotationRequest must specify ownerId";
            return new ValidationResult(true, errorMsg);
        }

        // check that dataSetId is specified, but don't check yet that it exists, that will be done by the handler job
        final String dataSetId = request.getDataSetId();
        if (dataSetId.isBlank()) {
            final String errorMsg = "CreateAnnotationRequest must specify dataSetId";
            return new ValidationResult(true, errorMsg);
        }

        // validation successful
        return new ValidationResult(false, "");
    }

    public static ValidationResult validateCreateCommentRequest(CreateAnnotationRequest request) {

        // validate correct oneof case for details payload
        if (!request.hasCommentAnnotation()) {
            final String errorMsg = "CreateAnnotationRequest does not contain CreateCommentDetails";
            return new ValidationResult(true, errorMsg);
        }

        // validate details
        final CommentAnnotation createCommentDetails = request.getCommentAnnotation();
        final String detailsComment = createCommentDetails.getComment();
        if (detailsComment == null || detailsComment.isBlank()) {
            final String errorMsg = "CreateAnnotationRequest.CreateCommentDetails.comment is null or empty";
            return new ValidationResult(true, errorMsg);
        }

        return new ValidationResult(false, "");
    }

}
