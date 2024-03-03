package com.ospreydcs.dp.service.annotation.handler;

import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.DataBlock;
import com.ospreydcs.dp.grpc.v1.annotation.DataSet;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.common.model.ValidationResult;

import java.util.List;

public class AnnotationValidationUtility {

    public static ValidationResult validateCreateAnnotationRequestCommon(CreateAnnotationRequest request) {

        // author must be specified
        int requestAuthorId = request.getAuthorId();
        if (requestAuthorId < 1) {
            String errorMsg = "CreateAnnotationRequest must specify non-zero authorId";
            return new ValidationResult(true, errorMsg);
        }

        DataSet requestDataSet = request.getDataSet();

        // DataSet must contain one or more DataBlocks
        List<DataBlock> requestDataBlocks = requestDataSet.getDataBlocksList();
        if (requestDataBlocks.isEmpty()) {
            String errorMsg = "CreateAnnotationRequest.DataSet must not be empty";
            return new ValidationResult(true, errorMsg);
        }

        // validate each DataBlock
        for (DataBlock dataBlock : requestDataBlocks) {

            // validate beginTime
            Timestamp blockBeginTime = dataBlock.getBeginTime();
            if (blockBeginTime.getEpochSeconds() < 1) {
                String errorMsg = "CreateAnnotationRequest.DataSet.DataBlock.beginTime must be non-zero";
                return new ValidationResult(true, errorMsg);
            }

            // validate endTime
            Timestamp blockEndTime = dataBlock.getEndTime();
            if (blockEndTime.getEpochSeconds() < 1) {
                String errorMsg = "CreateAnnotationRequest.DataSet.DataBlock.endTime must be non-zero";
                return new ValidationResult(true, errorMsg);
            }

            // validate pvNames list not empty
            List<String> blockPvNames = dataBlock.getPvNamesList();
            if (blockPvNames.isEmpty()) {
                String errorMsg = "CreateAnnotationRequest.DataSet.DataBlock.pvNames must not be empty";
                return new ValidationResult(true, errorMsg);
            }
        }

        // validation successful
        return new ValidationResult(false, "");
    }

    public static ValidationResult validateCreateCommentRequest(CreateAnnotationRequest request) {

        // validate correct oneof case for details payload
        if (!request.hasCommentDetails()) {
            String errorMsg = "CreateAnnotationRequest does not contain CreateCommentDetails";
            return new ValidationResult(true, errorMsg);
        }

        // validate details
        CreateAnnotationRequest.CommentDetails createCommentDetails = request.getCommentDetails();
        String detailsComment = createCommentDetails.getComment();
        if (detailsComment == null || detailsComment.isBlank()) {
            String errorMsg = "CreateAnnotationRequest.CreateCommentDetails.comment is null or empty";
            return new ValidationResult(true, errorMsg);
        }

        // validate common annotation details
        return validateCreateAnnotationRequestCommon(request);
    }
}
