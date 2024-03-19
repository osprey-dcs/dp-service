package com.ospreydcs.dp.service.common.bson.annotation;

import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationRequest;
import com.ospreydcs.dp.grpc.v1.common.CommentAnnotation;
import com.ospreydcs.dp.grpc.v1.query.QueryAnnotationsResponse;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@BsonDiscriminator(key = "type", value = AnnotationDocument.ANNOTATION_TYPE_COMMENT)
public class CommentAnnotationDocument extends AnnotationDocument {

    private String comment;

    public static CommentAnnotationDocument fromCreateRequest(CreateAnnotationRequest request) {
        CommentAnnotationDocument document = new CommentAnnotationDocument();
        document.applyRequestFieldValues(request);
        document.setComment(request.getCommentAnnotation().getComment());
        return document;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    protected List<String> diffRequestDetails(CreateAnnotationRequest request) {

        final List<String> diffs = new ArrayList<>();

        String requestComment = "";
        if (request.hasCommentAnnotation()) {
            requestComment = request.getCommentAnnotation().getComment();
        }
        if (! Objects.equals(requestComment, this.getComment())) {
            final String msg = "comment mismatch: " + this.getComment() + " expected: " + requestComment;
            diffs.add(msg);
        }

        return diffs;
    }

    @Override
    protected void addAnnotationDetails(
            QueryAnnotationsResponse.AnnotationsResult.Annotation.Builder responseAnnotation
    ) {
        final CommentAnnotation commentAnnotation = CommentAnnotation.newBuilder()
                .setComment(this.getComment())
                .build();
        responseAnnotation.setCommentAnnotation(commentAnnotation);
    }

}
