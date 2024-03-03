package com.ospreydcs.dp.service.common.bson.annotation;

import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationRequest;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;

@BsonDiscriminator(key = "type", value = "COMMENT")
public class CommentAnnotationDocument extends AnnotationDocument {

    private String comment;

    public static CommentAnnotationDocument fromCreateRequest(CreateAnnotationRequest request) {
        CommentAnnotationDocument document = new CommentAnnotationDocument();
        document.setFieldsFromRequest(request);
        document.setComment(request.getCommentDetails().getComment());
        return document;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

}
