package com.ospreydcs.dp.service.common.bson.annotation;

import com.ospreydcs.dp.grpc.v1.annotation.AnnotationDetails;
import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.DataSet;
import com.ospreydcs.dp.grpc.v1.annotation.QueryAnnotationsResponse;
import com.ospreydcs.dp.grpc.v1.common.Attribute;
import com.ospreydcs.dp.grpc.v1.common.EventMetadata;
import com.ospreydcs.dp.service.common.bson.bucket.EventMetadataDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.grpc.AttributesUtility;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;

import java.util.*;

public class AnnotationDocument {

    // instance variables
    private ObjectId id;
    private String ownerId;
    private List<String> dataSetIds;
    private String name;
    private String comment;
    private List<String> tags;
    private Map<String, String> attributeMap;
    private EventMetadataDocument eventMetadata;

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public String getOwnerId() {
        return ownerId;
    }

    public void setOwnerId(String ownerId) {
        this.ownerId = ownerId;
    }

    public List<String> getDataSetIds() {
        return dataSetIds;
    }

    public void setDataSetIds(List<String> dataSetIds) {
        this.dataSetIds = dataSetIds;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
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

    public EventMetadataDocument getEventMetadata() {
        return eventMetadata;
    }

    public void setEventMetadata(EventMetadataDocument eventMetadata) {
        this.eventMetadata = eventMetadata;
    }

    public static AnnotationDocument fromAnnotationDetails(final AnnotationDetails annotationDetails) {

        final AnnotationDocument document = new AnnotationDocument();

        // set request fields in document
        document.setOwnerId(annotationDetails.getOwnerId());
        document.setDataSetIds(annotationDetails.getDataSetIdsList());
        document.setName(annotationDetails.getName());
        document.setComment(annotationDetails.getComment());
        document.setTags(annotationDetails.getTagsList());

        // create map of attributes and add to document
        final Map<String, String> attributeMap =
                AttributesUtility.attributeMapFromList(annotationDetails.getAttributesList());
        document.setAttributeMap(attributeMap);

        // add event metadata from request to document
        if (annotationDetails.hasEventMetadata()) {
            final EventMetadataDocument eventMetadataDocument =
                    EventMetadataDocument.fromEventMetadata(annotationDetails.getEventMetadata());
            document.setEventMetadata(eventMetadataDocument);
        }

        return document;
    }

    public AnnotationDetails toAnnotationDetails(DataSetDocument dataSetDocument) {

        AnnotationDetails.Builder annotationDetailsBuilder = AnnotationDetails.newBuilder();
        annotationDetailsBuilder.setId(this.getId().toString());
        annotationDetailsBuilder.setOwnerId(this.getOwnerId());
        annotationDetailsBuilder.addAllDataSetIds(this.getDataSetIds());
        annotationDetailsBuilder.setName(this.getName());
        annotationDetailsBuilder.setComment(this.getComment());
        annotationDetailsBuilder.addAllTags(this.getTags());
        annotationDetailsBuilder.addAllAttributes(AttributesUtility.attributeListFromMap(this.getAttributeMap()));
        annotationDetailsBuilder.setEventMetadata(EventMetadataDocument.toEventMetadata(this.getEventMetadata()));

        return annotationDetailsBuilder.build();
    }

    public List<String> diffRequest(final CreateAnnotationRequest request) {

        final List<String> diffs = new ArrayList<>();
        final AnnotationDetails annotationDetails = request.getAnnotationDetails();

        // diff ownerId
        if (! Objects.equals(annotationDetails.getOwnerId(), this.getOwnerId())) {
            final String msg = 
                    "ownerId mismatch: " + this.getOwnerId()
                    + " expected: " + annotationDetails.getOwnerId();
            diffs.add(msg);
        }

        // diff dataSetIds list
        final Collection<String> dataSetIdsDisjunction = 
                CollectionUtils.disjunction(annotationDetails.getDataSetIdsList(), this.getDataSetIds());
        if ( ! dataSetIdsDisjunction.isEmpty()) {
            final String msg =
                    "dataSetIds mismatch: " + this.getDataSetIds()
                    + " disjunction: " + dataSetIdsDisjunction;
        }
        
        // diff name
        if ( ! Objects.equals(annotationDetails.getName(), this.getName())) {
            final String msg = "name mismatch: " + this.getName() + " expected: " + annotationDetails.getName();
            diffs.add(msg);
        }

        // diff comment
        if ( ! Objects.equals(annotationDetails.getComment(), this.getComment())) {
            final String msg = 
                    "comment mismatch: " + this.getComment() + " expected: " + annotationDetails.getComment();
            diffs.add(msg);
        }

        // diff tags list
        final Collection<String> tagsDisjunction = 
                CollectionUtils.disjunction(annotationDetails.getTagsList(), this.getTags());
        if ( ! tagsDisjunction.isEmpty()) {
            final String msg =
                    "tags mismatch: " + this.getTags()
                            + " disjunction: " + tagsDisjunction;
            diffs.add(msg);
        }
        
        // diff attributes
        final Collection<Attribute> attributesDisjunction =
                CollectionUtils.disjunction(
                        annotationDetails.getAttributesList(),
                        AttributesUtility.attributeListFromMap(this.getAttributeMap()));
        if ( ! attributesDisjunction.isEmpty()) {
            final String msg =
                    "attributes mismatch: " + this.getAttributeMap()
                            + " disjunction: " + attributesDisjunction;
            diffs.add(msg);
        }

        // diff eventMetadata
        final EventMetadata thisEventMetadata =
                EventMetadataDocument.toEventMetadata(this.getEventMetadata());
        if (! Objects.equals(annotationDetails.getEventMetadata(), thisEventMetadata)) {
            final String msg =
                    "eventMetadata mismatch: " + thisEventMetadata
                            + " expected: " + annotationDetails.getEventMetadata();
            diffs.add(msg);
        }

        return diffs;
    }

}
