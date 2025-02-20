package com.ospreydcs.dp.service.common.bson.annotation;

import com.ospreydcs.dp.grpc.v1.annotation.CreateAnnotationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryAnnotationsResponse;
import com.ospreydcs.dp.grpc.v1.common.Attribute;
import com.ospreydcs.dp.grpc.v1.common.EventMetadata;
import com.ospreydcs.dp.service.common.bson.EventMetadataDocument;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.protobuf.AttributesUtility;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;

import java.util.*;

public class AnnotationDocument {

    // instance variables
    private ObjectId id;
    private String ownerId;
    private List<String> dataSetIds;
    private String name;
    private List<String> annotationIds;
    private String comment;
    private List<String> tags;
    private Map<String, String> attributeMap;
    private EventMetadataDocument eventMetadata;
    private String calculationsId;

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

    public List<String> getAnnotationIds() {
        return annotationIds;
    }

    public void setAnnotationIds(List<String> annotationIds) {
        this.annotationIds = annotationIds;
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

    public String getCalculationsId() {
        return calculationsId;
    }

    public void setCalculationsId(String calculationsId) {
        this.calculationsId = calculationsId;
    }

    public static AnnotationDocument fromCreateAnnotationRequest(
            final CreateAnnotationRequest request,
            String calculationsDocumentId
    ) {
        final AnnotationDocument document = new AnnotationDocument();

        // set request fields in document
        document.setOwnerId(request.getOwnerId());
        document.setDataSetIds(request.getDataSetIdsList());
        document.setName(request.getName());
        document.setAnnotationIds(request.getAnnotationIdsList());
        document.setComment(request.getComment());
        document.setTags(request.getTagsList());

        // create map of attributes and add to document
        final Map<String, String> attributeMap =
                AttributesUtility.attributeMapFromList(request.getAttributesList());
        document.setAttributeMap(attributeMap);

        // add event metadata from request to document
        if (request.hasEventMetadata()) {
            final EventMetadataDocument eventMetadataDocument =
                    EventMetadataDocument.fromEventMetadata(request.getEventMetadata());
            document.setEventMetadata(eventMetadataDocument);
        }

        if (calculationsDocumentId != null) {
            document.setCalculationsId(calculationsDocumentId);
        }

        return document;
    }

    public QueryAnnotationsResponse.AnnotationsResult.Annotation toAnnotation(
            List<DataSetDocument> dataSetDocuments, CalculationsDocument calculationsDocument) throws DpException {

        QueryAnnotationsResponse.AnnotationsResult.Annotation.Builder annotationBuilder =
                QueryAnnotationsResponse.AnnotationsResult.Annotation.newBuilder();

        annotationBuilder.setId(this.getId().toString());
        annotationBuilder.setOwnerId(this.getOwnerId());
        annotationBuilder.addAllDataSetIds(this.getDataSetIds());
        annotationBuilder.setName(this.getName());
        annotationBuilder.addAllAnnotationIds(this.getAnnotationIds());
        annotationBuilder.setComment(this.getComment());
        annotationBuilder.addAllTags(this.getTags());
        annotationBuilder.addAllAttributes(AttributesUtility.attributeListFromMap(this.getAttributeMap()));
        if (this.getEventMetadata() != null) {
            annotationBuilder.setEventMetadata(EventMetadataDocument.toEventMetadata(this.getEventMetadata()));
        }

        // add content of related datasets
        for (DataSetDocument dataSetDocument : dataSetDocuments) {
            annotationBuilder.addDataSets(dataSetDocument.toDataSet());
        }

        // add calculations content
        if (calculationsDocument != null) {
            annotationBuilder.setCalculations(calculationsDocument.toCalculations());
        }

        return annotationBuilder.build();
    }

    public List<String> diffCreateAnnotationRequest(final CreateAnnotationRequest request) {

        final List<String> diffs = new ArrayList<>();

        // diff ownerId
        if (! Objects.equals(request.getOwnerId(), this.getOwnerId())) {
            final String msg = 
                    "ownerId mismatch: " + this.getOwnerId()
                    + " expected: " + request.getOwnerId();
            diffs.add(msg);
        }

        // diff dataSetIds list
        final Collection<String> dataSetIdsDisjunction = 
                CollectionUtils.disjunction(request.getDataSetIdsList(), this.getDataSetIds());
        if ( ! dataSetIdsDisjunction.isEmpty()) {
            final String msg =
                    "dataSetIds mismatch: " + this.getDataSetIds()
                    + " disjunction: " + dataSetIdsDisjunction;
        }
        
        // diff name
        if ( ! Objects.equals(request.getName(), this.getName())) {
            final String msg = "name mismatch: " + this.getName() + " expected: " + request.getName();
            diffs.add(msg);
        }

        // diff annotationIds list
        final Collection<String> annotationIdsDisjunction =
                CollectionUtils.disjunction(request.getAnnotationIdsList(), this.getAnnotationIds());
        if ( ! annotationIdsDisjunction.isEmpty()) {
            final String msg =
                    "annotationIds mismatch: " + this.getAnnotationIds()
                            + " disjunction: " + annotationIdsDisjunction;
        }

        // diff comment
        if ( ! Objects.equals(request.getComment(), this.getComment())) {
            final String msg = 
                    "comment mismatch: " + this.getComment() + " expected: " + request.getComment();
            diffs.add(msg);
        }

        // diff tags list
        final Collection<String> tagsDisjunction = 
                CollectionUtils.disjunction(request.getTagsList(), this.getTags());
        if ( ! tagsDisjunction.isEmpty()) {
            final String msg =
                    "tags mismatch: " + this.getTags()
                            + " disjunction: " + tagsDisjunction;
            diffs.add(msg);
        }
        
        // diff attributes
        final Collection<Attribute> attributesDisjunction =
                CollectionUtils.disjunction(
                        request.getAttributesList(),
                        AttributesUtility.attributeListFromMap(this.getAttributeMap()));
        if ( ! attributesDisjunction.isEmpty()) {
            final String msg =
                    "attributes mismatch: " + this.getAttributeMap()
                            + " disjunction: " + attributesDisjunction;
            diffs.add(msg);
        }

        // diff eventMetadata
        if (this.getEventMetadata() != null) {
            final EventMetadata thisEventMetadata =
                    EventMetadataDocument.toEventMetadata(this.getEventMetadata());
            if (!Objects.equals(request.getEventMetadata(), thisEventMetadata)) {
                final String msg =
                        "eventMetadata mismatch: " + thisEventMetadata
                                + " expected: " + request.getEventMetadata();
                diffs.add(msg);
            }
        }

        return diffs;
    }

}
