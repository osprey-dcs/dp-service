package com.ospreydcs.dp.service.common.bson.annotation;

import com.ospreydcs.dp.grpc.v1.annotation.SaveAnnotationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.Annotation;
import com.ospreydcs.dp.grpc.v1.common.Attribute;
import com.ospreydcs.dp.service.common.bson.DpBsonDocumentBase;
import com.ospreydcs.dp.service.common.protobuf.AttributesUtility;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;

import java.util.*;

public class AnnotationDocument extends DpBsonDocumentBase {

    // instance variables
    private ObjectId id;
    private String ownerId;
    private List<String> dataSetIds;
    private String name;
    private List<String> annotationIds;
    private String description;
    private String calculationsId;
    private String modifiedBy;

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

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getCalculationsId() {
        return calculationsId;
    }

    public void setCalculationsId(String calculationsId) {
        this.calculationsId = calculationsId;
    }

    public String getModifiedBy() {
        return modifiedBy;
    }

    public void setModifiedBy(String modifiedBy) {
        this.modifiedBy = modifiedBy;
    }

    public static AnnotationDocument fromSaveAnnotationRequest(
            final SaveAnnotationRequest request,
            String calculationsDocumentId
    ) {
        final AnnotationDocument document = new AnnotationDocument();

        // set request fields in document
        document.setOwnerId(request.getOwnerId());
        document.setDataSetIds(request.getDataSetIdsList());
        document.setName(request.getName());
        document.setAnnotationIds(request.getAnnotationIdsList());
        document.setDescription(request.getDescription());

        if (!request.getModifiedBy().isBlank()) {
            document.setModifiedBy(request.getModifiedBy());
        }

        // only set tags if specified in request; normalized per the house convention (lowercase,
        // deduplicated, sorted) as of #248 Phase 2 -- the v2 schema migration normalizes stored tags
        if (request.getTagsCount() > 0) {
            document.setTags(normalizedTags(request.getTagsList()));
        }

        // only set attributes if specified in request
        if (request.getAttributesCount() > 0) {
            final Map<String, String> attributeMap =
                    AttributesUtility.attributeMapFromList(request.getAttributesList());
            document.setAttributes(attributeMap);
        }

        if (calculationsDocumentId != null) {
            document.setCalculationsId(calculationsDocumentId);
        }

        return document;
    }

    /**
     * Builds the protobuf Annotation for this document.
     *
     * <p>Returns references, not embedded content: dataSetIds and calculationsId are populated and the
     * dataSets / calculations bodies are not.  The Annotation message no longer carries a dataSets
     * field at all (dp-grpc #132), and queryAnnotations() deliberately leaves calculations empty so
     * that listing annotations does not drag their full column sets along.  Callers fetch content
     * with queryDataSets() over the gathered ids, or getCalculations().
     *
     * <p>calculationsId doubles as the presence indicator: empty means the Annotation has no
     * calculations, non-empty with an empty calculations field means the content was not fetched by
     * this method.
     */
    public Annotation toAnnotation() {

        Annotation.Builder annotationBuilder = Annotation.newBuilder();

        annotationBuilder.setId(this.getId().toString());
        annotationBuilder.setOwnerId(this.getOwnerId());
        annotationBuilder.addAllDataSetIds(this.getDataSetIds());
        annotationBuilder.setName(this.getName());
        annotationBuilder.addAllAnnotationIds(this.getAnnotationIds());

        // description is optional; a document saved without one leaves the proto field at its default
        if (this.getDescription() != null) {
            annotationBuilder.setDescription(this.getDescription());
        }

        // only set tags if specified in document
        if (this.getTags() != null) {
            annotationBuilder.addAllTags(this.getTags());
        }

        // only set attributes if specified in document
        if (this.getAttributes() != null) {
            annotationBuilder.addAllAttributes(AttributesUtility.attributeListFromMap(this.getAttributes()));
        }

        // reference only; getCalculations() / getAnnotation() serve the content
        if (this.getCalculationsId() != null) {
            annotationBuilder.setCalculationsId(this.getCalculationsId());
        }

        if (this.getModifiedBy() != null) {
            annotationBuilder.setModifiedBy(this.getModifiedBy());
        }

        if (this.getCreatedAt() != null) {
            annotationBuilder.setCreatedTime(TimestampUtility.getTimestampFromInstant(this.getCreatedAt()));
        }

        if (this.getUpdatedAt() != null) {
            annotationBuilder.setUpdatedTime(TimestampUtility.getTimestampFromInstant(this.getUpdatedAt()));
        }

        return annotationBuilder.build();
    }

    public List<String> diffSaveAnnotationRequest(final SaveAnnotationRequest request) {

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
            diffs.add(msg);
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
            diffs.add(msg);
        }

        // diff description
        if ( ! Objects.equals(request.getDescription(), this.getDescription())) {
            final String msg =
                    "description mismatch: " + this.getDescription()
                            + " expected: " + request.getDescription();
            diffs.add(msg);
        }

        // diff modifiedBy (blank in request is stored as null)
        final String requestModifiedBy = request.getModifiedBy().isBlank() ? null : request.getModifiedBy();
        if ( ! Objects.equals(requestModifiedBy, this.getModifiedBy())) {
            final String msg =
                    "modifiedBy mismatch: " + this.getModifiedBy()
                            + " expected: " + request.getModifiedBy();
            diffs.add(msg);
        }

        // diff tags list against the normalized form the save path stores
        if (this.getTags() != null) {
            final Collection<String> tagsDisjunction =
                    CollectionUtils.disjunction(normalizedTags(request.getTagsList()), this.getTags());
            if (!tagsDisjunction.isEmpty()) {
                final String msg =
                        "tags mismatch: " + this.getTags()
                                + " disjunction: " + tagsDisjunction;
                diffs.add(msg);
            }
        } else {
            if (request.getTagsCount() > 0) {
                final String msg = "tags mismatch: null expected: " + request.getTagsList();
                diffs.add(msg);
            }
        }
        
        // diff attributes
        if (this.getAttributes() != null) {
            final Collection<Attribute> attributesDisjunction =
                    CollectionUtils.disjunction(
                            request.getAttributesList(),
                            AttributesUtility.attributeListFromMap(this.getAttributes()));
            if (!attributesDisjunction.isEmpty()) {
                final String msg =
                        "attributes mismatch: " + this.getAttributes()
                                + " disjunction: " + attributesDisjunction;
                diffs.add(msg);
            }
        } else {
            if (request.getAttributesCount() > 0) {
                final String msg = "attributes mismatch: null expected: " + request.getAttributesList();
                diffs.add(msg);
            }
        }

        return diffs;
    }

}
