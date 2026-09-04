package com.ospreydcs.dp.client.result;

public class SaveAnnotationApiResult extends ApiResultBase {

    // instance variables
    public final String annotationId;

    /**
     * Id of the saved Calculations document, null when the request carried no calculations.  This
     * is the addressing key for getCalculations(), CalculationsSpec, and ColumnProvenance links.
     */
    public final String calculationsId;

    public SaveAnnotationApiResult(boolean isError, String errorMessage) {
        super(isError, errorMessage);
        this.annotationId = null;
        this.calculationsId = null;
    }

    public SaveAnnotationApiResult(boolean isError, String errorMessage, ApiResultStatus apiResultStatus) {
        super(isError, errorMessage, apiResultStatus);
        this.annotationId = null;
        this.calculationsId = null;
    }

    public SaveAnnotationApiResult(String annotationId) {
        this(annotationId, null);
    }

    public SaveAnnotationApiResult(String annotationId, String calculationsId) {
        super(false, "");
        this.annotationId = annotationId;
        this.calculationsId = calculationsId;
    }

}
