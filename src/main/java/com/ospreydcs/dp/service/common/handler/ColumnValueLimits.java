package com.ospreydcs.dp.service.common.handler;

/**
 * Shared platform limits for column <i>value</i> content, enforced by every save path that stores
 * DataFrame columns: ingestion (IngestionValidationUtility) and the annotation save-calculations
 * validation (AnnotationValidationUtility, #248 Phase 4). The limits are one contract, so they
 * live in one place — a payload ingestion would reject must not be storable through
 * saveAnnotation. ColumnMetadataValidationUtility is the same pattern for the metadata limits;
 * MAX_STRING_LENGTH is shared with it because the 256-char bound applies to metadata strings and
 * string column values alike.
 */
public class ColumnValueLimits {

    /** Maximum length of a string column value (also the metadata string bound). */
    public static final int MAX_STRING_LENGTH = ColumnMetadataValidationUtility.MAX_STRING_LENGTH;

    /** Maximum per-sample element count for an array column (product of its dims). */
    public static final int MAX_ARRAY_ELEMENT_COUNT = 10_000_000;

    /** Maximum size of a single image value in an image column. */
    public static final int MAX_IMAGE_SIZE_BYTES = 50_000_000;  // 50MB

    /** Maximum size of a single struct value in a struct column. */
    public static final int MAX_STRUCT_SIZE_BYTES = 1_000_000;   // 1MB
}
