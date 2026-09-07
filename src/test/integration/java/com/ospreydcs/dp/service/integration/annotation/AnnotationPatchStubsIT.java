package com.ospreydcs.dp.service.integration.annotation;

import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * The two patch methods are deferred stubs (annotation.proto: "NOT YET IMPLEMENTED"): they answer
 * with the convention RESULT_STATUS_ERROR "not yet implemented" response rather than gRPC's default
 * UNIMPLEMENTED status.
 */
public class AnnotationPatchStubsIT extends AnnotationIntegrationTestIntermediate {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testPatchDataSetStub() {
        annotationServiceWrapper.sendAndVerifyPatchDataSetStub(new ObjectId().toHexString());
    }

    @Test
    public void testPatchAnnotationStub() {
        annotationServiceWrapper.sendAndVerifyPatchAnnotationStub(new ObjectId().toHexString());
    }
}
