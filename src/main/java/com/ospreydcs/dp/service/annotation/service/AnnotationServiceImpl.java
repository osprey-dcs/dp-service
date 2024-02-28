package com.ospreydcs.dp.service.annotation.service;

import com.ospreydcs.dp.grpc.v1.annotation.DpAnnotationServiceGrpc;
import com.ospreydcs.dp.service.annotation.handler.interfaces.AnnotationHandlerInterface;

public class AnnotationServiceImpl extends DpAnnotationServiceGrpc.DpAnnotationServiceImplBase {

    public boolean init(AnnotationHandlerInterface handler) {
        return true;
    }

    public boolean fini() {
        return true;
    }
}
