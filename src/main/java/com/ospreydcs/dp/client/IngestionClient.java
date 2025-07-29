package com.ospreydcs.dp.client;

import com.ospreydcs.dp.grpc.v1.ingestion.DpIngestionServiceGrpc;
import com.ospreydcs.dp.grpc.v1.ingestion.RegisterProviderRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.RegisterProviderResponse;
import com.ospreydcs.dp.service.common.protobuf.AttributesUtility;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class IngestionClient extends ServiceApiClientBase {

    public static class RegisterProviderRequestParams {

        public final String name;
        public final String description;
        public final List<String> tags;
        public final Map<String, String> attributes;

        public RegisterProviderRequestParams(String name, Map<String, String> attributes) {
            this.name = name;
            this.description = null;
            this.tags = null;
            this.attributes = attributes;
        }

        public RegisterProviderRequestParams(
                String name,
                String description,
                List<String> tags,
                Map<String, String> attributes
        ) {
            this.name = name;
            this.description = description;
            this.tags = tags;
            this.attributes = attributes;
        }
    }

    public static class RegisterProviderResponseObserver implements StreamObserver<RegisterProviderResponse> {

        // instance variables
        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<RegisterProviderResponse> responseList = Collections.synchronizedList(new ArrayList<>());

        public void await() {
            try {
                finishLatch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                final String errorMsg = "InterruptedException waiting for finishLatch";
                System.err.println(errorMsg);
                isError.set(true);
                errorMessageList.add(errorMsg);
            }
        }

        public boolean isError() { return isError.get(); }
        public String getErrorMessage() {
            if (!errorMessageList.isEmpty()) {
                return errorMessageList.get(0);
            } else {
                return "";
            }
        }

        public List<RegisterProviderResponse> getResponseList() {
            return responseList;
        }

        @Override
        public void onNext(RegisterProviderResponse response) {

            // handle response in separate thread to better simulate out of process grpc,
            // otherwise response is handled in same thread as service handler that sent it
            new Thread(() -> {

                // flag error if already received a response
                if (!responseList.isEmpty()) {
                    final String errorMsg = "onNext received more than one response";
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);

                } else {
                    responseList.add(response);
                    finishLatch.countDown();
                }
            }).start();

        }

        @Override
        public void onError(Throwable t) {
            // handle response in separate thread to better simulate out of process grpc,
            // otherwise response is handled in same thread as service handler that sent it
            new Thread(() -> {
                final Status status = Status.fromThrowable(t);
                final String errorMsg = "onError: " + status;
                System.err.println(errorMsg);
                isError.set(true);
                errorMessageList.add(errorMsg);
                finishLatch.countDown();
            }).start();
        }

        @Override
        public void onCompleted() {
        }
    }

    public IngestionClient(ManagedChannel channel) {
        super(channel);
    }

    public static RegisterProviderRequest buildRegisterProviderRequest(RegisterProviderRequestParams params) {

        RegisterProviderRequest.Builder builder = RegisterProviderRequest.newBuilder();

        if (params.name != null) {
            builder.setProviderName(params.name);
        }

        if (params.description != null) {
            builder.setDescription(params.description);
        }

        if (params.tags != null) {
            builder.addAllTags(params.tags);
        }

        if (params.attributes != null) {
            builder.addAllAttributes(AttributesUtility.attributeListFromMap(params.attributes));
        }

        return builder.build();
    }

    public RegisterProviderResponse sendRegsiterProvider(
            RegisterProviderRequestParams params
    ) {

        // build request
        final RegisterProviderRequest request = buildRegisterProviderRequest(params);

//        // send API request
//        final RegisterProviderResponse response = sendRegsiterProvider(request);

        final DpIngestionServiceGrpc.DpIngestionServiceStub asyncStub =
                DpIngestionServiceGrpc.newStub(channel);

        final RegisterProviderResponseObserver responseObserver =
                new RegisterProviderResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.registerProvider(request, responseObserver);
        }).start();

        responseObserver.await();

//        if (responseObserver.isError()) {
//            fail("responseObserver error: " + responseObserver.getErrorMessage());
//        }
//
        return responseObserver.getResponseList().get(0);
    }

//    public String registerProvider(
//            RegisterProviderUtility.RegisterProviderRequestParams params
//    ) {
//        // build request
//        final RegisterProviderRequest request = RegisterProviderUtility.buildRegisterProviderRequest(params);
//
//        // send API request
//        final RegisterProviderResponse response = sendRegsiterProvider(request);
//
//        // verify exceptional response
//        if (expectExceptionalResponse) {
//            assertTrue(response.hasExceptionalResult());
//            final ExceptionalResult exceptionalResult = response.getExceptionalResult();
//            assertEquals(expectedExceptionStatus, exceptionalResult.getExceptionalResultStatus());
//            assertTrue(exceptionalResult.getMessage().contains(expectedExceptionMessage));
//            return null;
//        }
//
//        // verify registration result
//        assertTrue(response.hasRegistrationResult());
//        final RegisterProviderResponse.RegistrationResult registrationResult = response.getRegistrationResult();
//        assertEquals(params.name, registrationResult.getProviderName());
//        assertEquals(expectedIsNew, registrationResult.getIsNewProvider());
//        final String providerId = registrationResult.getProviderId();
//
//        // verify ProviderDocument from database
//        final ProviderDocument providerDocument = mongoClient.findProvider(providerId);
//        assertEquals(params.name, providerDocument.getName());
//        if (params.description != null) {
//            assertEquals(params.description, providerDocument.getDescription());
//        } else {
//            assertEquals("", providerDocument.getDescription());
//        }
//        if (params.tags != null) {
//            assertEquals(params.tags, providerDocument.getTags());
//        } else {
//            assertTrue(providerDocument.getTags() == null);
//        }
//        if (params.attributes != null) {
//            assertEquals(params.attributes, providerDocument.getAttributes());
//        } else {
//            assertTrue(providerDocument.getAttributes() == null);
//        }
//        assertNotNull(providerDocument.getCreatedAt());
//        assertNotNull(providerDocument.getUpdatedAt());
//
//        // return id of ProviderDocument
//        return providerId;
//    }
}
