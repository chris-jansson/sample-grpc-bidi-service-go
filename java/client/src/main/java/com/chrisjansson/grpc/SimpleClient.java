package com.chrisjansson.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import lombok.extern.java.Log;
import samplepb.SampleServiceGrpc;
import samplepb.SampleProto.Request;
import samplepb.SampleProto.Response;

@Log
public class SimpleClient {

    public static void main(String[] args) throws InterruptedException {
        String target = "localhost:50051";
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .build();

        callHealthRpc(channel);
        callSampleServiceRpc(channel);

        Thread.sleep(Long.MAX_VALUE);
    }

    private static void callHealthRpc(ManagedChannel channel) {
        HealthGrpc.HealthStub healthStub = HealthGrpc.newStub(channel);

        log.info("Calling /Watch RPC");

        HealthCheckRequest healthRequest = HealthCheckRequest.newBuilder()
                .setService("")
                .build();

        healthStub.watch(healthRequest, new StreamObserver<HealthCheckResponse>() {
            @Override
            public void onNext(HealthCheckResponse response) {
                log.info("Received health update: " + response.getStatus());
            }

            @Override
            public void onError(Throwable t) {
                log.severe("Received error on health stream: " + t.getMessage());
            }

            @Override
            public void onCompleted() {
                log.info("Server completed health stream");
            }
        });
    }

    private static void callSampleServiceRpc(ManagedChannel channel) {
        SampleServiceGrpc.SampleServiceStub stub = SampleServiceGrpc.newStub(channel);

        log.info("Calling ProcessMessage RPC");

        StreamObserver<Request> requestObserver = stub.processMessage(new StreamObserver<Response>() {
            @Override
            public void onNext(Response value) {
                log.info("Received message from server");
            }

            @Override
            public void onError(Throwable t) {
                log.severe("Received error from server: " + t.getMessage());
            }

            @Override
            public void onCompleted() {
                log.info("Server closed stream");
            }
        });

        log.info("Sending message to server");

        Request req = Request.newBuilder().setPayload("foo").build();
        requestObserver.onNext(req);

        log.info("Closing stream");

        requestObserver.onCompleted();
    }
}
