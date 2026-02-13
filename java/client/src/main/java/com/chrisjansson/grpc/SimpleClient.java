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

        HealthGrpc.HealthStub healthStub = HealthGrpc.newStub(channel);

        log.info("Calling Watch RPC on health service");

        HealthCheckRequest healthRequest = HealthCheckRequest.newBuilder()
                .setService("")  // Empty string checks overall server health
                .build();

        healthStub.watch(healthRequest, new StreamObserver<HealthCheckResponse>() {
            @Override
            public void onNext(HealthCheckResponse response) {
                log.info("Health status update: " + response.getStatus());
            }

            @Override
            public void onError(Throwable t) {
                log.severe("Error in health watch: " + t.getMessage());
            }

            @Override
            public void onCompleted() {
                log.info("Health watch stream completed");
            }
        });

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

        Thread.sleep(Long.MAX_VALUE);
    }
}
