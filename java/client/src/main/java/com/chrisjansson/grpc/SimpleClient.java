package com.chrisjansson.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
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

        SampleServiceGrpc.SampleServiceStub stub = SampleServiceGrpc.newStub(channel);

        log.info("Calling RPC");

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
