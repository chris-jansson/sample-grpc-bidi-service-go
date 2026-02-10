package main

import (
	"context"
	"flag"
	"io"
	"log"

	pb "github.com/chris-jansson/sample-grpc-bidi-service-go/generated/sample"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

var serverAddr = flag.String("addr", "localhost:50051", "The server address in the format of host:port")

func openHealthStream(conn *grpc.ClientConn, healthRpcContext context.Context) {
	client := healthpb.NewHealthClient(conn)
	req := &healthpb.HealthCheckRequest{Service: ""}

	stream, err := client.Watch(healthRpcContext, req)
	if err != nil {
		log.Fatalf("Failed to open health stream: %v", err)
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			log.Println("Server closed health stream")
			break
		}
		if err != nil {
			log.Fatalf("Error receiving health status: %v", err)
		}

		log.Printf("Received health status: %s", resp.Status.String())
	}
}

func openStream(client pb.SampleServiceClient) {
	stream, err := client.ProcessMessage(context.Background())
	if err != nil {
		log.Fatalf("Failed to open stream: %v", err)
	}

	// Start receive goroutine
	waitc := make(chan struct{})
	go func() {
		for {
			in, err := stream.Recv()
			if err == io.EOF {
				log.Println("Server closed the stream")
				// close(waitc)
				return
			}
			if err != nil {
				log.Fatalf("Stream terminated unexpectedly")
			}

			log.Printf("Received message: %v", in.Payload)
		}
	}()

	err = stream.Send(&pb.Request{Payload: "foo"})
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}

	stream.CloseSend()

	// Block main thread until the wait channel is closed
	<-waitc
}

func main() {
	flag.Parse()

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.NewClient(*serverAddr, opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	healthRpcContext, _ := context.WithCancel(context.Background())
	go openHealthStream(conn, healthRpcContext)

	// Connection state listener goroutine
	go func() {
		state := conn.GetState()

		for {
			log.Printf("Connection state changed to %v", state.String())

			// if state == connectivity.Idle {
			// 	log.Println("Connection state is IDLE, canceling health stream")
			// 	cancel()
			// 	return
			// }

			conn.WaitForStateChange(context.Background(), state)
			state = conn.GetState()
		}
	}()

	client := pb.NewSampleServiceClient(conn)
	openStream(client)
}
