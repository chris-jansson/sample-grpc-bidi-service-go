package main

import (
	"context"
	"flag"
	"io"
	"log"
	"os"
	"time"

	pb "github.com/chris-jansson/sample-grpc-bidi-service-go/generated/sample"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var serverAddr = flag.String("addr", "localhost:50051", "The server address in the format of host:port")

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

	// Load service config from JSON file
	configBytes, err := os.ReadFile("service_config.json")
	if err != nil {
		log.Fatalf("Failed to read service config: %v", err)
	}
	serviceConfig := string(configBytes)

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	opts = append(opts, grpc.WithDefaultServiceConfig(serviceConfig))

	conn, err := grpc.NewClient(*serverAddr, opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	// Connection state listener goroutine
	go func() {
		state := conn.GetState()

		for {
			log.Printf("Connection state changed to %v", state.String())

			conn.WaitForStateChange(context.Background(), state)
			state = conn.GetState()
		}
	}()

	client := pb.NewSampleServiceClient(conn)

	// Wait 3 seconds before opening stream. Server will still be NOT_SERVING for a couple more seconds
	// log.Println("Waiting 3 seconds before opening stream...")
	time.Sleep(3 * time.Second)
	log.Println("Opening stream")

	openStream(client)
}
