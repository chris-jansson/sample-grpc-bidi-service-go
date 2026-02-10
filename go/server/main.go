package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	pb "github.com/chris-jansson/sample-grpc-bidi-service-go/generated/sample"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

var (
	port             = flag.Int("port", 50051, "The server port")
	streamShutdownCh = make(chan struct{})
)

type sampleServiceServer struct {
	pb.UnimplementedSampleServiceServer
}

func (s *sampleServiceServer) ProcessMessage(stream pb.SampleService_ProcessMessageServer) error {
	log.Println("Client opened stream")

	for {
		_, err := stream.Recv()
		if err == io.EOF {
			log.Println("Client completed sending messages")

			<-streamShutdownCh
			return nil
		}
		if err != nil {
			log.Fatalln("Stream terminated unexpectedly")
			return err
		}

		log.Println("Received message from client")
	}
}

func main() {
	log.Println("Starting gRPC server")

	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	pb.RegisterSampleServiceServer(grpcServer, &sampleServiceServer{})

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve gRPC: %v", err)
		}
	}()

	shutDownAppGracefully(grpcServer, healthServer)
}

func shutDownAppGracefully(grpcServer *grpc.Server, healthServer *health.Server) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()

		select {
		case s := <-sigCh:
			log.Printf("Got signal %v, initiating graceful shutdown", s)
			runShutdownSequence(grpcServer, healthServer)
		}
	}()

	wg.Wait()
}

func runShutdownSequence(grpcServer *grpc.Server, healthServer *health.Server) {
	healthServer.Shutdown()

	grpcShutdownChan := make(chan struct{}, 1)
	go func() {
		log.Println("Gracefully stopping gRPC server. No new connections or RPCs will be accepted, and existing streams will be drained")
		grpcServer.GracefulStop()
		grpcShutdownChan <- struct{}{}
	}()

	time.Sleep(1 * time.Second)

	log.Println("Closing existing stream")
	streamShutdownCh <- struct{}{}

	select {
	case <-grpcShutdownChan:
		log.Println("All streams closed cleanly")
	case <-time.After(1 * time.Second):
		log.Println("Failed to gracefully shut down all streams, forcefully terminating server")
		grpcServer.Stop()
	}
}
