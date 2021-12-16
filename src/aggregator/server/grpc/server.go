package grpc

import (
	"fmt"
	"net"
	"net/http"

	"github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/aggregator/generated/flatbuffer"
	xserver "github.com/m3db/m3/src/x/server"
	"google.golang.org/grpc"
)

type grpcAggregatorServer struct {
	flatbuffer.AggregatorServer
}

type server struct {
	srv grpcAggregatorServer
}

    // ListenAndServe forever listens to new incoming connections and
    // handles data from those connections.
    ListenAndServe() error

    // Serve accepts and handles incoming connections on the listener l forever.
    Serve(l net.Listener) error

    // Close closes the server.
    Close()

// TODO: add options
func NewServer(address string, aggregator aggregator.Aggregator) (xserver.Server, error) {
	return nil, nil
}

func (s *grpcAggregatorServer) ListenAndServe() error {
	// TODO @tallen configure where to listen.
	listener, err := net.Listen("tcp", "localhost:13370")
	if err != nil {
		return err
	}

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	pb.RegisterRouteGuideServer(grpcServer, newServer())
	grpcServer.Serve(lis)

	return s.Serve(listener)
}

func (s *grpcAggregatorServer) Serve(l net.Listener) error {
	/*
	registerHandlers(s.opts.Mux(), s.aggregator)

	// create and register debug handler
	debugWriter, err := xdebug.NewZipWriterWithDefaultSources(
		defaultCPUProfileDuration,
		s.iOpts,
	)
	if err != nil {
		return fmt.Errorf("unable to create debug writer: %v", err)
	}

	if err := debugWriter.RegisterHandler(xdebug.DebugURL, s.opts.Mux()); err != nil {
		return fmt.Errorf("unable to register debug writer endpoint: %v", err)
	}

	server := http.Server{
		Handler:      s.opts.Mux(),
		ReadTimeout:  s.opts.ReadTimeout(),
		WriteTimeout: s.opts.WriteTimeout(),
	}

	s.listener = l
	s.address = l.Addr().String()

	go func() {
		server.Serve(l)
	}()
	*/

	return nil
}

func (s *grpcAggregatorServer) Close() {
	if s.listener != nil {
		s.listener.Close()
	}
	s.listener = nil
}

func (srv *grpcAggregatorServer) WriteUntimedCounter(flatbuffer.Aggregator_WriteUntimedCounterServer) error {
	// todo
	return nil
}

func (srv *grpcAggregatorServer) WriteUntimedBatchTimer(flatbuffer.Aggregator_WriteUntimedBatchTimerServer) error {
	// todo
	return nil
}

func (srv *grpcAggregatorServer) WriteUntimedGauge(flatbuffer.Aggregator_WriteUntimedGaugeServer) error {
	// todo
	return nil
}

func (srv *grpcAggregatorServer) WriteTimed(flatbuffer.Aggregator_WriteTimedServer) error {
	// todo
	return nil
}

func (srv *grpcAggregatorServer) WritePassthrough(flatbuffer.Aggregator_WritePassthroughServer) error {
	// todo
	return nil
}

func (srv *grpcAggregatorServer) WriteTimedWithStagedMetadatas(flatbuffer.Aggregator_WriteTimedWithStagedMetadatasServer) error {
	// todo
	return nil
}
