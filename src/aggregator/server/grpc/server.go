package grpc

import (
	"context"
	"io"
	"log"
	"net"
	"sync"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/aggregator/generated/flatbuffer"
	"github.com/m3db/m3/src/metrics/encoding"
	xserver "github.com/m3db/m3/src/x/server"
	"google.golang.org/grpc"
)

const (
	MAX_INFLIGHT_REQUESTS       = 300
	DEFAULT_TIMEOUT             = 5 * time.Second
	FLATBUFFER_INITIAL_BUF_SIZE = 4096
)

type server struct {
	flatbuffer.AggregatorServer
	grpcServer *grpc.Server
	listener   net.Listener
	aggregator aggregator.Aggregator

	// We want to reuse the flatbuffer builders to minimize the allocation of buffers. Before
	// returning objects to the pool, they must be reset. This is done via the helper functions.
	builderPool      sync.Pool
	messageUnionPool sync.Pool
}

// TODO: add options
// Returns a new gRPC aggregator server.
func NewServer(address string, aggregator aggregator.Aggregator) (xserver.Server, error) {
	// todo

	// Create the gRPC server.
	var opts []grpc.ServerOption
	s := server{
		grpcServer: grpc.NewServer(opts...),
		aggregator: aggregator,
		builderPool: sync.Pool{
			New: func() interface{} {
				builder := flatbuffers.NewBuilder(FLATBUFFER_INITIAL_BUF_SIZE)
				return &builder
			},
		},
		messageUnionPool: sync.Pool{
			New: func() interface{} {
				return &encoding.UnaggregatedMessageUnion{}
			},
		},
	}

	flatbuffer.RegisterAggregatorServer(s.grpcServer, &s)

	return &s, nil
}

// Fetches a flatbuffer builder from the pool if one is available, otherwise a new one will be
// allocated and returned. The builders returned are already reset.
func (s *server) getBuilder() *flatbuffers.Builder {
	return s.builderPool.Get().(*flatbuffers.Builder)
}

// Resets and returns a builder to the pool that has no further use.
func (s *server) returnBuilder(b *flatbuffers.Builder) {
	b.Reset()
	s.builderPool.Put(b)
}

func (s *server) getMessageUnion() *encoding.UnaggregatedMessageUnion {
	return s.messageUnionPool.Get().(*encoding.UnaggregatedMessageUnion)
}

func (s *server) returnMessageUnion(mu *encoding.UnaggregatedMessageUnion) {
	// Rather than overwrite the values in each field, we'll simply set the type to unknown.
	mu.Type = encoding.UnknownMessageType
	s.messageUnionPool.Put(mu)
}

func (s *server) ListenAndServe() error {
	//todo
	var err error
	s.listener, err = net.Listen("tcp", "localhost:13370")
	if err != nil {
		return err
	}

	return s.Serve(s.listener)
}

func (s *server) Serve(l net.Listener) error {
	return s.grpcServer.Serve(l)
}

func (s *server) Close() {
	if s.listener != nil {
		s.listener.Close()
	}
	s.listener = nil
}

func (s *server) WriteUntimedCounter(stream flatbuffer.Aggregator_WriteUntimedCounterServer) error {
	for {
		req, err := stream.Recv()
		// TODO: configurable timeout
		ctx, cancelFunc := context.WithTimeout(stream.Context(), DEFAULT_TIMEOUT)
		defer cancelFunc()

		if err == io.EOF {
			// Wait for the active requests to finish up.
			return nil
		} else if err != nil {
			// TODO: gracefully handle in-flight requests?
			return err
		}

		log.Println("@tallen received a message. not writing out string since idk how now")

		// TODO: maybe spin off another goroutine.
		err = s.handleWriteUntimedCounter(ctx, req)
		if err != nil {
			return err
		}
	}
}

func (s *server) WriteUntimedBatchTimer(stream flatbuffer.Aggregator_WriteUntimedBatchTimerServer) error {
	for {
		req, err := stream.Recv()
		// TODO: configurable timeout
		ctx, cancelFunc := context.WithTimeout(stream.Context(), DEFAULT_TIMEOUT)
		defer cancelFunc()

		if err == io.EOF {
			// Wait for the active requests to finish up.
			return nil
		} else if err != nil {
			// TODO: gracefully handle in-flight requests?
			return err
		}

		log.Println("@tallen received a message. not writing out string since idk how now")

		// TODO: maybe spin off another goroutine.
		builder, err := s.handleWriteUntimedBatchCounter(ctx, req)
		if err != nil {
			return err
		}

		err = stream.Send(builder)
		if err != nil {
			return err
		}
	}
}

func (s *server) WriteUntimedGauge(stream flatbuffer.Aggregator_WriteUntimedGaugeServer) error {
	for {
		req, err := stream.Recv()
		// TODO: configurable timeout
		ctx, cancelFunc := context.WithTimeout(stream.Context(), DEFAULT_TIMEOUT)
		defer cancelFunc()

		if err == io.EOF {
			// Wait for the active requests to finish up.
			return nil
		} else if err != nil {
			// TODO: gracefully handle in-flight requests?
			return err
		}

		log.Println("@tallen received a message. not writing out string since idk how now")

		// TODO: maybe spin off another goroutine.
		builder, err := s.handleWriteUntimedGauge(ctx, req)
		if err != nil {
			return err
		}

		err = stream.Send(builder)
		if err != nil {
			return err
		}
	}
}

func (s *server) WriteTimed(stream flatbuffer.Aggregator_WriteTimedServer) error {
	for {
		req, err := stream.Recv()
		// TODO: configurable timeout
		ctx, cancelFunc := context.WithTimeout(stream.Context(), DEFAULT_TIMEOUT)
		defer cancelFunc()

		if err == io.EOF {
			// Wait for the active requests to finish up.
			return nil
		} else if err != nil {
			// TODO: gracefully handle in-flight requests?
			return err
		}

		log.Println("@tallen received a message. not writing out string since idk how now")

		// TODO: maybe spin off another goroutine.
		builder, err := s.handleWriteTimed(ctx, req)
		if err != nil {
			return err
		}

		err = stream.Send(builder)
		if err != nil {
			return err
		}
	}
}

func (s *server) WritePassthrough(stream flatbuffer.Aggregator_WritePassthroughServer) error {
	for {
		req, err := stream.Recv()
		// TODO: configurable timeout
		ctx, cancelFunc := context.WithTimeout(stream.Context(), DEFAULT_TIMEOUT)
		defer cancelFunc()

		if err == io.EOF {
			// Wait for the active requests to finish up.
			return nil
		} else if err != nil {
			// TODO: gracefully handle in-flight requests?
			return err
		}

		log.Println("@tallen received a message. not writing out string since idk how now")

		// TODO: maybe spin off another goroutine.
		builder, err := s.handleWritePassthrough(ctx, req)
		if err != nil {
			return err
		}

		err = stream.Send(builder)
		if err != nil {
			return err
		}
	}
}

func (s *server) WriteTimedWithStagedMetadatas(stream flatbuffer.Aggregator_WriteTimedWithStagedMetadatasServer) error {
	for {
		req, err := stream.Recv()
		// TODO: configurable timeout
		ctx, cancelFunc := context.WithTimeout(stream.Context(), DEFAULT_TIMEOUT)
		defer cancelFunc()

		if err == io.EOF {
			// Wait for the active requests to finish up.
			return nil
		} else if err != nil {
			// TODO: gracefully handle in-flight requests?
			return err
		}

		log.Println("@tallen received a message. not writing out string since idk how now")

		// TODO: maybe spin off another goroutine.
		builder, err := s.handleWriteTimedWithStagedMetadatas(ctx, req)
		if err != nil {
			return err
		}

		err = stream.Send(builder)
		if err != nil {
			return err
		}
	}
}
