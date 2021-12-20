package grpc

import (
	"context"
	"io"
	"log"
	"net"
	"sync"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"google.golang.org/grpc"

	"github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/aggregator/generated/flatbuffer"
	"github.com/m3db/m3/src/metrics/encoding"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	xserver "github.com/m3db/m3/src/x/server"
)

const (
	MAX_INFLIGHT_REQUESTS         = 1024
	DEFAULT_TIMEOUT               = 5 * time.Second
	FLATBUFFER_INITIAL_SIZE_BYTES = 2048
)

var (
	builderPool = sync.Pool{
		New: func() interface{} {
			builder := flatbuffers.NewBuilder(FLATBUFFER_INITIAL_SIZE_BYTES)
			return &builder
		},
	}

	metricUnionPool = sync.Pool{
		New: func() interface{} {
			return new(unaggregated.MetricUnion)
		},
	}

	messageUnionPool = sync.Pool{
		New: func() interface{} {
			return new(encoding.UnaggregatedMessageUnion)
		},
	}

	counterBufPool = sync.Pool{
		New: func() interface{} {
			return new(flatbuffer.Counter)
		},
	}

	gaugeBufPool = sync.Pool{
		New: func() interface{} {
			return new(flatbuffer.Gauge)
		},
	}

	batchTimerBufPool = sync.Pool{
		New: func() interface{} {
			return new(flatbuffer.BatchTimer)
		},
	}

	metricBufPool = sync.Pool{
		New: func() interface{} {
			return new(flatbuffer.Metric)
		},
	}
)

type server struct {
	flatbuffer.AggregatorServer
	grpcServer       *grpc.Server
	listener         net.Listener
	aggregator       aggregator.Aggregator
	activeRequestSem chan struct{}
}

// TODO: add options
// Returns a new gRPC aggregator server.
func NewServer(address string, aggregator aggregator.Aggregator) (xserver.Server, error) {
	// todo

	// Create the gRPC server.
	var opts []grpc.ServerOption
	s := server{
		grpcServer:       grpc.NewServer(opts...),
		aggregator:       aggregator,
		activeRequestSem: make(chan struct{}, MAX_INFLIGHT_REQUESTS),
	}

	flatbuffer.RegisterAggregatorServer(s.grpcServer, &s)

	return &s, nil
}

// Fetches a flatbuffer builder from the pool if one is available, otherwise a new one will be
// allocated and returned. The builders returned are already reset.
func getBuilder() *flatbuffers.Builder {
	return builderPool.Get().(*flatbuffers.Builder)
}

// Resets and returns a builder to the pool that has no further use.
func returnBuilder(b *flatbuffers.Builder) {
	b.Reset()
	builderPool.Put(b)
}

func getMessageUnion() *encoding.UnaggregatedMessageUnion {
	return messageUnionPool.Get().(*encoding.UnaggregatedMessageUnion)
}

func returnMessageUnion(mu *encoding.UnaggregatedMessageUnion) {
	// Rather than overwrite the values in each field, we'll simply set the type to unknown.
	mu.Type = encoding.UnknownMessageType
	messageUnionPool.Put(mu)
}

func getMetricUnion() *unaggregated.MetricUnion {
	return metricUnionPool.Get().(*unaggregated.MetricUnion)
}

func returnMetricUnion(mu *unaggregated.MetricUnion) {
	mu.Annotation = nil
	mu.ID = nil
	mu.BatchTimerVal = nil
	metricUnionPool.Put(mu)
}

func getGaugeBuf() *flatbuffer.Gauge {
	return gaugeBufPool.Get().(*flatbuffer.Gauge)
}

func returnGaugeBuf(c *flatbuffer.Gauge) {
	if c.MetadatasLength() > 0 {
		// todo
	}
	gaugeBufPool.Put(c)
}

func getBatchTimerBuf() *flatbuffer.BatchTimer {
	return batchTimerBufPool.Get().(*flatbuffer.BatchTimer)
}

func returnBatchTimerBuf(c *flatbuffer.BatchTimer) {
	if c.MetadatasLength() > 0 {
		// todo
	}
	batchTimerBufPool.Put(c)
}

func getMetricBuf() *flatbuffer.Metric {
	return metricBufPool.Get().(*flatbuffer.Metric)
}

func returnMetricBuf(c *flatbuffer.Metric) {
	if c.MetadatasLength() > 0 {
		// todo
	}
	metricBufPool.Put(c)
}

func getCounterBuf() *flatbuffer.Counter {
	return counterBufPool.Get().(*flatbuffer.Counter)
}

func returnCounterBuf(c *flatbuffer.Counter) {
	if c.MetadatasLength() > 0 {
		// todo
	}
	counterBufPool.Put(c)
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
	var wg sync.WaitGroup

	for {
		req, err := stream.Recv()
		// TODO: configurable timeout
		if err == io.EOF {
			wg.Wait()
			return nil
		} else if err != nil {
			// TODO: gracefully handle in-flight requests?
			return err
		}

		s.activeRequestSem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-s.activeRequestSem }()

			b := getBuilder()
			defer returnBuilder(b)

			flatbuffer.WriteUntimedCounterReplyStart(b)

			id, err := s.handleWriteUntimedCounter(req)
			if err != nil {
				offset := b.CreateByteString([]byte(err.Error()))
				flatbuffer.WriteUntimedCounterReplyAddError(b, offset)
			}
			offset := b.CreateByteString(id)
			flatbuffer.WriteUntimedCounterReplyAddId(b, offset)
			flatbuffer.WriteUntimedCounterReplyAddId(b, offset)
			flatbuffer.WriteUntimedCounterReplyEnd(b)

			// TODO: handle this error
			_ = stream.Send(b)
		}()
	}
}

func (s *server) WriteUntimedBatchTimer(stream flatbuffer.Aggregator_WriteUntimedBatchTimerServer) error {
	for {
		req, err := stream.Recv()

		if err == io.EOF {
			// Wait for the active requests to finish up.
			return nil
		} else if err != nil {
			// TODO: gracefully handle in-flight requests?
			return err
		}

		log.Println("@tallen received a message. not writing out string since idk how now")

		// TODO: maybe spin off another goroutine.
		builder, err := s.handleWriteUntimedBatchCounter(stream.Context(), req)
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
