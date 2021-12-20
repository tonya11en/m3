package grpc

import (
	"fmt"
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
			return flatbuffers.NewBuilder(FLATBUFFER_INITIAL_SIZE_BYTES)
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
	fmt.Println("@tallen making new server.. registering")

	// Create the gRPC server.
	opts := []grpc.ServerOption{
		grpc.CustomCodec(flatbuffers.FlatbuffersCodec{}),
	}
	s := server{
		grpcServer:       grpc.NewServer(opts...),
		aggregator:       aggregator,
		activeRequestSem: make(chan struct{}, MAX_INFLIGHT_REQUESTS),
	}

	flatbuffer.RegisterAggregatorServer(s.grpcServer, &s)
	fmt.Println("@tallen done registering")

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
	fmt.Println("@tallen listen and serving")
	var err error
	s.listener, err = net.Listen("tcp", "localhost:13371")
	if err != nil {
		fmt.Println("@tallen error trying to listen", err.Error())
		return err
	}

	return s.Serve(s.listener)
}

func (s *server) Serve(l net.Listener) error {
	fmt.Println("@tallen serving..")

	return s.grpcServer.Serve(l)
}

func (s *server) Close() {
	if s.listener != nil {
		s.listener.Close()
	}
	s.listener = nil
}

func (s *server) WriteUntimedCounter(req *flatbuffer.WriteUntimedCounterRequest, stream flatbuffer.Aggregator_WriteUntimedCounterServer) error {
	//	fmt.Println("@tallen in the server!")

	b := getBuilder()
	defer returnBuilder(b)

	var id []byte
	var errString string
	var err error

	id, err = s.handleWriteUntimedCounter(req)
	if err != nil {
		errString = err.Error()
	}
	idOffset := b.CreateByteString(id)
	errStringOffset := b.CreateByteString([]byte(errString))

	flatbuffer.WriteUntimedCounterReplyStart(b)
	flatbuffer.WriteUntimedCounterReplyAddId(b, idOffset)
	flatbuffer.WriteUntimedCounterReplyAddError(b, errStringOffset)
	offset := flatbuffer.WriteUntimedCounterReplyEnd(b)
	b.Finish(offset)

	//	fmt.Println("@tallen sending reply from server!")
	return stream.Send(b)
}

func (s *server) WriteUntimedBatchTimer(*flatbuffer.WriteUntimedBatchTimerRequest, flatbuffer.Aggregator_WriteUntimedBatchTimerServer) error {
	//return s.handleWriteUntimedBatchCounter(ctx, req)
	return nil
}

func (s *server) WriteUntimedGauge(*flatbuffer.WriteUntimedGaugeRequest, flatbuffer.Aggregator_WriteUntimedGaugeServer) error {
	return nil
}

func (s *server) WriteTimed(*flatbuffer.WriteTimedRequest, flatbuffer.Aggregator_WriteTimedServer) error {
	return nil
}

func (s *server) WritePassthrough(*flatbuffer.WritePassthroughRequest, flatbuffer.Aggregator_WritePassthroughServer) error {
	return nil
}

func (s *server) WriteTimedWithStagedMetadatas(*flatbuffer.WriteTimedWithStagedMetadatasRequest, flatbuffer.Aggregator_WriteTimedWithStagedMetadatasServer) error {
	return nil
}
