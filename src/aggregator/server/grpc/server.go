package grpc

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"google.golang.org/grpc"

	"github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/metrics/encoding"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/msg/generated/msgflatbuf"
	"github.com/m3db/m3/src/msg/generated/proto/msgpb"
)

const (
	defaultTimeout = 5 * time.Second

	bufferInitialCapacity = 2048

	maxActiveRequests = 2048
)

var (
	builderPool = sync.Pool{
		New: func() interface{} {
			return flatbuffers.NewBuilder(bufferInitialCapacity)
		},
	}

	messageProtoPool = sync.Pool{
		New: func() interface{} {
			return new(msgpb.Message)
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

	bufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, bufferInitialCapacity)
		},
	}
)

type server struct {
	msgflatbuf.MessageWriterServer

	address          string
	grpcServer       *grpc.Server
	listener         net.Listener
	aggregator       aggregator.Aggregator
	activeRequestSem chan struct{}
}

// TODO: add options
// Returns a new gRPC aggregator server.
func NewServer(address string, aggregator aggregator.Aggregator) (*server, error) {
	// todo
	fmt.Println("@tallen making new server.. registering")

	// Create the gRPC server.
	opts := []grpc.ServerOption{
		grpc.CustomCodec(flatbuffers.FlatbuffersCodec{}),
	}
	s := server{
		address:          address,
		grpcServer:       grpc.NewServer(opts...),
		aggregator:       aggregator,
		activeRequestSem: make(chan struct{}, maxActiveRequests),
	}

	msgflatbuf.RegisterMessageWriterServer(s.grpcServer, &s)
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

func getMessageProto() *msgpb.Message {
	return bufferPool.Get().(*msgpb.Message)
}

func returnMessageProto(m *msgpb.Message) {
	m.Reset()
	bufferPool.Put(m)
}

func getBuffer() []byte {
	return bufferPool.Get().([]byte)
}

func returnBuffer(buf []byte) {
	buf = buf[:0]
	bufferPool.Put(buf)
}

func (s *server) ListenAndServe() error {
	fmt.Println("@tallen listen and serving")
	var err error
	s.listener, err = net.Listen("tcp", s.address)
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

func (s *server) WriteMessage(stream msgflatbuf.MessageWriter_WriteMessageServer) error {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		s.activeRequestSem <- struct{}{}
		go s.processWriteMessage(msg)
	}
}

func (s *server) processWriteMessage(msg *msgflatbuf.Message) {
	defer func() { <-s.activeRequestSem }()
	buf := getBuffer()
	defer returnBuffer(buf)

	for i := 0; i < msg.ValueLength(); i++ {
		buf = append(buf, byte(msg.Value(i)))
	}

	msgProto := getMessageProto()
	defer returnMessageProto(msgProto)

	err := msgProto.Unmarshal(buf)
	if err != nil {
		fmt.Printf("error unmarshaling proto: %s\n", err.Error())
	}
}
