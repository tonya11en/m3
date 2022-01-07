package consumer

import (
	"fmt"
	"io"
	"net"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"google.golang.org/grpc"

	"github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/msg/generated/msgflatbuf"
)

const (
	defaultTimeout = 5 * time.Second

	bufferInitialCapacity = 2048

	maxActiveRequests = 2048
)

type GrpcConsumerServer struct {
	msgflatbuf.MessageWriterServer

	address          string
	grpcServer       *grpc.Server
	listener         net.Listener
	aggregator       aggregator.Aggregator
	activeRequestSem chan struct{}
	processFn        func(*msgflatbuf.Message) chan *AckInfo
}

type AckInfo struct {
	ID          uint64
	ShardID     uint64
	SentAtNanos uint64
}

// TODO: add options
// Returns a new gRPC aggregator server.
func NewGRPCConsumerServer(address string, processMsgFunc func(*msgflatbuf.Message) chan *AckInfo) (*GrpcConsumerServer, error) {
	// todo
	fmt.Println("@tallen making new server.. registering")

	// Create the gRPC server.
	opts := []grpc.ServerOption{
		grpc.CustomCodec(flatbuffers.FlatbuffersCodec{}),
	}
	s := GrpcConsumerServer{
		address:          address,
		grpcServer:       grpc.NewServer(opts...),
		processFn:        processMsgFunc,
		activeRequestSem: make(chan struct{}, maxActiveRequests),
	}

	msgflatbuf.RegisterMessageWriterServer(s.grpcServer, &s)
	fmt.Println("@tallen done registering")

	return &s, nil
}

func (s *GrpcConsumerServer) ListenAndServe() error {
	fmt.Println("@tallen listen and serving")
	var err error
	s.listener, err = net.Listen("tcp", s.address)
	if err != nil {
		fmt.Println("@tallen error trying to listen", err.Error())
		return err
	}

	return s.Serve(s.listener)
}

func (s *GrpcConsumerServer) Serve(l net.Listener) error {
	fmt.Println("@tallen serving..")

	return s.grpcServer.Serve(l)
}

func (s *GrpcConsumerServer) Close() {
	if s.listener != nil {
		s.listener.Close()
	}
	s.listener = nil
}

func (s *GrpcConsumerServer) WriteMessage(stream msgflatbuf.MessageWriter_WriteMessageServer) error {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		// @tallen so it doesn't panic, we'll just drop writes that aren't counters.
		if msg.ValueType() != msgflatbuf.MessageValueCounterWithMetadatas {
			continue
		}

		s.activeRequestSem <- struct{}{}
		go func() {
			defer func() { <-s.activeRequestSem }()

			ainfo := <-s.processFn(msg)

			b := msgflatbuf.GetBuilder()
			defer func() { msgflatbuf.ReturnBuilder(b) }()

			msgflatbuf.AckStart(b)
			msgflatbuf.AckAddId(b, ainfo.ID)
			msgflatbuf.AckAddShard(b, ainfo.ShardID)
			msgflatbuf.AckAddSentAtNanos(b, ainfo.SentAtNanos)
			offset := msgflatbuf.AckEnd(b)
			b.Finish(offset)

			err := stream.Send(b)
			if err != nil {
				panic(err.Error())
			}
		}()
	}
}
