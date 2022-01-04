// Copyright (c) 2021 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
)

const (
	defaultTimeout = 5 * time.Second

	bufferInitialCapacity = 2048

	maxActiveRequests = 2048
)

var (
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

type Options struct{}

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
		go s.processWriteMessage(msg, stream)
	}
}

func (s *server) processWriteMessage(msg *msgflatbuf.Message, stream msgflatbuf.MessageWriter_WriteMessageServer) {
	defer func() { <-s.activeRequestSem }()
	buf := getBuffer()
	defer returnBuffer(buf)

	if msg.ValueType() != msgflatbuf.MessageValueCounterWithMetadatas {
		panic("@tallen not implemented yet..")
	}

	b := msgflatbuf.GetBuilder()
	defer func() { msgflatbuf.ReturnBuilder(b) }()

	msgflatbuf.AckStart(b)
	msgflatbuf.AckAddId(b, msg.Id())
	msgflatbuf.AckAddShard(b, msg.Shard())
	msgflatbuf.AckAddSentAtNanos(b, msg.SentAtNanos())
	offset := msgflatbuf.AckEnd(b)
	b.Finish(offset)

	err := stream.Send(b)
	if err != nil {
		panic(err.Error())
	}
}
