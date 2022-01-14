// Copyright (c) 2022 Uber Technologies, Inc.
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

package server

/*

import (
	"fmt"
	"io"
	"math/rand"
	"net"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"google.golang.org/grpc"

	"github.com/m3db/m3/src/msg/consumer"
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
	processor        consumer.MessageProcessor
	activeRequestSem chan struct{}

	// @tallen rip this out
	srvID uint64
}

type AckInfo struct {
	ID          uint64
	ShardID     uint64
	SentAtNanos uint64
}

// TODO: add options
// Returns a new gRPC aggregator server.
func NewGRPCConsumerServer(address string, processor consumer.MessageProcessor) (*GrpcConsumerServer, error) {
	// todo
	fmt.Println("@tallen making new server.. registering")

	// Create the gRPC server.
	opts := []grpc.ServerOption{
		grpc.CustomCodec(flatbuffers.FlatbuffersCodec{}),
	}
	s := GrpcConsumerServer{
		address:          address,
		grpcServer:       grpc.NewServer(opts...),
		processor:        processor,
		activeRequestSem: make(chan struct{}, maxActiveRequests),
		srvID:            rand.Uint64(),
	}

	msgflatbuf.RegisterMessageWriterServer(s.grpcServer, &s)
	fmt.Println("@tallen done registering")

	return &s, nil
}

func (s *GrpcConsumerServer) ListenAndServe() error {
	fmt.Println("@tallen listen and serving ", s.srvID)
	var err error
	s.listener, err = net.Listen("tcp", s.address)
	if err != nil {
		fmt.Println("@tallen error trying to listen: ", err.Error())
		return err
	}

	return s.Serve(s.listener)
}

func (s *GrpcConsumerServer) Serve(l net.Listener) error {
	fmt.Println("@tallen serving.. ", s.srvID)

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
		fmt.Println("@tallen srv WriteMessage loop in base grpc server... ", s.srvID)
		msg, err := stream.Recv()
		if err == io.EOF {
			fmt.Println("@tallen srv WriteMessage base: EOF received ", s.srvID)
			return nil
		}
		if err != nil {
			fmt.Println("@tallen srv WriteMessage base: ", s.srvID, " : ", err.Error())
			return err
		}

		s.activeRequestSem <- struct{}{}
		go func() {
			defer func() { <-s.activeRequestSem }()

			ackFn := func() {
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

			msgBytes := make([]byte, msg.MsgValueLength())
			for i := 0; i < msg.MsgValueLength(); i++ {
				msgBytes[i] = byte(msg.MsgValue(i))
			}
			consumerMsg := newGRPCConsumerMessage(msgBytes, msg.Shard(), msg.SentAtNanos(), ackFn)

			fmt.Println("@tallen srv WriteMessage calling processor... ", s.srvID)
			s.processor.Process(consumerMsg)
			fmt.Printf("@tallen srv WriteMessage done calling processor... ", s.srvID)
		}()
	}
}

type GRPCConsumerMessage struct {
	bytes       []byte
	shardID     uint64
	sentAtNanos uint64
	ackFn       func()
}

func newGRPCConsumerMessage(bytes []byte, shardID uint64, sentAtNanos uint64, ackFn func()) *GRPCConsumerMessage {
	return &GRPCConsumerMessage{
		bytes:       bytes,
		shardID:     shardID,
		sentAtNanos: sentAtNanos,
		ackFn:       ackFn,
	}
}

// Bytes returns the bytes.
func (m *GRPCConsumerMessage) Bytes() []byte {
	return m.bytes
}

// Ack acks the message.
func (m *GRPCConsumerMessage) Ack() {
	m.ackFn()
}

// ShardID returns shard ID of the Message.
func (m *GRPCConsumerMessage) ShardID() uint64 {
	return m.shardID
}

// SentAtNanos returns when the producer sent the Message.
func (m *GRPCConsumerMessage) SentAtNanos() uint64 {
	return m.sentAtNanos
}
*/
