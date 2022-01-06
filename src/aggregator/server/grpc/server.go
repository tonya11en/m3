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
	"net"
	"sync"

	flatbuffers "github.com/google/flatbuffers/go"
	"google.golang.org/grpc"

	"github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/metrics/encoding"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/msg/consumer"
	"github.com/m3db/m3/src/msg/generated/msgflatbuf"
)

const (
	bufferInitialCapacity = 2048
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

	unionTablePool = sync.Pool{
		New: func() interface{} {
			return new(flatbuffers.Table)
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

	address    string
	grpcServer *grpc.Server
	listener   net.Listener
	aggregator aggregator.Aggregator
}

type Options struct{}

// TODO: add options
// Returns a new gRPC aggregator server.
func NewServer(address string, agg aggregator.Aggregator) (*consumer.GrpcConsumerServer, error) {
	fn := func(msg *msgflatbuf.Message) chan *consumer.AckInfo {
		return processFn(msg, agg)
	}

	return consumer.NewGRPCConsumerServer(address, fn)
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

func processFn(msg *msgflatbuf.Message, agg aggregator.Aggregator) chan *consumer.AckInfo {
	buf := getBuffer()
	defer returnBuffer(buf)

	if msg.ValueType() != msgflatbuf.MessageValueCounterWithMetadatas {
		panic("@tallen not implemented yet..")
	}

	union := getMessageUnion()
	defer returnMessageUnion(union)

	unionTable := unionTablePool.Get().(*flatbuffers.Table)
	defer unionTablePool.Put(unionTable)
	if !msg.Value(unionTable) {
		// todo don't do this
		panic("@tallen message has no value!")
	}

	switch msg.ValueType() {
	case msgflatbuf.MessageValueCounterWithMetadatas:
		var counterFB msgflatbuf.CounterWithMetadatas
		counterFB.Init(unionTable.Bytes, unionTable.Pos)
		err := union.CounterWithMetadatas.FromFlatbuffer(&counterFB)
		if err != nil {
			panic(err)
		}
		u := union.CounterWithMetadatas.ToUnion()
		err = agg.AddUntimed(u, union.CounterWithMetadatas.StagedMetadatas)
		if err != nil {
			panic(err)
		}
	case msgflatbuf.MessageValueMetric:
		fallthrough
	case msgflatbuf.MessageValueChunkedMetric:
		fallthrough
	case msgflatbuf.MessageValueAggregatedMetricWithStoragePolicy:
		fallthrough
	case msgflatbuf.MessageValueChunkedMetricWithStoragePolicy:
		fallthrough
	case msgflatbuf.MessageValueForwardedMetricWithMetadata:
		fallthrough
	case msgflatbuf.MessageValueTimedMetricWithMetadata:
		fallthrough
	case msgflatbuf.MessageValueTimedMetricWithMetadatas:
		fallthrough
	case msgflatbuf.MessageValuePassthroughMetricWithMetadata:
		fallthrough
	case msgflatbuf.MessageValueGaugeWithMetadatas:
		fallthrough
	case msgflatbuf.MessageValueBatchTimerWithMetadatas:
		panic("@tallen unsupported!!")
	}

	c := make(chan *consumer.AckInfo, 1)
	c <- &consumer.AckInfo{
		ID:          msg.Id(),
		ShardID:     msg.Shard(),
		SentAtNanos: msg.SentAtNanos(),
	}

	return c
}
