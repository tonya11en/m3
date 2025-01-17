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

package writer

import (
	"context"
	"fmt"
	"sync"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/msg/generated/msgflatbuf"
	"github.com/m3db/m3/src/msg/producer"
)

const (
	// The amount of memory pre-allocated for a flatbuffer builder upon creation. This is a starting
	// point and the builder will expand as needed.
	defaultFlatbufSize = 2048

	streamQueueSize = 64
)

// Keeps a mapping from shard to a set of stream writers.
//
// TODO: long-term it may make more sense to send to a single shard address and load balance across
// some set of hosts rather than have write channels.
type grpcShardWriter struct {
	ctx    context.Context
	cancel context.CancelFunc

	// If true, messages for each shard will be replicated to all responsible instances. Otherwise,
	// message processing burden is shared.
	replicatedTopic bool

	// Per-shard channels for writes that gRPC stream writers read from.
	msgWriteMtx     sync.RWMutex
	shardMsgBrokers []*resourceBroker

	ackStream chan *metadata

	streamsMtx    sync.Mutex
	activeStreams map[string]*grpcStreamWriter
}

func newGrpcShardWriter(numShards uint32, replicated bool) shardWriter {
	ctx, cancel := context.WithCancel(context.Background())

	// Create the write channels with buffers for higher throughput.
	shardMsgBrokers := make([]*resourceBroker, numShards)
	for idx := range shardMsgBrokers {
		broker := newResourceBroker(ctx)
		shardMsgBrokers[idx] = broker
	}

	return &grpcShardWriter{
		ctx:             ctx,
		cancel:          cancel,
		shardMsgBrokers: shardMsgBrokers,
		ackStream:       make(chan *metadata, streamQueueSize),
		activeStreams:   make(map[string](*grpcStreamWriter)),
		replicatedTopic: replicated,
	}
}

func (gw *grpcShardWriter) Write(rm *producer.RefCountedMessage) {
	// Just grab a reference to the channel with the read lock so that we don't starve out any thread
	// trying to grab the writer lock.
	gw.msgWriteMtx.RLock()
	broker := gw.shardMsgBrokers[rm.Shard()]
	gw.msgWriteMtx.RUnlock()

	// @tallen this is for debugging
	bcopy := make([]byte, 0, len(rm.Bytes()))
	bcopy = append(bcopy, rm.Bytes()...)
	fmt.Printf("@tallen write debug %+v\n", bcopy)
	builder := msgflatbuf.GetBuilder()
	offset := builder.CreateByteVector(bcopy)

	msgflatbuf.MessageStart(builder)
	msgflatbuf.MessageAddShard(builder, uint64(rm.Message.Shard()))
	msgflatbuf.MessageAddSentAtNanos(builder, uint64(time.Now().Nanosecond()))
	msgflatbuf.MessageAddMsgValue(builder, offset)
	finOffset := msgflatbuf.MessageEnd(builder)
	builder.Finish(finOffset)

	if gw.replicatedTopic {
		broker.Publish(builder)
	} else {
		broker.Select(builder)
	}
}

func (gw *grpcShardWriter) UpdateInstances(instances []placement.Instance, cws map[string]consumerWriter) {
	fmt.Printf("@tallen updating instances: %+v\n", instances)
	newActiveStreams := make(map[string]*grpcStreamWriter, len(instances))

	gw.streamsMtx.Lock()
	defer gw.streamsMtx.Unlock()

	for _, instance := range instances {
		address := instance.Endpoint()
		_, ok := gw.activeStreams[address]
		if ok {
			newActiveStreams[address] = gw.activeStreams[address]
			delete(gw.activeStreams, address)
			continue
		}

		msgChan := make(chan *flatbuffers.Builder, streamQueueSize)
		sw, err := newGRPCStreamWriter(gw.ctx, address, msgChan, gw.ackStream)
		if err != nil {
			// todo handle
			panic(err.Error())
		}

		sw.Init()
		newActiveStreams[address] = sw

		for _, shard := range instance.Shards().AllIDs() {
			fmt.Printf("@tallen adding shard %d to instance %s\n", shard, address)
			gw.shardMsgBrokers[shard] = newResourceBroker(gw.ctx)
			gw.shardMsgBrokers[shard].Subscribe(msgChan)
		}
	}

	// Whatever is not caried over to newActiveStremas should be deleted.
	for _, w := range gw.activeStreams {
		w.Close()
	}

	gw.activeStreams = newActiveStreams
}

func (gw *grpcShardWriter) SetMessageTTLNanos(value int64) {

}

func (gw *grpcShardWriter) Close() {
	gw.cancel()
}

func (gw *grpcShardWriter) QueueSize() int {
	return -1
}
