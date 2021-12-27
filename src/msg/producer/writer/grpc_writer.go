package writer

import (
	"context"
	"sync"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/m3db/m3/src/cluster/placement"
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

func newGrpcShardWriter(numShards int, replicated bool) shardWriter {
	ctx, cancel := context.WithCancel(context.Background())

	// Create the write channels with buffers for higher throughput.
	shardMsgBrokers := make([]*resourceBroker, numShards)
	for idx := range shardMsgBrokers {
		shardMsgBrokers[idx] = newResourceBroker(ctx)
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

func (gw *grpcShardWriter) startNewStream(addr string, msgChan chan *flatbuffers.Builder, ackStream chan *metadata) {
	streamWriter, err := newGRPCStreamWriter(gw.ctx, addr, msgChan, ackStream)
	if err != nil {
		// TODO @tallen handle properly
		panic(err.Error())
	}
	streamWriter.Init()

	// We'll now track the stream in the map.
	gw.streamsMtx.Lock()
	gw.activeStreams[addr] = streamWriter
	gw.streamsMtx.Unlock()
}

func (gw *grpcShardWriter) Write(rm *producer.RefCountedMessage) {
	// Just grab a reference to the channel with the read lock so that we don't starve out any thread
	// trying to grab the writer lock.
	gw.msgWriteMtx.RLock()
	broker := gw.shardMsgBrokers[rm.Shard()]
	gw.msgWriteMtx.RUnlock()

	if gw.replicatedTopic {
		broker.Publish(rm.Builder())
	} else {
		broker.Select(rm.Builder())
	}
}

func (gw *grpcShardWriter) UpdateInstances(instances []placement.Instance, cws map[string]consumerWriter) {
	newActiveStreams := make(map[string]*grpcStreamWriter, len(instances))

	for _, instance := range instances {
		address := instance.Endpoint()
		_, ok := gw.activeStreams[address]
		if ok {
			newActiveStreams[address] = gw.activeStreams[address]
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
			gw.shardMsgBrokers[shard].Subscribe(msgChan)
		}
	}

	// Whatever is not caried over to newActiveStremas should be deleted.
	for _, w := range gw.activeStreams {
		w.Close()
	}
}

func (gw *grpcShardWriter) SetMessageTTLNanos(value int64) {

}

func (gw *grpcShardWriter) Close() {
	gw.cancel()
}

func (gw *grpcShardWriter) QueueSize() int {
	return 0
}
