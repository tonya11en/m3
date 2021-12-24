package writer

import (
	"context"
	"sync"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/msg/producer"
)

// Keeps a mapping from shard to a set of stream writers.
//
// TODO: long-term it may make more sense to send to a single shard address and load balance across
// some set of hosts rather than have write channels.
type grpcShardWriter struct {
	ctx context.Context

	// Per-shard channels for writes that gRPC stream writers read from.
	shardWriteChannels []chan *producer.RefCountedMessage

	streamsMtx sync.Mutex
	streams    map[string]context.CancelFunc
}

func newGrpcShardWriter(numShards int) shardWriter {
	// Create the write channels with buffers for higher throughput.
	shardWriteChannels := make([]chan *producer.RefCountedMessage, numShards)
	for idx := range shardWriteChannels {
		shardWriteChannels[idx] = make(chan *producer.RefCountedMessage, 64)
	}

	return &grpcShardWriter{
		shardWriteChannels: shardWriteChannels,
	}
}

func (gw *grpcShardWriter) startNewStream(addr string, messages chan *producer.RefCountedMessage) {
	swCtx, cancel := context.WithCancel(gw.ctx)

	// TODO @tallen add the cancel to the map

	go func() {
		streamWriter, err := newGRPCStreamWriter(swCtx, addr)
		if err != nil {
			// TODO @tallen handle properly
			panic(err.Error())
		}

		for {
			select {
			case <-swCtx.Done():
			case msg := <-messages:
				streamWriter.Init()
				streamWriter.SendOnStream(msg)
			}
		}
	}()

	// We'll now track the stream in the map.
	gw.streamsMtx.Lock()
	gw.streams[addr] = cancel
	gw.streamsMtx.Unlock()
}

func (gw *grpcShardWriter) Write(rm *producer.RefCountedMessage) {
	shard := gw.sh
  shardWriteChannels[rm. // @tallenwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww
}

func (gw *grpcShardWriter) UpdateInstances(instances []placement.Instance, cws map[string]consumerWriter) {
	var (
		newInstancesMap = make(map[string]struct{}, len(instances))
		toBeDeleted     = w.instances
	)
	for _, instance := range instances {
		id := instance.Endpoint()
		newInstancesMap[id] = struct{}{}
		if _, ok := toBeDeleted[id]; ok {
			// Existing instance.
			delete(toBeDeleted, id)
			continue
		}
		// Add the consumer writer to the message writer.
		w.mw.AddConsumerWriter(cws[id])
	}
	for id := range toBeDeleted {
		w.mw.RemoveConsumerWriter(id)
	}
	w.instances = newInstancesMap
}

func (gw *grpcShardWriter) SetMessageTTLNanos(value int64) {

}

func (gw *grpcShardWriter) Close() {

}

func (gw *grpcShardWriter) QueueSize() int {
	return 0
}
