package client

import (
	"context"
	"fmt"
	"sync"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/m3db/m3/src/aggregator/generated/flatbuffer"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/policy"
	"google.golang.org/grpc"
)

var (
	builderPool = sync.Pool{
		New: func() interface{} {
			// TODO don't hardcode
			builder := flatbuffers.NewBuilder(2048)
			return builder
		},
	}
)

func getBuilder() *flatbuffers.Builder {
	return builderPool.Get().(*flatbuffers.Builder)
}

// Resets and returns a builder to the pool that has no further use.
func returnBuilder(b *flatbuffers.Builder) {
	b.Reset()
	builderPool.Put(b)
}

type gRPCClient struct {
	aggClient flatbuffer.AggregatorClient
	conn      *grpc.ClientConn
}

func NewGRPCClient( /* todo args */ ) (Client, error) {
	fmt.Println("@tallen making grpc client. dialing")
	dopts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithCodec(flatbuffers.FlatbuffersCodec{}),
	}

	// TODO: hardcoding server address
	conn, err := grpc.Dial("localhost:13371", dopts...)
	if err != nil {
		return nil, err
	}
	fmt.Println("@tallen done dialing. making new agg client")

	fbac := flatbuffer.NewAggregatorClient(conn)
	gclient := gRPCClient{
		aggClient: fbac,
		conn:      conn,
	}
	fmt.Println("@tallen done making agg client")
	// TODO more

	return &gclient, nil
}

func (c *gRPCClient) Init() error {
	// todo
	// Spin off sender.
	return nil
}

func (c *gRPCClient) spin() {

}

func (c *gRPCClient) WriteUntimedCounter(
	counter unaggregated.Counter, metadatas metadata.StagedMetadatas) error {

	//	fmt.Println("@tallen calling from client!")

	b := getBuilder()
	defer returnBuilder(b)

	offset := counter.ToFlatbuffer(b)

	flatbuffer.WriteUntimedCounterRequestStart(b)
	flatbuffer.WriteUntimedCounterRequestAddCounter(b, offset)
	offset = flatbuffer.WriteUntimedCounterRequestEnd(b)
	b.Finish(offset)

	stream, err := c.aggClient.WriteUntimedCounter(context.Background(), b)
	if err != nil {
		return err
	}

	_, err = stream.Recv()
	if err != nil {
		return err
	}

	return nil
}

func (c *gRPCClient) WriteUntimedBatchTimer(
	batchTimer unaggregated.BatchTimer, metadatas metadata.StagedMetadatas) error {
	// todo
	return nil
}

func (c *gRPCClient) WriteUntimedGauge(
	gauge unaggregated.Gauge, metadatas metadata.StagedMetadatas) error {
	// todo
	return nil
}

func (c *gRPCClient) WriteTimed(
	metric aggregated.Metric, metadata metadata.TimedMetadata) error {
	// todo
	return nil
}

func (c *gRPCClient) WritePassthrough(
	metric aggregated.Metric, storagePolicy policy.StoragePolicy) error {
	// todo
	return nil
}

func (c *gRPCClient) WriteTimedWithStagedMetadatas(
	metric aggregated.Metric, metadatas metadata.StagedMetadatas) error {
	return nil
}

func (c *gRPCClient) Flush() error {
	// todo
	return nil
}

func (c *gRPCClient) Close() error {
	c.conn.Close()
	return nil
}
