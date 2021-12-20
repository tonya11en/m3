package client

import (
	"context"
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
			return &builder
		},
	}
)

type gRPCClient struct {
	aggClient flatbuffer.AggregatorClient

	// Various streams.
}

func NewGRPCClient(opts Options) (Client, error) {
	err := opts.Validate()
	if err != nil {
		return nil, err
	}

	_ = opts.GRPCOptions()
	// TODO validate

	dopts := make([]grpc.DialOption, 0)
	// TODO: hardcoding server address
	conn, err := grpc.Dial("localhost:13370", dopts...)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	fbac := flatbuffer.NewAggregatorClient(conn)
	gclient := gRPCClient{
		aggClient: fbac,
	}
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
	c.aggClient.WriteUntimedCounter(context.TODO())
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
	// todo
	return nil
}
