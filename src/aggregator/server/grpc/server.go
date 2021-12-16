package grpc

import (
	"github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/aggregator/generated/flatbuffer"
	xserver "github.com/m3db/m3/src/x/server"
)

type grpcAggregatorServer struct {
	flatbuffer.AggregatorServer
}

// TODO: add options
func NewServer(address string, aggregator aggregator.Aggregator) (xserver.Server, error) {
	return nil, nil
}

func (srv *grpcAggregatorServer) WriteUntimedCounter(flatbuffer.Aggregator_WriteUntimedCounterServer) error {
	// todo
	return nil
}

func (srv *grpcAggregatorServer) WriteUntimedBatchTimer(flatbuffer.Aggregator_WriteUntimedBatchTimerServer) error {
	// todo
	return nil
}

func (srv *grpcAggregatorServer) WriteUntimedGauge(flatbuffer.Aggregator_WriteUntimedGaugeServer) error {
	// todo
	return nil
}

func (srv *grpcAggregatorServer) WriteTimed(flatbuffer.Aggregator_WriteTimedServer) error {
	// todo
	return nil
}

func (srv *grpcAggregatorServer) WritePassthrough(flatbuffer.Aggregator_WritePassthroughServer) error {
	// todo
	return nil
}

func (srv *grpcAggregatorServer) WriteTimedWithStagedMetadatas(flatbuffer.Aggregator_WriteTimedWithStagedMetadatasServer) error {
	// todo
	return nil
}
