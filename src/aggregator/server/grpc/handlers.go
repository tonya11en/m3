package grpc

import (
	"context"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/m3db/m3/src/aggregator/generated/flatbuffer"
)

// Abstracts away the various gRPC streams that the handler will deliver builders to.
type clientStream interface {
	Send(*flatbuffers.Builder) error
}

func (s *server) handleWriteUntimedCounter(
	ctx context.Context,
	req *flatbuffer.WriteUntimedCounterRequest) error {

	builder := s.getBuilder()
	union := s.getMessageUnion()

	// todo
	return nil
}

func (s *server) handleWriteUntimedBatchCounter(
	ctx context.Context,
	req *flatbuffer.WriteUntimedBatchTimerRequest) (*flatbuffers.Builder, error) {

	// todo
	return nil, nil
}

func (s *server) handleWriteUntimedGauge(
	ctx context.Context,
	req *flatbuffer.WriteUntimedGaugeRequest) (*flatbuffers.Builder, error) {

	// todo
	return nil, nil
}

func (s *server) handleWriteTimed(
	ctx context.Context,
	req *flatbuffer.WriteTimedRequest) (*flatbuffers.Builder, error) {

	// todo
	return nil, nil
}

func (s *server) handleWritePassthrough(
	ctx context.Context,
	req *flatbuffer.WritePassthroughRequest) (*flatbuffers.Builder, error) {

	// todo
	return nil, nil
}

func (s *server) handleWriteTimedWithStagedMetadatas(
	ctx context.Context,
	req *flatbuffer.WriteTimedWithStagedMetadatasRequest) (*flatbuffers.Builder, error) {

	// todo
	return nil, nil
}
