package grpc

import (
	"context"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/m3db/m3/src/aggregator/generated/flatbuffer"
	"github.com/m3db/m3/src/metrics/metric"
	xtime "github.com/m3db/m3/src/x/time"
)

// Abstracts away the various gRPC streams that the handler will deliver builders to.
type clientStream interface {
	Send(*flatbuffers.Builder) error
}

func (s *server) handleWriteUntimedCounter(
	req *flatbuffer.WriteUntimedCounterRequest) ([]byte, error) {

	metricUnion := getMetricUnion()
	defer returnMetricUnion(metricUnion)
	messageUnion := getMessageUnion()
	defer returnMessageUnion(messageUnion)
	c := getCounterBuf()
	defer returnCounterBuf(c)

	messageUnion.CounterWithMetadatas.FromFlatbuffer(req.Counter(c))
	metricUnion.Type = metric.CounterType
	metricUnion.ID = c.Id()
	metricUnion.CounterVal = c.Value()
	metricUnion.Annotation = c.Annotation()
	metricUnion.ClientTimeNanos = xtime.UnixNano(c.ClientTimeNanos())

	// TODO: wtf why are there so many copies in that aggregator handler?
	return c.Id(), s.aggregator.AddUntimed(*metricUnion, messageUnion.CounterWithMetadatas.StagedMetadatas)
}

func (s *server) handleWriteUntimedBatchCounter(
	ctx context.Context,
	req *flatbuffer.WriteUntimedBatchTimerRequest) (*flatbuffers.Builder, error) {

	builder := getBuilder()
	defer returnBuilder(builder)
	metricUnion := getMetricUnion()
	defer returnMetricUnion(metricUnion)
	messageUnion := getMessageUnion()
	defer returnMessageUnion(messageUnion)
	bt := getBatchTimerBuf()
	defer returnBatchTimerBuf(bt)

	// todo
	return nil, nil
}

func (s *server) handleWriteUntimedGauge(
	ctx context.Context,
	req *flatbuffer.WriteUntimedGaugeRequest) (*flatbuffers.Builder, error) {

	builder := getBuilder()
	defer returnBuilder(builder)
	metricUnion := getMetricUnion()
	defer returnMetricUnion(metricUnion)
	messageUnion := getMessageUnion()
	defer returnMessageUnion(messageUnion)
	g := getGaugeBuf()
	defer returnGaugeBuf(g)

	// todo
	return nil, nil
}

func (s *server) handleWriteTimed(
	ctx context.Context,
	req *flatbuffer.WriteTimedRequest) (*flatbuffers.Builder, error) {

	builder := getBuilder()
	defer returnBuilder(builder)
	metricUnion := getMetricUnion()
	defer returnMetricUnion(metricUnion)
	messageUnion := getMessageUnion()
	defer returnMessageUnion(messageUnion)
	m := getMetricBuf()
	defer returnMetricBuf(m)

	// todo
	return nil, nil
}

func (s *server) handleWritePassthrough(
	ctx context.Context,
	req *flatbuffer.WritePassthroughRequest) (*flatbuffers.Builder, error) {

	builder := getBuilder()
	defer returnBuilder(builder)
	metricUnion := getMetricUnion()
	defer returnMetricUnion(metricUnion)
	messageUnion := getMessageUnion()
	defer returnMessageUnion(messageUnion)
	m := getMetricBuf()
	defer returnMetricBuf(m)

	// todo
	return nil, nil
}

func (s *server) handleWriteTimedWithStagedMetadatas(
	ctx context.Context,
	req *flatbuffer.WriteTimedWithStagedMetadatasRequest) (*flatbuffers.Builder, error) {

	builder := getBuilder()
	defer returnBuilder(builder)
	metricUnion := getMetricUnion()
	defer returnMetricUnion(metricUnion)
	messageUnion := getMessageUnion()
	defer returnMessageUnion(messageUnion)
	m := getMetricBuf()
	defer returnMetricBuf(m)

	// todo
	return nil, nil
}
