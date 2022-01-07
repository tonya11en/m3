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

package client

import (
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/m3db/m3/src/aggregator/sharding"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/msg/generated/msgflatbuf"
	"github.com/m3db/m3/src/msg/producer"
	"github.com/m3db/m3/src/x/clock"
)

type grpcClient struct {
	producer  producer.Producer
	numShards uint32
	nowFn     clock.NowFn
	shardFn   sharding.ShardFn
}

func NewGRPCClient(opts Options) (Client, error) {
	fmt.Println("@tallen creating new grpc client (agg)")
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	grpcOpts := opts.M3MsgOptions()
	if err := grpcOpts.Validate(); err != nil {
		return nil, err
	}

	producer := grpcOpts.Producer()
	if err := producer.Init(); err != nil {
		return nil, err
	}

	gclient := grpcClient{
		producer:  producer,
		numShards: producer.NumShards(),
		nowFn:     opts.ClockOptions().NowFn(),
		shardFn:   opts.ShardFn(),
	}

	return &gclient, nil
}

// Init just satisfies Client interface, M3Msg client does not need explicit initialization.
func (c *grpcClient) Init() error {
	fmt.Println("@tallen initializing grpc client...")
	return nil
}

// WriteUntimedCounter writes untimed counter metrics.
func (c *grpcClient) WriteUntimedCounter(
	counter unaggregated.Counter,
	metadatas metadata.StagedMetadatas,
) error {
	//	callStart := c.nowFn()
	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    counter.ToUnion(),
			metadatas: metadatas,
		},
	}
	msg := newFlatbufMessage()

	shard := c.shardFn(payload.untimed.metric.ID, c.numShards)
	err := msg.Encode(shard, payload)
	if err != nil {
		panic(err.Error())
	}

	return c.write(msg)
}

// WriteUntimedBatchTimer writes untimed batch timer metrics.
func (c *grpcClient) WriteUntimedBatchTimer(
	batchTimer unaggregated.BatchTimer,
	metadatas metadata.StagedMetadatas,
) error {
	//	callStart := c.nowFn()
	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    batchTimer.ToUnion(),
			metadatas: metadatas,
		},
	}
	msg := newFlatbufMessage()

	shard := c.shardFn(payload.untimed.metric.ID, c.numShards)
	err := msg.Encode(shard, payload)
	if err != nil {
		panic(err.Error())
	}

	return c.write(msg)
}

// WriteUntimedGauge writes untimed gauge metrics.
func (c *grpcClient) WriteUntimedGauge(
	gauge unaggregated.Gauge,
	metadatas metadata.StagedMetadatas,
) error {
	//	callStart := c.nowFn()
	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    gauge.ToUnion(),
			metadatas: metadatas,
		},
	}

	msg := newFlatbufMessage()

	shard := c.shardFn(payload.untimed.metric.ID, c.numShards)
	err := msg.Encode(shard, payload)
	if err != nil {
		panic(err.Error())
	}

	return c.write(msg)
}

// WriteTimed writes timed metrics.
func (c *grpcClient) WriteTimed(
	metric aggregated.Metric,
	metadata metadata.TimedMetadata,
) error {
	panic("@tallen unimplemented")
	return nil
}

// WritePassthrough writes passthrough metrics.
func (c *grpcClient) WritePassthrough(
	metric aggregated.Metric,
	storagePolicy policy.StoragePolicy,
) error {
	panic("@tallen unimplemented")
	return nil
}

// WriteTimedWithStagedMetadatas writes timed metrics with staged metadatas.
func (c *grpcClient) WriteTimedWithStagedMetadatas(
	metric aggregated.Metric,
	metadatas metadata.StagedMetadatas,
) error {
	panic("@tallen unimplemented")
	return nil
}

// WriteForwarded writes forwarded metrics.
func (c *grpcClient) WriteForwarded(
	metric aggregated.ForwardedMetric,
	metadata metadata.ForwardMetadata,
) error {
	panic("@tallen unimplemented")
	return nil
}

func (c *grpcClient) write(msg *flatbufMessage) error {
	return c.producer.Produce(msg)
}

func (c *grpcClient) Close() error {
	//todo
	return nil
}

func (c *grpcClient) Flush() error {
	return nil
}

type flatbufMessage struct {
	shard   uint32
	buf     []byte
	builder *flatbuffers.Builder
}

// Ensure this message type implements m3msg producer message interface.
var _ producer.Message = (*flatbufMessage)(nil)

func newFlatbufMessage() *flatbufMessage {
	return &flatbufMessage{
		builder: msgflatbuf.GetBuilder(),
	}
}

func (m *flatbufMessage) Encode(
	shard uint32,
	payload payloadUnion) error {

	m.shard = shard

	switch payload.payloadType {
	case untimedType:
		return m.encodeUntimed(shard, &payload.untimed)
	case forwardedType:
		//todo
		fallthrough
	case timedType:
		//todo
		fallthrough
	case timedWithStagedMetadatasType:
		//todo
		fallthrough
	default:
		// @tallen
		panic("wat 2")
		return fmt.Errorf("unrecognized payload type: %v",
			payload.payloadType)
	}
}

// TODO @tallen: don't copy paste so much
func (m *flatbufMessage) encodeUntimed(shard uint32, untimed *untimedPayload) error {
	switch untimed.metric.Type {
	case metric.CounterType:
		// @tallen: Right now this is all we'll support for the demo...
		m.encodeUntimedCounter(shard, untimed)
	case metric.GaugeType:
		m.encodeUntimedGauge(shard, untimed)
	case metric.TimerType:
		m.encodeUntimedBatchTimer(shard, untimed)
	default:
		// @tallen I don't want to panic, just ignore metrics I haven't implemented yet.
		return nil
		//return fmt.Errorf("unrecognized metric type: %v", untimed.metric.Type)
	}

	return nil
}

func (m *flatbufMessage) encodeUntimedCounter(shard uint32, untimed *untimedPayload) error {
	var err error
	numMetadatas := len(untimed.metadatas)
	metadatasOffsets := make([]flatbuffers.UOffsetT, numMetadatas)
	for i, sm := range untimed.metadatas {
		metadatasOffsets[i], err = msgflatbuf.MakeStagedMetadataFlatbuf(&sm, m.builder)
		if err != nil {
			return err
		}
	}

	annotationOffset := m.builder.CreateByteVector(untimed.metric.Annotation)
	idOffset := m.builder.CreateByteVector(untimed.metric.ID)

	msgflatbuf.CounterWithMetadatasStart(m.builder)
	msgflatbuf.CounterWithMetadatasAddAnnotation(m.builder, annotationOffset)
	msgflatbuf.CounterWithMetadatasAddId(m.builder, idOffset)
	msgflatbuf.CounterWithMetadatasAddValue(m.builder, untimed.metric.CounterVal)
	valOffset := msgflatbuf.CounterWithMetadatasEnd(m.builder)

	msgflatbuf.MessageStart(m.builder)
	// omitting message ID..
	msgflatbuf.MessageAddValue(m.builder, valOffset)
	msgflatbuf.MessageAddValueType(m.builder, msgflatbuf.MessageValueCounterWithMetadatas)

	// this is probably wrong
	msgflatbuf.MessageAddSentAtNanos(m.builder, uint64(untimed.metric.ClientTimeNanos))
	msgflatbuf.MessageAddShard(m.builder, uint64(shard))
	fin := msgflatbuf.MessageEnd(m.builder)

	m.builder.Finish(fin)
	return nil
}

func (m *flatbufMessage) encodeUntimedGauge(shard uint32, untimed *untimedPayload) error {
	var err error
	numMetadatas := len(untimed.metadatas)
	metadatasOffsets := make([]flatbuffers.UOffsetT, numMetadatas)
	for i, sm := range untimed.metadatas {
		metadatasOffsets[i], err = msgflatbuf.MakeStagedMetadataFlatbuf(&sm, m.builder)
		if err != nil {
			return err
		}
	}

	annotationOffset := m.builder.CreateByteVector(untimed.metric.Annotation)
	idOffset := m.builder.CreateByteVector(untimed.metric.ID)

	msgflatbuf.GaugeWithMetadatasStart(m.builder)
	msgflatbuf.GaugeWithMetadatasAddAnnotation(m.builder, annotationOffset)
	msgflatbuf.GaugeWithMetadatasAddId(m.builder, idOffset)
	msgflatbuf.GaugeWithMetadatasAddValue(m.builder, untimed.metric.GaugeVal)
	valOffset := msgflatbuf.GaugeWithMetadatasEnd(m.builder)

	msgflatbuf.MessageStart(m.builder)
	// omitting message ID..
	msgflatbuf.MessageAddValue(m.builder, valOffset)
	msgflatbuf.MessageAddValueType(m.builder, msgflatbuf.MessageValueCounterWithMetadatas)

	// this is probably wrong
	msgflatbuf.MessageAddSentAtNanos(m.builder, uint64(untimed.metric.ClientTimeNanos))
	msgflatbuf.MessageAddShard(m.builder, uint64(shard))
	fin := msgflatbuf.MessageEnd(m.builder)

	m.builder.Finish(fin)
	return nil
}

func (m *flatbufMessage) encodeUntimedBatchTimer(shard uint32, untimed *untimedPayload) error {
	var err error
	numMetadatas := len(untimed.metadatas)
	metadatasOffsets := make([]flatbuffers.UOffsetT, numMetadatas)
	for i, sm := range untimed.metadatas {
		metadatasOffsets[i], err = msgflatbuf.MakeStagedMetadataFlatbuf(&sm, m.builder)
		if err != nil {
			return err
		}
	}

	annotationOffset := m.builder.CreateByteVector(untimed.metric.Annotation)
	idOffset := m.builder.CreateByteVector(untimed.metric.ID)

	btv := untimed.metric.BatchTimerVal
	fmt.Printf("@tallen metric (for batch timer) output = %+v\n", untimed.metric)
	msgflatbuf.BatchTimerWithMetadatasStartValuesVector(m.builder, len(btv))
	for i := len(btv) - 1; i >= 0; i-- {
		m.builder.PrependFloat64(btv[i])
	}
	batchTimerValuesOffset := m.builder.EndVector(len(btv))

	msgflatbuf.BatchTimerWithMetadatasAddValues(m.builder, batchTimerValuesOffset)
	msgflatbuf.BatchTimerWithMetadatasStart(m.builder)
	msgflatbuf.BatchTimerWithMetadatasAddAnnotation(m.builder, annotationOffset)
	msgflatbuf.BatchTimerWithMetadatasAddId(m.builder, idOffset)

	valOffset := msgflatbuf.GaugeWithMetadatasEnd(m.builder)

	msgflatbuf.MessageStart(m.builder)
	// omitting message ID..
	msgflatbuf.MessageAddValue(m.builder, valOffset)
	msgflatbuf.MessageAddValueType(m.builder, msgflatbuf.MessageValueCounterWithMetadatas)

	// this is probably wrong
	msgflatbuf.MessageAddSentAtNanos(m.builder, uint64(untimed.metric.ClientTimeNanos))
	msgflatbuf.MessageAddShard(m.builder, uint64(shard))
	fin := msgflatbuf.MessageEnd(m.builder)

	m.builder.Finish(fin)
	return nil
}

func (m *flatbufMessage) Shard() uint32 {
	return m.shard
}

func (m *flatbufMessage) Bytes() []byte {
	return m.buf
}

func (m *flatbufMessage) Builder() *flatbuffers.Builder {
	return m.builder
}

func (m *flatbufMessage) Size() int {
	return len(m.buf)
}

func (m *flatbufMessage) Finalize(reason producer.FinalizeReason) {
}
