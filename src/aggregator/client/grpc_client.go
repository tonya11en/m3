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
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	m3msgOpts := opts.M3MsgOptions()
	if err := m3msgOpts.Validate(); err != nil {
		return nil, err
	}

	producer := m3msgOpts.Producer()
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
	msg.Encode(shard, payload)
	return c.write(msg)
}

// WriteUntimedBatchTimer writes untimed batch timer metrics.
func (c *grpcClient) WriteUntimedBatchTimer(
	batchTimer unaggregated.BatchTimer,
	metadatas metadata.StagedMetadatas,
) error {
	return nil
}

// WriteUntimedGauge writes untimed gauge metrics.
func (c *grpcClient) WriteUntimedGauge(
	gauge unaggregated.Gauge,
	metadatas metadata.StagedMetadatas,
) error {
	return nil
}

// WriteTimed writes timed metrics.
func (c *grpcClient) WriteTimed(
	metric aggregated.Metric,
	metadata metadata.TimedMetadata,
) error {
	return nil
}

// WritePassthrough writes passthrough metrics.
func (c *grpcClient) WritePassthrough(
	metric aggregated.Metric,
	storagePolicy policy.StoragePolicy,
) error {
	return nil
}

// WriteTimedWithStagedMetadatas writes timed metrics with staged metadatas.
func (c *grpcClient) WriteTimedWithStagedMetadatas(
	metric aggregated.Metric,
	metadatas metadata.StagedMetadatas,
) error {
	return nil
}

// WriteForwarded writes forwarded metrics.
func (c *grpcClient) WriteForwarded(
	metric aggregated.ForwardedMetric,
	metadata metadata.ForwardMetadata,
) error {
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

// Ensure message implements m3msg producer message interface.
var _ producer.Message = (*message)(nil)

type flatbufMessage struct {
	shard uint32

	buf     []byte
	builder *flatbuffers.Builder
}

func newFlatbufMessage() *flatbufMessage {
	return &flatbufMessage{
		builder: msgflatbuf.GetBuilder(),
	}
}

// Encode encodes a m3msg payload
//nolint:gocyclo,gocritic
func (m *flatbufMessage) Encode(
	shard uint32,
	payload payloadUnion) error {

	var err error
	m.shard = shard

	switch payload.payloadType {
	case untimedType:
		switch payload.untimed.metric.Type {
		case metric.CounterType:
			// @tallen: Right now this is all we'll support for the demo...
			numMetadatas := len(payload.untimed.metadatas)
			metadatasOffsets := make([]flatbuffers.UOffsetT, numMetadatas)
			for i, sm := range payload.untimed.metadatas {
				metadatasOffsets[i], err = msgflatbuf.MakeStagedMetadataFlatbuf(&sm, m.builder)
				if err != nil {
					return err
				}
			}

			annotationOffset := m.builder.CreateByteVector(payload.untimed.metric.Annotation)
			idOffset := m.builder.CreateByteVector(payload.untimed.metric.ID)

			msgflatbuf.CounterWithMetadatasStart(m.builder)
			msgflatbuf.CounterWithMetadatasAddAnnotation(m.builder, annotationOffset)
			msgflatbuf.CounterWithMetadatasAddId(m.builder, idOffset)
			msgflatbuf.CounterWithMetadatasAddValue(m.builder, payload.untimed.metric.CounterVal)
			fin := msgflatbuf.CounterWithMetadatasEnd(m.builder)
			m.builder.Finish(fin)

		case metric.TimerType:
			//todo
			fallthrough
		case metric.GaugeType:
			//todo
			fallthrough
		default:
			return fmt.Errorf("unrecognized metric type: %v",
				payload.untimed.metric.Type)
		}
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
		return fmt.Errorf("unrecognized payload type: %v",
			payload.payloadType)
	}

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
	msgflatbuf.ReturnBuilder(m.builder)
}
