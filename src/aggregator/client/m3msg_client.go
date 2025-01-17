// Copyright (c) 2020 Uber Technologies, Inc.
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
	"runtime"
	"sync"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/uber-go/tally"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/aggregator/sharding"
	"github.com/m3db/m3/src/metrics/generated/proto/metricpb"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/msg/generated/msgflatbuf"
	"github.com/m3db/m3/src/msg/producer"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"
)

var _ AdminClient = (*M3MsgClient)(nil)

// M3MsgClient sends metrics to M3 Aggregator over m3msg.
type M3MsgClient struct {
	m3msg   m3msgClient
	nowFn   clock.NowFn
	shardFn sharding.ShardFn
	metrics m3msgClientMetrics
}

type m3msgClient struct {
	producer    producer.Producer
	numShards   uint32
	messagePool *messagePool
}

// NewM3MsgClient creates a new M3 Aggregator client that uses M3Msg.
func NewM3MsgClient(opts Options) (Client, error) {
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

	msgClient := m3msgClient{
		producer:    producer,
		numShards:   producer.NumShards(),
		messagePool: newMessagePool(),
	}

	var (
		iOpts  = opts.InstrumentOptions()
		logger = iOpts.Logger()
	)

	logger.Info("creating M3MsgClient", zap.Uint32("numShards", msgClient.numShards))

	return &M3MsgClient{
		m3msg:   msgClient,
		nowFn:   opts.ClockOptions().NowFn(),
		shardFn: opts.ShardFn(),
		metrics: newM3msgClientMetrics(iOpts.MetricsScope(), iOpts.TimerOptions()),
	}, nil
}

// Init just satisfies Client interface, M3Msg client does not need explicit initialization.
func (c *M3MsgClient) Init() error {
	return nil
}

// WriteUntimedCounter writes untimed counter metrics.
func (c *M3MsgClient) WriteUntimedCounter(
	counter unaggregated.Counter,
	metadatas metadata.StagedMetadatas,
) error {
	callStart := c.nowFn()
	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    counter.ToUnion(),
			metadatas: metadatas,
		},
	}
	err := c.write(counter.ID, payload)
	c.metrics.writeUntimedCounter.ReportSuccessOrError(err, c.nowFn().Sub(callStart))
	return err
}

// WriteUntimedBatchTimer writes untimed batch timer metrics.
func (c *M3MsgClient) WriteUntimedBatchTimer(
	batchTimer unaggregated.BatchTimer,
	metadatas metadata.StagedMetadatas,
) error {
	callStart := c.nowFn()
	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    batchTimer.ToUnion(),
			metadatas: metadatas,
		},
	}
	err := c.write(batchTimer.ID, payload)
	c.metrics.writeUntimedBatchTimer.ReportSuccessOrError(err, c.nowFn().Sub(callStart))
	return err
}

// WriteUntimedGauge writes untimed gauge metrics.
func (c *M3MsgClient) WriteUntimedGauge(
	gauge unaggregated.Gauge,
	metadatas metadata.StagedMetadatas,
) error {
	callStart := c.nowFn()
	payload := payloadUnion{
		payloadType: untimedType,
		untimed: untimedPayload{
			metric:    gauge.ToUnion(),
			metadatas: metadatas,
		},
	}
	err := c.write(gauge.ID, payload)
	c.metrics.writeUntimedGauge.ReportSuccessOrError(err, c.nowFn().Sub(callStart))
	return err
}

// WriteTimed writes timed metrics.
func (c *M3MsgClient) WriteTimed(
	metric aggregated.Metric,
	metadata metadata.TimedMetadata,
) error {
	callStart := c.nowFn()
	payload := payloadUnion{
		payloadType: timedType,
		timed: timedPayload{
			metric:   metric,
			metadata: metadata,
		},
	}
	err := c.write(metric.ID, payload)
	c.metrics.writeForwarded.ReportSuccessOrError(err, c.nowFn().Sub(callStart))
	return err
}

// WritePassthrough writes passthrough metrics.
func (c *M3MsgClient) WritePassthrough(
	metric aggregated.Metric,
	storagePolicy policy.StoragePolicy,
) error {
	callStart := c.nowFn()
	payload := payloadUnion{
		payloadType: passthroughType,
		passthrough: passthroughPayload{
			metric:        metric,
			storagePolicy: storagePolicy,
		},
	}
	err := c.write(metric.ID, payload)
	c.metrics.writePassthrough.ReportSuccessOrError(err, c.nowFn().Sub(callStart))
	return err
}

// WriteTimedWithStagedMetadatas writes timed metrics with staged metadatas.
func (c *M3MsgClient) WriteTimedWithStagedMetadatas(
	metric aggregated.Metric,
	metadatas metadata.StagedMetadatas,
) error {
	callStart := c.nowFn()
	payload := payloadUnion{
		payloadType: timedWithStagedMetadatasType,
		timedWithStagedMetadatas: timedWithStagedMetadatas{
			metric:    metric,
			metadatas: metadatas,
		},
	}
	err := c.write(metric.ID, payload)
	c.metrics.writeForwarded.ReportSuccessOrError(err, c.nowFn().Sub(callStart))
	return err
}

// WriteForwarded writes forwarded metrics.
func (c *M3MsgClient) WriteForwarded(
	metric aggregated.ForwardedMetric,
	metadata metadata.ForwardMetadata,
) error {
	callStart := c.nowFn()
	payload := payloadUnion{
		payloadType: forwardedType,
		forwarded: forwardedPayload{
			metric:   metric,
			metadata: metadata,
		},
	}
	err := c.write(metric.ID, payload)
	c.metrics.writeForwarded.ReportSuccessOrError(err, c.nowFn().Sub(callStart))
	return err
}

//nolint:gocritic
func (c *M3MsgClient) write(metricID id.RawID, payload payloadUnion) error {
	fmt.Println("@tallen msgmsg write metric ", metricID.String())
	shard := c.shardFn(metricID, c.m3msg.numShards)

	msg := c.m3msg.messagePool.Get()
	if err := msg.Encode(shard, payload); err != nil {
		msg.Finalize(producer.Dropped)
		return err
	}

	if err := c.m3msg.producer.Produce(msg); err != nil {
		msg.Finalize(producer.Dropped)
		return err
	}

	return nil
}

// Flush satisfies Client interface, as M3Msg client does not need explicit flushing.
func (c *M3MsgClient) Flush() error {
	return nil
}

// Close closes the client.
func (c *M3MsgClient) Close() error {
	c.m3msg.producer.Close(producer.WaitForConsumption)
	return nil
}

type m3msgClientMetrics struct {
	writeUntimedCounter    instrument.MethodMetrics
	writeUntimedBatchTimer instrument.MethodMetrics
	writeUntimedGauge      instrument.MethodMetrics
	writePassthrough       instrument.MethodMetrics
	writeForwarded         instrument.MethodMetrics
}

func newM3msgClientMetrics(
	scope tally.Scope,
	opts instrument.TimerOptions,
) m3msgClientMetrics {
	return m3msgClientMetrics{
		writeUntimedCounter:    instrument.NewMethodMetrics(scope, "writeUntimedCounter", opts),
		writeUntimedBatchTimer: instrument.NewMethodMetrics(scope, "writeUntimedBatchTimer", opts),
		writeUntimedGauge:      instrument.NewMethodMetrics(scope, "writeUntimedGauge", opts),
		writePassthrough:       instrument.NewMethodMetrics(scope, "writePassthrough", opts),
		writeForwarded:         instrument.NewMethodMetrics(scope, "writeForwarded", opts),
	}
}

type messagePool struct {
	pool sync.Pool
}

func newMessagePool() *messagePool {
	p := &messagePool{}
	p.pool.New = func() interface{} {
		return newMessage(p)
	}
	return p
}

func (m *messagePool) Get() *message {
	return m.pool.Get().(*message)
}

func (m *messagePool) Put(msg *message) {
	m.pool.Put(msg)
}

// Ensure message implements m3msg producer message interface.
var _ producer.Message = (*message)(nil)

type message struct {
	pool  *messagePool
	shard uint32

	metric metricpb.MetricWithMetadatas
	cm     metricpb.CounterWithMetadatas
	bm     metricpb.BatchTimerWithMetadatas
	gm     metricpb.GaugeWithMetadatas
	fm     metricpb.ForwardedMetricWithMetadata
	tm     metricpb.TimedMetricWithMetadata
	tms    metricpb.TimedMetricWithMetadatas

	buf []byte

	builder *flatbuffers.Builder
}

func newMessage(pool *messagePool) *message {
	return &message{
		pool: pool,
	}
}

// Encode encodes a m3msg payload
//nolint:gocyclo,gocritic
func (m *message) Encode(
	shard uint32,
	payload payloadUnion,
) error {
	m.shard = shard
	m.builder = msgflatbuf.GetBuilder()

	switch payload.payloadType {
	case untimedType:
		switch payload.untimed.metric.Type {
		case metric.CounterType:
			value := unaggregated.CounterWithMetadatas{
				Counter:         payload.untimed.metric.Counter(),
				StagedMetadatas: payload.untimed.metadatas,
			}
			if err := value.ToProto(&m.cm); err != nil {
				return err
			}

			m.metric = metricpb.MetricWithMetadatas{
				Type:                 metricpb.MetricWithMetadatas_COUNTER_WITH_METADATAS,
				CounterWithMetadatas: &m.cm,
			}
		case metric.TimerType:
			value := unaggregated.BatchTimerWithMetadatas{
				BatchTimer:      payload.untimed.metric.BatchTimer(),
				StagedMetadatas: payload.untimed.metadatas,
			}
			if err := value.ToProto(&m.bm); err != nil {
				return err
			}

			m.metric = metricpb.MetricWithMetadatas{
				Type:                    metricpb.MetricWithMetadatas_BATCH_TIMER_WITH_METADATAS,
				BatchTimerWithMetadatas: &m.bm,
			}
		case metric.GaugeType:
			value := unaggregated.GaugeWithMetadatas{
				Gauge:           payload.untimed.metric.Gauge(),
				StagedMetadatas: payload.untimed.metadatas,
			}
			if err := value.ToProto(&m.gm); err != nil {
				return err
			}

			m.metric = metricpb.MetricWithMetadatas{
				Type:               metricpb.MetricWithMetadatas_GAUGE_WITH_METADATAS,
				GaugeWithMetadatas: &m.gm,
			}
		default:
			return fmt.Errorf("unrecognized metric type: %v",
				payload.untimed.metric.Type)
		}
	case forwardedType:
		value := aggregated.ForwardedMetricWithMetadata{
			ForwardedMetric: payload.forwarded.metric,
			ForwardMetadata: payload.forwarded.metadata,
		}
		if err := value.ToProto(&m.fm); err != nil {
			return err
		}

		m.metric = metricpb.MetricWithMetadatas{
			Type:                        metricpb.MetricWithMetadatas_FORWARDED_METRIC_WITH_METADATA,
			ForwardedMetricWithMetadata: &m.fm,
		}
	case timedType:
		value := aggregated.TimedMetricWithMetadata{
			Metric:        payload.timed.metric,
			TimedMetadata: payload.timed.metadata,
		}
		if err := value.ToProto(&m.tm); err != nil {
			return err
		}

		m.metric = metricpb.MetricWithMetadatas{
			Type:                    metricpb.MetricWithMetadatas_TIMED_METRIC_WITH_METADATA,
			TimedMetricWithMetadata: &m.tm,
		}
	case timedWithStagedMetadatasType:
		value := aggregated.TimedMetricWithMetadatas{
			Metric:          payload.timedWithStagedMetadatas.metric,
			StagedMetadatas: payload.timedWithStagedMetadatas.metadatas,
		}
		if err := value.ToProto(&m.tms); err != nil {
			return err
		}

		m.metric = metricpb.MetricWithMetadatas{
			Type:                     metricpb.MetricWithMetadatas_TIMED_METRIC_WITH_METADATAS,
			TimedMetricWithMetadatas: &m.tms,
		}
	default:
		return fmt.Errorf("unrecognized payload type: %v",
			payload.payloadType)
	}

	size := m.metric.Size()
	if size > cap(m.buf) {
		const growthFactor = 2
		m.buf = make([]byte, int(growthFactor*float64(size)))
	}

	// Resize buffer to exactly how long we need for marshaling.
	m.buf = m.buf[:size]

	_, err := m.metric.MarshalTo(m.buf)

	// @tallen RIP THIS OUT AFTER DEBUGGING
	if m.metric.Type != metricpb.MetricWithMetadatas_TIMED_METRIC_WITH_METADATAS {
		panic(fmt.Sprintf("@tallen wtf rip this out when finished debugging - %+v\n", m.metric.String()))
	}

	return err
}

func (m *message) Shard() uint32 {
	return m.shard
}

func (m *message) Bytes() []byte {
	return m.buf
}

func (m *message) Builder() *flatbuffers.Builder {
	return m.builder
}

func (m *message) Size() int {
	return len(m.buf)
}

func (m *message) Finalize(reason producer.FinalizeReason) {
	// Return to pool.
	m.pool.Put(m)
	runtime.SetFinalizer(m.builder, func(b *flatbuffers.Builder) {
		msgflatbuf.ReturnBuilder(b)
	})
	m.builder = msgflatbuf.GetBuilder()
}
