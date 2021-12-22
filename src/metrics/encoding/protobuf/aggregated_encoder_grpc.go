package protobuf

import (
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/m3db/m3/src/metrics/generated/proto/metricpb"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/x/pool"
)

type aggregatedGRPCEncoder struct {
	builder *flatbuffers.Builder
	pb      metricpb.AggregatedMetric
}

func NewAggregatedGRPCEncoder(p pool.BytesPool) AggregatedEncoder {
	e := &aggregatedEncoder{}
	return e
}

func (enc *aggregatedGRPCEncoder) Encode(
	m aggregated.MetricWithStoragePolicy,
	encodedAtNanos int64,
) error {
	ReuseAggregatedMetricProto(&enc.pb)
	if err := m.ToProto(&enc.pb.Metric); err != nil {
		return err
	}
	enc.pb.EncodeNanos = encodedAtNanos
	// Always allocate a new byte slice to avoid modifying the existing one which may still being used.
	enc.buf = allocate(enc.pool, enc.pb.Size())
	n, err := enc.pb.MarshalTo(enc.buf)
	enc.buf = enc.buf[:n]
	return err
}

func (enc *aggregatedGRPCEncoder) Buffer() Buffer {
	var fn PoolReleaseFn
	if enc.pool != nil {
		fn = enc.pool.Put
	}
	return NewBuffer(enc.buf, fn)
}
