package protobuf

import (
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/msg/generated/msgflatbuf"
)

type aggregatedFlatbufEncoder struct {
	b *flatbuffers.Builder
}

func NewAggregatedFlatbufEncoder() AggregatedEncoder {
	e := &aggregatedFlatbufEncoder{}
	return e
}

func (enc *aggregatedFlatbufEncoder) Encode(
	m aggregated.MetricWithStoragePolicy,
	encodedAtNanos int64,
) error {
	enc.b = msgflatbuf.GetBuilder()

	storagePolicyOffset, err := msgflatbuf.MakeStoragePolicyFlatbuf(&m.StoragePolicy, enc.b)
	if err != nil {
		return err
	}

	msgflatbuf.MetricStartAnnotationVector(enc.b, len(m.Annotation))
	for i := len(m.Annotation) - 1; i >= 0; i-- {
		enc.b.PrependByte(m.Annotation[i])
	}
	annotationOffset := enc.b.EndVector(len(m.Annotation))

	msgflatbuf.MetricStartIdVector(enc.b, len(m.ID))
	for i := len(m.ID) - 1; i >= 0; i-- {
		enc.b.PrependByte(m.ID[i])
	}
	idOffset := enc.b.EndVector(len(m.ID))

	msgflatbuf.MetricStart(enc.b)
	msgflatbuf.MetricAddType(enc.b, int32(m.Type))
	msgflatbuf.MetricAddTimeNanos(enc.b, m.TimeNanos)
	msgflatbuf.MetricAddAnnotation(enc.b, annotationOffset)
	msgflatbuf.MetricAddId(enc.b, idOffset)
	msgflatbuf.MetricAddValue(enc.b, m.Value)

	metricOffset := msgflatbuf.MetricEnd(enc.b)

	msgflatbuf.AggregatedMetricWithStoragePolicyStart(enc.b)
	msgflatbuf.AggregatedMetricWithStoragePolicyAddMetric(enc.b, metricOffset)
	msgflatbuf.AggregatedMetricWithStoragePolicyAddStoragePolicy(enc.b, storagePolicyOffset)
	valueOffset := msgflatbuf.AggregatedMetricWithStoragePolicyEnd(enc.b)

	msgflatbuf.MessageStart(enc.b)
	msgflatbuf.MessageAddValueType(enc.b, msgflatbuf.MessageValueAggregatedMetricWithStoragePolicy)
	msgflatbuf.MessageAddValue(enc.b, valueOffset)
	// not setting message id
	msgflatbuf.MessageAddSentAtNanos(enc.b, uint64(encodedAtNanos))
	msgOffset := msgflatbuf.MessageEnd(enc.b)

	enc.b.Finish(msgOffset)
	return nil
}

func (enc *aggregatedFlatbufEncoder) Buffer() Buffer {
	b := enc.b
	return NewBuffer(b.FinishedBytes(), func([]byte) {
		msgflatbuf.ReturnBuilder(b)
	})
}
