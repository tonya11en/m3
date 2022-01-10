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

	annotationOffset := enc.b.CreateByteVector(m.Annotation)

	idOffset := enc.b.CreateByteVector(m.ID)

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
