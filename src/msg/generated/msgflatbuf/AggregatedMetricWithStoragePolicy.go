// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package msgflatbuf

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type AggregatedMetricWithStoragePolicy struct {
	_tab flatbuffers.Table
}

func GetRootAsAggregatedMetricWithStoragePolicy(buf []byte, offset flatbuffers.UOffsetT) *AggregatedMetricWithStoragePolicy {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &AggregatedMetricWithStoragePolicy{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *AggregatedMetricWithStoragePolicy) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *AggregatedMetricWithStoragePolicy) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *AggregatedMetricWithStoragePolicy) Metric(obj *Metric) *Metric {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		x := rcv._tab.Indirect(o + rcv._tab.Pos)
		if obj == nil {
			obj = new(Metric)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

func (rcv *AggregatedMetricWithStoragePolicy) StoragePolicy(obj *StoragePolicy) *StoragePolicy {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		x := rcv._tab.Indirect(o + rcv._tab.Pos)
		if obj == nil {
			obj = new(StoragePolicy)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

func AggregatedMetricWithStoragePolicyStart(builder *flatbuffers.Builder) {
	builder.StartObject(2)
}
func AggregatedMetricWithStoragePolicyAddMetric(builder *flatbuffers.Builder, metric flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(metric), 0)
}
func AggregatedMetricWithStoragePolicyAddStoragePolicy(builder *flatbuffers.Builder, storagePolicy flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(storagePolicy), 0)
}
func AggregatedMetricWithStoragePolicyEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
