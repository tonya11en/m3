// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package msgflatbuf

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type TimedMetadata struct {
	_tab flatbuffers.Table
}

func GetRootAsTimedMetadata(buf []byte, offset flatbuffers.UOffsetT) *TimedMetadata {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &TimedMetadata{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *TimedMetadata) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *TimedMetadata) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *TimedMetadata) AggregationId(j int) uint64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.GetUint64(a + flatbuffers.UOffsetT(j*8))
	}
	return 0
}

func (rcv *TimedMetadata) AggregationIdLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func (rcv *TimedMetadata) MutateAggregationId(j int, n uint64) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.MutateUint64(a+flatbuffers.UOffsetT(j*8), n)
	}
	return false
}

func (rcv *TimedMetadata) StoragePolicy(obj *StoragePolicy, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 4
		x = rcv._tab.Indirect(x)
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *TimedMetadata) StoragePolicyLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func TimedMetadataStart(builder *flatbuffers.Builder) {
	builder.StartObject(2)
}
func TimedMetadataAddAggregationId(builder *flatbuffers.Builder, aggregationId flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(aggregationId), 0)
}
func TimedMetadataStartAggregationIdVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(8, numElems, 8)
}
func TimedMetadataAddStoragePolicy(builder *flatbuffers.Builder, storagePolicy flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(storagePolicy), 0)
}
func TimedMetadataStartStoragePolicyVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func TimedMetadataEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
