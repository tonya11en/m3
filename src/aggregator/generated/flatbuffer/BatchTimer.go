// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package flatbuffer

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type BatchTimer struct {
	_tab flatbuffers.Table
}

func GetRootAsBatchTimer(buf []byte, offset flatbuffers.UOffsetT) *BatchTimer {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &BatchTimer{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *BatchTimer) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *BatchTimer) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *BatchTimer) Id() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *BatchTimer) Annotation() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *BatchTimer) Values(j int) float64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.GetFloat64(a + flatbuffers.UOffsetT(j*8))
	}
	return 0
}

func (rcv *BatchTimer) ValuesLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func (rcv *BatchTimer) MutateValues(j int, n float64) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.MutateFloat64(a+flatbuffers.UOffsetT(j*8), n)
	}
	return false
}

func (rcv *BatchTimer) ClientTimeNanos() int64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		return rcv._tab.GetInt64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *BatchTimer) MutateClientTimeNanos(n int64) bool {
	return rcv._tab.MutateInt64Slot(10, n)
}

func (rcv *BatchTimer) Metadatas(obj *StagedMetadata, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(12))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 4
		x = rcv._tab.Indirect(x)
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *BatchTimer) MetadatasLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(12))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func BatchTimerStart(builder *flatbuffers.Builder) {
	builder.StartObject(5)
}
func BatchTimerAddId(builder *flatbuffers.Builder, id flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(id), 0)
}
func BatchTimerAddAnnotation(builder *flatbuffers.Builder, annotation flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(annotation), 0)
}
func BatchTimerAddValues(builder *flatbuffers.Builder, values flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(2, flatbuffers.UOffsetT(values), 0)
}
func BatchTimerStartValuesVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(8, numElems, 8)
}
func BatchTimerAddClientTimeNanos(builder *flatbuffers.Builder, clientTimeNanos int64) {
	builder.PrependInt64Slot(3, clientTimeNanos, 0)
}
func BatchTimerAddMetadatas(builder *flatbuffers.Builder, metadatas flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(4, flatbuffers.UOffsetT(metadatas), 0)
}
func BatchTimerStartMetadatasVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func BatchTimerEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}