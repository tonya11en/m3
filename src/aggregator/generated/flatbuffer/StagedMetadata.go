// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package flatbuffer

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type StagedMetadata struct {
	_tab flatbuffers.Table
}

func GetRootAsStagedMetadata(buf []byte, offset flatbuffers.UOffsetT) *StagedMetadata {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &StagedMetadata{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *StagedMetadata) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *StagedMetadata) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *StagedMetadata) Metadata(obj *Metadata) *Metadata {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		x := rcv._tab.Indirect(o + rcv._tab.Pos)
		if obj == nil {
			obj = new(Metadata)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

func (rcv *StagedMetadata) CutoverNanos() int64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.GetInt64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *StagedMetadata) MutateCutoverNanos(n int64) bool {
	return rcv._tab.MutateInt64Slot(6, n)
}

func (rcv *StagedMetadata) Tombstoned() bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.GetBool(o + rcv._tab.Pos)
	}
	return false
}

func (rcv *StagedMetadata) MutateTombstoned(n bool) bool {
	return rcv._tab.MutateBoolSlot(8, n)
}

func StagedMetadataStart(builder *flatbuffers.Builder) {
	builder.StartObject(3)
}
func StagedMetadataAddMetadata(builder *flatbuffers.Builder, metadata flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(metadata), 0)
}
func StagedMetadataAddCutoverNanos(builder *flatbuffers.Builder, cutoverNanos int64) {
	builder.PrependInt64Slot(1, cutoverNanos, 0)
}
func StagedMetadataAddTombstoned(builder *flatbuffers.Builder, tombstoned bool) {
	builder.PrependBoolSlot(2, tombstoned, false)
}
func StagedMetadataEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}