package protobuf

import (
	"fmt"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/msg/generated/msgflatbuf"
	xtime "github.com/m3db/m3/src/x/time"
)

// todo @tallen make the decoder and test things work. use flatbuffer encoding over m3msg and also
// directl use the flatbuf struct implementation in the grpc client. m3msg will use the interface.

var (
	invalidBufferTypeError = fmt.Errorf("invalid message value type")

	noValueTypeError = fmt.Errorf("message flatbuffer does not contain value")
)

type AggregatedFlatbufDecoder struct {
	amsp          *msgflatbuf.AggregatedMetricWithStoragePolicy
	unionTable    *flatbuffers.Table
	metric        *msgflatbuf.Metric
	storagePolicy *msgflatbuf.StoragePolicy
	resolution    *msgflatbuf.Resolution
	encodeNanos   int64
	precision     xtime.Unit
}

func NewAggregatedFlatbufDecoder() *AggregatedFlatbufDecoder {
	dec := &AggregatedFlatbufDecoder{}
	dec.resetVars()
	return dec
}

func (d *AggregatedFlatbufDecoder) resetVars() {
	d.amsp = new(msgflatbuf.AggregatedMetricWithStoragePolicy)
	d.unionTable = new(flatbuffers.Table)
	d.metric = new(msgflatbuf.Metric)
	d.storagePolicy = new(msgflatbuf.StoragePolicy)
	d.resolution = new(msgflatbuf.Resolution)
}

func (d *AggregatedFlatbufDecoder) Decode(b []byte) error {
	var err error
	message := msgflatbuf.GetRootAsMessage(b, 0)
	if !message.Value(d.unionTable) {
		return noValueTypeError
	}
	vtype := message.ValueType()
	if vtype != msgflatbuf.MessageValueAggregatedMetricWithStoragePolicy {
		return invalidBufferTypeError
	}

	d.encodeNanos = int64(message.SentAtNanos())
	d.amsp.Init(d.unionTable.Bytes, d.unionTable.Pos)
	d.amsp.Metric(d.metric)
	d.amsp.StoragePolicy(d.storagePolicy)
	d.storagePolicy.Resolution(d.resolution)

	// We want to parse out a valid precision unit during decode, rather than the storage policy
	// creation.
	dur := time.Duration(d.resolution.Precision())
	d.precision, err = xtime.UnitFromDuration(dur)
	return err
}

// ID returns a copy of the decoded id.
func (d *AggregatedFlatbufDecoder) ID() []byte {
	ret := make([]byte, d.metric.IdLength())
	for i := range ret {
		ret[i] = byte(d.metric.Id(i))
	}
	return ret
}

// TimeNanos returns the decoded timestamp.
func (d *AggregatedFlatbufDecoder) TimeNanos() int64 {
	return d.metric.TimeNanos()
}

// Value returns the decoded value.
func (d *AggregatedFlatbufDecoder) Value() float64 {
	return d.metric.Value()
}

// Annotation returns the decoded annotation.
func (d *AggregatedFlatbufDecoder) Annotation() []byte {
	ret := make([]byte, d.metric.AnnotationLength())
	for i, _ := range ret {
		ret[i] = byte(d.metric.Annotation(i))
	}
	return ret
}

// StoragePolicy returns the decoded storage policy.
func (d *AggregatedFlatbufDecoder) StoragePolicy() policy.StoragePolicy {
	return policy.NewStoragePolicy(
		time.Duration(d.resolution.Window()),
		d.precision,
		time.Duration(d.storagePolicy.Retention()))
}

// EncodeNanos returns the decoded encodeNanos.
func (d *AggregatedFlatbufDecoder) EncodeNanos() int64 {
	return d.encodeNanos
}

// Close closes the decoder.
func (d *AggregatedFlatbufDecoder) Close() {
}
