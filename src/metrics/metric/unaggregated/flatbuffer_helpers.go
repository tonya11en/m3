package unaggregated

import (
	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/m3db/m3/src/aggregator/generated/flatbuffer"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/pipeline/applied"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/transformation"
	"github.com/m3db/m3/src/query/models"
	xtime "github.com/m3db/m3/src/x/time"
)

func (c *Counter) populateCounterFlatbuf(b *flatbuffers.Builder) {
	// Create the complex types.
	id := b.CreateByteVector(c.ID)
	annotation := b.CreateByteVector(c.Annotation)

	// Add things to the buffer.
	flatbuffer.CounterAddId(b, id)
	flatbuffer.CounterAddAnnotation(b, annotation)
	flatbuffer.CounterAddClientTimeNanos(b, int64(c.ClientTimeNanos))
	flatbuffer.CounterAddValue(b, c.Value)
}

func (c *Counter) ToFlatbuffer(b *flatbuffers.Builder) {
	flatbuffer.CounterStart(b)
	c.populateCounterFlatbuffer(b)
	b.Finish(flatbuffer.CounterEnd(b))
}

func (c *Counter) FromFlatbuffer(buf *flatbuffer.Counter) {
	c.ID = buf.Id()
	c.Annotation = buf.Annotation()
	c.ClientTimeNanos = xtime.UnixNano(buf.ClientTimeNanos())
	c.Value = buf.Value()
}

func (t *BatchTimer) populateBatchTimerFlatbuffer(b *flatbuffers.Builder) {
	// Create the complex types.
	id := b.CreateByteVector(t.ID)
	annotation := b.CreateByteVector(t.Annotation)

	numValues := len(t.Values)
	flatbuffer.BatchTimerStartValuesVector(b, numValues)
	for i := numValues - 1; i >= 0; i-- {
		b.PrependFloat64(t.Values[i])
	}
	values := b.EndVector(numValues)

	// Add things to the buffer.
	flatbuffer.BatchTimerAddId(b, id)
	flatbuffer.BatchTimerAddAnnotation(b, annotation)
	flatbuffer.BatchTimerAddClientTimeNanos(b, int64(t.ClientTimeNanos))
	flatbuffer.BatchTimerAddValues(b, values)
}

func (t *BatchTimer) ToFlatbuffer(b *flatbuffers.Builder) {
	flatbuffer.BatchTimerStart(b)
	t.populateBatchTimerFlatbuffer(b)
	b.Finish(flatbuffer.BatchTimerEnd(b))
}

func (t *BatchTimer) FromFlatbuffer(buf *flatbuffer.BatchTimer) {
	t.ID = buf.Id()

	t.Values = t.Values[:0]
	for i := 0; i < buf.ValuesLength(); i++ {
		t.Values = append(t.Values, buf.Values(i))
	}

	t.Annotation = buf.Annotation()
	t.ClientTimeNanos = xtime.UnixNano(buf.ClientTimeNanos())
}

func (g *Gauge) populateGaugeFlatbuffer(b *flatbuffers.Builder) {
	// Create the complex types.
	id := b.CreateByteVector(g.ID)
	annotation := b.CreateByteVector(g.Annotation)

	// Add things to the buffer.
	flatbuffer.GaugeAddId(b, id)
	flatbuffer.GaugeAddAnnotation(b, annotation)
	flatbuffer.GaugeAddValue(b, g.Value)
	flatbuffer.GaugeAddClientTimeNanos(b, int64(g.ClientTimeNanos))
}

func (g *Gauge) ToFlatbuffer(b *flatbuffers.Builder) {
	flatbuffer.GaugeStart(b)
	g.populateGaugeFlatbuffer(b)
	b.Finish(flatbuffer.GaugeEnd(b))
}

func (g *Gauge) FromFlatbuffer(buf *flatbuffer.Gauge) {
	g.ID = buf.Id()
	g.Annotation = buf.Annotation()
	g.Value = buf.Value()
	g.ClientTimeNanos = xtime.UnixNano(buf.ClientTimeNanos())
}

// todo @tallennn
func (cm *CounterWithMetadatas) ToFlatbuffer(b *flatbuffers.Builder) {
	flatbuffer.CounterStart(b)
	cm.populateCounterFlatbuf(b)
	b.Finish(flatbuffer.CounterEnd(b))
}

// todo @tallen
func (cm *CounterWithMetadatas) FromFlatbuffer(buf *flatbuffer.Counter) {
	cm.ID = buf.Id()
	cm.Annotation = buf.Annotation()
	cm.Value = buf.Value()
	cm.ClientTimeNanos = xtime.UnixNano(buf.ClientTimeNanos())
	cm.StagedMetadatas = make(metadata.StagedMetadatas, buf.MetadatasLength())
	for i := 0; i < buf.MetadatasLength(); i++ {

	}
}

// todo BatchTimerWithMetadatas

// todo GaugeWithMetadatas

func makeResolutionFlatbuf(r *policy.Resolution, b *flatbuffers.Builder) (flatbuffers.UOffsetT, error) {
	flatbuffer.ResolutionStart(b)

	p, err := r.Precision.Value()
	if err != nil {
		return 0, err
	}
	flatbuffer.ResolutionAddPrecision(b, p.Nanoseconds())
	flatbuffer.ResolutionAddWindow(b, r.Window.Nanoseconds())

	return flatbuffer.ResolutionEnd(b), nil
}

// make transformation op
func makeTransformationOpFlatbuf(t transformation.Type, b *flatbuffers.Builder) flatbuffers.UOffsetT {
	flatbuffer.TransformationOpStart(b)
	flatbuffer.TransformationOpAddType(b, int64(t))
	return flatbuffer.TransformationOpEnd(b)
}

// make rollup op
func makeRollupOpFlatbuf(ro *applied.RollupOp, b *flatbuffers.Builder) flatbuffers.UOffsetT {
	id := b.CreateByteString(ro.ID)
	numAggIds := len(ro.AggregationID)

	flatbuffer.RollupOpStartAggregationIdVector(b, numAggIds)
	for i := numAggIds - 1; i >= 0; i-- {
		b.PrependUint64(ro.AggregationID[i])
	}
	aggId := b.EndVector(numAggIds)

	flatbuffer.RollupOpStart(b)
	flatbuffer.RollupOpAddId(b, id)
	flatbuffer.RollupOpAddAggregationId(b, aggId)

	return flatbuffer.RollupOpEnd(b)
}

// make op union
func makeOpUnionFlatbuf(ou *applied.OpUnion, b *flatbuffers.Builder) flatbuffers.UOffsetT {
	transformationOp := makeTransformationOpFlatbuf(ou.Transformation.Type, b)
	rollupOp := makeRollupOpFlatbuf(&ou.Rollup, b)

	flatbuffer.OpUnionStart(b)
	flatbuffer.OpUnionAddTransformation(b, transformationOp)
	flatbuffer.OpUnionAddRollup(b, rollupOp)
	flatbuffer.OpUnionAddType(b, int32(ou.Type))

	return flatbuffer.OpUnionEnd(b)
}

// make storage policy
func makeStoragePolicyFlatbuf(sp *policy.StoragePolicy, b *flatbuffers.Builder) (flatbuffers.UOffsetT, error) {
	r := sp.Resolution()
	resolution, err := makeResolutionFlatbuf(&r, b)
	if err != nil {
		return 0, err
	}

	flatbuffer.StoragePolicyStart(b)
	flatbuffer.StoragePolicyAddResolution(b, resolution)
	flatbuffer.StoragePolicyAddRetention(b, sp.Retention().Duration().Nanoseconds())
	return flatbuffer.StoragePolicyEnd(b), nil
}

// make pipeline
func makePipelineFlatbuf(p *applied.Pipeline, b *flatbuffers.Builder) flatbuffers.UOffsetT {
	numOps := len(p.Operations)
	opOffsets := make([]flatbuffers.UOffsetT, numOps)
	for idx, opUnion := range p.Operations {
		opOffsets[idx] = makeOpUnionFlatbuf(&opUnion, b)
	}

	flatbuffer.PipelineStartOperationsVector(b, numOps)
	for i := numOps - 1; i >= 0; i-- {
		b.PrependUOffsetT(opOffsets[i])
	}
	ops := b.EndVector(numOps)

	flatbuffer.PipelineStart(b)
	flatbuffer.PipelineAddOperations(b, ops)
	return flatbuffer.PipelineEnd(b)
}

// make tag
func makeTagFlatbuf(t *models.Tag, b *flatbuffers.Builder) flatbuffers.UOffsetT {
	name := b.CreateByteString(t.Name)
	val := b.CreateByteString(t.Value)
	flatbuffer.TagStart(b)
	flatbuffer.TagAddName(b, name)
	flatbuffer.TagAddValue(b, val)
	return flatbuffer.TagEnd(b)
}

// make graphite prefix
func makeGraphitePrefixFlatbuf(gp []byte, b *flatbuffers.Builder) flatbuffers.UOffsetT {
	prefix := b.CreateByteVector(gp)
	flatbuffer.GraphitePrefixStart(b)
	flatbuffer.GraphitePrefixAddPrefix(b, prefix)
	return flatbuffer.GraphitePrefixEnd(b)
}

func makeStoragePolicyVector(policies policy.StoragePolicies, b *flatbuffers.Builder) (flatbuffers.UOffsetT, error) {
	var err error

	offsets := make([]flatbuffers.UOffsetT, len(policies))

	numStoragePolicies := len(policies)
	for idx, sp := range policies {
		offsets[idx], err = makeStoragePolicyFlatbuf(&sp, b)
		if err != nil {
			return 0, err
		}
	}
	flatbuffer.PipelineMetadataStartStoragePoliciesVector(b, numStoragePolicies)
	for i := numStoragePolicies - 1; i >= 0; i-- {
		b.PrependUOffsetT(offsets[i])
	}
	return b.EndVector(numStoragePolicies), nil
}

func makeTagVector(tags []models.Tag, b *flatbuffers.Builder) flatbuffers.UOffsetT {
	offsets := make([]flatbuffers.UOffsetT, len(tags))
	numTags := len(pm.Tags)
	for idx, t := range pm.Tags {
		offsets[idx] = makeTagFlatbuf(&t, b)
	}
	flatbuffer.PipelineMetadataStartTagsVector(b, numTags)
	for i := numTags - 1; i >= 0; i-- {
		b.PrependUOffsetT(offsets[i])
	}
	return b.EndVector(numTags)
}

func makeGraphitePrefixVector(prefixes [][]byte, b *flatbuffers.Builder) flatbuffers.UOffsetT {
	numPrefixes := len(prefixes)
	offsets := make([]flatbuffers.UOffsetT, numPrefixes)
	for idx, p := range prefixes {
		offsets[idx] = makeGraphitePrefixFlatbuf(p, b)
	}
	flatbuffer.GraphitePrefixStart(b)
	for i := numPrefixes; i >= 0; i-- {
		b.PrependUOffsetT(offsets[i])
	}
	return b.EndVector(numPrefixes)
}

func makePipelineMetadataFlatbuf(pm *metadata.PipelineMetadata, b *flatbuffers.Builder) (flatbuffers.UOffsetT, error) {
	storagePolicyOffset, err := makeStoragePolicyVector(pm.StoragePolicies, b)
	if err != nil {
		return 0, err
	}
	pipelineOffset := makePipelineFlatbuf(&pm.Pipeline, b)
	tagsOffset := makeTagVector(pm.Tags, b)
	prefixesOffset := makeGraphitePrefixVector(pm.GraphitePrefix, b)

	numAggregationIds := len(pm.AggregationID)
	flatbuffer.PipelineMetadataStartAggregationIdVector(b, numAggregationIds)
	for i := numAggregationIds; i >= 0; i-- {
		b.PrependUint64(pm.AggregationID[i])
	}
	aggIdsOffset := b.EndVector(numAggregationIds)

	flatbuffer.PipelineMetadataStart(b)
	flatbuffer.PipelineMetadataAddStoragePolicies(b, storagePolicyOffset)
	flatbuffer.PipelineMetadataAddPipeline(b, pipelineOffset)
	flatbuffer.PipelineMetadataAddTags(b, tagsOffset)
	flatbuffer.PipelineMetadataAddGraphitePrefix(b, prefixesOffset)
	flatbuffer.PipelineMetadataAddResendEnabled(b, pm.ResendEnabled)
	flatbuffer.PipelineMetadataAddAggregationId(b, aggIdsOffset)
	return flatbuffer.PipelineMetadataEnd(b), nil
}

func makeMetadataFlatbuf(m metadata.Metadata, b *flatbuffers.Builder) (flatbuffers.UOffsetT, error) {
	var err error
	numPipelines := len(m.Pipelines)
	pipelineOffsets := make([]flatbuffers.UOffsetT, numPipelines)
	for idx, p := range m.Pipelines {
		pipelineOffsets[idx], err = makePipelineMetadataFlatbuf(&p, b)
		if err != nil {
			return 0, nil
		}
	}

	flatbuffer.MetadataStartPipelinesVector(b, numPipelines)
	for i := numPipelines; i >= 0; i-- {
		b.PrependUOffsetT(pipelineOffsets[i])
	}
	pvOffset := b.EndVector(numPipelines)

	flatbuffer.MetadataStart(b)
	flatbuffer.MetadataAddPipelines(b, pvOffset)
	return flatbuffer.MetadataEnd(b), nil
}

func makeStagedMetadataFlatbuf(sm *metadata.StagedMetadata, b *flatbuffers.Builder) (flatbuffers.UOffsetT, error) {
	metadataOffset, err := makeMetadataFlatbuf(sm.Metadata, b)
	if err != nil {
		return 0, nil
	}

	flatbuffer.StagedMetadataStart(b)
	flatbuffer.StagedMetadataAddMetadata(b, metadataOffset)
	flatbuffer.StagedMetadataAddCutoverNanos(b, sm.CutoverNanos)
	flatbuffer.StagedMetadataAddTombstoned(b, sm.Tombstoned)
	return flatbuffer.StagedMetadataEnd(b), nil
}
