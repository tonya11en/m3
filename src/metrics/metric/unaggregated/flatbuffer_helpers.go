package unaggregated

import (
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/m3db/m3/src/aggregator/generated/flatbuffer"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/pipeline"
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
	c.populateCounterFlatbuf(b)
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
func (cm *CounterWithMetadatas) FromFlatbuffer(buf *flatbuffer.Counter) error {
	cm.ID = buf.Id()
	cm.Annotation = buf.Annotation()
	cm.Value = buf.Value()
	cm.ClientTimeNanos = xtime.UnixNano(buf.ClientTimeNanos())
	cm.StagedMetadatas = make([]metadata.StagedMetadata, buf.MetadatasLength())

	smbuf := new(flatbuffer.StagedMetadata)
	var err error
	for i := 0; i < buf.MetadatasLength(); i++ {
		if buf.Metadatas(smbuf, i) {
			cm.StagedMetadatas[i], err = getStagedMetadata(smbuf)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// todo BatchTimerWithMetadatas

// todo GaugeWithMetadatas

func getPipeline(pbuf *flatbuffer.Pipeline) applied.Pipeline {
	p := applied.Pipeline{
		Operations: make([]applied.OpUnion, pbuf.OperationsLength()),
	}

	opUnionPlaceholder := new(flatbuffer.OpUnion)
	for i := 0; i < pbuf.OperationsLength(); i++ {
		if !pbuf.Operations(opUnionPlaceholder, i) {
			continue
		}

		p.Operations[i].Type = pipeline.OpType(opUnionPlaceholder.Type())

		tPlaceholder := new(flatbuffer.TransformationOp)
		// TODO: do I have to make a new pointer var?
		t := opUnionPlaceholder.Transformation(tPlaceholder)
		p.Operations[i].Transformation.Type = transformation.Type(t.Type())

		rPlaceholder := new(flatbuffer.RollupOp)
		r := opUnionPlaceholder.Rollup(rPlaceholder)
		p.Operations[i].Rollup.ID = r.Id()
		for i := 0; i < r.AggregationIdLength(); i++ {
			p.Operations[i].Rollup.AggregationID[i] = r.AggregationId(i)
		}
	}

	return p
}

func getPipelineMetadata(pmbuf *flatbuffer.PipelineMetadata) (metadata.PipelineMetadata, error) {
	pm := metadata.PipelineMetadata{}

	for i := 0; i < pmbuf.AggregationIdLength(); i++ {
		pm.AggregationID[i] = pmbuf.AggregationId(i)
	}

	pm.DropPolicy = policy.DropPolicy(pmbuf.DropPolicy())
	pm.ResendEnabled = pmbuf.ResendEnabled()

	pipelinePlaceholder := new(flatbuffer.Pipeline)
	pm.Pipeline = getPipeline(pmbuf.Pipeline(pipelinePlaceholder))

	prefixPlaceholder := new(flatbuffer.GraphitePrefix)
	pm.GraphitePrefix = make([][]byte, pmbuf.GraphitePrefixLength())
	for i := 0; i < pmbuf.GraphitePrefixLength(); i++ {
		if !pmbuf.GraphitePrefix(prefixPlaceholder, i) {
			continue
		}
		pm.GraphitePrefix[i] = prefixPlaceholder.Prefix()
	}

	tagPlaceholder := new(flatbuffer.Tag)
	pm.Tags = make([]models.Tag, pmbuf.TagsLength())
	for i := 0; i < pmbuf.TagsLength(); i++ {
		if !pmbuf.Tags(tagPlaceholder, i) {
			continue
		}
		pm.Tags[i].Name = tagPlaceholder.Name()
		pm.Tags[i].Value = tagPlaceholder.Value()
	}

	spPlaceholder := new(flatbuffer.StoragePolicy)
	resPlaceholder := new(flatbuffer.Resolution)
	pm.StoragePolicies = make(policy.StoragePolicies, pmbuf.StoragePoliciesLength())
	for i := 0; i < pmbuf.StoragePoliciesLength(); i++ {
		if !pmbuf.StoragePolicies(spPlaceholder, i) {
			continue
		}
		retention := time.Duration(spPlaceholder.Retention())
		resolution := spPlaceholder.Resolution(resPlaceholder)
		window := time.Duration(resolution.Window())
		precision, err := xtime.UnitFromDuration(time.Duration(resolution.Precision()))
		if err != nil {
			return pm, err
		}

		pm.StoragePolicies[i] = policy.NewStoragePolicy(window, precision, retention)
	}

	return pm, nil
}

func getStagedMetadata(mbuf *flatbuffer.StagedMetadata) (metadata.StagedMetadata, error) {
	toReturn := metadata.StagedMetadata{}

	// TODO: these allocations are awful. fix it with pooling or something
	placeholderMetadata := new(flatbuffer.Metadata)
	metadataBuf := mbuf.Metadata(placeholderMetadata)
	toReturn.Pipelines = make(metadata.PipelineMetadatas, 0, metadataBuf.PipelinesLength())
	placeholderPipelineMetadata := new(flatbuffer.PipelineMetadata)
	for i := 0; i < metadataBuf.PipelinesLength(); i++ {
		if !metadataBuf.Pipelines(placeholderPipelineMetadata, i) {
			continue
		}
		pm, err := getPipelineMetadata(placeholderPipelineMetadata)
		if err != nil {
			return toReturn, err
		}
		toReturn.Pipelines = append(toReturn.Pipelines, pm)
	}

	toReturn.CutoverNanos = mbuf.CutoverNanos()
	toReturn.Tombstoned = mbuf.Tombstoned()
	return toReturn, nil
}

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
	flatbuffer.TransformationOpAddType(b, int32(t))
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
	numTags := len(tags)
	for idx, t := range tags {
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