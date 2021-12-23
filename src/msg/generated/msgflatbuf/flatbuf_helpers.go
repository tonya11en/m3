package msgflatbuf

import (
	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/pipeline/applied"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/transformation"
	"github.com/m3db/m3/src/query/models"
	xtime "github.com/m3db/m3/src/x/time"
)

// make transformation op
func MakeTransformationOpFlatbuf(t transformation.Type, b *flatbuffers.Builder) flatbuffers.UOffsetT {
	TransformationOpStart(b)
	TransformationOpAddType(b, int32(t))
	return TransformationOpEnd(b)
}

// make rollup op
func MakeRollupOpFlatbuf(ro *applied.RollupOp, b *flatbuffers.Builder) flatbuffers.UOffsetT {
	id := b.CreateByteString(ro.ID)
	numAggIds := len(ro.AggregationID)

	RollupOpStartAggregationIdVector(b, numAggIds)
	for i := numAggIds - 1; i >= 0; i-- {
		b.PrependUint64(ro.AggregationID[i])
	}
	aggId := b.EndVector(numAggIds)

	RollupOpStart(b)
	RollupOpAddId(b, id)
	RollupOpAddAggregationId(b, aggId)

	return RollupOpEnd(b)
}

// make op union
func MakeOpUnionFlatbuf(ou *applied.OpUnion, b *flatbuffers.Builder) flatbuffers.UOffsetT {
	transformationOp := MakeTransformationOpFlatbuf(ou.Transformation.Type, b)
	rollupOp := MakeRollupOpFlatbuf(&ou.Rollup, b)

	OpUnionStart(b)
	OpUnionAddTransformation(b, transformationOp)
	OpUnionAddRollup(b, rollupOp)
	OpUnionAddType(b, int32(ou.Type))

	return OpUnionEnd(b)
}

// make storage policy
func MakeStoragePolicyFlatbuf(sp *policy.StoragePolicy, b *flatbuffers.Builder) (flatbuffers.UOffsetT, error) {
	r := sp.Resolution()
	p, err := xtime.DurationFromUnit(r.Precision)
	if err != nil {
		return 0, err
	}

	resOffset := CreateResolution(b, r.Window.Nanoseconds(), p.Nanoseconds())
	StoragePolicyStart(b)
	StoragePolicyAddResolution(b, resOffset)
	StoragePolicyAddRetention(b, sp.Retention().Duration().Nanoseconds())
	return StoragePolicyEnd(b), nil
}

// make pipeline
func MakePipelineFlatbuf(p *applied.Pipeline, b *flatbuffers.Builder) flatbuffers.UOffsetT {
	numOps := len(p.Operations)
	opOffsets := make([]flatbuffers.UOffsetT, numOps)
	for idx, opUnion := range p.Operations {
		opOffsets[idx] = MakeOpUnionFlatbuf(&opUnion, b)
	}

	PipelineStartOperationsVector(b, numOps)
	for i := numOps - 1; i >= 0; i-- {
		b.PrependUOffsetT(opOffsets[i])
	}
	ops := b.EndVector(numOps)

	PipelineStart(b)
	PipelineAddOperations(b, ops)
	return PipelineEnd(b)
}

// make tag
func MakeTagFlatbuf(t *models.Tag, b *flatbuffers.Builder) flatbuffers.UOffsetT {
	name := b.CreateByteString(t.Name)
	val := b.CreateByteString(t.Value)
	TagStart(b)
	TagAddName(b, name)
	TagAddValue(b, val)
	return TagEnd(b)
}

// make graphite prefix
func MakeGraphitePrefixFlatbuf(gp []byte, b *flatbuffers.Builder) flatbuffers.UOffsetT {
	prefix := b.CreateByteVector(gp)
	GraphitePrefixStart(b)
	GraphitePrefixAddPrefix(b, prefix)
	return GraphitePrefixEnd(b)
}

func MakeStoragePolicyVector(policies policy.StoragePolicies, b *flatbuffers.Builder) (flatbuffers.UOffsetT, error) {
	var err error

	offsets := make([]flatbuffers.UOffsetT, len(policies))

	numStoragePolicies := len(policies)
	for idx, sp := range policies {
		offsets[idx], err = MakeStoragePolicyFlatbuf(&sp, b)
		if err != nil {
			return 0, err
		}
	}
	PipelineMetadataStartStoragePoliciesVector(b, numStoragePolicies)
	for i := numStoragePolicies - 1; i >= 0; i-- {
		b.PrependUOffsetT(offsets[i])
	}
	return b.EndVector(numStoragePolicies), nil
}

func MakeTagVector(tags []models.Tag, b *flatbuffers.Builder) flatbuffers.UOffsetT {
	offsets := make([]flatbuffers.UOffsetT, len(tags))
	numTags := len(tags)
	for idx, t := range tags {
		offsets[idx] = MakeTagFlatbuf(&t, b)
	}
	PipelineMetadataStartTagsVector(b, numTags)
	for i := numTags - 1; i >= 0; i-- {
		b.PrependUOffsetT(offsets[i])
	}
	return b.EndVector(numTags)
}

func MakeGraphitePrefixVector(prefixes [][]byte, b *flatbuffers.Builder) flatbuffers.UOffsetT {
	numPrefixes := len(prefixes)
	offsets := make([]flatbuffers.UOffsetT, numPrefixes)
	for idx, p := range prefixes {
		offsets[idx] = MakeGraphitePrefixFlatbuf(p, b)
	}
	GraphitePrefixStart(b)
	for i := numPrefixes; i >= 0; i-- {
		b.PrependUOffsetT(offsets[i])
	}
	return b.EndVector(numPrefixes)
}

func MakePipelineMetadataFlatbuf(pm *metadata.PipelineMetadata, b *flatbuffers.Builder) (flatbuffers.UOffsetT, error) {
	storagePolicyOffset, err := MakeStoragePolicyVector(pm.StoragePolicies, b)
	if err != nil {
		return 0, err
	}
	pipelineOffset := MakePipelineFlatbuf(&pm.Pipeline, b)
	tagsOffset := MakeTagVector(pm.Tags, b)
	prefixesOffset := MakeGraphitePrefixVector(pm.GraphitePrefix, b)

	numAggregationIds := len(pm.AggregationID)
	PipelineMetadataStartAggregationIdVector(b, numAggregationIds)
	for i := numAggregationIds; i >= 0; i-- {
		b.PrependUint64(pm.AggregationID[i])
	}
	aggIdsOffset := b.EndVector(numAggregationIds)

	PipelineMetadataStart(b)
	PipelineMetadataAddStoragePolicies(b, storagePolicyOffset)
	PipelineMetadataAddPipeline(b, pipelineOffset)
	PipelineMetadataAddTags(b, tagsOffset)
	PipelineMetadataAddGraphitePrefix(b, prefixesOffset)
	PipelineMetadataAddResendEnabled(b, pm.ResendEnabled)
	PipelineMetadataAddAggregationId(b, aggIdsOffset)
	return PipelineMetadataEnd(b), nil
}

func MakeMetadataFlatbuf(m metadata.Metadata, b *flatbuffers.Builder) (flatbuffers.UOffsetT, error) {
	var err error
	numPipelines := len(m.Pipelines)
	pipelineOffsets := make([]flatbuffers.UOffsetT, numPipelines)
	for idx, p := range m.Pipelines {
		pipelineOffsets[idx], err = MakePipelineMetadataFlatbuf(&p, b)
		if err != nil {
			return 0, nil
		}
	}

	MetadataStartPipelinesVector(b, numPipelines)
	for i := numPipelines; i >= 0; i-- {
		b.PrependUOffsetT(pipelineOffsets[i])
	}
	pvOffset := b.EndVector(numPipelines)

	MetadataStart(b)
	MetadataAddPipelines(b, pvOffset)
	return MetadataEnd(b), nil
}

func MakeStagedMetadataFlatbuf(sm *metadata.StagedMetadata, b *flatbuffers.Builder) (flatbuffers.UOffsetT, error) {
	metadataOffset, err := MakeMetadataFlatbuf(sm.Metadata, b)
	if err != nil {
		return 0, nil
	}

	StagedMetadataStart(b)
	StagedMetadataAddMetadata(b, metadataOffset)
	StagedMetadataAddCutoverNanos(b, sm.CutoverNanos)
	StagedMetadataAddTombstoned(b, sm.Tombstoned)
	return StagedMetadataEnd(b), nil
}
