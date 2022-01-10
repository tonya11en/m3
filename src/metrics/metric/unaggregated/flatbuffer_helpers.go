package unaggregated

import (
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/pipeline"
	"github.com/m3db/m3/src/metrics/pipeline/applied"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/transformation"
	"github.com/m3db/m3/src/msg/generated/msgflatbuf"
	"github.com/m3db/m3/src/query/models"
	xtime "github.com/m3db/m3/src/x/time"
)

var (
	bufferInitialCapacity = 2048

	bufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, bufferInitialCapacity)
		},
	}
)

func GetBuffer() []byte {
	return bufferPool.Get().([]byte)
}

func ReturnBuffer(buf []byte) {
	buf = buf[:0]
	bufferPool.Put(buf)
}

func (c *Counter) FromFlatbuffer(buf *msgflatbuf.CounterWithMetadatas) {
	c.ID = buf.Id()
	c.ClientTimeNanos = xtime.UnixNano(buf.ClientTimeNanos())
	c.Value = buf.Value()

	c.Annotation = GetBuffer()
	for i := 0; i < buf.AnnotationLength(); i++ {
		c.Annotation = append(c.Annotation, byte(buf.Annotation(i)))
	}
}

func (t *BatchTimer) FromFlatbuffer(buf *msgflatbuf.BatchTimerWithMetadatas) {
	t.ID = buf.Id()

	t.Values = t.Values[:0]
	for i := 0; i < buf.ValuesLength(); i++ {
		t.Values = append(t.Values, buf.Values(i))
	}

	t.Annotation = GetBuffer()
	for i := 0; i < buf.AnnotationLength(); i++ {
		t.Annotation = append(t.Annotation, byte(buf.Annotation(i)))
	}
	t.ClientTimeNanos = xtime.UnixNano(buf.ClientTimeNanos())
}

func (g *Gauge) FromFlatbuffer(buf *msgflatbuf.GaugeWithMetadatas) {
	g.ID = buf.Id()
	for i := 0; i < buf.AnnotationLength(); i++ {
		g.Annotation = append(g.Annotation, byte(buf.Annotation(i)))
	}
	g.Value = buf.Value()
	g.ClientTimeNanos = xtime.UnixNano(buf.ClientTimeNanos())
}

func (cm *CounterWithMetadatas) FromFlatbuffer(buf *msgflatbuf.CounterWithMetadatas) error {
	cm.ID = buf.Id()
	cm.Annotation = GetBuffer()
	for i := 0; i < buf.AnnotationLength(); i++ {
		cm.Annotation = append(cm.Annotation, byte(buf.Annotation(i)))
	}
	cm.Value = buf.Value()
	cm.ClientTimeNanos = xtime.UnixNano(buf.ClientTimeNanos())
	cm.StagedMetadatas = make([]metadata.StagedMetadata, buf.MetadatasLength())

	var err error
	for i := 0; i < buf.MetadatasLength(); i++ {
		smbuf := new(msgflatbuf.StagedMetadata)
		if buf.Metadatas(smbuf, i) {
			cm.StagedMetadatas[i], err = getStagedMetadata(smbuf)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (bt *BatchTimerWithMetadatas) FromFlatbuffer(buf *msgflatbuf.BatchTimerWithMetadatas) error {
	bt.ID = buf.Id()
	bt.Annotation = GetBuffer()
	for i := 0; i < buf.AnnotationLength(); i++ {
		bt.Annotation = append(bt.Annotation, byte(buf.Annotation(i)))
	}
	bt.ClientTimeNanos = xtime.UnixNano(buf.ClientTimeNanos())
	bt.StagedMetadatas = make([]metadata.StagedMetadata, buf.MetadatasLength())

	bt.Values = make([]float64, buf.ValuesLength())
	for i := 0; i < buf.ValuesLength(); i++ {
		bt.Values[i] = buf.Values(i)
	}

	var err error
	for i := 0; i < buf.MetadatasLength(); i++ {
		smbuf := new(msgflatbuf.StagedMetadata)
		if buf.Metadatas(smbuf, i) {
			bt.StagedMetadatas[i], err = getStagedMetadata(smbuf)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (g *GaugeWithMetadatas) FromFlatbuffer(buf *msgflatbuf.GaugeWithMetadatas) error {
	g.ID = buf.Id()
	g.Annotation = GetBuffer()
	for i := 0; i < buf.AnnotationLength(); i++ {
		g.Annotation = append(g.Annotation, byte(buf.Annotation(i)))
	}
	g.Value = buf.Value()
	g.ClientTimeNanos = xtime.UnixNano(buf.ClientTimeNanos())
	g.StagedMetadatas = make([]metadata.StagedMetadata, buf.MetadatasLength())

	var err error
	for i := 0; i < buf.MetadatasLength(); i++ {
		smbuf := new(msgflatbuf.StagedMetadata)
		if buf.Metadatas(smbuf, i) {
			g.StagedMetadatas[i], err = getStagedMetadata(smbuf)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func getPipeline(pbuf *msgflatbuf.Pipeline) applied.Pipeline {
	p := applied.Pipeline{
		Operations: make([]applied.OpUnion, pbuf.OperationsLength()),
	}

	opUnionPlaceholder := new(msgflatbuf.OpUnion)
	for i := 0; i < pbuf.OperationsLength(); i++ {
		if !pbuf.Operations(opUnionPlaceholder, i) {
			continue
		}

		p.Operations[i].Type = pipeline.OpType(opUnionPlaceholder.Type())

		tPlaceholder := new(msgflatbuf.TransformationOp)
		// TODO: do I have to make a new pointer var?
		t := opUnionPlaceholder.Transformation(tPlaceholder)
		p.Operations[i].Transformation.Type = transformation.Type(t.Type())

		rPlaceholder := new(msgflatbuf.RollupOp)
		r := opUnionPlaceholder.Rollup(rPlaceholder)
		p.Operations[i].Rollup.ID = r.Id()
		for i := 0; i < r.AggregationIdLength(); i++ {
			p.Operations[i].Rollup.AggregationID[i] = r.AggregationId(i)
		}
	}

	return p
}

func getPipelineMetadata(pmbuf *msgflatbuf.PipelineMetadata) (metadata.PipelineMetadata, error) {
	pm := metadata.PipelineMetadata{}

	for i := 0; i < pmbuf.AggregationIdLength(); i++ {
		pm.AggregationID[i] = pmbuf.AggregationId(i)
	}

	pm.DropPolicy = policy.DropPolicy(pmbuf.DropPolicy())
	pm.ResendEnabled = pmbuf.ResendEnabled()

	// @tallen - this is broken and causing agg crashes..
	pipelinePlaceholder := new(msgflatbuf.Pipeline)
	fmt.Printf("@tallen - pmbuf=%+v\n", pmbuf)
	pm.Pipeline = getPipeline(pmbuf.Pipeline(pipelinePlaceholder))

	prefixPlaceholder := new(msgflatbuf.GraphitePrefix)
	pm.GraphitePrefix = make([][]byte, pmbuf.GraphitePrefixLength())
	for i := 0; i < pmbuf.GraphitePrefixLength(); i++ {
		if !pmbuf.GraphitePrefix(prefixPlaceholder, i) {
			continue
		}
		pm.GraphitePrefix[i] = prefixPlaceholder.Prefix()
	}

	tagPlaceholder := new(msgflatbuf.Tag)
	pm.Tags = make([]models.Tag, pmbuf.TagsLength())
	for i := 0; i < pmbuf.TagsLength(); i++ {
		if !pmbuf.Tags(tagPlaceholder, i) {
			continue
		}
		pm.Tags[i].Name = tagPlaceholder.Name()
		pm.Tags[i].Value = tagPlaceholder.Value()
	}

	spPlaceholder := new(msgflatbuf.StoragePolicy)
	resPlaceholder := new(msgflatbuf.Resolution)
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

func getStagedMetadata(mbuf *msgflatbuf.StagedMetadata) (metadata.StagedMetadata, error) {
	toReturn := metadata.StagedMetadata{}

	// TODO: these allocations are awful. fix it with pooling or something
	placeholderMetadata := new(msgflatbuf.Metadata)
	metadataBuf := mbuf.Metadata(placeholderMetadata)
	toReturn.Pipelines = make(metadata.PipelineMetadatas, 0, metadataBuf.PipelinesLength())
	placeholderPipelineMetadata := new(msgflatbuf.PipelineMetadata)
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
