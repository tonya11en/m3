package msgflatbuf

import (
	"sync"

	flatbuffers "github.com/google/flatbuffers/go"
)

const (
	bufferInitialCapacity = 4096
)

var (
	builderPool = sync.Pool{
		New: func() interface{} {
			return flatbuffers.NewBuilder(bufferInitialCapacity)
		},
	}
)

// Fetches a flatbuffer builder from the pool if one is available, otherwise a new one will be
// allocated and returned. The builders returned are already reset.
func GetBuilder() *flatbuffers.Builder {
	return builderPool.Get().(*flatbuffers.Builder)
}

// Resets and returns a builder to the pool that has no further use.
func ReturnBuilder(b *flatbuffers.Builder) {
	b.Reset()
	builderPool.Put(b)
}
