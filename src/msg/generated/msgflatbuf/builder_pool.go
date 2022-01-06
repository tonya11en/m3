// Copyright (c) 2021 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
