// Copyright (c) 2020 Uber Technologies, Inc.
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

package protobuf

import (
	"runtime"
	"testing"

	"github.com/m3db/m3/src/metrics/policy"
)

func doBenchmarkEncode(b *testing.B, enc AggregatedEncoder, dec AggregatedDecoder) {
	for i := 0; i < b.N; i++ {
		if err := enc.Encode(testAggregatedMetric1, 2000); err != nil {
			b.Fatal(err)
		}

		buf := enc.Buffer()
		buf.Close()
	}
}

func doBenchmarkDecodeStoragePolicy(b *testing.B, enc AggregatedEncoder, dec AggregatedDecoder) {
	var sp policy.StoragePolicy
	if err := enc.Encode(testAggregatedMetric1, 2000); err != nil {
		b.Fatal(err)
	}

	buf := enc.Buffer().Bytes()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = dec.Decode(buf)
		sp = dec.StoragePolicy()
		dec.Close()
	}
	runtime.KeepAlive(sp)
}

func BenchmarkEncodeProtobuf(b *testing.B) {
	enc := NewAggregatedEncoder(nil)
	dec := NewAggregatedDecoder(nil)
	doBenchmarkEncode(b, enc, dec)
}

func BenchmarkEncodeFlatbuf(b *testing.B) {
	enc := NewAggregatedFlatbufEncoder()
	dec := NewAggregatedFlatbufDecoder()
	doBenchmarkEncode(b, enc, dec)
}

func BenchmarkDecodeStoragePolicyProtobuf(b *testing.B) {
	enc := NewAggregatedEncoder(nil)
	dec := NewAggregatedDecoder(nil)
	doBenchmarkDecodeStoragePolicy(b, enc, dec)
}

func BenchmarkDecodeStoragePolicyFlatbuf(b *testing.B) {
	enc := NewAggregatedFlatbufEncoder()
	dec := NewAggregatedFlatbufDecoder()
	doBenchmarkDecodeStoragePolicy(b, enc, dec)
}
