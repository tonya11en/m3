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

package writer

import (
	"context"
	"sync"
	"testing"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/assert"
)

func TestSubscribe(t *testing.T) {
	broker := newResourceBroker(context.Background())
	assert.Nil(t, broker.Start())
	defer broker.Stop()

	s1 := make(chan *flatbuffers.Builder, 4)
	s2 := make(chan *flatbuffers.Builder, 4)
	s3 := make(chan *flatbuffers.Builder, 4)
	broker.Subscribe(s1)
	broker.Subscribe(s2)
	broker.Subscribe(s3)

	rfoo := flatbuffers.NewBuilder(0)
	rbar := flatbuffers.NewBuilder(0)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		for i := 0; i < 1000; i++ {
			broker.Publish(rfoo)
			broker.Publish(rbar)
		}
		wg.Done()
	}()

	fooCount := []int{0, 0, 0}
	barCount := []int{0, 0, 0}

	go func() {
		for i := 0; i < 2000; i++ {
			s1r := <-s1
			s2r := <-s2
			s3r := <-s3

			if s1r == rfoo {
				fooCount[0]++
			} else {
				barCount[0]++
			}
			if s2r == rfoo {
				fooCount[1]++
			} else {
				barCount[1]++
			}
			if s3r == rfoo {
				fooCount[2]++
			} else {
				barCount[2]++
			}
		}
		wg.Done()
	}()
	wg.Wait()

	for i := 0; i < 3; i++ {
		assert.Equal(t, 1000, fooCount[i])
		assert.Equal(t, 1000, barCount[i])
	}
}

func TestUnsubscribe(t *testing.T) {
	broker := newResourceBroker(context.Background())
	assert.Nil(t, broker.Start())
	defer broker.Stop()

	s1 := make(chan *flatbuffers.Builder, 4)
	s2 := make(chan *flatbuffers.Builder, 4)
	s3 := make(chan *flatbuffers.Builder, 4)
	broker.Subscribe(s1)
	broker.Subscribe(s2)
	broker.Subscribe(s3)

	rfoo := flatbuffers.NewBuilder(0)
	broker.Publish(rfoo)

	_, ok := <-s1
	assert.Equal(t, len(s1), 0)
	assert.True(t, ok)

	_, ok = <-s2
	assert.Equal(t, len(s2), 0)
	assert.True(t, ok)

	_, ok = <-s3
	assert.Equal(t, len(s3), 0)
	assert.True(t, ok)

	broker.Unsubscribe(s2)
	broker.Publish(rfoo)

	_, ok = <-s1
	assert.Equal(t, len(s1), 0)
	assert.True(t, ok)

	// s2 was unsubscribed, so there's no need to pull from channel.
	assert.Equal(t, len(s2), 0)

	_, ok = <-s3
	assert.Equal(t, len(s3), 0)
	assert.True(t, ok)
}

func TestDoubleStart(t *testing.T) {
	broker := newResourceBroker(context.TODO())
	assert.Nil(t, broker.Start())

	assert.NotNil(t, broker.Start())
}

func broadcastRunner(i int, b *testing.B) {
	broker := newResourceBroker(context.TODO())
	broker.Start()

	// Setup.
	for n := 0; n < i; n++ {
		ch := make(chan *flatbuffers.Builder)
		go func() {
			for {
				<-ch
			}
		}()

		broker.Subscribe(ch)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		broker.Publish(nil)
	}
}

func BenchmarkBroadcast1(b *testing.B) {
	broadcastRunner(1, b)
}

func BenchmarkBroadcast2(b *testing.B) {
	broadcastRunner(2, b)
}

func BenchmarkBroadcast4(b *testing.B) {
	broadcastRunner(4, b)
}

func BenchmarkBroadcast100(b *testing.B) {
	broadcastRunner(100, b)
}

func BenchmarkBroadcast1000(b *testing.B) {
	broadcastRunner(1000, b)
}

func BenchmarkBroadcast10000(b *testing.B) {
	broadcastRunner(10000, b)
}

func BenchmarkBroadcast100000(b *testing.B) {
	broadcastRunner(100000, b)
}
