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
	"fmt"
	"sync"
	"sync/atomic"

	flatbuffers "github.com/google/flatbuffers/go"
)

type resourceBroker struct {
	subs    []chan *flatbuffers.Builder
	counter uint64
	rwlock  sync.RWMutex
}

func newResourceBroker(ctx context.Context) *resourceBroker {
	return &resourceBroker{
		counter: 0,
	}
}

func (b *resourceBroker) Subscribe(msgCh ...chan *flatbuffers.Builder) {
	b.rwlock.Lock()
	defer b.rwlock.Unlock()
	b.subs = append(b.subs, msgCh...)
}

func (b *resourceBroker) Unsubscribe(msgCh ...chan *flatbuffers.Builder) {
	b.rwlock.Lock()
	defer b.rwlock.Unlock()
	for _, ch := range msgCh {
		for idx, existingCh := range b.subs {
			if ch == existingCh {
				b.subs[idx] = b.subs[len(b.subs)-1]
				b.subs = b.subs[:len(b.subs)-1]
			}
		}
	}
}

func (b *resourceBroker) Select(msg *flatbuffers.Builder) {
	b.rwlock.RLock()
	defer b.rwlock.RUnlock()
	if len(b.subs) == 0 {
		return
	}
	idx := atomic.AddUint64(&b.counter, 1) % uint64(len(b.subs))
	b.subs[idx] <- msg
}

func (b *resourceBroker) Publish(msg *flatbuffers.Builder) {
	fmt.Println("@tallen resource broker publishing")
	b.rwlock.RLock()
	defer b.rwlock.RUnlock()
	for _, ch := range b.subs {
		ch <- msg
	}
}
