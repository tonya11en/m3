package writer

import (
	"context"
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"
)

type resourceBroker struct {
	publishCh chan *flatbuffers.Builder
	selectCh  chan *flatbuffers.Builder
	subCh     chan []chan *flatbuffers.Builder
	unsubCh   chan []chan *flatbuffers.Builder
	ctx       context.Context
	cancel    context.CancelFunc
	subs      []chan *flatbuffers.Builder
	running   bool
	counter   uint64
}

func newResourceBroker(ctx context.Context) *resourceBroker {
	ctx, cancel := context.WithCancel(ctx)
	return &resourceBroker{
		subs:      make([]chan *flatbuffers.Builder, 0),
		publishCh: make(chan *flatbuffers.Builder, streamQueueSize),
		selectCh:  make(chan *flatbuffers.Builder, streamQueueSize),
		subCh:     make(chan []chan *flatbuffers.Builder),
		unsubCh:   make(chan []chan *flatbuffers.Builder),
		ctx:       ctx,
		cancel:    cancel,
		running:   false,
		counter:   0,
	}
}

func (b *resourceBroker) Start() error {
	if b.running {
		return fmt.Errorf("calling Start() on running broker")
	}
	b.running = true

	go b.work()

	return nil
}

func (b *resourceBroker) work() {
	for {
		select {
		case <-b.ctx.Done():
			// Termination condition.
			b.running = false
			return

		// Subscribe.
		case subChs := <-b.subCh:
			for _, ch := range subChs {
				b.subs = append(b.subs, ch)
			}

		// Unsubscribe.
		case unsubChs := <-b.unsubCh:
			for _, ch := range unsubChs {
				for idx, existingCh := range b.subs {
					if ch == existingCh {
						b.subs[idx] = b.subs[len(b.subs)-1]
						b.subs = b.subs[:len(b.subs)-1]
					}
				}
			}

		// Publish the *flatbuffers.Builder out to subscribers.
		case msg := <-b.publishCh:
			for _, ch := range b.subs {
				ch <- msg
			}

		// Publish to only a single subscriber.
		case msg := <-b.selectCh:
			idx := b.counter % uint64(len(b.subs))
			b.counter++
			b.subs[idx] <- msg
		}
	}
}

func (b *resourceBroker) Subscribe(msgCh ...chan *flatbuffers.Builder) {
	b.subCh <- msgCh
}

func (b *resourceBroker) Unsubscribe(msgCh ...chan *flatbuffers.Builder) {
	b.unsubCh <- msgCh
}

func (b *resourceBroker) Select(msg *flatbuffers.Builder) {
	b.selectCh <- msg
}

func (b *resourceBroker) Publish(msg *flatbuffers.Builder) {
	b.publishCh <- msg
}

func (b *resourceBroker) PublisherChannel() chan *flatbuffers.Builder {
	return b.publishCh
}

func (b *resourceBroker) Stop() {
	b.cancel()
}
