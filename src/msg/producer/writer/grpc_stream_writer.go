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
	"io"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/m3db/m3/src/msg/generated/msgflatbuf"
	"google.golang.org/grpc"
)

type grpcStreamWriter struct {
	ctx    context.Context
	cancel context.CancelFunc

	client  msgflatbuf.MessageWriterClient
	conn    *grpc.ClientConn
	address string

	// Messages to be written to the stream. This is passed to the stream writer in the constructor func.
	msgChan <-chan *flatbuffers.Builder

	// Where to push the received acks.
	ackChan chan<- *metadata
}

func newGRPCStreamWriter(ctx context.Context, addr string, msgChan <-chan *flatbuffers.Builder, ackChan chan<- *metadata) (*grpcStreamWriter, error) {
	ctx, cancel := context.WithCancel(ctx)

	gclient := grpcStreamWriter{
		ctx:     ctx,
		cancel:  cancel,
		address: addr,
		msgChan: msgChan,
		ackChan: ackChan,
	}

	fmt.Println("@tallen done making grpc client")
	// TODO more

	return &gclient, nil
}

// Init initializes the consumer writer.
func (w *grpcStreamWriter) Init() {
	fmt.Println("@tallen making grpc client. dialing")
	dopts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithCodec(flatbuffers.FlatbuffersCodec{}),
	}

	conn, err := grpc.Dial(w.address, dopts...)
	if err != nil {
		// TODO: don't panic here...
		panic(err.Error())
	}
	w.conn = conn

	fmt.Println("@tallen done dialing. making new grpc client")

	mrc := msgflatbuf.NewMessageWriterClient(conn)
	w.client = mrc

	go w.connectLoop()
}

func (w *grpcStreamWriter) connectLoop() {
	for {
		select {
		case <-w.ctx.Done():
			fmt.Println("context done, no longer creating streams")
			return
		default:
		}

		err := w.startStream(w.ctx)
		if err != nil {
			fmt.Printf("error encountered during stream: %s\n", err.Error())
			// TODO @tallen: don't sleep if we can help it.
			time.Sleep(500 * time.Millisecond)
			fmt.Printf("retrying stream establishment")
		}
	}
}

func (w *grpcStreamWriter) receiveAcks(
	ctx context.Context,
	stream msgflatbuf.MessageWriter_WriteMessageClient,
	errChan chan error) {

	for {
		select {
		case <-stream.Context().Done():
			return
		default:
		}

		ack, err := stream.Recv()
		if err == io.EOF {
			fmt.Println("stream terminated")
			// This will also signal the sender goroutine to return.
			errChan <- nil
			return
		}
		if err != nil {
			fmt.Printf("error received from stream: %s", err.Error())
			errChan <- err
			return
		}

		w.ackChan <- &metadata{
			metadataKey: metadataKey{
				shard: ack.Shard(),
				id:    ack.Id(),
			},
			sentAtNanos: ack.SentAtNanos(),
		}
	}
}

func (w *grpcStreamWriter) startStream(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	fmt.Printf("@tallen establishing message writer stream for %s\n", w.address)
	defer fmt.Printf("stream terminated for %s\n", w.address)

	stream, err := w.client.WriteMessage(ctx)
	if err != nil {
		fmt.Printf("error creating stream: %s\n", err.Error())
		return err
	}
	defer stream.CloseSend()

	recvErrChan := make(chan error)
	go w.receiveAcks(ctx, stream, recvErrChan)

	for {
		select {
		case <-stream.Context().Done():
			fmt.Printf("stream context cancelled")
			return nil

		case err := <-recvErrChan:
			return err

		case b := <-w.msgChan:
			b.FinishedBytes() // @tallen
			err := stream.Send(b)
			if err != nil {
				fmt.Printf("stream send failed")
				return err
			}
		}
	}
}

// Address returns the consumer address.
func (w *grpcStreamWriter) Address() string {
	return w.address
}

// Close closes the consumer writer.
func (w *grpcStreamWriter) Close() {
	fmt.Println("closing stream writer")
	w.cancel()
}

func (w *grpcStreamWriter) MessageChannel() <-chan *flatbuffers.Builder {
	return w.msgChan
}
