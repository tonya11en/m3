package writer

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/m3db/m3/src/msg/generated/msgflatbuf"
	"github.com/m3db/m3/src/msg/producer"
	"google.golang.org/grpc"
)

const (
	// The amount of memory pre-allocated for a flatbuffer builder upon creation. This is a starting
	// point and the builder will expand as needed.
	defaultFlatbufSize = 4096

	// Number of requests that can be queued and waiting to be sent over the gRPC stream.
	reqStreamQueueSize = 64
)

var (
	builderPool = sync.Pool{
		New: func() interface{} {
			// TODO don't hardcode
			builder := flatbuffers.NewBuilder(defaultFlatbufSize)
			return builder
		},
	}
)

func getBuilder() *flatbuffers.Builder {
	return builderPool.Get().(*flatbuffers.Builder)
}

// Resets and returns a builder to the pool that has no further use.
func returnBuilder(b *flatbuffers.Builder) {
	b.Reset()
	builderPool.Put(b)
}

type grpcStreamWriter struct {
	ctx     context.Context
	cancel  context.CancelFunc
	client  msgflatbuf.MessageWriterClient
	conn    *grpc.ClientConn
	address string

	inboundWrites chan *flatbuffers.Builder
	inboundAcks   chan *msgflatbuf.Ack
}

func newGRPCStreamWriter(ctx context.Context, addr string) (*grpcStreamWriter, error) {
	gclient := grpcStreamWriter{
		ctx:           ctx,
		address:       addr,
		inboundWrites: make(chan *flatbuffers.Builder, reqStreamQueueSize),
		inboundAcks:   make(chan *msgflatbuf.Ack, reqStreamQueueSize),
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

	go func() {
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
	}()
}

func (w *grpcStreamWriter) receiveAcks(
	ctx context.Context,
	stream msgflatbuf.MessageWriter_WriteMessageClient,
	errChan chan error) {

	for {
		in, err := stream.Recv()
		if err == io.EOF {
			errChan <- nil
			return
		}
		if err != nil {
			fmt.Printf("error received from stream: %s", err.Error())
			errChan <- err
			return
		}

		w.inboundAcks <- in
	}
}

func (w *grpcStreamWriter) startStream(ctx context.Context) error {
	fmt.Printf("establishing message writer stream for %s\n", w.address)
	defer fmt.Printf("stream terminated for %s\n", w.address)

	stream, err := w.client.WriteMessage(ctx)
	if err != nil {
		fmt.Printf("error creating stream: %s\n", err.Error())
		return err
	}

	recvErrChan := make(chan error)
	go w.receiveAcks(ctx, stream, recvErrChan)

	for {
		select {
		case <-w.ctx.Done():
			fmt.Printf("writer closing")
			return nil
		case <-stream.Context().Done():
			fmt.Printf("stream context cancelled")
			return nil
		case err := <-recvErrChan:
			// NOTE: there could be writes/acks queued up and these will need to be handled!
			fmt.Println("stream terminated by server")
			return err
		case b := <-w.inboundWrites:
			// These come from the calls to Write().
			err := stream.Send(b)
			if err != nil {
				fmt.Printf("stream send failed")
				return err
			}
		case <-w.inboundAcks:
			// These come from the server.

			// TODODODODODOD this is a pickle... acks don't do anything until this is all moved into the
			// message writer, not the consumer writer.
		}
	}
}

// Address returns the consumer address.
func (w *grpcStreamWriter) Address() string {
	return w.address
}

func (w *grpcStreamWriter) SendOnStream(msg *producer.RefCountedMessage) {
	for {
		select {
		case <-w.ctx.Done():
			fmt.Printf("stream writer halting ingest")
		case w.inboundWrites <- msg:
	}
}

// Close closes the consumer writer.
func (w *grpcStreamWriter) Close() {
}
