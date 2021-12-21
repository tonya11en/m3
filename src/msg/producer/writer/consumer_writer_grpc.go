package writer

import (
	"fmt"
	"sync"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/m3db/m3/src/msg/generated/msgflatbuf"
	"google.golang.org/grpc"
)

const (
	defaultFlatbufSize = 4096
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

type gRPCConsumerWriter struct {
	client  msgflatbuf.MessageWriterClient
	conn    *grpc.ClientConn
	address string
	stream  msgflatbuf.MessageWriter_WriteMessageClient
}

func newGRPCConsumerWriter(addr string, opts Options) (*gRPCConsumerWriter, error) {

	fmt.Println("@tallen making grpc client. dialing")
	dopts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithCodec(flatbuffers.FlatbuffersCodec{}),
	}

	// TODO: hardcoding server address
	conn, err := grpc.Dial(addr, dopts...)
	if err != nil {
		return nil, err
	}
	fmt.Println("@tallen done dialing. making new grpc client")

	mrc := msgflatbuf.NewMessageWriterClient(conn)
	gclient := gRPCConsumerWriter{
		client:  mrc,
		conn:    conn,
		address: addr,
	}

	fmt.Println("@tallen done making grpc client")
	// TODO more

	return &gclient, nil
}

func (w *gRPCConsumerWriter) initiateStream() {
  go func() {
    // todo toil
	}
}

// Address returns the consumer address.
func (w *gRPCConsumerWriter) Address() string {
	return w.address
}

// Write writes the bytes. The connection index doesn't matter for our purposes.
func (w *gRPCConsumerWriter) Write(int, buf []byte, m *message) error {
	b := getBuilder()
	defer returnBuilder(b)

	valOffset := b.CreateByteVector(buf)

	msgflatbuf.MessageStart(b)
	msgflatbuf.MessageAddId(b, m.pb.Metadata.GetId())
	msgflatbuf.MessageAddShard(b, m.pb.GetMetadata().Shard)
	msgflatbuf.MessageAddValue(b, valOffset)
	now := time.Now().UnixNano()
	msgflatbuf.MessageAddSentAtNanos(b, uint64(now))
	offset := msgflatbuf.MessageEnd(b)
	b.Finish(offset)

	return w.stream.Send(b)
}

// Init initializes the consumer writer.
func (w *gRPCConsumerWriter) Init() {
	// todo
}

// Close closes the consumer writer.
func (w *gRPCConsumerWriter) Close() {
	// todo
}
