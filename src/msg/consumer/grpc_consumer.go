package consumer

import (
	"fmt"
	"sync"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/m3db/m3/src/msg/generated/msgflatbuf"
)

var (
	bufferPool = sync.Pool{
		New: func() interface{} {
			return new([]byte)
		},
	}

	unionTablePool = sync.Pool{
		New: func() interface{} {
			return new(flatbuffers.Table)
		},
	}
)

type msgResult struct {
	msg Message
	err error
}

type grpcConsumer struct {
	addr    string
	srv     *GrpcConsumerServer
	msgChan chan *msgResult
}

func newGRPCConsumer(addr string) Consumer {
	c := &grpcConsumer{
		addr:    addr,
		msgChan: make(chan *msgResult),
	}

	srv, err := NewGRPCConsumerServer(addr, c.processMsg)
	if err != nil {
		panic(err)
	}

	c.srv = srv
	return c
}

func (c *grpcConsumer) Init() {
	go func() {
		fmt.Println("@tallen starting consumer grpc server")
		err := c.srv.ListenAndServe()
		if err != nil {
			panic(err)
		}
	}()
}

func (c *grpcConsumer) Message() (Message, error) {
	result := <-c.msgChan
	return result.msg, result.err
}

func (c *grpcConsumer) processMsg(msg *msgflatbuf.Message) chan *AckInfo {
	unionTable := unionTablePool.Get().(*flatbuffers.Table)
	defer unionTablePool.Put(unionTable)

	if !msg.Value(unionTable) {
		panic("@tallen no value..")
	}

	ackInfoChan := make(chan *AckInfo, 1)

	c.msgChan <- &msgResult{
		msg: newFlatbufMessage(unionTable.Bytes, msg.Id(), msg.Shard(), msg.SentAtNanos(), ackInfoChan),
		err: nil,
	}

	return ackInfoChan
}

func (c *grpcConsumer) Close() {
}

type grpcMessage struct {
	b           []byte
	shardID     uint64
	id          uint64
	sentAtNanos uint64
	ackInfoChan chan *AckInfo
}

func newFlatbufMessage(b []byte, id uint64, shardID uint64, sentAtNanos uint64, ackInfoChan chan *AckInfo) Message {
	buf := bufferPool.Get().([]byte)
	copy(buf, b)

	return &grpcMessage{
		b:           b,
		id:          id,
		shardID:     shardID,
		sentAtNanos: sentAtNanos,
		ackInfoChan: ackInfoChan,
	}
}

func (m *grpcMessage) Bytes() []byte {
	return m.b
}

func (m *grpcMessage) Ack() {
	m.ackInfoChan <- &AckInfo{
		ID:          m.id,
		SentAtNanos: m.sentAtNanos,
		ShardID:     m.shardID,
	}
}

func (m *grpcMessage) ShardID() uint64 {
	return m.shardID
}

func (m *grpcMessage) SentAtNanos() uint64 {
	return m.sentAtNanos
}
