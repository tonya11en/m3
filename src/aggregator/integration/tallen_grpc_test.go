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

package integration

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/m3db/m3/src/aggregator/aggregator"
	agg_client "github.com/m3db/m3/src/aggregator/client"
	agg_grpc "github.com/m3db/m3/src/aggregator/server/grpc"
	"github.com/m3db/m3/src/aggregator/server/m3msg"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/msg/consumer"
	"github.com/m3db/m3/src/msg/producer"
	"github.com/m3db/m3/src/msg/producer/buffer"
	"github.com/m3db/m3/src/msg/producer/writer"
	"github.com/m3db/m3/src/x/instrument"
	xserver "github.com/m3db/m3/src/x/server"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"
)

func BenchmarkGRPCStuff(b *testing.B) {
	b.StopTimer()
	fmt.Println("@tallen benching")

	const bufSize = 1024 * 1024
	lis := bufconn.Listen(bufSize)

	controller := xtest.NewController(b)
	mockAgg := aggregator.NewMockAggregator(controller)
	mockAgg.EXPECT().AddUntimed(gomock.Any(), gomock.Any()).AnyTimes()

	srv, err := agg_grpc.NewServer("localhost:99011", mockAgg)
	assert.Nil(b, err, "failed to make server")

	go func() {
		fmt.Println("@tallen in teh goroutine")
		err = srv.Serve(lis)
		assert.Nil(b, err, "failed to listen/serve")
	}()

	bufDialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	conn, err := grpc.DialContext(context.Background(), "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	assert.Nil(b, err)
	b.Cleanup(func() { conn.Close() })

	wopts := writer.NewOptions()
	wopts.SetUseGRPC(true)

	bopts := buffer.NewOptions()
	bufr, err := buffer.NewBuffer(bopts)

	w := writer.NewWriter(wopts)
	popts := producer.NewOptions().SetWriter(w).SetBuffer(bufr)
	pr := producer.NewProducer(popts)

	grpcOpts := agg_client.NewGRPCOptions().SetProducer(pr)
	copts := agg_client.NewOptions().SetGRPCOptions(grpcOpts)
	c, err := agg_client.NewGRPCClient(copts)
	assert.Nil(b, err)

	time.Sleep(time.Second)
	fmt.Println("@tallen making counter in test")

	count := unaggregated.Counter{
		ID:              []byte("whatever man"),
		Annotation:      []byte("some annotation"),
		Value:           1337,
		ClientTimeNanos: xtime.Now(),
	}
	mdatas := make(metadata.StagedMetadatas, 0)

	b.StartTimer()

	for n := 0; n < b.N; n++ {
		_ = c.WriteUntimedCounter(count, mdatas)
	}
}

/*
//func BenchmarkGRPCSrv(b *testing.B) {
func TestGRPCSrv(b *testing.T) {
	var err error

	controller := xtest.NewController(b)
	mockAgg := aggregator.NewMockAggregator(controller)
	mockAgg.EXPECT().AddUntimed(gomock.Any(), gomock.Any()).AnyTimes()

	srv, err := agg_grpc.NewServer("localhost:11337", mockAgg)
	assert.Nil(b, err, "failed to make server")
	if err != nil {
		b.Fatal(err.Error())
	}

	go func() {
		assert.Nil(b, srv.ListenAndServe())
	}()

	grpcOpts := agg_client.NewGRPCOptions()
	copts := agg_client.NewOptions().SetGRPCOptions(grpcOpts)
	client, err := agg_client.NewGRPCClient(copts)
	assert.Nil(b, err)

	builder := flatbuffers.NewBuilder(4096)
	anno := builder.CreateByteString([]byte("anno"))
	id := builder.CreateByteString([]byte("some_id"))

	msgflatbuf.CounterWithMetadatasStart(builder)
	msgflatbuf.CounterWithMetadatasAddAnnotation(builder, anno)
	msgflatbuf.CounterWithMetadatasAddId(builder, id)
	msgflatbuf.CounterWithMetadatasAddClientTimeNanos(builder, 1234566)
	msgflatbuf.CounterWithMetadatasAddValue(builder, 1337)
	counter := msgflatbuf.CounterWithMetadatasEnd(builder)

	msgflatbuf.MessageStart(builder)
	msgflatbuf.MessageAddId(builder, 12345)
	msgflatbuf.MessageAddMsgValue(builder, counter)
	msgflatbuf.MessageAddValueType(builder, msgflatbuf.MessageValueCounterWithMetadatas)
	msgflatbuf.MessageAddShard(builder, 0)
	msgflatbuf.MessageAddSentAtNanos(builder, 0)
	offset := msgflatbuf.MessageEnd(builder)

	builder.Finish(offset)

	assert.Nil(b, client.Init())
	unaggCounter := unaggregated.Counter{
		ID:    []byte("some id"),
		Value: 6969.0,
	}

	err = client.WriteUntimedCounter(unaggCounter, nil)
	assert.Nil(b, err)
}
*/

func BenchmarkM3MsgClient(b *testing.B) {
	var err error
	b.StopTimer()

	controller := xtest.NewController(b)
	mockAgg := aggregator.NewMockAggregator(controller)
	mockAgg.EXPECT().AddUntimed(gomock.Any(), gomock.Any()).AnyTimes()

	const bufSize = 1024 * 1024
	lis := bufconn.Listen(bufSize)

	instrumentOpts := instrument.NewOptions()
	xserveropts := xserver.NewOptions()
	serverOpts := m3msg.NewOptions().
		SetServerOptions(xserver.NewOptions()).
		SetConsumerOptions(consumer.NewOptions()).
		SetInstrumentOptions(instrumentOpts).SetServerOptions(xserveropts)

	srv, err := m3msg.NewServer("what", mockAgg, serverOpts)
	if err != nil {
		b.Fatal(err.Error())
	}

	go func() {
		fmt.Println("@tallen in teh goroutine")
		err = srv.Serve(lis)
		if err != nil {
			panic(err.Error())
		}
	}()

	pr := producer.NewMockProducer(controller)
	pr.EXPECT().Close(gomock.Any()).AnyTimes()
	pr.EXPECT().Init().AnyTimes()
	pr.EXPECT().NumShards().Return(uint32(1)).AnyTimes()
	pr.EXPECT().Produce(gomock.Any()).AnyTimes()

	cm3MsgOpts := agg_client.NewM3MsgOptions().SetProducer(pr)
	copts := agg_client.NewOptions().
		SetM3MsgOptions(cm3MsgOpts)
	copts.SetM3MsgOptions(cm3MsgOpts)

	c, err := agg_client.NewM3MsgClient(copts)
	if err != nil {
		b.Fatal(err.Error())
	}

	time.Sleep(time.Second)
	fmt.Println("@tallen making counter in test")

	count := unaggregated.Counter{
		ID:              []byte("whatever man"),
		Annotation:      []byte("some annotation"),
		Value:           1337,
		ClientTimeNanos: xtime.Now(),
	}

	metadatas := metadata.StagedMetadatas{}

	b.StartTimer()

	for n := 0; n < b.N; n++ {
		err = c.WriteUntimedCounter(count, metadatas)
		b.StopTimer()
		if err != nil {
			b.Fatal(err.Error())
		}
		b.StartTimer()
	}
}

/*
func BenchmarkM3MsgSrv(b *testing.B) {
	var err error
	b.StopTimer()

	controller := xtest.NewController(b)
	mockAgg := aggregator.NewMockAggregator(controller)
	mockAgg.EXPECT().AddUntimed(gomock.Any(), gomock.Any()).AnyTimes()

	const bufSize = 1024 * 1024
	lis := bufconn.Listen(bufSize)

	instrumentOpts := instrument.NewOptions()
	xserveropts := xserver.NewOptions()
	serverOpts := m3msg.NewOptions().
		SetServerOptions(xserver.NewOptions()).
		SetConsumerOptions(consumer.NewOptions()).
		SetInstrumentOptions(instrumentOpts).SetServerOptions(xserveropts)

	srv, err := m3msg.NewServer("what", mockAgg, serverOpts)
	if err != nil {
		b.Fatal(err.Error())
	}

	go func() {
		fmt.Println("@tallen in teh goroutine")
		err = srv.Serve(lis)
		if err != nil {
			panic(err.Error())
		}
	}()

	pr := producer.NewMockProducer(controller)
	pr.EXPECT().Close(gomock.Any()).AnyTimes()
	pr.EXPECT().Init().AnyTimes()
	pr.EXPECT().NumShards().Return(uint32(1)).AnyTimes()
	pr.EXPECT().Produce(gomock.Any()).AnyTimes()

	cm3MsgOpts := agg_client.NewM3MsgOptions().SetProducer(pr)
	copts := agg_client.NewOptions().
		SetM3MsgOptions(cm3MsgOpts)
	copts.SetM3MsgOptions(cm3MsgOpts)

	time.Sleep(time.Second)
	fmt.Println("@tallen making counter in test")

	pb := metricpb.MetricWithMetadatas{
		Type: metricpb.MetricWithMetadatas_COUNTER_WITH_METADATAS,
		CounterWithMetadatas: &metricpb.CounterWithMetadatas{
			Metadatas: metricpb.StagedMetadatas{},
			Counter: metricpb.Counter{
				Id:              []byte("ididid"),
				Value:           1337,
				Annotation:      []byte("anno"),
				ClientTimeNanos: 99999999999,
			},
		},
	}

	pbbytes, err := pb.Marshal()
	if err != nil {
		panic(err.Error())
	}
	msg := consumer.NewMockMessage(controller)
	msg.EXPECT().Ack().AnyTimes()
	msg.EXPECT().Bytes().Return(pbbytes).AnyTimes()
	msg.EXPECT().SentAtNanos().AnyTimes()
	msg.EXPECT().ShardID().Return(uint64(0)).AnyTimes()

	logger := zap.NewNop()
	proc := m3msg.NewMessageProcessorHax(mockAgg, logger)
	b.StartTimer()

	for n := 0; n < b.N; n++ {
		proc.Process(msg)
	}
}
*/
