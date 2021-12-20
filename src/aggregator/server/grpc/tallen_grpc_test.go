package grpc

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
	"github.com/m3db/m3/src/aggregator/client"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
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

	srv, err := NewServer("what", mockAgg)
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

	c, err := client.NewGRPCClient()
	assert.Nil(b, err)

	time.Sleep(time.Second)
	fmt.Println("@tallen making counter in test")

	count := unaggregated.Counter{
		ID:              []byte("whatever man"),
		Annotation:      []byte("some annotation"),
		Value:           1337,
		ClientTimeNanos: xtime.Now(),
	}

	b.StartTimer()

	for n := 0; n < b.N; n++ {
		_ = c.WriteUntimedCounter(count, nil)
	}
}
