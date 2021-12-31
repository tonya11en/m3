//go:build integration
// +build integration

// Copyright (c) 2016 Uber Technologies, Inc.
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
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/aggregator/aggregator"
	grpc_srv "github.com/m3db/m3/src/aggregator/server/grpc"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/metrics/metric"
)

func TestMultiClientOneTypeWithStagedMetadatas(t *testing.T) {
	metadataFn := func(int) metadataUnion {
		return metadataUnion{
			mType:           stagedMetadatasType,
			stagedMetadatas: testStagedMetadatas,
		}
	}
	testMultiClientOneType(t, metadataFn)
}

func testMultiClientOneType(t *testing.T, metadataFn metadataFn) {
	fmt.Println("@tallen hi its the test")
	if testing.Short() {
		t.SkipNow()
	}

	serverOpts := newTestServerOptions(t)

	// Clock setup.
	clock := newTestClock(time.Now().Truncate(time.Hour))
	serverOpts = serverOpts.SetClockOptions(clock.Options())

	// Placement setup.
	numShards := 8
	cfg := placementInstanceConfig{
		instanceID:          serverOpts.InstanceID(),
		shardSetID:          serverOpts.ShardSetID(),
		shardStartInclusive: 0,
		shardEndExclusive:   uint32(numShards),
	}
	instance := cfg.newPlacementInstance()
	placement := newPlacement(numShards, []placement.Instance{instance})
	placementKey := serverOpts.PlacementKVKey()
	setPlacement(t, placementKey, serverOpts.ClusterClient(), placement)

	serverOpts = setupTopic(t, serverOpts, placement)

	// Create server.
	fmt.Println("@tallen hi 1")
	testServer := newTestServerSetup(t, serverOpts)
	defer testServer.close()

	agg := aggregator.NewAggregator(testServer.aggregatorOpts)
	//grpcSrv, err := grpc_srv.NewServer("localhost:6002", agg)
	grpcSrv, err := grpc_srv.NewServer("localhost:13377", agg)
	if err != nil {
		panic(err.Error())
	}

	fmt.Println("@tallen hi 2")
	go func() {
		fmt.Println("@tallen trying to start the server")
		err := grpcSrv.ListenAndServe()
		if err != nil {
			panic(err.Error())
		}
	}()

	var (
		idPrefix   = "foo"
		numIDs     = 100
		start      = clock.Now()
		stop       = start.Add(10 * time.Second)
		interval   = time.Second
		numClients = 10
		clients    = make([]*client, numClients)
	)
	for i := 0; i < numClients; i++ {
		fmt.Println("@tallen test client ", i)
		clients[i] = testServer.newClient(t)
		require.NoError(t, clients[i].connect())
	}

	ids := generateTestIDs(idPrefix, numIDs)
	typeFn := constantMetricTypeFnFactory(metric.CounterType)
	dataset := mustGenerateTestDataset(t, datasetGenOpts{
		start:        start,
		stop:         stop,
		interval:     interval,
		ids:          ids,
		category:     untimedMetric,
		typeFn:       typeFn,
		valueGenOpts: defaultValueGenOpts,
		metadataFn:   metadataFn,
	})

	fmt.Println("@tallen 3")

	for _, data := range dataset {
		clock.SetNow(data.timestamp)
		for _, mm := range data.metricWithMetadatas {
			// Randomly pick one client to write the metric.
			client := clients[rand.Int63n(int64(numClients))]
			require.NoError(t, client.writeUntimedMetricWithMetadatas(mm.metric.untimed, mm.metadata.stagedMetadatas))
		}
		for _, client := range clients {
			require.NoError(t, client.flush())
		}

		// Give server some time to process the incoming packets.
		time.Sleep(100 * time.Millisecond)
	}

	// Move time forward and wait for ticking to happen. The sleep time
	// must be the longer than the lowest resolution across all policies.
	finalTime := stop.Add(6 * time.Second)
	clock.SetNow(finalTime)
	time.Sleep(4 * time.Second)

	for i := 0; i < numClients; i++ {
		require.NoError(t, clients[i].close())
	}
}
