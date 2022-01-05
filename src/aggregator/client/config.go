// Copyright (c) 2018 Uber Technologies, Inc.
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

package client

import (
	"errors"
	"fmt"
	"time"

	"github.com/m3db/m3/src/aggregator/sharding"
	m3clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/metrics/encoding/protobuf"
	producerconfig "github.com/m3db/m3/src/msg/producer/config"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"
	xio "github.com/m3db/m3/src/x/io"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/retry"

	"github.com/uber-go/tally"
)

var errNoM3MsgOptions = errors.New("m3msg aggregator client: missing m3msg options")

// Configuration contains client configuration.
type Configuration struct {
	Type                       AggregatorClientType            `yaml:"type"`
	M3Msg                      *M3MsgConfiguration             `yaml:"m3msg"`
	GRPC                       *GRPCConfiguration              `yaml:"grpc"`
	PlacementKV                *kv.OverrideConfiguration       `yaml:"placementKV"`
	Watcher                    *placement.WatcherConfiguration `yaml:"placementWatcher"`
	HashType                   *sharding.HashType              `yaml:"hashType"`
	ShardCutoverWarmupDuration *time.Duration                  `yaml:"shardCutoverWarmupDuration"`
	ShardCutoffLingerDuration  *time.Duration                  `yaml:"shardCutoffLingerDuration"`
	Encoder                    EncoderConfiguration            `yaml:"encoder"`
	FlushSize                  int                             `yaml:"flushSize,omitempty"` // FlushSize is deprecated
	FlushWorkerCount           int                             `yaml:"flushWorkerCount"`
	ForceFlushEvery            time.Duration                   `yaml:"forceFlushEvery"`
	MaxBatchSize               int                             `yaml:"maxBatchSize"`
	MaxTimerBatchSize          int                             `yaml:"maxTimerBatchSize"`
	QueueSize                  int                             `yaml:"queueSize"`
	QueueDropType              *DropType                       `yaml:"queueDropType"`
	Connection                 ConnectionConfiguration         `yaml:"connection"`
}

// NewAdminClient creates a new admin client.
func (c *Configuration) NewAdminClient(
	kvClient m3clusterclient.Client,
	clockOpts clock.Options,
	instrumentOpts instrument.Options,
	rwOpts xio.Options,
) (AdminClient, error) {
	client, err := c.NewClient(kvClient, clockOpts, instrumentOpts, rwOpts)
	if err != nil {
		return nil, err
	}
	return client.(AdminClient), nil
}

// NewClient creates a new client.
func (c *Configuration) NewClient(
	kvClient m3clusterclient.Client,
	clockOpts clock.Options,
	instrumentOpts instrument.Options,
	rwOpts xio.Options,
) (Client, error) {
	opts, err := c.newClientOptions(kvClient, clockOpts, instrumentOpts, rwOpts)
	if err != nil {
		return nil, err
	}

	return NewClient(opts)
}

var (
	errLegacyClientNoPlacementKVConfig = errors.New("no placement KV config set")
	errLegacyClientNoWatcherConfig     = errors.New("no placement watcher config set")
)

func (c *Configuration) newClientOptions(
	kvClient m3clusterclient.Client,
	clockOpts clock.Options,
	instrumentOpts instrument.Options,
	rwOpts xio.Options,
) (Options, error) {
	opts := NewOptions().
		SetAggregatorClientType(c.Type).
		SetClockOptions(clockOpts).
		SetInstrumentOptions(instrumentOpts).
		SetRWOptions(rwOpts)

	fmt.Printf("@tallen config: %v\n", c)

	grpcCfg := c.GRPC
	if grpcCfg == nil {
		return nil, fmt.Errorf("no grpc options") // @tallen
	}

	grpcOpts, err := grpcCfg.NewGRPCOptions(kvClient, instrumentOpts, rwOpts)
	if err != nil {
		return nil, err
	}

	// Set the M3Msg options configured.
	fmt.Println("@tallen settings grpc opts")
	opts = opts.SetGRPCOptions(grpcOpts)

	// Validate the options.
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	return opts, nil
}

// ConnectionConfiguration contains the connection configuration.
type ConnectionConfiguration struct {
	ConnectionTimeout            time.Duration        `yaml:"connectionTimeout"`
	ConnectionKeepAlive          *bool                `yaml:"connectionKeepAlive"`
	WriteTimeout                 time.Duration        `yaml:"writeTimeout"`
	InitReconnectThreshold       int                  `yaml:"initReconnectThreshold"`
	MaxReconnectThreshold        int                  `yaml:"maxReconnectThreshold"`
	ReconnectThresholdMultiplier int                  `yaml:"reconnectThresholdMultiplier"`
	MaxReconnectDuration         *time.Duration       `yaml:"maxReconnectDuration"`
	WriteRetries                 *retry.Configuration `yaml:"writeRetries"`
}

// NewConnectionOptions creates new connection options.
func (c *ConnectionConfiguration) NewConnectionOptions(scope tally.Scope) ConnectionOptions {
	opts := NewConnectionOptions()
	if c.ConnectionTimeout != 0 {
		opts = opts.SetConnectionTimeout(c.ConnectionTimeout)
	}
	if c.ConnectionKeepAlive != nil {
		opts = opts.SetConnectionKeepAlive(*c.ConnectionKeepAlive)
	}
	if c.WriteTimeout != 0 {
		opts = opts.SetWriteTimeout(c.WriteTimeout)
	}
	if c.InitReconnectThreshold != 0 {
		opts = opts.SetInitReconnectThreshold(c.InitReconnectThreshold)
	}
	if c.MaxReconnectThreshold != 0 {
		opts = opts.SetMaxReconnectThreshold(c.MaxReconnectThreshold)
	}
	if c.ReconnectThresholdMultiplier != 0 {
		opts = opts.SetReconnectThresholdMultiplier(c.ReconnectThresholdMultiplier)
	}
	if c.MaxReconnectDuration != nil {
		opts = opts.SetMaxReconnectDuration(*c.MaxReconnectDuration)
	}
	if c.WriteRetries != nil {
		retryOpts := c.WriteRetries.NewOptions(scope)
		opts = opts.SetWriteRetryOptions(retryOpts)
	}
	return opts
}

// EncoderConfiguration configures the encoder.
type EncoderConfiguration struct {
	InitBufferSize *int                              `yaml:"initBufferSize"`
	MaxMessageSize *int                              `yaml:"maxMessageSize"`
	BytesPool      *pool.BucketizedPoolConfiguration `yaml:"bytesPool"`
}

// NewEncoderOptions create a new set of encoder options.
func (c *EncoderConfiguration) NewEncoderOptions(
	instrumentOpts instrument.Options,
) protobuf.UnaggregatedOptions {
	opts := protobuf.NewUnaggregatedOptions()
	if c.InitBufferSize != nil {
		opts = opts.SetInitBufferSize(*c.InitBufferSize)
	}
	if c.MaxMessageSize != nil {
		opts = opts.SetMaxMessageSize(*c.MaxMessageSize)
	}
	if c.BytesPool != nil {
		sizeBuckets := c.BytesPool.NewBuckets()
		objectPoolOpts := c.BytesPool.NewObjectPoolOptions(instrumentOpts)
		bytesPool := pool.NewBytesPool(sizeBuckets, objectPoolOpts)
		opts = opts.SetBytesPool(bytesPool)
		bytesPool.Init()
	}
	return opts
}

type GRPCConfiguration struct {
	Producer producerconfig.ProducerConfiguration `yaml:"producer"`
}

func (c *GRPCConfiguration) NewGRPCOptions(
	kvClient m3clusterclient.Client,
	instrumentOpts instrument.Options,
	rwOpts xio.Options,
) (GRPCOptions, error) {
	opts := NewGRPCOptions()

	producer, err := c.Producer.NewProducer(kvClient, instrumentOpts, rwOpts)
	if err != nil {
		return nil, err
	}

	opts = opts.SetProducer(producer)

	// Validate the options.
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	return opts, nil
}

// M3MsgConfiguration contains the M3Msg client configuration, required
// if using M3Msg client type.
type M3MsgConfiguration struct {
	Producer producerconfig.ProducerConfiguration `yaml:"producer"`
}

// NewM3MsgOptions returns new M3Msg options from configuration.
func (c *M3MsgConfiguration) NewM3MsgOptions(
	kvClient m3clusterclient.Client,
	instrumentOpts instrument.Options,
	rwOpts xio.Options,
) (M3MsgOptions, error) {
	opts := NewM3MsgOptions()

	// For M3Msg clients we want to use the default timer options
	// as defined by the default M3Msg options for low overhead
	// timers.
	instrumentOpts = instrumentOpts.SetTimerOptions(opts.TimerOptions())

	producer, err := c.Producer.NewProducer(kvClient, instrumentOpts, rwOpts)
	if err != nil {
		return nil, err
	}

	opts = opts.SetProducer(producer)

	// Validate the options.
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	return opts, nil
}
