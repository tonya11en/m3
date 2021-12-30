package client

import "github.com/m3db/m3/src/msg/producer"

type GRPCOptions interface {
	Validate() error

	SetProducer(value producer.Producer) GRPCOptions

	Producer() producer.Producer
}

type grpcOptions struct {
	producer producer.Producer
}

func NewGRPCOptions() GRPCOptions {
	return &grpcOptions{}
}

func (o *grpcOptions) Validate() error {
	return nil
}

func (o *grpcOptions) SetProducer(value producer.Producer) GRPCOptions {
	opts := *o
	opts.producer = value
	return &opts
}

func (o *grpcOptions) Producer() producer.Producer {
	return o.producer
}
