package redistream

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Producer is a redis stream producer
type Producer struct {
	rdb *redis.Client
}

// NewProducer creates a new producer
func NewProducer(rdb *redis.Client) *Producer {
	return &Producer{
		rdb: rdb,
	}
}

// Produce produces a new message to the specified topic
// When opts is not provided DefaultProducerOptions is used by default
// To bring your options use the ProducerOptions struct
func (p *Producer) Produce(ctx context.Context, topic string, payload []byte, opts ...*ProducerOptions) error {
	usedOpts := DefaultProducerOptions
	if len(opts) == 1 {
		usedOpts = opts[0]
	}

	return p.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream:     topic,
		MaxLen:     int64(usedOpts.StreamLength),
		Approx:     !usedOpts.ExactLength,
		NoMkStream: usedOpts.CreateStreamIfNotExists,
		Values:     []interface{}{"payload", payload, "timestamp", time.Now().Format(time.RFC3339Nano)},
	}).Err()
}

// ProducerOptions contains options for the producer
type ProducerOptions struct {
	// StreamLength is the max length of the stream
	StreamLength int
	// ExactLength specifies if the stream length should be exact
	ExactLength bool
	// CreateStreamIfNotExists specifies if the stream should be created if it doesn't exist
	CreateStreamIfNotExists bool
}

// DefaultProducerOptions is the default producer options
var DefaultProducerOptions = &ProducerOptions{
	StreamLength:            10000,
	ExactLength:             false,
	CreateStreamIfNotExists: true,
}
