package redistream

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type Producer struct {
	rdb *redis.Client
}

func NewProducer(rdb *redis.Client) *Producer {
	return &Producer{
		rdb: rdb,
	}
}

func (p *Producer) Produce(ctx context.Context, topic string, payload []byte) error {
	return p.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream:     topic,
		MaxLen:     10000,
		MinID:      "~",
		Approx:     true,
		NoMkStream: false,
		Values:     []interface{}{"payload", payload, "timestamp", time.Now().Format(time.RFC3339Nano)},
	}).Err()
}
