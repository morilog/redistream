package redistream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
	"unsafe"

	"github.com/redis/go-redis/v9"
	"github.com/spf13/cast"
)

func isConsumerGroupExistsError(err error) bool {
	return "BUSYGROUP Consumer Group name already exists" == err.Error()
}

// ConsumerGroupOptions contains options for the consumer group
type ConsumerGroupOptions struct {
	// GroupID is the name of the consumer group
	GroupID string
	// Topic is the name of the topic
	Topic string
	// BlockDuration is the duration to wait for new messages
	// Default: 1 second
	BlockDuration time.Duration
	// MessagesBufferSize is the size of the messages buffer
	// Default: 100
	MessagesBufferSize int
	// InitialOffset is the initial offset of the consumer group
	InitialOffset string
	// ReclaimPendingMessagesInterval is the interval to reclaim pending messages
	ReclaimPendingMessagesInterval time.Duration
	// ReturnErrors is the flag to return errors
	// Default: false
	// If true, errors will be returned in the Errors() channel
	// and should be handled by the user to avoid deadlock
	ReturnErrors bool
}

// DefaultConsumerGroupOptions is the default options
// Topic and GroupID is empty and should be set on usage
var DefaultConsumerGroupOptions = &ConsumerGroupOptions{
	BlockDuration:                  time.Millisecond * 10,
	MessagesBufferSize:             100,
	InitialOffset:                  "0",
	ReclaimPendingMessagesInterval: time.Second * 5,
	ReturnErrors:                   false,
}

// ConsumerGroup is a consumer group of redis
type ConsumerGroup struct {
	opts     *ConsumerGroupOptions
	rdb      *redis.Client
	closed   chan struct{}
	wg       *sync.WaitGroup
	messages chan Message
	once     sync.Once
	errors   chan error
}

// NewConsumerGroup creates a new consumer group
// It uses DefaultConsumerGroupOptions when not any opts provided
func NewConsumerGroup(rdb *redis.Client, opts ...*ConsumerGroupOptions) (*ConsumerGroup, error) {
	usedOpts := DefaultConsumerGroupOptions
	if len(opts) == 1 {
		usedOpts = opts[0]
	}

	if usedOpts.GroupID == "" {
		return nil, errors.New("group id is required")
	}

	if usedOpts.Topic == "" {
		return nil, errors.New("topic is required")
	}

	return &ConsumerGroup{
		opts:     usedOpts,
		rdb:      rdb,
		wg:       &sync.WaitGroup{},
		messages: make(chan Message, usedOpts.MessagesBufferSize),
		errors:   make(chan error, usedOpts.MessagesBufferSize),
	}, nil
}

// Consume consumes messages from the topic

func (c *ConsumerGroup) Consume(ctx context.Context) (<-chan Message, error) {
	// check the group exists or create it
	err := c.rdb.XGroupCreateMkStream(ctx, c.opts.Topic, c.opts.GroupID, c.opts.InitialOffset).Err()
	if err != nil && !isConsumerGroupExistsError(err) {
		return nil, err
	}

	// the consumer group starts to read from stream periodically
	// It reads pending messages and reclaim them
	// It closes when the context is done
	c.once.Do(func() {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()

			reclaimTimer := time.NewTicker(c.opts.ReclaimPendingMessagesInterval)

			for {
				select {
				case <-ctx.Done():
					c.closed <- struct{}{}
					return
				case <-reclaimTimer.C:
					err = c.reclaimPendingMessages(ctx)
					if err != nil {
						c.handleError(ctx, err)
					}
				default:
					err = c.readStream(ctx)
					if err != nil {
						c.handleError(ctx, err)
					}
				}
			}
		}()
	})

	// And return the messages channel
	// To process by higher level user
	return c.messages, nil
}

func (c *ConsumerGroup) Errors() <-chan error {
	return c.errors
}

func (c *ConsumerGroup) handleError(ctx context.Context, err error) {
	if !c.opts.ReturnErrors {
		return
	}

	select {
	case <-ctx.Done():
		return
	case c.errors <- err:
		return
	default:
		slog.Error("errors buffer is full")
	}
}

func (c *ConsumerGroup) putMessageToChannel(ctx context.Context, m redis.XMessage) error {
	select {
	case <-ctx.Done():
		return nil
	case c.messages <- Message{
		Payload:   stringToBytes(cast.ToString(m.Values["payload"])),
		GroupID:   c.opts.GroupID,
		Topic:     c.opts.Topic,
		ID:        m.ID,
		Timestamp: cast.ToTime(m.Values["timestamp"]),
	}:
		return nil
	default:
		return errors.New("messages buffer is full")
	}
}

func (c *ConsumerGroup) reclaimPendingMessages(ctx context.Context) error {
	xps, err := c.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{Stream: c.opts.Topic, Group: c.opts.GroupID, Start: "0", End: "+", Count: int64(c.opts.MessagesBufferSize)}).Result()
	if err != nil {
		return err
	}

	for _, xp := range xps {
		claimedMessage, err := c.rdb.XClaim(ctx, &redis.XClaimArgs{
			Stream:   c.opts.Topic,
			Group:    c.opts.GroupID,
			Consumer: c.opts.GroupID,
			MinIdle:  time.Hour,
			Messages: []string{xp.ID},
		}).Result()
		if err != nil {
			return err
		}

		if len(claimedMessage) != 1 {
			continue
		}

		if err := c.putMessageToChannel(ctx, claimedMessage[0]); err != nil {
			return err
		}
	}

	return nil
}

func (c *ConsumerGroup) readStream(ctx context.Context) error {
	streams, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    c.opts.GroupID,
		Consumer: c.opts.GroupID,
		Block:    c.opts.BlockDuration,
		Count:    int64(c.opts.MessagesBufferSize),
		Streams:  []string{c.opts.Topic, ">"},
	}).Result()

	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("failed to read stream: %w", err)
	}

	for _, stream := range streams {
		for _, msg := range stream.Messages {
			if err := c.putMessageToChannel(ctx, msg); err != nil {
				c.handleError(ctx, err)
			}
		}
	}
	return nil
}

// Ack acknowledges message
// Every message should be acknowledged after processing
// To avoid double process
func (c *ConsumerGroup) Ack(ctx context.Context, m Message) error {
	return c.rdb.XAck(ctx, c.opts.Topic, c.opts.GroupID, m.ID).Err()
}

// Close closes the consumer group
// and its internal channels
func (c *ConsumerGroup) Close() error {
	<-c.closed
	c.wg.Wait()
	close(c.messages)
	close(c.errors)
	return nil
}

func stringToBytes(s string) (b []byte) {
	return *(*[]byte)(unsafe.Pointer(
		&struct {
			string
			Cap int
		}{s, len(s)},
	))
}
