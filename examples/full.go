package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/go-faker/faker/v4"
	"github.com/morilog/redistream"
	"github.com/redis/go-redis/v9"
)

func main() {
	// connecting to redis server
	rdb := redis.NewClient(&redis.Options{
		Addr: ":6379",
		DB:   0,
	})

	opts := &redistream.ConsumerGroupOptions{
		GroupID:                        "events-consumer-group",
		Topic:                          "events",
		BlockDuration:                  time.Second,
		MessagesBufferSize:             100,
		InitialOffset:                  "0",
		ReclaimPendingMessagesInterval: time.Second * 5,
		ReturnErrors:                   true,
	}

	consumer, err := redistream.NewConsumerGroup(rdb, opts)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	messages, err := consumer.Consume(ctx)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	// process messages
	go func() {
		defer wg.Done()

		for {
			select {
			case <-ctx.Done():
				return
			case m := <-messages:
				slog.Info("message received", "message", string(m.Payload))
				err = consumer.Ack(ctx, m)
				if err != nil {
					slog.Error("ack error", "error", err.Error())
				}
			}
		}
	}()

	// handle incoming errors
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-consumer.Errors():
				slog.Error("consumer error", "error", err.Error())
			}
		}
	}()

	// produce messages
	producer := redistream.NewProducer(rdb)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {

			select {
			case <-ctx.Done():
				return
			default:
				err := producer.Produce(ctx, opts.Topic, []byte(faker.FirstName()))
				if err != nil {
					slog.Error("producer error", "error", err.Error())
				}
			}
		}
	}()

	<-ctx.Done()
	cancel()
	wg.Wait()
}
