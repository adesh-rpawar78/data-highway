// This file only compiles when you build with `-tags redis`, so the
// core project stays dependency-free by default. To use it:
//
//	go get github.com/redis/go-redis/v9
//	go build -tags redis ./...
package store

import (
	"context"
	"encoding/json"
	"fmt"

	"datahighway/internal/model"

	"github.com/redis/go-redis/v9"
)

const eventsKey = "datahighway:events"

type RedisStore struct {
	client *redis.Client
}

func NewRedisStore(addr, password string, db int) (*RedisStore, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("connect to redis: %w", err)
	}
	return &RedisStore{client: client}, nil
}

func (r *RedisStore) Save(ctx context.Context, e model.Event) error {
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	if err := r.client.RPush(ctx, eventsKey, data).Err(); err != nil {
		return fmt.Errorf("rpush event: %w", err)
	}
	return nil
}

func (r *RedisStore) Count() int {
	n, err := r.client.LLen(context.Background(), eventsKey).Result()
	if err != nil {
		return 0
	}
	return int(n)
}

func (r *RedisStore) All() []model.Event {
	vals, err := r.client.LRange(context.Background(), eventsKey, 0, -1).Result()
	if err != nil {
		return nil
	}
	events := make([]model.Event, 0, len(vals))
	for _, v := range vals {
		var e model.Event
		if json.Unmarshal([]byte(v), &e) == nil {
			events = append(events, e)
		}
	}
	return events
}

func (r *RedisStore) Close() error {
	return r.client.Close()
}
