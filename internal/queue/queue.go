package queue

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	StreamCritical = "task_queue:critical"
	StreamDefault  = "task_queue:default"
	StreamLow      = "task_queue:low"
	ConsumerGroup  = "workers"
)

type Queue struct {
	rdb *redis.Client
}

func New(rdb *redis.Client) *Queue {
	return &Queue{rdb: rdb}
}

func (q *Queue) EnsureGroup(ctx context.Context) error {
	streams := []string{StreamCritical, StreamDefault, StreamLow}
	for _, s := range streams {
		// Using starting ID "0" forces Redis to read messages from the beginning of the stream.
		err := q.rdb.XGroupCreateMkStream(ctx, s, ConsumerGroup, "0").Err()
		if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
			return err
		}
	}
	return nil
}

func (q *Queue) Publish(ctx context.Context, jobID, jobType, priority string, payload map[string]any) error {
	pBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	stream := StreamDefault
	if priority == "critical" {
		stream = StreamCritical
	} else if priority == "low" {
		stream = StreamLow
	}

	return q.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{
			"job_id":   jobID,
			"type":     jobType,
			"priority": priority,
			"payload":  string(pBytes),
		},
	}).Err()
}

func (q *Queue) Ack(ctx context.Context, streamKey, msgID string) error {
	return q.rdb.XAck(ctx, streamKey, ConsumerGroup, msgID).Err()
}

func (q *Queue) Read(ctx context.Context, consumer string, block time.Duration) ([]redis.XMessage, error) {
	streams := []string{StreamCritical, StreamDefault, StreamLow}
	for i, s := range streams {
		b := block

		res, err := q.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    ConsumerGroup,
			Consumer: consumer,
			Streams:  []string{s, ">"},
			Count:    10,
			Block:    b,
		}).Result()

		if err != nil {
			if err == redis.Nil {
				continue
			}
			return nil, err
		}

		if len(res) > 0 && len(res[0].Messages) > 0 {
			msgs := res[0].Messages
			for j := range msgs {
				msgs[j].Values["_stream"] = s
			}
			return msgs, nil
		}
	}
	return nil, nil
}
