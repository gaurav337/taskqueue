package queue

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

const (
	StreamCritical  = "task_queue:critical"
	StreamDefault   = "task_queue:default"
	StreamLow       = "task_queue:low"
	StreamScheduled = "task_queue:scheduled"
	StreamDLQ       = "task_queue:dlq"
	ConsumerGroup   = "workers"
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

	values := map[string]any{
		"job_id":   jobID,
		"type":     jobType,
		"priority": priority,
		"payload":  string(pBytes),
	}

	// Propagate W3C trace context
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	for k, v := range carrier {
		values[k] = v
	}

	return q.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: values,
	}).Err()
}

func (q *Queue) Ack(ctx context.Context, streamKey, msgID string) error {
	return q.rdb.XAck(ctx, streamKey, ConsumerGroup, msgID).Err()
}

func (q *Queue) Read(ctx context.Context, consumer string, block time.Duration) ([]redis.XMessage, error) {
	streams := []string{StreamCritical, StreamDefault, StreamLow}
	for i, s := range streams {
		// Use -1 to omit the BLOCK parameter, executing a fast, non-blocking check on empty streams
		b := time.Duration(-1)
		if i == 2 {
			b = block
		}

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

var dueJobsScript = redis.NewScript(`
	local jobs = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 100)
	if #jobs > 0 then
		redis.call('ZREM', KEYS[1], unpack(jobs))
	end
	return jobs
`)

func (q *Queue) Schedule(ctx context.Context, jobID string, runAt time.Time) error {
	return q.rdb.ZAdd(ctx, StreamScheduled, redis.Z{
		Score:  float64(runAt.Unix()),
		Member: jobID,
	}).Err()
}

func (q *Queue) DueJobs(ctx context.Context) ([]string, error) {
	now := strconv.FormatInt(time.Now().Unix(), 10)
	res, err := dueJobsScript.Run(ctx, q.rdb, []string{StreamScheduled}, now).StringSlice()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	return res, nil
}

func (q *Queue) PublishDLQ(ctx context.Context, j interface{}) error {
	data, err := json.Marshal(j)
	if err != nil {
		return err
	}
	return q.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: StreamDLQ,
		Values: map[string]any{"data": string(data)},
	}).Err()
}

var ReclaimAfter = 30 * time.Second

func (q *Queue) Reclaim(ctx context.Context, consumer string) ([]redis.XMessage, error) {
	var allMsgs []redis.XMessage
	streams := []string{StreamCritical, StreamDefault, StreamLow}
	for _, s := range streams {
		msgs, _, err := q.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   s,
			Group:    ConsumerGroup,
			Consumer: consumer,
			MinIdle:  ReclaimAfter,
			Start:    "0-0",
			Count:    10,
		}).Result()
		if err != nil && err != redis.Nil {
			return nil, err
		}
		for i := range msgs {
			msgs[i].Values["_stream"] = s
		}
		allMsgs = append(allMsgs, msgs...)
	}
	return allMsgs, nil
}

