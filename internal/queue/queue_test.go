package queue_test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/gaurav337/taskqueue/internal/queue"
)

func TestQueue_PublishAndAck(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("failed to connect to redis: %v", err)
	}

	// Cleanup keys before/after
	cleanUp := func() {
		rdb.Del(ctx, queue.StreamCritical, queue.StreamDefault, queue.StreamLow)
	}
	cleanUp()
	defer cleanUp()

	q := queue.New(rdb)
	if err := q.EnsureGroup(ctx); err != nil {
		t.Fatalf("failed to ensure group: %v", err)
	}

	t.Run("Publish to Default Stream", func(t *testing.T) {
		err := q.Publish(ctx, "job-1", "test-type", "default", map[string]any{"data": "value"})
		if err != nil {
			t.Fatalf("failed to publish: %v", err)
		}

		// Read from default stream group to verify
		streams, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    queue.ConsumerGroup,
			Consumer: "worker-1",
			Streams:  []string{queue.StreamDefault, ">"},
			Count:    1,
			Block:    -1,
		}).Result()
		if err != nil {
			t.Fatalf("failed to read from group: %v", err)
		}

		if len(streams) != 1 || len(streams[0].Messages) != 1 {
			t.Fatalf("expected 1 stream and 1 message, got streams=%d", len(streams))
		}

		msg := streams[0].Messages[0]
		if msg.Values["job_id"] != "job-1" {
			t.Errorf("expected job_id 'job-1', got %v", msg.Values["job_id"])
		}

		// Ack the message
		err = q.Ack(ctx, queue.StreamDefault, msg.ID)
		if err != nil {
			t.Fatalf("failed to ack message: %v", err)
		}
	})
}

func TestQueue_ColdBootBacklogBug(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("failed to connect to redis: %v", err)
	}

	t.Run("Behavior with $ (ignores backlog)", func(t *testing.T) {
		streamKey := "task_queue:test_backlog_bug_dollar"
		rdb.Del(ctx, streamKey)
		defer rdb.Del(ctx, streamKey)

		// 1. Publish historical message BEFORE group exists
		err := rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			Values: map[string]any{"job_id": "historical-1"},
		}).Err()
		if err != nil {
			t.Fatalf("failed to publish: %v", err)
		}

		// 2. Create consumer group with starting ID "$" (represents the bug)
		err = rdb.XGroupCreateMkStream(ctx, streamKey, "group_dollar", "$").Err()
		if err != nil {
			t.Fatalf("failed to create group with $: %v", err)
		}

		// 3. Try to read from the group (should get redis.Nil, as historical is ignored)
		_, err = rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    "group_dollar",
			Consumer: "worker-1",
			Streams:  []string{streamKey, ">"},
			Count:    1,
			Block:    -1,
		}).Result()
		if err != redis.Nil {
			t.Errorf("expected redis.Nil (no messages read), got error: %v", err)
		}
	})

	t.Run("Behavior with 0 (processes backlog)", func(t *testing.T) {
		streamKey := "task_queue:test_backlog_bug_zero"
		rdb.Del(ctx, streamKey)
		defer rdb.Del(ctx, streamKey)

		// 1. Publish historical message BEFORE group exists
		err := rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			Values: map[string]any{"job_id": "historical-2"},
		}).Err()
		if err != nil {
			t.Fatalf("failed to publish: %v", err)
		}

		// 2. Create consumer group with starting ID "0" (the fix)
		err = rdb.XGroupCreateMkStream(ctx, streamKey, "group_zero", "0").Err()
		if err != nil {
			t.Fatalf("failed to create group with 0: %v", err)
		}

		// 3. Read from group should succeed and return the historical message
		streams, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    "group_zero",
			Consumer: "worker-1",
			Streams:  []string{streamKey, ">"},
			Count:    1,
			Block:    -1,
		}).Result()
		if err != nil {
			t.Fatalf("expected to read historical message, got error: %v", err)
		}

		if len(streams) != 1 || len(streams[0].Messages) != 1 {
			t.Fatalf("expected 1 message, got streams: %v", streams)
		}

		if streams[0].Messages[0].Values["job_id"] != "historical-2" {
			t.Errorf("expected job_id 'historical-2', got %v", streams[0].Messages[0].Values["job_id"])
		}
	})
}

func TestQueue_ReadPriority(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("failed to connect to redis: %v", err)
	}

	cleanUp := func() {
		rdb.Del(ctx, queue.StreamCritical, queue.StreamDefault, queue.StreamLow)
	}
	cleanUp()
	defer cleanUp()

	q := queue.New(rdb)
	if err := q.EnsureGroup(ctx); err != nil {
		t.Fatalf("failed to ensure group: %v", err)
	}

	// 1. Publish a low priority job
	err := q.Publish(ctx, "low-job", "test", "low", map[string]any{"key": "low"})
	if err != nil {
		t.Fatalf("failed to publish low: %v", err)
	}

	// 2. Publish a critical priority job
	err = q.Publish(ctx, "crit-job", "test", "critical", map[string]any{"key": "crit"})
	if err != nil {
		t.Fatalf("failed to publish critical: %v", err)
	}

	// 3. Read from queue. Since "critical" is checked first, it should return the critical job!
	msgs, err := q.Read(ctx, "worker-1", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}

	if msgs[0].Values["job_id"] != "crit-job" {
		t.Errorf("expected critical job first, got %v", msgs[0].Values["job_id"])
	}

	// 4. Ack the critical job
	err = q.Ack(ctx, queue.StreamCritical, msgs[0].ID)
	if err != nil {
		t.Fatalf("failed to ack: %v", err)
	}

	// 5. Read again. Now it should return the low priority job!
	msgs, err = q.Read(ctx, "worker-1", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to read second time: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}

	if msgs[0].Values["job_id"] != "low-job" {
		t.Errorf("expected low priority job next, got %v", msgs[0].Values["job_id"])
	}
}
