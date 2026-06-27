package queue_test

import (
	"context"
	"testing"

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
