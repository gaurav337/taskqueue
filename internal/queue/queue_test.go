package queue_test

import (
	"context"
	"fmt"
	"sync"
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

func TestQueue_Schedule(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("failed to connect to redis: %v", err)
	}

	// Clean up
	rdb.Del(ctx, queue.StreamScheduled)
	defer rdb.Del(ctx, queue.StreamScheduled)

	q := queue.New(rdb)

	now := time.Now()
	// Schedule 1 job in the past (due now) and 1 in the future (not due yet)
	err := q.Schedule(ctx, "job-due-1", now.Add(-5*time.Second))
	if err != nil {
		t.Fatalf("failed to schedule: %v", err)
	}

	err = q.Schedule(ctx, "job-future-1", now.Add(5*time.Minute))
	if err != nil {
		t.Fatalf("failed to schedule future: %v", err)
	}

	// Retrieve due jobs
	due, err := q.DueJobs(ctx)
	if err != nil {
		t.Fatalf("failed to fetch due: %v", err)
	}

	if len(due) != 1 {
		t.Fatalf("expected exactly 1 due job, got %d: %v", len(due), due)
	}

	if due[0] != "job-due-1" {
		t.Errorf("expected due job 'job-due-1', got %s", due[0])
	}
}

func TestQueue_DueJobsConcurrency(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("failed to connect to redis: %v", err)
	}

	rdb.Del(ctx, queue.StreamScheduled)
	defer rdb.Del(ctx, queue.StreamScheduled)

	q := queue.New(rdb)

	// Schedule a batch of 50 due jobs
	numJobs := 50
	for i := 1; i <= numJobs; i++ {
		jobID := fmt.Sprintf("concur-job-%d", i)
		err := q.Schedule(ctx, jobID, time.Now().Add(-10*time.Second))
		if err != nil {
			t.Fatalf("failed to schedule job %d: %v", i, err)
		}
	}

	// Run multiple concurrent scheduler workers popping from the sorted set
	numWorkers := 5
	poppedChan := make(chan string, numJobs*2)
	errChan := make(chan error, numWorkers)

	var wg sync.WaitGroup
	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				due, err := q.DueJobs(ctx)
				if err != nil {
					errChan <- err
					return
				}
				if len(due) == 0 {
					// No more due jobs
					break
				}
				for _, id := range due {
					poppedChan <- id
				}
				time.Sleep(10 * time.Millisecond) // Yield/jitter
			}
		}(w)
	}

	wg.Wait()
	close(poppedChan)
	close(errChan)

	for err := range errChan {
		t.Fatalf("worker error: %v", err)
	}

	// Verify that each job was popped EXACTLY once
	seen := make(map[string]int)
	for id := range poppedChan {
		seen[id]++
	}

	if len(seen) != numJobs {
		t.Errorf("expected to pop all %d jobs, but only got %d unique jobs", numJobs, len(seen))
	}

	for id, count := range seen {
		if count > 1 {
			t.Errorf("job %s was popped %d times! (Double pop / TOCTOU race detected)", id, count)
		}
	}
}
