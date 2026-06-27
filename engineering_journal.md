# Engineering Journal

This journal tracks important, peculiar bugs, architectural pivots, and software issues encountered during development.

## 2026-06-28 - Incident #3: go-redis XReadGroup Hangs Indefinitely due to Default Block Duration

*   **The Symptom & Context:** During unit tests in `queue_test.go`, the test execution hung indefinitely when attempting to verify that the consumer group ignores historical messages under the `$` start ID.
*   **The Initial Hypothesis:** We assumed that leaving the `Block` parameter empty or zero in `redis.XReadGroupArgs` would cause `XReadGroup` to return immediately when no messages were present, returning `redis.Nil`.
*   **The Root Cause:** In `go-redis/v9`, the `Block` field is a `time.Duration` (zero value is `0`). The library checks if `Block >= 0` to decide whether to append the `BLOCK` argument to the Redis command. Since `0 >= 0` is true, it passes `BLOCK 0` to Redis. In Redis, `BLOCK 0` tells the server to block the connection indefinitely until a new message arrives.
*   **The Fix & Evolution:**
    To perform non-blocking reads, we must specify a negative duration (e.g., `Block: -1`), which makes `Block >= 0` false, so go-redis omits the `BLOCK` argument from the Redis call, or specify a short positive duration (like `Block: 100 * time.Millisecond`) to act as a fail-safe poll.
    ```go
    streams, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
        Group:    "group_dollar",
        Consumer: "worker-1",
        Streams:  []string{streamKey, ">"},
        Count:    1,
        Block:    -1, // Avoids blocking indefinitely
    }).Result()
    ```
*   **Git Commit Suggestion:** `test(queue): fix hang in XReadGroup tests by setting non-blocking Block duration`

## 2026-06-28 - Incident #4: Task Starvation on Cold Boot (Starting Group from ID $)

*   **The Symptom & Context:** When task workers start up after a period of offline downtime (cold boot), any tasks that were published to the Redis Stream while the workers were offline are never processed, leading to task starvation.
*   **The Initial Hypothesis:** Setting up the stream consumer group with `$` as the start ID would allow workers to read all tasks.
*   **The Root Cause:** In Redis Streams, creating a consumer group with the start ID of `$` instructs the stream to only deliver messages that arrive *after* the consumer group's creation. Any messages already in the stream backlog (pre-existing/historical tasks) are ignored by the consumer group.
*   **The Fix & Evolution:**
    Change the starting ID from `$` to `"0"` in `EnsureGroup` during consumer group creation. This forces the consumer group to include the existing backlog of tasks in its scope.
    ```diff
    - err := q.rdb.XGroupCreateMkStream(ctx, s, ConsumerGroup, "$").Err()
    + err := q.rdb.XGroupCreateMkStream(ctx, s, ConsumerGroup, "0").Err()
    ```
*   **Git Commit Suggestion:** `fix(queue): resolve task starvation on cold boot by starting group from ID 0`
