package job

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

type Store struct {
	rdb *redis.Client
}

func NewStore(rdb *redis.Client) *Store {
	return &Store{rdb: rdb}
}

func (s *Store) Save(ctx context.Context, j *Job) error {
	data, err := json.Marshal(j)
	if err != nil {
		return err
	}
	return s.rdb.Set(ctx, "job:"+j.ID, data, 24*time.Hour).Err()
}

func (s *Store) Get(ctx context.Context, id string) (*Job, error) {
	val, err := s.rdb.Get(ctx, "job:"+id).Result()
	if err != nil {
		return nil, err
	}
	var j Job
	if err := json.Unmarshal([]byte(val), &j); err != nil {
		return nil, err
	}
	return &j, nil
}

func (s *Store) UpdateStatus(ctx context.Context, id string, status Status, errMsg string) error {
	j, err := s.Get(ctx, id)
	if err != nil {
		return err
	}
	j.Status = status
	j.Error = errMsg
	j.UpdatedAt = time.Now()
	return s.Save(ctx, j)
}
