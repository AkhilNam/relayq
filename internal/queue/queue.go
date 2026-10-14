// Package queue provides leased, at-least-once webhook delivery.
package queue

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrNotFound = errors.New("job not found or lease no longer owned")

type Job struct {
	ID          string    `json:"id"`
	Key         string    `json:"idempotency_key"`
	URL         string    `json:"url"`
	Payload     string    `json:"payload"`
	Status      string    `json:"status"`
	Attempts    int       `json:"attempts"`
	MaxAttempts int       `json:"max_attempts"`
	NextAt      time.Time `json:"next_at"`
	Lease       string    `json:"-"`
	LeaseUntil  time.Time `json:"-"`
	LastError   string    `json:"last_error"`
	CreatedAt   time.Time `json:"created_at"`
}
type Store interface {
	Put(context.Context, Job) (Job, bool, error)
	Claim(context.Context, string, time.Time, time.Duration) (Job, bool, error)
	Finish(context.Context, string, string, bool, string, time.Time) error
	List(context.Context) ([]Job, error)
	Retry(context.Context, string, time.Time) error
}

// Memory is the explicitly non-durable development backend.
type Memory struct {
	mu   sync.Mutex
	jobs map[string]Job
	keys map[string]string
}

func NewMemory() *Memory { return &Memory{jobs: map[string]Job{}, keys: map[string]string{}} }
func (m *Memory) Put(_ context.Context, j Job) (Job, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if id, ok := m.keys[j.Key]; ok {
		return m.jobs[id], false, nil
	}
	m.jobs[j.ID] = j
	m.keys[j.Key] = j.ID
	return j, true, nil
}
func (m *Memory) Claim(_ context.Context, lease string, now time.Time, ttl time.Duration) (Job, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var chosen Job
	found := false
	for _, j := range m.jobs {
		if (j.Status == "pending" && !j.NextAt.After(now)) || (j.Status == "processing" && !j.LeaseUntil.After(now)) {
			if !found || j.NextAt.Before(chosen.NextAt) {
				chosen = j
				found = true
			}
		}
	}
	if !found {
		return Job{}, false, nil
	}
	chosen.Status = "processing"
	chosen.Lease = lease
	chosen.LeaseUntil = now.Add(ttl)
	chosen.Attempts++
	m.jobs[chosen.ID] = chosen
	return chosen, true, nil
}
func transition(j Job, success bool, why string, now time.Time) Job {
	j.Lease = ""
	j.LeaseUntil = time.Time{}
	j.LastError = why
	if success {
		j.Status = "delivered"
	} else if j.Attempts >= j.MaxAttempts {
		j.Status = "dead"
	} else {
		j.Status = "pending"
		n := j.Attempts
		if n > 8 {
			n = 8
		}
		j.NextAt = now.Add(time.Duration(1<<n) * time.Second)
	}
	return j
}
func (m *Memory) Finish(_ context.Context, id, lease string, ok bool, why string, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, exists := m.jobs[id]
	if !exists || j.Status != "processing" || j.Lease != lease {
		return ErrNotFound
	}
	m.jobs[id] = transition(j, ok, why, now)
	return nil
}
func (m *Memory) List(_ context.Context) ([]Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Job, 0, len(m.jobs))
	for _, j := range m.jobs {
		out = append(out, j)
	}
	return out, nil
}
func (m *Memory) Retry(_ context.Context, id string, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok || j.Status != "dead" {
		return ErrNotFound
	}
	j.Status = "pending"
	j.Attempts = 0
	j.NextAt = now
	j.LastError = ""
	m.jobs[id] = j
	return nil
}
