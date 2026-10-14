package queue

import (
	"context"
	"sync"
	"testing"
	"time"
)

func fixture(s Store) (Job, time.Time) {
	now := time.Now()
	j := Job{ID: "one", Key: "unique", URL: "https://example.com", Payload: "{}", Status: "pending", MaxAttempts: 2, NextAt: now, CreatedAt: now}
	s.Put(context.Background(), j)
	return j, now
}
func TestIdempotency(t *testing.T) {
	s := NewMemory()
	j, _ := fixture(s)
	j.ID = "two"
	got, created, e := s.Put(context.Background(), j)
	if e != nil || created || got.ID != "one" {
		t.Fatal(got, created, e)
	}
}
func TestConcurrentClaim(t *testing.T) {
	s := NewMemory()
	_, now := fixture(s)
	var wg sync.WaitGroup
	var mu sync.Mutex
	count := 0
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, ok, e := s.Claim(context.Background(), "lease", now, time.Minute)
			if e != nil {
				t.Error(e)
			}
			if ok {
				mu.Lock()
				count++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if count != 1 {
		t.Fatalf("claimed %d times", count)
	}
}
func TestLeaseRecoveryAndFencing(t *testing.T) {
	s := NewMemory()
	_, now := fixture(s)
	s.Claim(context.Background(), "old", now, time.Second)
	_, ok, _ := s.Claim(context.Background(), "new", now.Add(2*time.Second), time.Second)
	if !ok {
		t.Fatal("not reclaimed")
	}
	if e := s.Finish(context.Background(), "one", "old", true, "", now); e != ErrNotFound {
		t.Fatal("old lease accepted")
	}
	if e := s.Finish(context.Background(), "one", "new", true, "", now); e != nil {
		t.Fatal(e)
	}
}
func TestRetryAndDeadLetter(t *testing.T) {
	s := NewMemory()
	_, now := fixture(s)
	s.Claim(context.Background(), "a", now, time.Second)
	s.Finish(context.Background(), "one", "a", false, "fail", now)
	if _, ok, _ := s.Claim(context.Background(), "b", now, time.Second); ok {
		t.Fatal("backoff ignored")
	}
	s.Claim(context.Background(), "b", now.Add(time.Minute), time.Second)
	s.Finish(context.Background(), "one", "b", false, "fail", now)
	jobs, _ := s.List(context.Background())
	if jobs[0].Status != "dead" {
		t.Fatal(jobs)
	}
	if e := s.Retry(context.Background(), "one", now); e != nil {
		t.Fatal(e)
	}
	jobs, _ = s.List(context.Background())
	if jobs[0].Attempts != 0 || jobs[0].Status != "pending" {
		t.Fatal(jobs)
	}
}
