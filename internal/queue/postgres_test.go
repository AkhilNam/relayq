package queue

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

func TestPostgres(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL for live integration")
	}
	ctx := context.Background()
	p, e := NewPostgres(ctx, url)
	if e != nil {
		t.Fatal(e)
	}
	defer p.Pool.Close()
	key := fmt.Sprintf("test-%d", time.Now().UnixNano())
	now := time.Now()
	j := Job{ID: key, Key: key, URL: "https://example.com", Payload: "{}", Status: "pending", MaxAttempts: 2, NextAt: now, CreatedAt: now}
	defer p.Pool.Exec(ctx, `DELETE FROM jobs WHERE id=$1`, key)
	if _, ok, e := p.Put(ctx, j); e != nil || !ok {
		t.Fatal(ok, e)
	}
	j.ID = "unused"
	if got, ok, e := p.Put(ctx, j); e != nil || ok || got.ID != key {
		t.Fatal(got, ok, e)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	claims := 0
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, ok, e := p.Claim(ctx, key, now, time.Second)
			if e != nil {
				t.Error(e)
			}
			if ok {
				mu.Lock()
				claims++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if claims != 1 {
		t.Fatal(claims)
	}
	if _, ok, e := p.Claim(ctx, "recovery", now.Add(2*time.Second), time.Second); e != nil || !ok {
		t.Fatal(ok, e)
	}
	if e := p.Finish(ctx, key, key, true, "", now); e != ErrNotFound {
		t.Fatal("stale completion", e)
	}
	if e := p.Finish(ctx, key, "recovery", false, "fail", now); e != nil {
		t.Fatal(e)
	}
	if e := p.Retry(ctx, key, now); e != nil {
		t.Fatal(e)
	}
}
