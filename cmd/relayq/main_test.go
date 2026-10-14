package main

import (
	"context"
	"github.com/AkhilNam/relayq/internal/queue"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAPI(t *testing.T) {
	t.Setenv("ALLOWED_HOSTS", "example.com")
	s := queue.NewMemory()
	h := api(s, "test-key")
	r := httptest.NewRequest("GET", "/api/jobs", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal(w.Code)
	}
	body := `{"url":"https://example.com/hook","idempotency_key":"k","payload":{"ok":true}}`
	for i, want := range []int{201, 200} {
		r = httptest.NewRequest("POST", "/api/jobs", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer test-key")
		w = httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != want {
			t.Fatalf("request %d: %d %s", i, w.Code, w.Body)
		}
	}
	r = httptest.NewRequest("POST", "/api/jobs", strings.NewReader(strings.ReplaceAll(body, "example.com", "evil.com")))
	r.Header.Set("Authorization", "Bearer test-key")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 400 {
		t.Fatal(w.Code)
	}
}
func TestPrivateDestinationBlocked(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if c, e := publicDial(ctx, "tcp", "127.0.0.1:80"); e == nil {
		c.Close()
		t.Fatal("private address accepted")
	}
}
func TestDestinationValidation(t *testing.T) {
	t.Setenv("ALLOWED_HOSTS", "example.com")
	for _, u := range []string{"http://example.com", "https://user:pass@example.com", "https://evil.com"} {
		if allowed(u) == nil {
			t.Fatal(u)
		}
	}
	if allowed("https://example.com/hook") != nil {
		t.Fatal("allowlist rejected")
	}
}
