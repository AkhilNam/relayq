package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/AkhilNam/relayq/internal/queue"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func id() string {
	b := make([]byte, 16)
	if _, e := rand.Read(b); e != nil {
		panic(e)
	}
	return hex.EncodeToString(b)
}
func allowed(raw string) error {
	u, e := url.Parse(raw)
	if e != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil {
		return fmt.Errorf("destination must be an HTTPS URL without credentials")
	}
	hosts := strings.Split(os.Getenv("ALLOWED_HOSTS"), ",")
	for _, h := range hosts {
		if strings.TrimSpace(h) == u.Hostname() {
			return nil
		}
	}
	return fmt.Errorf("destination hostname is not in ALLOWED_HOSTS")
}
func publicDial(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, e := net.SplitHostPort(addr)
	if e != nil {
		return nil, e
	}
	ips, e := net.DefaultResolver.LookupIPAddr(ctx, host)
	if e != nil {
		return nil, e
	}
	for _, ip := range ips {
		if ip.IP.IsPrivate() || ip.IP.IsLoopback() || ip.IP.IsLinkLocalUnicast() || ip.IP.IsUnspecified() {
			return nil, fmt.Errorf("private destination address blocked")
		}
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("no destination address")
	}
	d := net.Dialer{Timeout: 5 * time.Second}
	return d.DialContext(ctx, network, net.JoinHostPort(ips[0].IP.String(), port))
}
func deliver(ctx context.Context, client *http.Client, j queue.Job) (bool, string) {
	if e := allowed(j.URL); e != nil {
		return false, e.Error()
	}
	req, e := http.NewRequestWithContext(ctx, "POST", j.URL, bytes.NewBufferString(j.Payload))
	if e != nil {
		return false, e.Error()
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", j.Key)
	req.Header.Set("X-RelayQ-Job", j.ID)
	resp, e := client.Do(req)
	if e != nil {
		return false, e.Error()
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return true, ""
	}
	return false, fmt.Sprintf("HTTP %d", resp.StatusCode)
}
func worker(ctx context.Context, s queue.Store, client *http.Client) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			token := id()
			j, ok, e := s.Claim(ctx, token, time.Now(), 30*time.Second)
			if e != nil {
				log.Printf("claim: %v", e)
				continue
			}
			if !ok {
				continue
			}
			ok, why := deliver(ctx, client, j)
			if e = s.Finish(ctx, j.ID, token, ok, why, time.Now()); e != nil {
				log.Printf("finish: %v", e)
			}
		}
	}
}
func api(s queue.Store, key string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("GET /api/jobs", func(w http.ResponseWriter, r *http.Request) {
		jobs, e := s.List(r.Context())
		if e != nil {
			http.Error(w, "storage unavailable", 503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(jobs)
	})
	mux.HandleFunc("POST /api/jobs", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 65536)
		var input struct {
			URL     string          `json:"url"`
			Payload json.RawMessage `json:"payload"`
			Key     string          `json:"idempotency_key"`
		}
		if e := json.NewDecoder(r.Body).Decode(&input); e != nil || input.Key == "" || len(input.Key) > 200 || len(input.Payload) == 0 || !json.Valid(input.Payload) {
			http.Error(w, "valid JSON payload and idempotency_key required", 400)
			return
		}
		if e := allowed(input.URL); e != nil {
			http.Error(w, e.Error(), 400)
			return
		}
		now := time.Now()
		j, created, e := s.Put(r.Context(), queue.Job{ID: id(), Key: input.Key, URL: input.URL, Payload: string(input.Payload), Status: "pending", MaxAttempts: 5, NextAt: now, CreatedAt: now})
		if e != nil {
			http.Error(w, "storage unavailable", 503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if created {
			w.WriteHeader(201)
		}
		json.NewEncoder(w).Encode(j)
	})
	mux.HandleFunc("POST /api/jobs/{id}/retry", func(w http.ResponseWriter, r *http.Request) {
		if e := s.Retry(r.Context(), r.PathValue("id"), time.Now()); e != nil {
			http.Error(w, "dead job not found", 404)
			return
		}
		w.WriteHeader(204)
	})
	mux.Handle("/", http.FileServer(http.Dir("web/dist")))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/") && (key == "" || r.Header.Get("Authorization") != "Bearer "+key) {
			http.Error(w, "unauthorized", 401)
			return
		}
		mux.ServeHTTP(w, r)
	})
}
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var s queue.Store = queue.NewMemory()
	if db := os.Getenv("DATABASE_URL"); db != "" {
		p, e := queue.NewPostgres(ctx, db)
		if e != nil {
			log.Fatal(e)
		}
		defer p.Pool.Close()
		s = p
	} else {
		log.Print("development in-memory backend: jobs are lost on restart")
	}
	key := os.Getenv("API_KEY")
	if key == "" {
		log.Fatal("set API_KEY")
	}
	client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{DialContext: publicDial}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for i := 0; i < 4; i++ {
		go worker(ctx, s, client)
	}
	server := &http.Server{Addr: ":8080", Handler: api(s, key), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(c)
	}()
	log.Print("RelayQ on :8080")
	if e := server.ListenAndServe(); e != nil && e != http.ErrServerClosed {
		log.Fatal(e)
	}
}
