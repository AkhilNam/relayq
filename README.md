# RelayQ

A small webhook delivery service built to explore reliable backend work: idempotent enqueue, leased worker claims, retry backoff, dead-letter recovery and a React/TypeScript dashboard. Built in October 2026. This is a portfolio implementation, not a hosted service or a claim of production usage.

## Architecture

`React dashboard -> Go REST API -> PostgreSQL jobs -> Go workers -> HTTPS receiver`

- `POST /api/jobs` creates a delivery. Reusing an idempotency key returns the existing job, not another delivery. Payload and destination are immutable for that key.
- PostgreSQL workers claim one ready job through `FOR UPDATE SKIP LOCKED` inside an atomic UPDATE. Multiple service replicas can share the database.
- Each claim has a fresh lease token. A crashed worker's lease expires after 30 seconds; another worker can reclaim it. Completion requires the current token so a stale worker cannot overwrite a newer claim.
- Failed deliveries retry with bounded exponential delays. After five attempts they become dead letters. `POST /api/jobs/{id}/retry` starts another attempt cycle.
- HTTP receivers get `Idempotency-Key` and `X-RelayQ-Job` headers. **Delivery is at least once, not exactly once.** A receiver must deduplicate: a worker can crash after the receiver accepts the webhook but before the database records success.
- The React UI reads recent jobs on demand, queues JSON deliveries and replays dead letters. Its API key stays in memory, not local storage.

## Run without containers

Requires Go 1.24+ and Node 22+.

```sh
cd web && npm ci && npm run build && cd ..
export API_KEY=local-demo-key
export ALLOWED_HOSTS=your-webhook-host.example
# Optional. Without this variable, the development backend loses jobs on restart.
export DATABASE_URL='postgres://user:password@localhost:5432/relayq?sslmode=disable'
go run ./cmd/relayq
```

Open http://localhost:8080 and enter the API key. Destination hosts must be explicitly allowlisted. Do not put real credentials in the repository.

## Docker Compose

```sh
export API_KEY=local-demo-key
export ALLOWED_HOSTS=your-webhook-host.example
docker compose up --build
```

Compose includes PostgreSQL and a durable named volume. The sample database password is only a local-development example. Replace secrets and use TLS before any remote deployment.

## API example

```sh
curl http://localhost:8080/api/jobs \
  -H 'Authorization: Bearer local-demo-key' \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://your-webhook-host.example/hook","idempotency_key":"order-1","payload":{"event":"order.created"}}'
```

`GET /healthz` is process liveness, not a database readiness check. `GET /api/jobs` returns at most 200 recent jobs on PostgreSQL. API endpoints require a bearer key.

## Tests

```sh
go test -race ./...
go vet ./...
cd web && npm ci && npm run build
```

Unit tests exercise duplicate enqueue, competing claims, stale-lease fencing, crash recovery, backoff, dead letters, replay, bearer auth and destination validation. Optional PostgreSQL integration test:

```sh
TEST_DATABASE_URL='postgres://user:password@localhost:5432/relayq_test?sslmode=disable' go test -race ./internal/queue
```

Use a disposable test database. The test inserts and deletes only its own generated rows.

## Security and limitations

HTTPS destinations only; exact hostname allowlist; redirects are not followed. The delivery transport resolves the hostname, rejects private/loopback/link-local addresses and dials a validated address to reduce SSRF and DNS-rebinding risk. The allowlist is checked again during delivery. Run behind TLS in a trusted environment. The service intentionally lacks multi-tenant auth, per-user quotas, webhook signing, audit retention, jitter and partitioning. It does not implement Kafka or exactly-once transactions. Docker Compose and Kubernetes examples are deployment scaffolding, not evidence of a production cluster. Tests do not constitute a security audit.
