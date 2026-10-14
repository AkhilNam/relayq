package queue

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"strings"
	"time"
)

type Postgres struct{ Pool *pgxpool.Pool }

const schema = `CREATE TABLE IF NOT EXISTS jobs (id text PRIMARY KEY, idempotency_key text UNIQUE NOT NULL, url text NOT NULL, payload text NOT NULL, status text NOT NULL, attempts int NOT NULL DEFAULT 0, max_attempts int NOT NULL, next_at timestamptz NOT NULL, lease text NOT NULL DEFAULT '', lease_until timestamptz NOT NULL DEFAULT 'epoch', last_error text NOT NULL DEFAULT '', created_at timestamptz NOT NULL); CREATE INDEX IF NOT EXISTS jobs_ready ON jobs(status,next_at);`

func NewPostgres(ctx context.Context, url string) (*Postgres, error) {
	p, e := pgxpool.New(ctx, url)
	if e != nil {
		return nil, e
	}
	if _, e = p.Exec(ctx, schema); e != nil {
		p.Close()
		return nil, e
	}
	return &Postgres{p}, nil
}

const cols = `id,idempotency_key,url,payload,status,attempts,max_attempts,next_at,lease,lease_until,last_error,created_at`

func scan(row pgx.Row) (j Job, e error) {
	e = row.Scan(&j.ID, &j.Key, &j.URL, &j.Payload, &j.Status, &j.Attempts, &j.MaxAttempts, &j.NextAt, &j.Lease, &j.LeaseUntil, &j.LastError, &j.CreatedAt)
	return
}
func (p *Postgres) Put(ctx context.Context, j Job) (Job, bool, error) {
	saved, e := scan(p.Pool.QueryRow(ctx, `INSERT INTO jobs(id,idempotency_key,url,payload,status,max_attempts,next_at,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(idempotency_key) DO NOTHING RETURNING `+cols, j.ID, j.Key, j.URL, j.Payload, j.Status, j.MaxAttempts, j.NextAt, j.CreatedAt))
	if e == pgx.ErrNoRows {
		saved, e = scan(p.Pool.QueryRow(ctx, `SELECT `+cols+` FROM jobs WHERE idempotency_key=$1`, j.Key))
		return saved, false, e
	}
	return saved, e == nil, e
}
func (p *Postgres) Claim(ctx context.Context, lease string, now time.Time, ttl time.Duration) (Job, bool, error) {
	j, e := scan(p.Pool.QueryRow(ctx, `WITH candidate AS (SELECT id FROM jobs WHERE (status='pending' AND next_at<=$1) OR (status='processing' AND lease_until<=$1) ORDER BY next_at FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE jobs j SET status='processing',attempts=j.attempts+1,lease=$2,lease_until=$3 FROM candidate c WHERE j.id=c.id RETURNING j.`+strings.ReplaceAll(cols, ",", ",j."), now, lease, now.Add(ttl)))
	if e == pgx.ErrNoRows {
		return Job{}, false, nil
	}
	return j, e == nil, e
}
func (p *Postgres) Finish(ctx context.Context, id, lease string, ok bool, why string, now time.Time) error {
	j, e := scan(p.Pool.QueryRow(ctx, `SELECT `+cols+` FROM jobs WHERE id=$1 AND lease=$2 AND status='processing'`, id, lease))
	if e != nil {
		return ErrNotFound
	}
	j = transition(j, ok, why, now)
	tag, e := p.Pool.Exec(ctx, `UPDATE jobs SET status=$3,next_at=$4,last_error=$5,lease='',lease_until='epoch' WHERE id=$1 AND lease=$2 AND status='processing'`, id, lease, j.Status, j.NextAt, j.LastError)
	if e != nil {
		return e
	}
	if tag.RowsAffected() != 1 {
		return ErrNotFound
	}
	return nil
}
func (p *Postgres) List(ctx context.Context) ([]Job, error) {
	rows, e := p.Pool.Query(ctx, `SELECT `+cols+` FROM jobs ORDER BY created_at DESC LIMIT 200`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Job{}
	for rows.Next() {
		j, e := scan(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, j)
	}
	return out, rows.Err()
}
func (p *Postgres) Retry(ctx context.Context, id string, now time.Time) error {
	tag, e := p.Pool.Exec(ctx, `UPDATE jobs SET status='pending',attempts=0,next_at=$2,last_error='' WHERE id=$1 AND status='dead'`, id, now)
	if e != nil {
		return e
	}
	if tag.RowsAffected() != 1 {
		return ErrNotFound
	}
	return nil
}
