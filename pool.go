package pgsq

import (
	"context"
	"fmt"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Pool contains operations necessary to query with the database.
type Pool interface {
	Queryable
	BeginTx(ctx context.Context, txOptions *pgx.TxOptions) (Tx, error)
}

type sqlizer interface {
	ToSql() (sql string, args []any, err error)
}

type execer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// NewPool creates new Pool.
func NewPool(pool *pgxpool.Pool) Pool {
	return &poolWrapper{pool: pool}
}

// poolWrapper pgx pool wrapper.
type poolWrapper struct {
	pool *pgxpool.Pool
}

// BeginTx starts a transaction.
// Commit or Rollback must be called on the returned transaction to finalize the transaction block.
func (p *poolWrapper) BeginTx(ctx context.Context, txOptions *pgx.TxOptions) (Tx, error) {
	var txOpts pgx.TxOptions
	if txOptions != nil {
		txOpts = *txOptions
	}

	tx, err := p.pool.BeginTx(ctx, txOpts)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}

	return &txWrapper{tx: tx}, nil
}

// Exec executes the builder query.
func (p *poolWrapper) Exec(ctx context.Context, query sqlizer) (pgconn.CommandTag, error) {
	return execFn(ctx, p.pool, query)
}

// ExecRaw executes the raw query.
func (p *poolWrapper) ExecRaw(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return p.pool.Exec(ctx, sql, args...)
}

// Get queries a single row. Returns pgx.ErrNoRows, if there are no rows satisfying the builder query.
func (p *poolWrapper) Get(ctx context.Context, dst any, query sqlizer) error {
	return getFn(ctx, p.pool, dst, query)
}

// GetRaw queries a single row. Returns pgx.ErrNoRows, if there are no rows satisfying the raw query.
func (p *poolWrapper) GetRaw(ctx context.Context, dst any, sql string, args ...any) error {
	return pgxscan.Get(ctx, p.pool, dst, sql, args...)
}

// Select queries multiple rows. Returns nil, if there are no rows satisfying the builder query.
func (p *poolWrapper) Select(ctx context.Context, dst any, query sqlizer) error {
	return selectFn(ctx, p.pool, dst, query)
}

// SelectRaw queries multiple rows. Returns nil, if there are no rows satisfying the raw query.
func (p *poolWrapper) SelectRaw(ctx context.Context, dst any, sql string, args ...any) error {
	return pgxscan.Select(ctx, p.pool, dst, sql, args...)
}
