package pgsq

import (
	"context"
	"fmt"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5/pgconn"
)

// Queryable interface containing operations necessary to query database.
// Both Pool and Tx implement it.
type Queryable interface {
	// Exec executes the builder query.
	Exec(ctx context.Context, query sqlizer) (pgconn.CommandTag, error)

	// ExecRaw executes the raw query.
	ExecRaw(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)

	// Get queries a single row. Returns pgx.ErrNoRows, if there are no rows satisfying the builder query.
	Get(ctx context.Context, dst any, sqlizer sqlizer) error

	// GetRaw queries a single row. Returns pgx.ErrNoRows, if there are no rows satisfying the raw query.
	GetRaw(ctx context.Context, dst any, sql string, args ...any) error

	// Select queries multiple rows. Returns nil, if there are no rows satisfying the builder query.
	Select(ctx context.Context, dst any, query sqlizer) error

	// SelectRaw queries multiple rows. Returns nil, if there are no rows satisfying the raw query.
	SelectRaw(ctx context.Context, dst any, sql string, args ...any) error
}

func execFn(ctx context.Context, q execer, query sqlizer) (pgconn.CommandTag, error) {
	sql, args, err := query.ToSql()
	if err != nil {
		return pgconn.CommandTag{}, fmt.Errorf("to sql: %w", err)
	}

	return q.Exec(ctx, sql, args...)
}

func selectFn(ctx context.Context, q pgxscan.Querier, dst any, query sqlizer) error {
	sql, args, err := query.ToSql()
	if err != nil {
		return fmt.Errorf("to sql: %w", err)
	}

	return pgxscan.Select(ctx, q, dst, sql, args...)
}

func getFn(ctx context.Context, q pgxscan.Querier, dst any, query sqlizer) error {
	sql, args, err := query.ToSql()
	if err != nil {
		return fmt.Errorf("to sql: %w", err)
	}

	return pgxscan.Get(ctx, q, dst, sql, args...)
}
