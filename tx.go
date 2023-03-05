package pgsq

import (
	"context"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Tx transaction interface.
type Tx interface {
	Queryable

	// Commit commits transaction.
	// Will return an error where errors.Is(pgx.ErrTxClosed) is true if the Tx is already closed, but is
	// otherwise safe to call multiple times. If the commit fails with a rollback status (e.g. the transaction was already
	// in a broken state) then an error where errors.Is(ErrTxCommitRollback) is true will be returned.
	Commit(ctx context.Context) error

	// Rollback cancels transaction.
	// Will return an error where errors.Is(pgx.ErrTxClosed) is true if the Tx is already
	// closed, but is otherwise safe to call multiple times. Hence, a defer tx.Rollback() is safe even if tx.Commit() will
	// be called first in a non-error condition. Any other failure of a real transaction will result in the connection
	// being closed.
	Rollback(ctx context.Context) error
}

// Tx transaction wrapper.
type txWrapper struct {
	tx pgx.Tx
}

// Commit commits transaction.
// Will return an error where errors.Is(pgx.ErrTxClosed) is true if the Tx is already closed, but is
// otherwise safe to call multiple times. If the commit fails with a rollback status (e.g. the transaction was already
// in a broken state) then an error where errors.Is(ErrTxCommitRollback) is true will be returned.
func (t *txWrapper) Commit(ctx context.Context) error {
	return t.tx.Commit(ctx)
}

// Rollback cancels transaction.
// Will return an error where errors.Is(pgx.ErrTxClosed) is true if the Tx is already
// closed, but is otherwise safe to call multiple times. Hence, a defer tx.Rollback() is safe even if tx.Commit() will
// be called first in a non-error condition. Any other failure of a real transaction will result in the connection
// being closed.
func (t *txWrapper) Rollback(ctx context.Context) error {
	return t.tx.Rollback(ctx)
}

// Exec executes the builder query.
func (t *txWrapper) Exec(ctx context.Context, query sqlizer) (pgconn.CommandTag, error) {
	return execFn(ctx, t.tx, query)
}

// ExecRaw executes the raw query.
func (t *txWrapper) ExecRaw(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return t.tx.Exec(ctx, sql, args...)
}

// Get queries a single row. Returns pgx.ErrNoRows, if there are no rows satisfying the builder query.
func (t *txWrapper) Get(ctx context.Context, dst any, query sqlizer) error {
	return getFn(ctx, t.tx, dst, query)
}

// GetRaw queries a single row. Returns pgx.ErrNoRows, if there are no rows satisfying the raw query.
func (t *txWrapper) GetRaw(ctx context.Context, dst any, sql string, args ...any) error {
	return pgxscan.Get(ctx, t.tx, dst, sql, args...)
}

// Select queries multiple rows. Returns nil, if there are no rows satisfying the builder query.
func (t *txWrapper) Select(ctx context.Context, dst any, query sqlizer) error {
	return selectFn(ctx, t.tx, dst, query)
}

// SelectRaw queries multiple rows. Returns nil, if there are no rows satisfying the raw query.
func (t *txWrapper) SelectRaw(ctx context.Context, dst any, sql string, args ...any) error {
	return pgxscan.Select(ctx, t.tx, dst, sql, args...)
}
