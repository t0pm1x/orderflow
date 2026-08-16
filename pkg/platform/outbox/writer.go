package outbox

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	pgxpool "github.com/jackc/pgx/v5/pgxpool"
)

// DBTX is the subset of pgx/pgxpool that a Writer needs. Both
// *pgxpool.Pool and pgx.Tx satisfy this interface, so the same
// Writer works in unit tests (against a tx from a tx-aware fixture)
// and in production (against the service's tx-wrapped pool).
type DBTX interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Writer appends outbox records inside a caller-supplied transaction.
// Atomicity is the contract: if the caller's business state change
// later rolls back, the outbox row must roll back with it. If the
// business change commits, the outbox row must commit with it.
//
// Implementations live in each service's internal/outbox package
// because they are tied to that service's table name and schema.
type Writer interface {
	Append(ctx context.Context, tx DBTX, r Record) error
}

// Compile-time assertions that the real pgx types satisfy DBTX.
var (
	_ DBTX = (pgx.Tx)(nil)
	_ DBTX = (*pgxpool.Pool)(nil)
)
