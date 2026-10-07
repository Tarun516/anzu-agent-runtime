package storage


import (
	"context"
	"fmt"

	"anzu-agent-runtime/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Postgres owns Anzu's PostgreSQL connection pool.
//
// The pool is shared by concurrent database operations
// instead of creating a new connection for every query.

type Postgres struct {
	pool *pgxpool.Pool
}

// OpenPostgres creates the PostgreSQL connection pool
// and verifies that the database can actually be reached.
//
// Returning successfully means the required startup
// database dependency is available.

func OpenPostgres(
	ctx context.Context,
	cfg config.Database,
) (*Postgres,error){
	// 1. Parse the PostgreSQL connection string
	// into pgxpool's structured configuration.

	poolConfig, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf(
			"parse PostgreSQL configuration: %w",
			err,
		)
	}


	// 2. Create a bounded context for initial database connection work.
	// We do not want startup to wait forever if Postgres is unreachable.
	connectCtx, cancel := context.WithTimeout(
		ctx,
		cfg.ConnectTimeout,
	)

	defer cancel()

	// 3. Create the pool alone does not guarantee PostgreSQL
	// is reachable, so we explicitly Ping it below.
	pool, err := pgxpool.NewWithConfig(
		connectCtx,
		poolConfig,
	)

	if err != nil {
		return nil, fmt.Errorf(
			"create PostgreSQL pool: %w",
			err,
		
		)
	}

	//4. Verify that a real connection can be established
	if err := pool.Ping(connectCtx); err != nil{
		// 5. The pool was already created,so release its
		// resources before returning the startup error.
		pool.Close()

		return nil, fmt.Errorf(
			"ping PostgreSQL: %w",
			err,
		)
	}

	// 6.Return the database dependency after connectivity
	// has been successfully verified,
	return &Postgres{
		pool: pool,
	}, nil
}

func (p *Postgres) Close(){
	// 1. Close the entire connection pool.
	p.pool.Close()
}
