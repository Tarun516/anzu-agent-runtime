package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Tarun516/anzu-agent-runtime/internal/config"
	"github.com/Tarun516/anzu-agent-runtime/internal/health"
	"github.com/Tarun516/anzu-agent-runtime/internal/media/audiosocket/echo"
	"github.com/Tarun516/anzu-agent-runtime/internal/media/audiosocket/session"
	"github.com/Tarun516/anzu-agent-runtime/internal/storage"
)

// main is the OS entrypoint for the realtime process.
// It initializes logging and reports fatal application failures.
func main() {
	logger := slog.New(
		slog.NewJSONHandler(os.Stdout, nil),
	)

	if err := run(logger); err != nil {
		logger.Error(
			"realtime service failed",
			"error", err,
		)
		os.Exit(1)
	}
}

// run owns the realtime application's lifecycle.
// It initializes dependencies, starts the servers,
// and coordinates their shutdown.
func run(logger *slog.Logger) error {
	// 1. Load the HTTP service configuration.
	cfg, err := config.LoadService(
		"realtime",
		":8081",
	)
	if err != nil {
		return fmt.Errorf(
			"load realtime configuration: %w", err,
		)
	}

	// 2. Load PostgreSQL configuration.
	dbCfg, err := config.LoadDatabase()
	if err != nil {
		return fmt.Errorf(
			"load database configuration: %w", err,
		)
	}

	// 3. Load AudioSocket configuration.
	// When no audio address is configured,
	// the service continues in HTTP-only mode.
	audioCfg, err := config.LoadAudioSocket()
	if err != nil {
		return fmt.Errorf(
			"load AudioSocket configuration: %w", err,
		)
	}

	// 4. Create the process shutdown context.
	// SIGINT and SIGTERM cancel this context.
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// 5. Initialize the PostgreSQL connection pool.
	db, err := storage.OpenPostgres(ctx, dbCfg)
	if err != nil {
		return fmt.Errorf(
			"initialize PostgreSQL: %w", err,
		)
	}
	defer db.Close()

	logger.Info(
		"database connected",
		"service", cfg.Name,
	)

	// 6. Construct the HTTP health server.
	// Readiness depends on PostgreSQL connectivity
	// and the application lifecycle state.
	server := health.NewServer(
		cfg.Name,
		cfg.Address,
		cfg.ShutdownTimeout,
		logger,
		[]health.ReadinessCheck{
			{
				Name:  "postgres",
				Check: db.Ping,
			},
		},
	)

	// 7. Create the AudioSocket session manager.
	// Each admitted TCP connection will be processed
	// by the existing reusable echo handler.
	manager, err := session.NewManager(
		echo.HandleConnection,
	)
	if err != nil {
		return fmt.Errorf(
			"create AudioSocket session manager: %w", err,
		)
	}

	// 8. Bind both listening sockets before serving.
	// If AudioSocket binding fails, bindListeners
	// releases the HTTP listener automatically.
	listeners, err := bindListeners(
		cfg.Address,
		audioCfg,
	)
	if err != nil {
		return fmt.Errorf(
			"bind realtime listeners: %w", err,
		)
	}

	logger.Info(
		"HTTP listener bound",
		"address", listeners.http.Addr().String(),
	)

	if audioCfg.Enabled {
		logger.Info(
			"AudioSocket listener bound",
			"address", listeners.audio.Addr().String(),
		)
	}

	// 9. Supervise both servers.
	// The supervisor handles:
	// - concurrent serving
	// - application readiness
	// - shutdown signals
	// - unexpected server failures
	// - active session draining
	// - listener cleanup
	if err := superviseListeners(
		ctx,
		server,
		listeners,
		manager,
		audioCfg,
	); err != nil {
		return fmt.Errorf(
			"supervise realtime service: %w", err,
		)
	}

	// 10. Both servers stopped successfully.
	logger.Info(
		"realtime service stopped",
		"service", cfg.Name,
	)

	return nil
}
