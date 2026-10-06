package main

import (
	"anzu-agent-runtime/internal/config"
	"anzu-agent-runtime/internal/health"
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	logger := slog.New(
		slog.NewJSONHandler(os.Stdout, nil),
	)

	cfg, err := config.LoadService(
		"api",
		":8080",
	)
	if err != nil {
		logger.Error(
			"failed to load configuration",
			"error",
			err,
		)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	server := health.NewServer(
		cfg.Name,
		cfg.Address,
		cfg.ShutdownTimeout,
		logger,
	)

	server.SetReady(true)

	logger.Info(
		"service starting",
		"service",
		cfg.Name,
		"address",
		cfg.Address,
	)

	if err := server.Run(ctx); err != nil {
		logger.Error(
			"service stopped with error",
			"error",
			err,
		)
		os.Exit(1)
	}

	logger.Info(
		"service stopped",
		"service",
		cfg.Name,
	)
}
