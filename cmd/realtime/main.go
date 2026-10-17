package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Tarun516/anzu-agent-runtime/internal/config"
	"github.com/Tarun516/anzu-agent-runtime/internal/health"
	"github.com/Tarun516/anzu-agent-runtime/internal/media/audiosocket/echo"
	"github.com/Tarun516/anzu-agent-runtime/internal/media/audiosocket/session"
	"github.com/Tarun516/anzu-agent-runtime/internal/speech/stt/deepgram"
	"github.com/Tarun516/anzu-agent-runtime/internal/speech/stt/deepgram/gorilla"
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

	// 7. Select the existing echo-only handler by default.
	// Explicit opt-in enables a separate 16-kHz shadow consumer while the
	// ORIGINAL 8-kHz PCM is still written back to Asterisk.
	handler := session.Handler(echo.HandleConnection)
	shadowFlag := os.Getenv("ANZU_AUDIO_SHADOW")
	if shadowFlag != "" {
		enabled, parseErr := strconv.ParseBool(shadowFlag)
		if parseErr != nil {
			return fmt.Errorf("ANZU_AUDIO_SHADOW must be a boolean: %w", parseErr)
		}
		if enabled {
			handler, err = echo.NewShadowHandler(echo.ShadowConfig{
				MaxFrames:    10,
				MaxBuffered:  200 * time.Millisecond,
				DrainTimeout: 2 * time.Second,
			})
			if err != nil {
				return fmt.Errorf("initialize audio shadow pipeline: %w", err)
			}
			logger.Info("audio shadow pipeline enabled", "mode", "echo plus test consumer", "rate_hz", 16000)
		}
	}

	// CP4.3: opt-in real STT observer. Never enable implicitly just because
	// DEEPGRAM_API_KEY is present: this sends call audio to an external service.
	sttFlag := os.Getenv("ANZU_AUDIO_STT")
	if sttFlag != "" {
		enabled, parseErr := strconv.ParseBool(sttFlag)
		if parseErr != nil {
			return fmt.Errorf("ANZU_AUDIO_STT must be boolean: %w", parseErr)
		}
		if enabled {
			if shadowFlag != "" {
				shadowEnabled, _ := strconv.ParseBool(shadowFlag)
				if shadowEnabled {
					return fmt.Errorf("ANZU_AUDIO_STT and ANZU_AUDIO_SHADOW cannot both be true")
				}
			}
			logText := false
			if raw := os.Getenv("ANZU_STT_LOG_TEXT"); raw != "" {
				logText, err = strconv.ParseBool(raw)
				if err != nil {
					return fmt.Errorf("ANZU_STT_LOG_TEXT must be boolean: %w", err)
				}
			}
			provider, providerErr := deepgram.New(deepgram.Config{
				APIKey: os.Getenv("DEEPGRAM_API_KEY"),
				Model:  "nova-3",
				Dialer: gorilla.Dialer{},
			})
			if providerErr != nil {
				return fmt.Errorf("initialize Deepgram: %w", providerErr)
			}
			language := os.Getenv("ANZU_STT_LANGUAGE")
			if language == "" {
				language = "en-IN"
			}
			handler, err = echo.NewSTTShadowHandler(echo.STTShadowConfig{
				Provider:      provider,
				Language:      language,
				QueueFrames:   10,
				QueueDuration: 200 * time.Millisecond,
				DrainTimeout:  8 * time.Second,
				FinishTimeout: 6 * time.Second,
				LogText:       logText,
			})
			if err != nil {
				return fmt.Errorf("initialize STT observer: %w", err)
			}
			logger.Info("STT observation mode enabled", "rate_hz", 16000, "transcript_text_logging", logText)
		}
	}

	// 8. Create the AudioSocket session manager.
	manager, err := session.NewManager(handler)
	if err != nil {
		return fmt.Errorf(
			"create AudioSocket session manager: %w", err,
		)
	}

	// 9. Bind both listening sockets before serving.
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

	// 10. Supervise both servers.
	// The supervisor handles concurrent serving, readiness, unexpected
	// failures, active-session draining and listener cleanup.
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

	// 11. Both servers stopped successfully.
	logger.Info(
		"realtime service stopped",
		"service", cfg.Name,
	)

	return nil
}
