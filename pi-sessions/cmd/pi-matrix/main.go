// pi-matrix - A Matrix appservice that talks to the forge REST API.
// Routes Matrix events to forge and forwards forge's message log
// back as Matrix room messages.
// Copyright (C) 2026 Mule AI
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"go.mau.fi/pi-matrix/pkg/appservice"
	"go.mau.fi/pi-matrix/pkg/config"
	"go.mau.fi/pi-matrix/pkg/forge"
	"go.mau.fi/pi-matrix/pkg/matrix"
	"go.mau.fi/pi-matrix/pkg/store"
)

var (
	Tag       = "unknown"
	Commit    = "unknown"
	BuildTime = "unknown"
)

var (
	flagConfig  = flag.String("c", "config.yaml", "Path to config file")
	flagExport  = flag.Bool("e", false, "Export example config file")
	flagVersion = flag.Bool("v", false, "Print version")
)

func main() {
	flag.Parse()

	if *flagVersion {
		fmt.Printf("pi-matrix version %s (commit: %s, built: %s)\n", Tag, Commit, BuildTime)
		return
	}

	setupLogger()

	if *flagExport {
		configPath := *flagConfig
		if err := exportConfig(configPath); err != nil {
			log.Fatal().Err(err).Msg("failed to export config")
		}
		fmt.Printf("Example config exported to %s\n", configPath)
		return
	}

	cfg, err := loadConfig(*flagConfig)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load config")
	}

	log.Info().
		Str("homeserver", cfg.Homeserver.Address).
		Str("domain", cfg.Homeserver.Domain).
		Str("forge_url", cfg.Forge.URL).
		Int("api_port", cfg.API.Port).
		Msg("configuration loaded")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Info().Str("signal", sig.String()).Msg("received signal, shutting down")
		cancel()
	}()

	// Forge client. Health-check it once at startup so we fail fast
	// if the URL is wrong.
	forgeClient := forge.NewClient(cfg.Forge.URL, cfg.Forge.APIKey)
	if err := forgeClient.Health(ctx); err != nil {
		log.Warn().Err(err).Str("forge_url", cfg.Forge.URL).Msg("forge is not reachable at startup; will keep trying")
	} else {
		log.Info().Str("forge_url", cfg.Forge.URL).Msg("connected to forge")
	}

	// SQLite store for portal + forge profile cache. Failures
	// here are non-fatal: the appservice still works, just
	// without persistence.
	var st *store.Store
	dbPath := "/opt/pi-matrix/pi-matrix.db"
	if cfg.Database.URL != "" && cfg.Database.URL != "bridge.db" {
		dbPath = cfg.Database.URL
	}
	if s, err := store.NewStore(dbPath, log.Logger); err != nil {
		log.Warn().Err(err).Str("db_path", dbPath).Msg("failed to open store, continuing without persistence")
	} else {
		st = s
	}

	// Event consumer. We start it as soon as the appservice is
	// wired up; new sessions are added via Track() as they're
	// created. The consumer opens one SSE connection per active
	// session to forge's GET /sessions/{id}/events endpoint.
	consumer := forge.NewEventConsumer(forge.EventConsumerConfig{
		Client:      forgeClient,
		Logger:      log.Logger,
		TypingQuiet: cfg.Forge.TypingQuiet(),
	})

	mxClient, err := matrix.NewClient(matrix.ClientConfig{
		Homeserver: cfg.Homeserver,
		Appservice: cfg.Appservice,
		Bridge:     cfg.Bridge,
		Logger:     &log.Logger,
	}, ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create Matrix client")
	}

	as := appservice.NewAppService(*cfg, mxClient, forgeClient, consumer, st, log.Logger)

	if err := mxClient.Start(); err != nil {
		log.Fatal().Err(err).Msg("failed to start Matrix client")
	}

	// Kick off the forge event consumer. StartEvents re-tracks
	// any session-room bindings that were restored from the
	// portal store; without that the consumer would be running
	// but not subscribed to any forge SSE stream, so user
	// messages would still be forwarded to forge (the
	// sessionRooms map is restored) but forge's responses
	// would never reach the room.
	stopConsumer := as.StartEvents(ctx)

	// HTTP server for the appservice endpoint and the health check.
	httpMux := http.NewServeMux()
	httpMux.Handle("/_matrix/app/v1/", mxClient)
	httpMux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"ok","service":"appservice"}`)
	})
	// Programmatic agent creation: called by forge's
	// `forge-agent-setup` to provision a Matrix room for
	// a scheduled agent from the host shell. See
	// pkg/appservice/agent.go for the implementation.
	httpMux.HandleFunc("/api/v1/agents", as.HandleCreateAgent)

	httpServer := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", cfg.API.Host, cfg.API.Port),
		Handler: httpMux,
	}

	go func() {
		log.Info().Str("addr", httpServer.Addr).Msg("starting HTTP server")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("HTTP server error")
		}
	}()

	log.Info().
		Str("version", Tag).
		Str("appservice_url", fmt.Sprintf("http://%s/_matrix/app/v1/", httpServer.Addr)).
		Str("forge_url", cfg.Forge.URL).
		Msg("pi-matrix appservice started")

	<-ctx.Done()
	log.Info().Msg("shutting down...")
	mxClient.Stop()
	stopConsumer()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	httpServer.Shutdown(shutdownCtx)

	log.Info().Msg("shutdown complete")
}

func setupLogger() {
	zerolog.TimeFieldFormat = time.RFC3339
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
}

func loadConfig(path string) (*config.Config, error) {
	cfg, err := config.Load(path)
	if err != nil {
		if os.IsNotExist(err) {
			if err2 := exportConfig(path); err2 != nil {
				return nil, fmt.Errorf("failed to export example config: %w", err2)
			}
			cfg, err = config.Load(path)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	return cfg, nil
}

func exportConfig(path string) error {
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}
	}
	exampleConfig := config.GetExampleConfig()
	return os.WriteFile(path, []byte(exampleConfig), 0644)
}

// startConsumer wires the consumer into the appservice's event
// callback and starts the streaming loop. Returns a stop function.
func startConsumer(ctx context.Context, consumer *forge.EventConsumer) func() {
	consumer.Start(ctx)
	return consumer.Stop
}
