// sse-smoke is a small driver that exercises the EventConsumer
// against a real running forge instance. It creates a session,
// tracks it, sends a prompt, and prints every event delivered
// by the SSE stream. Used to verify the new forge SSE endpoint
// works end-to-end with the matrix appservice's consumer code.
//
// Usage: sse-smoke <forge-url> <api-key>
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"go.mau.fi/pi-matrix/pkg/forge"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: sse-smoke <forge-url> <api-key>")
		os.Exit(2)
	}
	forgeURL := os.Args[1]
	apiKey := os.Args[2]

	zerolog.TimeFieldFormat = time.RFC3339
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Info().Msg("shutting down")
		cancel()
	}()

	client := forge.NewClient(forgeURL, apiKey)
	var err error
	if err = client.Health(ctx); err != nil {
		log.Fatal().Err(err).Msg("forge health check failed")
	}
	log.Info().Str("forge_url", forgeURL).Msg("connected to forge")

	// Mint a profile for /tmp/sse-smoke. We use the proxy-anthropic
	// provider (works with the bitfrost proxy). Forge returns the
	// existing profile's `tools` as a JSON string, so we don't
	// bother fetching it — we just hardcode the right values.
	systemPrompt := "You are a smoke test assistant. Reply with exactly: SSE OK"
	profileAPIKey := "bifrost"
	profileBaseURL := "http://bitfrost.botnet:8080/anthropic"
	profile, err := client.CreateProfile(ctx, forge.Profile{
		Name:         fmt.Sprintf("sse-smoke-%d", time.Now().Unix()),
		Provider:     "proxy-anthropic",
		Model:        "minimax-anthropic/minimax-m2.7-highspeed",
		BaseURL:      &profileBaseURL,
		APIKey:       &profileAPIKey,
		WorkingDir:   "/tmp/sse-smoke",
		SystemPrompt: &systemPrompt,
		Tools:        []string{"bash"},
	})
	if err != nil {
		log.Fatal().Err(err).Msg("create profile")
	}
	log.Info().Str("profile_id", profile.ID).Msg("profile created")

	// Create a session
	title := "sse-smoke"
	sess, err := client.CreateSession(ctx, profile.ID, &title)
	if err != nil {
		log.Fatal().Err(err).Msg("create session")
	}
	log.Info().Str("session_id", sess.Session.ID).Str("working_dir", sess.WorkingDir).Msg("session created")

	// Start the consumer
	consumer := forge.NewEventConsumer(forge.EventConsumerConfig{
		Client:       client,
		Logger:       log.Logger,
		ReconnectMin: 100 * time.Millisecond,
		ReconnectMax: 1 * time.Second,
		TypingQuiet:  10 * time.Second,
	})

	var eventCount int
	consumer.OnEvent(func(ev forge.SessionEvent) {
		eventCount++
		switch ev.Type {
		case forge.EventMessage:
			log.Info().
				Int("n", eventCount).
				Str("type", "message").
				Str("content", truncate(ev.Content, 200)).
				Msg("event")
		case forge.EventTypingStart:
			log.Info().Int("n", eventCount).Str("type", "typing_start").Msg("event")
		case forge.EventTypingStop:
			log.Info().Int("n", eventCount).Str("type", "typing_stop").Msg("event")
		case forge.EventToolStart:
			log.Info().Int("n", eventCount).Str("type", "tool_start").Str("tool_name", ev.ToolName).Msg("event")
		case forge.EventToolEnd:
			log.Info().Int("n", eventCount).Str("type", "tool_end").Str("tool_name", ev.ToolName).Bool("is_error", ev.IsError).Msg("event")
		}
	})

	consumer.Start(ctx)
	defer consumer.Stop()

	if err := consumer.Track(ctx, sess.Session.ID); err != nil {
		log.Fatal().Err(err).Msg("track")
	}
	log.Info().Str("session_id", sess.Session.ID).Msg("consumer tracking session")

	// Send a prompt. This will start the agent. We expect to see
	// a typing_start, then a series of tool_start/tool_end events
	// (if the agent uses tools), then a final assistant message,
	// then a typing_stop.
	if err := client.SendMessage(ctx, sess.Session.ID, "list the files in /tmp using a tool, then say done"); err != nil {
		log.Fatal().Err(err).Msg("send message")
	}
	log.Info().Msg("prompt sent, waiting for events...")

	// Wait up to 60s for the agent to finish
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
		if eventCount > 0 {
			// Give it 5 more seconds after last event to catch
			// the trailing typing_stop / final message.
			extra := time.Now().Add(5 * time.Second)
			for time.Now().Before(extra) {
				<-time.After(500 * time.Millisecond)
			}
			break
		}
	}

	log.Info().Int("total_events", eventCount).Msg("smoke test complete")
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
