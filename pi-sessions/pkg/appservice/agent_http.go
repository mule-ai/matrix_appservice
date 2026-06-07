// pkg/appservice - HTTP handler for POST /api/v1/agents.
//
// This is the wire format used by the forge
// `forge-agent-setup` script to provision a Matrix room for a
// scheduled agent from the host shell, without going through
// the matrix_appservice's DM-driven `/start` flow.
//
// See appservice/agent.go for CreateAgent itself (the body
// of the work). This file is just the HTTP transport.
//
// Copyright (C) 2026 Mule AI
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package appservice

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

// HandleCreateAgent is the HTTP handler mounted at
// POST /api/v1/agents. Body: CreateAgentRequest (JSON).
// Response: 200 + CreateAgentResponse on success, 4xx/5xx
// + {"error": "..."} on failure.
//
// Auth: X-API-Key header must match cfg.API.APIKey. The
// matrix-issued as_token is the wrong secret for this path
// (it's a per-user-room secret, not an operator secret);
// we reuse forge's X-API-Key for now, which is what the
// forge-agent-setup script sends. A future config field
// (cfg.API.AdminToken) can layer in a separate admin secret
// if desired; today the API key is the admin key.
func (as *AppService) HandleCreateAgent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAgentError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Auth. Empty APIKey disables auth, which matches the
	// behavior of the rest of the appservice's HTTP surface
	// (e.g. /health). Operators running on a public network
	// should set cfg.API.APIKey.
	if as.config.API.APIKey != "" {
		got := r.Header.Get("X-API-Key")
		if got == "" || got != as.config.API.APIKey {
			writeAgentError(w, http.StatusUnauthorized, "missing or invalid X-API-Key")
			return
		}
	}

	// Decode. 1 MiB is well above any reasonable CreateAgent
	// payload (working_dir is the longest field) but stops
	// a malicious client from making us buffer an unbounded
	// body.
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	defer r.Body.Close()

	var req CreateAgentRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeAgentError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}

	resp, err := as.CreateAgent(r.Context(), req)
	if err != nil {
		// Map known validation failures to 400, everything
		// else to 500. The CreateAgent error messages are
		// already operator-facing; we surface them verbatim
		// in the body.
		status := http.StatusInternalServerError
		if isClientError(err) {
			status = http.StatusBadRequest
		}
		writeAgentError(w, status, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// isClientError treats validation-shaped errors as 400 and
// the rest as 500. Today the only thing CreateAgent
// validates explicitly is the request shape, but anything
// containing "is required" or "is not a valid" matches
// that pattern; we also treat the "looks-like-a-validation"
// errors as client errors. Everything else is a server
// problem (forge down, matrix down, store failure) and
// gets a 500.
func isClientError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, hint := range []string{"is required", "is not a valid", "missing or invalid"} {
		if strings.Contains(msg, hint) {
			return true
		}
	}
	// forge.GetSession returns an error wrapping the HTTP
	// status. 404 here means the operator passed a bogus
	// session_id; that's a client error too.
	if strings.Contains(msg, "forge session") && strings.Contains(msg, "404") {
		return true
	}
	return false
}

// writeAgentError centralizes the error-response shape so
// every code path produces the same {"error": "..."} body
// with a consistent Content-Type. The matrix_appservice
// doesn't currently have a structured error type, so we
// roll a small one inline.
func writeAgentError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Error string `json:"error"`
	}{Error: msg})
}

// errors.As guard so the package compiles even when this
// file is the only consumer of errors. Cheap.
var _ = errors.As
