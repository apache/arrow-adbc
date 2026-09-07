// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// A simple OAuth 2.0 test server supporting Client Credentials (RFC 6749)
// and Token Exchange (RFC 8693) flows for testing ADBC FlightSQL authentication.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("Failed to encode JSON response: %v", err)
	}
}

func oauthError(w http.ResponseWriter, code, desc string) {
	writeJSON(w, http.StatusBadRequest, map[string]string{
		"error":             code,
		"error_description": desc,
	})
}

func issueToken(w http.ResponseWriter, prefix, scope string) {
	token := fmt.Sprintf("oauth-%s-token-%d", prefix, time.Now().Unix())
	log.Printf("Issuing %s token: %s", prefix, token)
	writeJSON(w, http.StatusOK, map[string]any{
		"access_token":      token,
		"token_type":        "Bearer",
		"expires_in":        3600,
		"issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
		"scope":             scope,
	})
}

func main() {
	host := flag.String("host", "0.0.0.0", "Host to bind")
	port := flag.Int("port", 8181, "Port to bind")
	clientID := flag.String("client-id", "test-client", "Expected client ID")
	clientSecret := flag.String("client-secret", "test-secret", "Expected client secret")
	flag.Parse()

	http.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := r.ParseForm(); err != nil {
			oauthError(w, "invalid_request", "Failed to parse form data")
			return
		}

		grantType := r.FormValue("grant_type")
		log.Printf("Token request: grant_type=%s", grantType)

		switch grantType {
		case "client_credentials":
			if r.FormValue("client_id") != *clientID || r.FormValue("client_secret") != *clientSecret {
				oauthError(w, "invalid_client", "Invalid client credentials")
				return
			}
			scope := r.FormValue("scope")
			if scope == "" {
				scope = "dremio.all"
			}
			issueToken(w, "cc", scope)

		case "urn:ietf:params:oauth:grant-type:token-exchange":
			if r.FormValue("subject_token") == "" || r.FormValue("subject_token_type") == "" {
				oauthError(w, "invalid_request", "Missing subject_token or subject_token_type")
				return
			}
			issueToken(w, "exchange", r.FormValue("scope"))

		default:
			oauthError(w, "unsupported_grant_type", fmt.Sprintf("Grant type '%s' not supported", grantType))
		}
	})

	http.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("OK"))
	})

	addr := fmt.Sprintf("%s:%d", *host, *port)
	log.Printf("Starting OAuth test server on %s (client_id=%s)", addr, *clientID)
	log.Fatal(http.ListenAndServe(addr, nil))
}
