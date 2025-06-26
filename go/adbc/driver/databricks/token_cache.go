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

package databricks

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/google/uuid"
	"golang.org/x/oauth2"
)

const (
	cacheLeaseTimeout  = 30 * time.Second
	leaseRenewInterval = 10 * time.Second
	// owner rw
	cacheFilePermission = 0600
	// owner rwx
	cacheDirPermission = 0700
)

// LeaseInfo contains lease metadata with UUID-based identification
type LeaseInfo struct {
	LeaseID      string `json:"lease_id"`
	DeadlineUnix int64  `json:"deadline_unix"`
}

type TokenCacheEntry struct {
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token,omitempty"`
	TokenType    string    `json:"token_type"`
	Expiry       time.Time `json:"expiry"`
	Host         string    `json:"host"`
	ClientID     string    `json:"client_id"`
}

func (e *TokenCacheEntry) ToToken() *oauth2.Token {
	return &oauth2.Token{
		AccessToken:  e.AccessToken,
		RefreshToken: e.RefreshToken,
		TokenType:    e.TokenType,
		Expiry:       e.Expiry,
	}
}

// TokenCache handles caching of OAuth tokens using a lease
type TokenCache struct {
	cacheDir string
	leaseID  string
}

func NewTokenCache() (*TokenCache, error) {
	cacheDir, err := getCacheDirectory()
	if err != nil {
		return nil, fmt.Errorf("failed to get cache directory: %w", err)
	}

	// Ensure cache directory exists with proper permissions
	if err := os.MkdirAll(cacheDir, cacheDirPermission); err != nil {
		return nil, fmt.Errorf("failed to create cache directory: %w", err)
	}

	return &TokenCache{
		cacheDir: cacheDir,
	}, nil
}

func getCacheDirectory() (string, error) {
	var cacheDir string

	switch runtime.GOOS {
	case "windows":
		// Windows: %APPDATA%\databricks-adbc
		appData := os.Getenv("APPDATA")
		if appData == "" {
			homeDir, err := os.UserHomeDir()
			if err != nil {
				return "", fmt.Errorf("failed to get user home directory: %w", err)
			}
			appData = filepath.Join(homeDir, "AppData", "Roaming")
		}
		cacheDir = filepath.Join(appData, "databricks-adbc")

	case "darwin":
		// macOS: ~/Library/Caches/databricks-adbc
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("failed to get user home directory: %w", err)
		}
		cacheDir = filepath.Join(homeDir, "Library", "Caches", "databricks-adbc")

	default:
		// Linux and other Unix-like systems: ~/.cache/databricks-adbc or $XDG_CACHE_HOME/databricks-adbc
		xdgCache := os.Getenv("XDG_CACHE_HOME")
		if xdgCache != "" {
			cacheDir = filepath.Join(xdgCache, "databricks-adbc")
		} else {
			homeDir, err := os.UserHomeDir()
			if err != nil {
				return "", fmt.Errorf("failed to get user home directory: %w", err)
			}
			cacheDir = filepath.Join(homeDir, ".cache", "databricks-adbc")
		}
	}

	return cacheDir, nil
}

func (tc *TokenCache) getCacheKey(host, clientID string) string {
	// Create a hash of host+clientID to ensure unique cache entries
	hash := sha256.Sum256([]byte(host + ":" + clientID))
	return fmt.Sprintf("token_%x.json", hash[:8])
}

func (tc *TokenCache) getFilePaths(host, clientID string) (string, string) {
	cacheKey := tc.getCacheKey(host, clientID)
	cacheFile := filepath.Join(tc.cacheDir, cacheKey)
	leaseFile := cacheFile + ".lease"
	return cacheFile, leaseFile
}

// acquireLease attempts to acquire a lease using UUID-based approach
func (tc *TokenCache) acquireLease(ctx context.Context, leaseFile string) error {
	// Generate UUID for this lease attempt
	leaseID := uuid.New().String()

	for {
		deadline := time.Now().Add(cacheLeaseTimeout)

		// Try to open lease file (create if doesn't exist)
		file, err := os.OpenFile(leaseFile, os.O_CREATE|os.O_RDWR, cacheFilePermission)
		if err != nil {
			return fmt.Errorf("failed to open lease file: %w", err)
		}

		// Read existing lease info if present
		data, readErr := os.ReadFile(leaseFile)
		var existingLease LeaseInfo

		if readErr == nil && len(data) > 0 {
			json.Unmarshal(data, &existingLease)
		}

		// Check if deadline has passed
		now := time.Now()
		existingDeadline := time.Unix(existingLease.DeadlineUnix, 0)

		if existingLease.LeaseID == "" || now.After(existingDeadline) {
			// Lease is available - acquire it
			newLease := LeaseInfo{
				LeaseID:      leaseID,
				DeadlineUnix: deadline.Unix(),
			}

			leaseData, _ := json.Marshal(newLease)

			// Truncate and write new lease
			file.Truncate(0)
			file.Seek(0, 0)
			file.Write(leaseData)
			file.Close()

			tc.leaseID = leaseID
			return nil
		} else {
			// Lease is held by another process - sleep until deadline and retry
			file.Close()

			sleepDuration := time.Until(existingDeadline)
			if sleepDuration > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(sleepDuration):
					// Continue to retry
				}
			}
		}
	}
}

// renewLease extends the lease deadline for the current lease ID
func (tc *TokenCache) renewLease(leaseFile string) error {
	if tc.leaseID == "" {
		return fmt.Errorf("no active lease to renew")
	}

	// Open lease file
	file, err := os.OpenFile(leaseFile, os.O_RDWR, cacheFilePermission)
	if err != nil {
		return fmt.Errorf("failed to open lease file for renewal: %w", err)
	}
	defer file.Close()

	// Read current lease
	data, err := os.ReadFile(leaseFile)
	if err != nil {
		return fmt.Errorf("failed to read lease file: %w", err)
	}

	var currentLease LeaseInfo
	if err := json.Unmarshal(data, &currentLease); err != nil {
		return fmt.Errorf("failed to parse lease file: %w", err)
	}

	// Check if we still own the lease and it's not expired
	now := time.Now()
	deadline := time.Unix(currentLease.DeadlineUnix, 0)

	if currentLease.LeaseID == tc.leaseID && now.Before(deadline) {
		// Renew the lease
		currentLease.DeadlineUnix = now.Add(cacheLeaseTimeout).Unix()

		renewedData, _ := json.Marshal(currentLease)

		// Truncate and write renewed lease
		file.Truncate(0)
		file.Seek(0, 0)
		file.Write(renewedData)

		return nil
	} else {
		// We no longer own the lease or it expired
		return fmt.Errorf("lease no longer owned or expired")
	}
}

// releaseLease removes the lease file completely
func (tc *TokenCache) releaseLease(leaseFile string) {
	if tc.leaseID == "" {
		return
	}

	os.Remove(leaseFile)
	tc.leaseID = ""
}

// LoadToken attempts to load a cached token
func (tc *TokenCache) LoadToken(ctx context.Context, host, clientID string) (*TokenCacheEntry, error) {
	cacheFile, leaseFile := tc.getFilePaths(host, clientID)

	// Acquire lease
	if err := tc.acquireLease(ctx, leaseFile); err != nil {
		return nil, fmt.Errorf("failed to acquire cache lease: %w", err)
	}
	defer tc.releaseLease(leaseFile)

	// Renew lease before file operations
	tc.renewLease(leaseFile)

	// Check if cache file exists
	if _, err := os.Stat(cacheFile); os.IsNotExist(err) {
		return nil, fmt.Errorf("no cached token found")
	}

	// Read and parse cache file
	data, err := os.ReadFile(cacheFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read cache file: %w", err)
	}

	// Renew lease during parsing
	tc.renewLease(leaseFile)

	var entry TokenCacheEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, fmt.Errorf("failed to parse cached token: %w", err)
	}

	// Verify token is for the correct host and client
	if entry.Host != host || entry.ClientID != clientID {
		return nil, fmt.Errorf("cached token is for different host/client")
	}

	// Return the cache entry for caller to handle
	return &entry, nil
}

// SaveToken saves a token to the cache
func (tc *TokenCache) SaveToken(ctx context.Context, token *oauth2.Token, host, clientID string) error {
	if token == nil {
		return fmt.Errorf("token cannot be nil")
	}

	cacheFile, leaseFile := tc.getFilePaths(host, clientID)

	// Acquire lease
	if err := tc.acquireLease(ctx, leaseFile); err != nil {
		return fmt.Errorf("failed to acquire cache lease: %w", err)
	}
	defer tc.releaseLease(leaseFile)

	// Renew lease before operations
	tc.renewLease(leaseFile)

	// Create cache entry
	entry := TokenCacheEntry{
		AccessToken:  token.AccessToken,
		RefreshToken: token.RefreshToken,
		TokenType:    token.TokenType,
		Expiry:       token.Expiry,
		Host:         host,
		ClientID:     clientID,
	}

	// Marshal to JSON
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal token cache entry: %w", err)
	}

	// Renew lease before file write
	tc.renewLease(leaseFile)

	// Write to temporary file first, then rename for atomic operation
	tempFile := cacheFile + ".tmp"
	if err := os.WriteFile(tempFile, data, cacheFilePermission); err != nil {
		return fmt.Errorf("failed to write temporary cache file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tempFile, cacheFile); err != nil {
		os.Remove(tempFile) // Clean up temp file on failure
		return fmt.Errorf("failed to rename cache file: %w", err)
	}

	return nil
}

// ClearExpiredTokens removes expired tokens from the cache
func (tc *TokenCache) ClearExpiredTokens(ctx context.Context) error {
	entries, err := os.ReadDir(tc.cacheDir)
	if err != nil {
		return fmt.Errorf("failed to read cache directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filePath := filepath.Join(tc.cacheDir, entry.Name())

		// Handle .json cache files
		if strings.HasSuffix(entry.Name(), ".json") {
			data, err := os.ReadFile(filePath)
			if err != nil {
				continue
			}

			var cacheEntry TokenCacheEntry
			if err := json.Unmarshal(data, &cacheEntry); err != nil {
				continue
			}

			// Remove expired tokens and their corresponding lease files
			if time.Now().After(cacheEntry.Expiry) {
				os.Remove(filePath)
				os.Remove(filePath + ".lease")
			}
		}

		// Handle orphaned .lease files (lease files without corresponding .json files)
		if strings.HasSuffix(entry.Name(), ".lease") {
			jsonFile := strings.TrimSuffix(filePath, ".lease")
			if _, err := os.Stat(jsonFile); os.IsNotExist(err) {
				os.Remove(filePath)
			}
		}
	}

	return nil
}
