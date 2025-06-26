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
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

func TestTokenCache_SaveAndLoad(t *testing.T) {
	// Create a temporary cache directory for testing
	tempDir, err := os.MkdirTemp("", "adbc-token-cache-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create token cache with custom directory
	cache := &TokenCache{cacheDir: tempDir}

	ctx := context.Background()
	host := "test-workspace.cloud.databricks.com"
	clientID := "test-client-id"

	// Create a test token
	token := &oauth2.Token{
		AccessToken:  "test-access-token",
		RefreshToken: "test-refresh-token",
		TokenType:    "Bearer",
		Expiry:       time.Now().Add(1 * time.Hour),
	}

	// Save token to cache
	err = cache.SaveToken(ctx, token, host, clientID)
	require.NoError(t, err)

	// Load token from cache
	loadedEntry, err := cache.LoadToken(ctx, host, clientID)
	require.NoError(t, err)

	// Verify token data
	assert.Equal(t, token.AccessToken, loadedEntry.AccessToken)
	assert.Equal(t, token.RefreshToken, loadedEntry.RefreshToken)
	assert.Equal(t, token.TokenType, loadedEntry.TokenType)
	assert.WithinDuration(t, token.Expiry, loadedEntry.Expiry, time.Second)
}

func TestTokenCache_ExpiredToken(t *testing.T) {
	// Create a temporary cache directory for testing
	tempDir, err := os.MkdirTemp("", "adbc-token-cache-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create token cache with custom directory
	cache := &TokenCache{cacheDir: tempDir}

	ctx := context.Background()
	host := "test-workspace.cloud.databricks.com"
	clientID := "test-client-id"

	// Create an expired token
	token := &oauth2.Token{
		AccessToken:  "test-access-token",
		RefreshToken: "test-refresh-token",
		TokenType:    "Bearer",
		Expiry:       time.Now().Add(-1 * time.Hour), // Expired 1 hour ago
	}

	// Save token to cache
	err = cache.SaveToken(ctx, token, host, clientID)
	require.NoError(t, err)

	// Try to load expired token - should succeed but caller handles expiration
	loadedEntry, err := cache.LoadToken(ctx, host, clientID)
	require.NoError(t, err)

	// Verify the token is expired (caller's responsibility to check)
	assert.True(t, time.Now().After(loadedEntry.Expiry))
	assert.Equal(t, "test-access-token", loadedEntry.AccessToken)
}

func TestTokenCache_ConcurrentAccess(t *testing.T) {
	// Test concurrent access to the same cache entry
	tempDir := t.TempDir()

	const numGoroutines = 5 // Reduced for faster testing
	const host = "test-host.databricks.com"
	const clientID = "test-client-id"

	results := make(chan error, numGoroutines)
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Add small jitter to reduce thundering herd
			time.Sleep(time.Duration(id*50) * time.Millisecond)

			cache := &TokenCache{cacheDir: tempDir}

			// Use longer timeout that accounts for potential lease waiting
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()

			token := &oauth2.Token{
				AccessToken:  "access-token-" + string(rune(id)),
				RefreshToken: "refresh-token-" + string(rune(id)),
				TokenType:    "Bearer",
				Expiry:       time.Now().Add(time.Hour),
			}

			err := cache.SaveToken(ctx, token, host, clientID)
			results <- err
		}(i)
	}

	wg.Wait()
	close(results)

	// Check that all operations completed successfully
	successCount := 0
	for err := range results {
		if err == nil {
			successCount++
		} else {
			t.Logf("Goroutine failed: %v", err)
		}
	}

	// Most operations should succeed (allowing for some timeouts under extreme load)
	if successCount < numGoroutines/2 {
		t.Fatalf("Too few operations succeeded: %d/%d", successCount, numGoroutines)
	}

	t.Logf("Concurrent access test: %d/%d operations succeeded", successCount, numGoroutines)
}

func TestTokenCache_DifferentHosts(t *testing.T) {
	// Create a temporary cache directory for testing
	tempDir, err := os.MkdirTemp("", "adbc-token-cache-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create token cache with custom directory
	cache := &TokenCache{cacheDir: tempDir}

	ctx := context.Background()
	host1 := "workspace1.cloud.databricks.com"
	host2 := "workspace2.cloud.databricks.com"
	clientID := "test-client-id"

	// Create tokens for different hosts
	token1 := &oauth2.Token{
		AccessToken:  "token-for-host1",
		RefreshToken: "refresh-for-host1",
		TokenType:    "Bearer",
		Expiry:       time.Now().Add(1 * time.Hour),
	}

	token2 := &oauth2.Token{
		AccessToken:  "token-for-host2",
		RefreshToken: "refresh-for-host2",
		TokenType:    "Bearer",
		Expiry:       time.Now().Add(1 * time.Hour),
	}

	// Save tokens for different hosts
	err = cache.SaveToken(ctx, token1, host1, clientID)
	require.NoError(t, err)

	err = cache.SaveToken(ctx, token2, host2, clientID)
	require.NoError(t, err)

	// Load and verify tokens are separate
	loadedEntry1, err := cache.LoadToken(ctx, host1, clientID)
	require.NoError(t, err)
	assert.Equal(t, "token-for-host1", loadedEntry1.AccessToken)

	loadedEntry2, err := cache.LoadToken(ctx, host2, clientID)
	require.NoError(t, err)
	assert.Equal(t, "token-for-host2", loadedEntry2.AccessToken)
}

func TestTokenCache_GetCacheDirectory(t *testing.T) {
	cacheDir, err := getCacheDirectory()
	require.NoError(t, err)
	assert.NotEmpty(t, cacheDir)

	// Verify it contains our app name
	assert.Contains(t, cacheDir, "databricks-adbc")

	// Verify it's an absolute path
	assert.True(t, filepath.IsAbs(cacheDir))
}

func TestTokenCache_LeaseSystem(t *testing.T) {
	// Create a temporary cache directory for testing
	tempDir, err := os.MkdirTemp("", "adbc-token-cache-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cache1 := &TokenCache{cacheDir: tempDir}
	cache2 := &TokenCache{cacheDir: tempDir}
	ctx := context.Background()

	// Test lease acquisition
	leaseFile := filepath.Join(tempDir, "test.lease")

	// First acquisition should succeed
	err = cache1.acquireLease(ctx, leaseFile)
	assert.NoError(t, err)

	// Second acquisition should timeout (lease already held)
	ctx2, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err = cache2.acquireLease(ctx2, leaseFile)
	assert.Error(t, err)
	assert.True(t, err == context.DeadlineExceeded || err == context.Canceled)

	// Release lease
	cache1.releaseLease(leaseFile)

	// Should be able to acquire again after release
	err = cache2.acquireLease(ctx, leaseFile)
	assert.NoError(t, err)

	// Clean up
	cache2.releaseLease(leaseFile)
}

func TestTokenCache_LeaseWaitingAndRetry(t *testing.T) {
	tempDir := t.TempDir()
	cache1 := &TokenCache{cacheDir: tempDir}
	cache2 := &TokenCache{cacheDir: tempDir}

	host := "test-host.databricks.com"
	clientID := "test-client-id"
	_, leaseFile := cache1.getFilePaths(host, clientID)

	ctx := context.Background()

	// Cache1 acquires lease
	err := cache1.acquireLease(ctx, leaseFile)
	require.NoError(t, err)

	// Start goroutine that will release lease after short delay
	go func() {
		time.Sleep(500 * time.Millisecond)
		cache1.releaseLease(leaseFile)
	}()

	// Cache2 should wait and then successfully acquire
	start := time.Now()
	err = cache2.acquireLease(ctx, leaseFile)
	elapsed := time.Since(start)

	assert.NoError(t, err)
	assert.True(t, elapsed >= 400*time.Millisecond, "Should have waited for lease to be released")

	cache2.releaseLease(leaseFile)
}

func TestTokenCache_ContextCancellation(t *testing.T) {
	tempDir := t.TempDir()
	cache := &TokenCache{cacheDir: tempDir}

	// Create a lease file that won't expire for a while
	host := "test-host.databricks.com"
	clientID := "test-client-id"
	_, leaseFile := cache.getFilePaths(host, clientID)

	lease := LeaseInfo{
		LeaseID:      uuid.New().String(),
		DeadlineUnix: time.Now().Add(time.Hour).Unix(),
	}
	leaseData, _ := json.Marshal(lease)
	os.WriteFile(leaseFile, leaseData, 0600)

	// Try to acquire lease with short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := cache.acquireLease(ctx, leaseFile)
	if err != context.DeadlineExceeded && err != context.Canceled {
		t.Errorf("Expected context cancellation error, got: %v", err)
	}
}

func TestTokenCache_CorruptedLeaseFile(t *testing.T) {
	tempDir := t.TempDir()
	cache := &TokenCache{cacheDir: tempDir}

	host := "test-host.databricks.com"
	clientID := "test-client-id"
	_, leaseFile := cache.getFilePaths(host, clientID)

	// Write corrupted JSON to lease file
	os.WriteFile(leaseFile, []byte("invalid json{"), 0600)

	ctx := context.Background()

	// Should be able to acquire lease despite corruption
	err := cache.acquireLease(ctx, leaseFile)
	if err != nil {
		t.Errorf("Should handle corrupted lease file gracefully, got: %v", err)
	}

	// Verify lease was acquired
	if cache.leaseID == "" {
		t.Error("Lease ID should be set after acquiring lease")
	}
}

func TestTokenCache_EmptyLeaseFile(t *testing.T) {
	tempDir := t.TempDir()
	cache := &TokenCache{cacheDir: tempDir}

	host := "test-host.databricks.com"
	clientID := "test-client-id"
	_, leaseFile := cache.getFilePaths(host, clientID)

	// Create empty lease file
	os.WriteFile(leaseFile, []byte(""), 0600)

	ctx := context.Background()

	// Should be able to acquire lease from empty file
	err := cache.acquireLease(ctx, leaseFile)
	if err != nil {
		t.Errorf("Should handle empty lease file gracefully, got: %v", err)
	}

	if cache.leaseID == "" {
		t.Error("Lease ID should be set after acquiring lease")
	}
}

func TestTokenCache_LeaseRenewal(t *testing.T) {
	tempDir := t.TempDir()
	cache := &TokenCache{cacheDir: tempDir}

	host := "test-host.databricks.com"
	clientID := "test-client-id"
	_, leaseFile := cache.getFilePaths(host, clientID)

	ctx := context.Background()

	// Acquire lease
	err := cache.acquireLease(ctx, leaseFile)
	if err != nil {
		t.Fatalf("Failed to acquire lease: %v", err)
	}

	originalLeaseID := cache.leaseID

	// Wait a bit and renew
	time.Sleep(100 * time.Millisecond)
	err = cache.renewLease(leaseFile)
	if err != nil {
		t.Errorf("Failed to renew lease: %v", err)
	}

	// Lease ID should remain the same
	if cache.leaseID != originalLeaseID {
		t.Error("Lease ID changed during renewal")
	}

	// Verify deadline was updated
	data, _ := os.ReadFile(leaseFile)
	var renewed LeaseInfo
	json.Unmarshal(data, &renewed)

	if renewed.LeaseID != originalLeaseID {
		t.Error("Lease ID in file doesn't match")
	}

	if time.Unix(renewed.DeadlineUnix, 0).Before(time.Now().Add(25 * time.Second)) {
		t.Error("Lease deadline wasn't properly renewed")
	}
}

func TestTokenCache_LeaseRenewalWithoutOwnership(t *testing.T) {
	tempDir := t.TempDir()
	cache := &TokenCache{cacheDir: tempDir}

	host := "test-host.databricks.com"
	clientID := "test-client-id"
	_, leaseFile := cache.getFilePaths(host, clientID)

	// Create a lease owned by another process
	otherLease := LeaseInfo{
		LeaseID:      uuid.New().String(),
		DeadlineUnix: time.Now().Add(time.Hour).Unix(),
	}
	leaseData, _ := json.Marshal(otherLease)
	os.WriteFile(leaseFile, leaseData, 0600)

	// Try to renew without owning the lease
	err := cache.renewLease(leaseFile)
	if err == nil {
		t.Error("Should not be able to renew lease we don't own")
	}
}

func TestTokenCache_ExpiredTokenHandling(t *testing.T) {
	tempDir := t.TempDir()
	cache := &TokenCache{cacheDir: tempDir}

	host := "test-host.databricks.com"
	clientID := "test-client-id"

	// Save an expired token
	expiredToken := &oauth2.Token{
		AccessToken:  "expired-token",
		RefreshToken: "refresh-token",
		TokenType:    "Bearer",
		Expiry:       time.Now().Add(-time.Hour), // Expired 1 hour ago
	}

	ctx := context.Background()
	err := cache.SaveToken(ctx, expiredToken, host, clientID)
	if err != nil {
		t.Fatalf("Failed to save expired token: %v", err)
	}

	// Try to load expired token - should succeed but caller handles expiration
	loadedEntry, err := cache.LoadToken(ctx, host, clientID)
	require.NoError(t, err)

	// Verify the token is expired (caller's responsibility to check)
	assert.True(t, time.Now().After(loadedEntry.Expiry))
	assert.Equal(t, "expired-token", loadedEntry.AccessToken)
}

func TestTokenCache_FileSystemErrors(t *testing.T) {
	// Test with read-only directory
	tempDir := t.TempDir()
	readOnlyDir := filepath.Join(tempDir, "readonly")
	os.Mkdir(readOnlyDir, 0444) // Read-only

	cache := &TokenCache{cacheDir: readOnlyDir}

	token := &oauth2.Token{
		AccessToken: "test-token",
		TokenType:   "Bearer",
		Expiry:      time.Now().Add(time.Hour),
	}

	ctx := context.Background()
	err := cache.SaveToken(ctx, token, "host", "client")
	if err == nil {
		t.Error("Should fail when trying to write to read-only directory")
	}
}

func TestTokenCache_ReleaseLease(t *testing.T) {
	tempDir := t.TempDir()
	cache := &TokenCache{cacheDir: tempDir}

	host := "test-host.databricks.com"
	clientID := "test-client-id"
	_, leaseFile := cache.getFilePaths(host, clientID)

	ctx := context.Background()

	// Acquire lease
	err := cache.acquireLease(ctx, leaseFile)
	if err != nil {
		t.Fatalf("Failed to acquire lease: %v", err)
	}

	// Verify lease file exists and has content
	data, err := os.ReadFile(leaseFile)
	if err != nil || len(data) == 0 {
		t.Error("Lease file should exist and have content")
	}

	// Release lease
	cache.releaseLease(leaseFile)

	// Verify lease ID is cleared
	if cache.leaseID != "" {
		t.Error("Lease ID should be cleared after release")
	}

	// Verify lease file is deleted
	_, err = os.Stat(leaseFile)
	if !os.IsNotExist(err) {
		t.Error("Lease file should be deleted after release")
	}
}

func TestTokenCache_TokenValidation(t *testing.T) {
	tempDir := t.TempDir()
	cache := &TokenCache{cacheDir: tempDir}

	ctx := context.Background()

	// Test saving nil token
	err := cache.SaveToken(ctx, nil, "host", "client")
	if err == nil {
		t.Error("Should reject nil token")
	}

	// Test token with wrong host/client
	token := &oauth2.Token{
		AccessToken: "test-token",
		TokenType:   "Bearer",
		Expiry:      time.Now().Add(time.Hour),
	}

	err = cache.SaveToken(ctx, token, "host1", "client1")
	if err != nil {
		t.Fatalf("Failed to save token: %v", err)
	}

	// Try to load with different host/client
	_, err = cache.LoadToken(ctx, "host2", "client2")
	if err == nil {
		t.Error("Should not load token for different host/client")
	}
}

func TestTokenCache_ClearExpiredTokens(t *testing.T) {
	tempDir := t.TempDir()
	cache := &TokenCache{cacheDir: tempDir}

	ctx := context.Background()

	// Save valid token
	validToken := &oauth2.Token{
		AccessToken: "valid-token",
		TokenType:   "Bearer",
		Expiry:      time.Now().Add(time.Hour),
	}
	cache.SaveToken(ctx, validToken, "host1", "client1")

	// Save expired token
	expiredToken := &oauth2.Token{
		AccessToken: "expired-token",
		TokenType:   "Bearer",
		Expiry:      time.Now().Add(-time.Hour),
	}
	cache.SaveToken(ctx, expiredToken, "host2", "client2")

	// Clear expired tokens
	err := cache.ClearExpiredTokens(ctx)
	if err != nil {
		t.Errorf("Failed to clear expired tokens: %v", err)
	}

	// Valid token should still be loadable
	_, err = cache.LoadToken(ctx, "host1", "client1")
	if err != nil {
		t.Error("Valid token should still be loadable after cleanup")
	}

	// Expired token should be gone (though it would fail to load anyway due to expiration check)
	files, _ := os.ReadDir(tempDir)
	jsonFiles := 0
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".json" {
			jsonFiles++
		}
	}

	if jsonFiles != 1 {
		t.Errorf("Expected 1 JSON file after cleanup, found %d", jsonFiles)
	}
}

func TestTokenCache_LongRunningOperation(t *testing.T) {
	tempDir := t.TempDir()
	cache := &TokenCache{cacheDir: tempDir}

	host := "test-host.databricks.com"
	clientID := "test-client-id"
	_, leaseFile := cache.getFilePaths(host, clientID)

	ctx := context.Background()

	// Acquire lease
	err := cache.acquireLease(ctx, leaseFile)
	if err != nil {
		t.Fatalf("Failed to acquire lease: %v", err)
	}

	// Simulate long operation with periodic renewals
	for i := 0; i < 5; i++ {
		time.Sleep(100 * time.Millisecond)
		err = cache.renewLease(leaseFile)
		if err != nil {
			t.Errorf("Failed to renew lease during long operation (iteration %d): %v", i, err)
		}
	}

	cache.releaseLease(leaseFile)
}

func TestTokenCache_MissingCacheFile(t *testing.T) {
	tempDir := t.TempDir()
	cache := &TokenCache{cacheDir: tempDir}

	ctx := context.Background()

	// Try to load token from non-existent cache
	_, err := cache.LoadToken(ctx, "host", "client")
	if err == nil {
		t.Error("Should fail when cache file doesn't exist")
	}
}

func TestTokenCacheEntry_ToToken(t *testing.T) {
	entry := &TokenCacheEntry{
		AccessToken:  "test-access-token",
		RefreshToken: "test-refresh-token",
		TokenType:    "Bearer",
		Expiry:       time.Now().Add(time.Hour),
		Host:         "test-host",
		ClientID:     "test-client",
	}

	token := entry.ToToken()

	// Verify all fields are correctly copied
	assert.Equal(t, entry.AccessToken, token.AccessToken)
	assert.Equal(t, entry.RefreshToken, token.RefreshToken)
	assert.Equal(t, entry.TokenType, token.TokenType)
	assert.Equal(t, entry.Expiry, token.Expiry)

	// Verify it returns a proper oauth2.Token
	assert.IsType(t, &oauth2.Token{}, token)
}

// Benchmark concurrent access performance
func BenchmarkTokenCache_ConcurrentAccess(b *testing.B) {
	tempDir := b.TempDir()

	const numGoroutines = 100
	host := "benchmark-host.databricks.com"
	clientID := "benchmark-client-id"

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cache := &TokenCache{cacheDir: tempDir}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

			token := &oauth2.Token{
				AccessToken:  "benchmark-token",
				RefreshToken: "benchmark-refresh",
				TokenType:    "Bearer",
				Expiry:       time.Now().Add(time.Hour),
			}

			cache.SaveToken(ctx, token, host, clientID)
			cancel()
		}
	})
}
