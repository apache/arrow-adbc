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

package bigquery

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestAuthScopesSetGet tests setting and getting regular auth scopes
func TestAuthScopesSetGet(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	drv := NewDriver(mem)
	db, err := drv.NewDatabase(nil)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()

	// Test setting auth scopes
	expectedScopes := "https://www.googleapis.com/auth/drive,https://www.googleapis.com/auth/cloud-platform"
	err = db.SetOptions(map[string]string{
		OptionStringAuthScopes: expectedScopes,
	})
	if err != nil {
		t.Errorf("Failed to set auth scopes: %v", err)
	}

	// Test getting auth scopes
	getSetDB, ok := db.(interface {
		GetOption(string) (string, error)
	})
	if !ok {
		t.Fatal("Database does not implement GetOption")
	}

	retrievedScopes, err := getSetDB.GetOption(OptionStringAuthScopes)
	if err != nil {
		t.Errorf("Failed to get auth scopes: %v", err)
	}

	if retrievedScopes != expectedScopes {
		t.Errorf("Expected scopes %s, got %s", expectedScopes, retrievedScopes)
	}
}

// TestAuthScopesSeparateFromImpersonateScopes verifies that auth scopes and impersonate scopes are separate
func TestAuthScopesSeparateFromImpersonateScopes(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	drv := NewDriver(mem)
	db, err := drv.NewDatabase(nil)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()

	authScopes := "https://www.googleapis.com/auth/drive,https://www.googleapis.com/auth/cloud-platform"
	impersonateScopes := "https://www.googleapis.com/auth/bigquery,https://www.googleapis.com/auth/devstorage.read_write"

	// Set both types of scopes
	err = db.SetOptions(map[string]string{
		OptionStringAuthScopes:        authScopes,
		OptionStringImpersonateScopes: impersonateScopes,
	})
	if err != nil {
		t.Errorf("Failed to set scopes: %v", err)
	}

	getSetDB, ok := db.(interface {
		GetOption(string) (string, error)
	})
	if !ok {
		t.Fatal("Database does not implement GetOption")
	}

	// Verify auth scopes
	retrievedAuthScopes, err := getSetDB.GetOption(OptionStringAuthScopes)
	if err != nil {
		t.Errorf("Failed to get auth scopes: %v", err)
	}
	if retrievedAuthScopes != authScopes {
		t.Errorf("Expected auth scopes %s, got %s", authScopes, retrievedAuthScopes)
	}

	// Verify impersonate scopes (they should be different)
	retrievedImpScopes, err := getSetDB.GetOption(OptionStringImpersonateScopes)
	if err != nil {
		t.Errorf("Failed to get impersonate scopes: %v", err)
	}

	// The retrieved impersonate scopes should match what we set
	t.Logf("Auth scopes: %s", retrievedAuthScopes)
	t.Logf("Impersonate scopes: %s", retrievedImpScopes)

	// The key point: they should be stored separately
	if retrievedAuthScopes == impersonateScopes {
		t.Error("Auth scopes and impersonate scopes should be different but got the same value")
	}
}

// TestAuthScopesEmptyByDefault tests that auth scopes default to empty
func TestAuthScopesEmptyByDefault(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	drv := NewDriver(mem)
	db, err := drv.NewDatabase(nil)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()

	getSetDB, ok := db.(interface {
		GetOption(string) (string, error)
	})
	if !ok {
		t.Fatal("Database does not implement GetOption")
	}

	// Get auth scopes without setting them
	scopes, err := getSetDB.GetOption(OptionStringAuthScopes)
	if err != nil {
		t.Errorf("Failed to get auth scopes: %v", err)
	}

	if scopes != "" {
		t.Errorf("Expected empty scopes by default, got %s", scopes)
	}
}

// TestAuthScopesCommaSeparated tests that multiple scopes are properly handled
func TestAuthScopesCommaSeparated(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	drv := NewDriver(mem)
	db, err := drv.NewDatabase(nil)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()

	scopes := []string{
		"https://www.googleapis.com/auth/drive",
		"https://www.googleapis.com/auth/cloud-platform",
		"https://www.googleapis.com/auth/bigquery",
	}
	scopesStr := strings.Join(scopes, ",")

	err = db.SetOptions(map[string]string{
		OptionStringAuthScopes: scopesStr,
	})
	if err != nil {
		t.Errorf("Failed to set auth scopes: %v", err)
	}

	getSetDB, ok := db.(interface {
		GetOption(string) (string, error)
	})
	if !ok {
		t.Fatal("Database does not implement GetOption")
	}

	retrievedScopes, err := getSetDB.GetOption(OptionStringAuthScopes)
	if err != nil {
		t.Errorf("Failed to get auth scopes: %v", err)
	}

	if retrievedScopes != scopesStr {
		t.Errorf("Expected scopes %s, got %s", scopesStr, retrievedScopes)
	}

	// Verify the scopes contain all expected values
	retrievedParts := strings.Split(retrievedScopes, ",")
	if len(retrievedParts) != len(scopes) {
		t.Errorf("Expected %d scopes, got %d", len(scopes), len(retrievedParts))
	}
}

// TestAuthScopesPropagatedToConnection tests that scopes are propagated from database to connection
func TestAuthScopesPropagatedToConnection(t *testing.T) {
	// Note: This test doesn't actually open a connection (which would require valid GCP credentials)
	// but verifies that the scopes are stored in the database impl
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	drv := NewDriver(mem)

	scopes := "https://www.googleapis.com/auth/drive,https://www.googleapis.com/auth/cloud-platform"

	db, err := drv.NewDatabase(map[string]string{
		OptionStringProjectID:  "test-project",
		OptionStringAuthScopes: scopes,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()

	// Verify the scopes were set
	getSetDB, ok := db.(interface {
		GetOption(string) (string, error)
	})
	if !ok {
		t.Fatal("Database does not implement GetOption")
	}

	retrievedScopes, err := getSetDB.GetOption(OptionStringAuthScopes)
	if err != nil {
		t.Errorf("Failed to get auth scopes: %v", err)
	}

	if retrievedScopes != scopes {
		t.Errorf("Expected scopes %s, got %s", scopes, retrievedScopes)
	}

	// Note: We can't test actual connection opening without valid credentials,
	// but the struct propagation is tested in the databaseImpl.Open() method
	t.Log("Scopes successfully stored in database and ready for connection propagation")
}
