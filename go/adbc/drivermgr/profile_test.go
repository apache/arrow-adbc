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

//go:build cgo

package drivermgr_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/stretchr/testify/require"
)

// simpleProfile references a nonexistent driver. These tests only verify that
// the profile mechanism routes correctly — i.e., the error comes from failing
// to load the driver, not from failing to find or parse the profile.
const simpleProfile = `
profile_version = 1
driver = "/nonexistent/driver.so"

[Options]
`

func writeProfile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name+".toml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0600))
	return path
}

// loads a profile via the absolute path in the "profile" option.
func TestProfileAbsolutePath(t *testing.T) {
	dir := t.TempDir()
	profilePath := writeProfile(t, dir, "myprofile", simpleProfile)

	var drv drivermgr.Driver
	_, err := drv.NewDatabase(map[string]string{
		"profile": profilePath,
	})
	require.ErrorContains(t, err, "nonexistent")
}

// loads a profile by name using additional_profile_search_path_list.
func TestProfileByNameViaSearchPath(t *testing.T) {
	dir := t.TempDir()
	writeProfile(t, dir, "myprofile", simpleProfile)

	var drv drivermgr.Driver
	_, err := drv.NewDatabase(map[string]string{
		"profile":                             "myprofile",
		"additional_profile_search_path_list": dir,
	})
	require.ErrorContains(t, err, "nonexistent")
}

// loads a profile by name with ADBC_PROFILE_PATH set.
func TestProfileByNameViaEnvVar(t *testing.T) {
	dir := t.TempDir()
	writeProfile(t, dir, "myprofile", simpleProfile)

	t.Setenv("ADBC_PROFILE_PATH", dir)

	var drv drivermgr.Driver
	_, err := drv.NewDatabase(map[string]string{
		"profile": "myprofile",
	})
	require.ErrorContains(t, err, "nonexistent")
}

// loads a profile using a profile:// URI in the "uri" key.
func TestProfileViaURIOption(t *testing.T) {
	dir := t.TempDir()
	profilePath := writeProfile(t, dir, "myprofile", simpleProfile)

	var drv drivermgr.Driver
	_, err := drv.NewDatabase(map[string]string{
		"uri": "profile://" + profilePath,
	})
	require.ErrorContains(t, err, "nonexistent")
}

// loads a profile using a profile:// URI in the "driver" key.
func TestProfileViaDriverOption(t *testing.T) {
	dir := t.TempDir()
	profilePath := writeProfile(t, dir, "myprofile", simpleProfile)

	var drv drivermgr.Driver
	_, err := drv.NewDatabase(map[string]string{
		"driver": "profile://" + profilePath,
	})
	require.ErrorContains(t, err, "nonexistent")
}

// verifies that a missing profile returns an error.
func TestProfileNotFound(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("ADBC_PROFILE_PATH", dir)

	var drv drivermgr.Driver
	_, err := drv.NewDatabase(map[string]string{
		"profile": "does_not_exist",
	})
	require.ErrorContains(t, err, "Profile not found: does_not_exist")
}
