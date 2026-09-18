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

package driverbase

import (
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildDriverVersion(t *testing.T) {
	t.Run("release version with revision", func(t *testing.T) {
		info := &debug.BuildInfo{
			Main: debug.Module{Version: "1.2.3"},
			Settings: []debug.BuildSetting{
				{Key: "vcs.revision", Value: "1234567890abcdef"},
			},
		}

		require.Equal(t, "1.2.3+1234567890ab", buildDriverVersion(info))
	})

	t.Run("devel revision dirty", func(t *testing.T) {
		info := &debug.BuildInfo{
			Main: debug.Module{Version: "(devel)"},
			Settings: []debug.BuildSetting{
				{Key: "vcs.revision", Value: "abcdef1234567890"},
				{Key: "vcs.modified", Value: "true"},
			},
		}

		require.Equal(t, "abcdef123456-dev", buildDriverVersion(info))
	})

	t.Run("release version dirty without revision", func(t *testing.T) {
		info := &debug.BuildInfo{
			Main: debug.Module{Version: "2.0.0"},
			Settings: []debug.BuildSetting{
				{Key: "vcs.modified", Value: "true"},
			},
		}

		require.Equal(t, "2.0.0-dev", buildDriverVersion(info))
	})

	t.Run("no useful metadata", func(t *testing.T) {
		info := &debug.BuildInfo{
			Main: debug.Module{Version: "(devel)"},
		}

		require.Empty(t, buildDriverVersion(info))
	})
}
