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

package driverbase_test

import (
	"testing"

	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/stretchr/testify/require"
)

func TestRotatingFileWriter(t *testing.T) {

	fw, err := driverbase.NewRotatingFileWriter(
		driverbase.WithFileSizeMaxKb(1),
		driverbase.WithFileCountMax(10),
	)
	require.NoError(t, err)
	defer func() {
		err := fw.Clear()
		require.NoError(t, err)
	}()

	const value = "my string\n"
	valueLen := len(value)

	for range 1000 {
		len, err := fw.Write([]byte(value))
		require.NoError(t, err)
		require.Equal(t, valueLen, len)
	}
	err = fw.Close()
	require.NoError(t, err)
}

func TestFileResuse(t *testing.T) {
	fw1, err := driverbase.NewRotatingFileWriter(
		driverbase.WithFileSizeMaxKb(1000),
		driverbase.WithFileCountMax(10),
	)
	require.NoError(t, err)

	const value = "my string\n"
	valueLen := len(value)

	for range 10 {
		len, err := fw1.Write([]byte(value))
		require.NoError(t, err)
		require.Equal(t, valueLen, len)
	}
	fileInfo1, err := fw1.Stat()
	require.NoError(t, err)
	fileName1 := fileInfo1.Name()
	err = fw1.Close()
	require.NoError(t, err)

	fw2, err := driverbase.NewRotatingFileWriter(
		driverbase.WithFileSizeMaxKb(1000),
		driverbase.WithFileCountMax(10),
	)
	require.NoError(t, err)
	defer func() {
		err := fw2.Clear()
		require.NoError(t, err)
	}()

	for range 10 {
		len, err := fw2.Write([]byte(value))
		require.NoError(t, err)
		require.Equal(t, valueLen, len)
	}
	fileInfo2, err := fw2.Stat()
	require.NoError(t, err)
	fileName2 := fileInfo2.Name()
	require.Equal(t, fileName1, fileName2)

	err = fw2.Close()
	require.NoError(t, err)
}
