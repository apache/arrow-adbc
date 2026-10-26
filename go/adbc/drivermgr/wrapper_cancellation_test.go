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

package drivermgr

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/require"
)

func newSQLiteStatement(t *testing.T) adbc.Statement {
	t.Helper()
	db, err := (Driver{}).NewDatabase(map[string]string{"driver": "adbc_driver_sqlite"})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	conn, err := db.Open(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	st, err := conn.NewStatement()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	require.NoError(t, st.SetSqlQuery("SELECT 1"))
	return st
}

// cancelOnThirdCheck simulates cancellation just after native execution.
// The first two Err checks occur before C is called; an old post-call check
// discarded successful results, while the current code returns them.
type cancelOnThirdCheck struct {
	context.Context
	cancel context.CancelFunc
	checks atomic.Int32
}

func (c *cancelOnThirdCheck) Err() error {
	if c.checks.Add(1) == 3 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestSQLiteUpdateSuccessDespiteLateCancellation(t *testing.T) {
	st := newSQLiteStatement(t)
	require.NoError(t, st.SetSqlQuery("CREATE TEMP TABLE update_probe (id INTEGER)"))
	_, err := st.ExecuteUpdate(context.Background())
	require.NoError(t, err)
	require.NoError(t, st.SetSqlQuery("INSERT INTO update_probe (id) VALUES (1)"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	late := &cancelOnThirdCheck{Context: ctx, cancel: cancel}
	affected, err := st.ExecuteUpdate(late)
	require.NoError(t, err)
	require.Equal(t, int64(1), affected)
	require.ErrorIs(t, late.Err(), context.Canceled)
}

func TestSQLiteQuerySuccessDespiteLateCancellation(t *testing.T) {
	st := newSQLiteStatement(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	late := &cancelOnThirdCheck{Context: ctx, cancel: cancel}

	rdr, _, err := st.ExecuteQuery(late)
	require.NoError(t, err)
	require.NotNil(t, rdr)
	defer rdr.Release()
	require.ErrorIs(t, late.Err(), context.Canceled)
	require.False(t, rdr.Next())
	var adbcErr adbc.Error
	require.ErrorAs(t, rdr.Err(), &adbcErr)
	require.Equal(t, adbc.StatusCancelled, adbcErr.Code)
}

func TestContextDriverErrorKeepsDriverDetail(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	driverErr := adbc.Error{Code: adbc.StatusInvalidArgument, Msg: "driver detail", VendorCode: 42}
	var result adbc.Error
	require.ErrorAs(t, contextDriverError(ctx, driverErr), &result)
	require.Equal(t, adbc.StatusCancelled, result.Code)
	require.Contains(t, result.Msg, driverErr.Msg)
	require.Equal(t, driverErr.VendorCode, result.VendorCode)

	driverErr.Code = adbc.StatusCancelled
	require.Equal(t, driverErr, contextDriverError(ctx, driverErr))
}

func TestSQLiteStatementCancelReaderLifetime(t *testing.T) {
	// This test swaps a package-level function; it must never run in parallel.
	original := cancelStatement
	var calls atomic.Int32
	called := make(chan struct{}, 1)
	cancelStatement = func(s *stmt) {
		original(s) // Exercise the actual C driver-manager cancellation path first.
		calls.Add(1)
		select {
		case called <- struct{}{}:
		default:
		}
	}
	t.Cleanup(func() { cancelStatement = original })

	st := newSQLiteStatement(t)
	var rdr array.RecordReader
	defer func() {
		if rdr != nil {
			rdr.Release()
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var err error
	rdr, _, err = st.ExecuteQuery(ctx)
	require.NoError(t, err)
	require.NotNil(t, rdr)
	cancel()
	select {
	case <-called:
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation never reached the C driver-manager call while reader was alive")
	}
	rdr.Release() // Joins the callback before the hook is restored.
	rdr = nil
	require.Equal(t, int32(1), calls.Load())

	// A released reader must not retain the cancellation watcher.
	ctxReleased, cancelReleased := context.WithCancel(context.Background())
	defer cancelReleased()
	rdr, _, err = st.ExecuteQuery(ctxReleased)
	require.NoError(t, err)
	require.NotNil(t, rdr)
	rdr.Release()
	rdr = nil
	cancelReleased()
	select {
	case <-called:
		t.Fatal("cancellation reached C after reader Release")
	case <-time.After(100 * time.Millisecond):
	}

	// Exhaustion also ends the reader's cancellation lifetime without Release.
	ctxExhausted, cancelExhausted := context.WithCancel(context.Background())
	defer cancelExhausted()
	rdr, _, err = st.ExecuteQuery(ctxExhausted)
	require.NoError(t, err)
	require.NotNil(t, rdr)
	require.True(t, rdr.Next())
	require.False(t, rdr.Next())
	require.NoError(t, rdr.Err())
	cancelExhausted()
	select {
	case <-called:
		t.Fatal("cancellation reached C after reader exhaustion")
	case <-time.After(100 * time.Millisecond):
	}
	rdr.Release()
	rdr = nil
	require.Equal(t, int32(1), calls.Load())
}

func TestSQLiteBackgroundExecutionDoesNotWatchCancellation(t *testing.T) {
	st := newSQLiteStatement(t)
	before := runtime.NumGoroutine()
	for range 64 {
		rdr, _, err := st.ExecuteQuery(context.Background())
		require.NoError(t, err)
		_, wrapped := rdr.(*cancellationReader)
		require.False(t, wrapped, "Background should return the unwrapped reader")
		rdr.Release()
		require.NoError(t, st.SetSqlQuery("CREATE TEMP TABLE IF NOT EXISTS background_probe (id INTEGER)"))
		_, err = st.ExecuteUpdate(context.Background())
		require.NoError(t, err)
		require.NoError(t, st.SetSqlQuery("SELECT 1"))
	}
	runtime.Gosched()
	require.LessOrEqual(t, runtime.NumGoroutine(), before+4, "background executions leaked goroutines")
}
