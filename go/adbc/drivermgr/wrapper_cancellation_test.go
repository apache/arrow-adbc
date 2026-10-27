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

	"github.com/apache/arrow-adbc/go/adbc"
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

// cancelOnCheck controls when the context is cancelled around a real SQLite call.
// For Execute*, the first two Err checks precede the native call.
type cancelOnCheck struct {
	context.Context
	cancel context.CancelFunc
	at     int32
	checks atomic.Int32
}

func (c *cancelOnCheck) Err() error {
	if c.checks.Add(1) == c.at {
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
	late := &cancelOnCheck{Context: ctx, cancel: cancel, at: 3}
	affected, err := st.ExecuteUpdate(late)
	require.NoError(t, err)
	require.Equal(t, int64(1), affected)
	require.ErrorIs(t, late.Err(), context.Canceled)
}

func TestSQLiteQuerySuccessDespiteLateCancellation(t *testing.T) {
	st := newSQLiteStatement(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	late := &cancelOnCheck{Context: ctx, cancel: cancel, at: 3}

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
	st := newSQLiteStatement(t).(*stmt)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rdr, _, err := st.ExecuteQuery(ctx)
	require.NoError(t, err)
	require.NotNil(t, st.active)
	cancel()
	require.False(t, rdr.Next())
	var adbcErr adbc.Error
	require.ErrorAs(t, rdr.Err(), &adbcErr)
	require.Equal(t, adbc.StatusCancelled, adbcErr.Code)
	rdr.Release()
	require.Nil(t, st.active)

	// Releasing a reader disarms its context before the context is cancelled.
	ctxReleased, cancelReleased := context.WithCancel(context.Background())
	defer cancelReleased()
	rdr, _, err = st.ExecuteQuery(ctxReleased)
	require.NoError(t, err)
	require.NotNil(t, st.active)
	rdr.Release()
	require.Nil(t, st.active)
	cancelReleased()

	// Exhausting a reader also disarms its context without requiring Release.
	ctxExhausted, cancelExhausted := context.WithCancel(context.Background())
	defer cancelExhausted()
	rdr, _, err = st.ExecuteQuery(ctxExhausted)
	require.NoError(t, err)
	require.True(t, rdr.Next())
	require.False(t, rdr.Next())
	require.NoError(t, rdr.Err())
	require.Nil(t, st.active)
	cancelExhausted()
	rdr.Release()
}

func TestSQLiteReexecutionRetiresPreviousCancellation(t *testing.T) {
	st := newSQLiteStatement(t).(*stmt)
	oldCtx, cancelOld := context.WithCancel(context.Background())
	defer cancelOld()
	oldRdr, _, err := st.ExecuteQuery(oldCtx)
	require.NoError(t, err)
	defer func() {
		if oldRdr != nil {
			oldRdr.Release()
		}
	}()
	oldWatch := st.active
	require.NotNil(t, oldWatch)

	newCtx, cancelNew := context.WithCancel(context.Background())
	defer cancelNew()
	newRdr, _, err := st.ExecuteQuery(newCtx)
	require.NoError(t, err)
	defer newRdr.Release()
	newWatch := st.active
	require.NotNil(t, newWatch)
	require.NotSame(t, oldWatch, newWatch)

	cancelOld()
	require.Same(t, newWatch, st.active)
	oldRdr.Release()
	oldRdr = nil
	require.Same(t, newWatch, st.active, "releasing a stale reader must not disarm the new execution")
	require.True(t, newRdr.Next(), "cancelling the stale context must not affect the new query")
	require.NoError(t, newRdr.Err())
	cancelNew()
	require.False(t, newRdr.Next())
	var adbcErr adbc.Error
	require.ErrorAs(t, newRdr.Err(), &adbcErr)
	require.Equal(t, adbc.StatusCancelled, adbcErr.Code)
}

func TestSQLiteCancelledReadKeepsDriverDetail(t *testing.T) {
	st := newSQLiteStatement(t)
	require.NoError(t, st.SetOption("adbc.sqlite.query.batch_rows", "1"))
	require.NoError(t, st.SetSqlQuery("SELECT 1 AS value UNION ALL SELECT 'text' AS value"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// SQLite's native StatementCancel is not implemented; arrange for the
	// context to expire when its real stream reports a second-batch type error.
	late := &cancelOnCheck{Context: ctx, cancel: cancel, at: 6}
	rdr, _, err := st.ExecuteQuery(late)
	require.NoError(t, err)
	defer rdr.Release()
	require.True(t, rdr.Next())
	require.False(t, rdr.Next())
	var adbcErr adbc.Error
	require.ErrorAs(t, rdr.Err(), &adbcErr)
	require.Equal(t, adbc.StatusCancelled, adbcErr.Code)
	require.Contains(t, adbcErr.Msg, "Type mismatch")
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
