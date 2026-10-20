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
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/validation"
	"github.com/apache/arrow-adbc/go/driver/internal/driverbase"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

const (
	// Keys with this prefix are only understood by the string setters,
	// like a driver that parses numbers from SetOption itself.
	stringOnlyKeyPrefix = "string."
	// This key is handled natively by the integer setter.
	nativeIntKey = "native.int"
)

// typedOptionsDatabase, typedOptionsConnection and typedOptionsStatement
// record the options they accept so tests can see which setter got a value.
type typedOptionsDatabase struct {
	driverbase.DatabaseImplBase
	opts map[string]string
}

func (d *typedOptionsDatabase) SetOption(key, value string) error {
	if strings.HasPrefix(key, stringOnlyKeyPrefix) {
		d.opts[key] = value
		return nil
	}
	return d.DatabaseImplBase.SetOption(key, value)
}

func (d *typedOptionsDatabase) SetOptionInt(key string, value int64) error {
	if key == nativeIntKey {
		d.opts[key] = "native"
		return nil
	}
	return d.DatabaseImplBase.SetOptionInt(key, value)
}

func (d *typedOptionsDatabase) Open(ctx context.Context) (adbc.Connection, error) {
	cnxn := &typedOptionsConnection{
		ConnectionImplBase: driverbase.NewConnectionImplBase(&d.DatabaseImplBase),
		opts:               d.opts,
	}
	return driverbase.NewConnectionBuilder(cnxn).Connection(), nil
}

type typedOptionsConnection struct {
	driverbase.ConnectionImplBase
	opts map[string]string
}

func (c *typedOptionsConnection) SetOption(key, value string) error {
	if strings.HasPrefix(key, stringOnlyKeyPrefix) {
		c.opts[key] = value
		return nil
	}
	return c.ConnectionImplBase.SetOption(key, value)
}

func (c *typedOptionsConnection) NewStatement() (adbc.Statement, error) {
	stmt := &typedOptionsStatement{
		statement: statement{StatementImplBase: driverbase.NewStatementImplBase(c.Base(), c.ErrorHelper)},
		opts:      c.opts,
	}
	return driverbase.NewStatement(stmt), nil
}

type typedOptionsStatement struct {
	statement
	opts map[string]string
}

func (s *typedOptionsStatement) SetOption(key, value string) error {
	if strings.HasPrefix(key, stringOnlyKeyPrefix) {
		s.opts[key] = value
		return nil
	}
	return s.StatementImplBase.SetOption(key, value)
}

func (s *typedOptionsStatement) Close() error { return nil }

func TestTypedOptionsFallBackToStringSetter(t *testing.T) {
	ctx := context.Background()
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	drv := driverbase.NewDriverImplBase(driverbase.DefaultDriverInfo("MockDriver"), alloc)
	dbBase, err := driverbase.NewDatabaseImplBase(ctx, &drv, driverbase.TracingOptions{})
	require.NoError(t, err)
	opts := map[string]string{}
	db := driverbase.NewDatabase(&typedOptionsDatabase{DatabaseImplBase: dbBase, opts: opts})
	defer validation.CheckedClose(t, db)

	cnxn, err := db.Open(ctx)
	require.NoError(t, err)
	defer validation.CheckedClose(t, cnxn)

	stmt, err := cnxn.NewStatement()
	require.NoError(t, err)
	defer validation.CheckedClose(t, stmt)

	for _, tc := range []struct {
		name   string
		getset adbc.GetSetOptions
	}{
		{"database", db},
		{"connection", cnxn.(adbc.GetSetOptions)},
		{"statement", stmt.(adbc.GetSetOptions)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			intKey := stringOnlyKeyPrefix + tc.name + ".int"
			require.NoError(t, tc.getset.SetOptionInt(intKey, -3))
			require.Equal(t, "-3", opts[intKey])

			doubleKey := stringOnlyKeyPrefix + tc.name + ".double"
			require.NoError(t, tc.getset.SetOptionDouble(doubleKey, 0.25))
			require.Equal(t, "0.25", opts[doubleKey])
			require.NoError(t, tc.getset.SetOptionDouble(doubleKey, 1e21))
			require.Equal(t, "1e+21", opts[doubleKey])

			// Keys that neither setter understands are still rejected.
			var adbcErr adbc.Error
			err := tc.getset.SetOptionInt(OptionKeyUnrecognized, 1)
			require.ErrorAs(t, err, &adbcErr)
			require.Equal(t, adbc.StatusNotImplemented, adbcErr.Code)
			require.Contains(t, adbcErr.Msg, "'"+OptionKeyUnrecognized+"'")

			err = tc.getset.SetOptionDouble(OptionKeyUnrecognized, 1.5)
			require.ErrorAs(t, err, &adbcErr)
			require.Equal(t, adbc.StatusNotImplemented, adbcErr.Code)
		})
	}

	// A typed setter that handles the key itself is not routed through
	// SetOption.
	require.NoError(t, db.SetOptionInt(nativeIntKey, 7))
	require.Equal(t, "native", opts[nativeIntKey])
}
