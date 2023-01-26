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

package sqldriver

import (
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseConnectStr(t *testing.T) {
	const (
		scheme   = "grpc+tcp"
		host     = "host"
		port     = 443
		dbname   = "dbname"
		username = "username"
		password = "token=="
	)

	var (
		uri = fmt.Sprintf("%s://%s:%d/%s", scheme, host, port, dbname)
	)

	dsn := strings.Join([]string{
		fmt.Sprintf("%s=%s", adbc.OptionKeyURI, uri),
		fmt.Sprintf("%s=%s", adbc.OptionKeyUsername, username),
		fmt.Sprintf("%s=%s", adbc.OptionKeyPassword, password),
		fmt.Sprintf("%s=%s", adbc.OptionKeyReadOnly, adbc.OptionValueEnabled),
	}, " ; ")

	expectOpts := map[string]string{
		adbc.OptionKeyURI:      uri,
		adbc.OptionKeyUsername: username,
		adbc.OptionKeyPassword: password,
		adbc.OptionKeyReadOnly: adbc.OptionValueEnabled,
	}

	gotOpts, err := parseConnectStr(dsn)
	if assert.NoError(t, err) {
		assert.Equal(t, expectOpts, gotOpts)
	}
}

func TestColumnTypeDatabaseTypeName(t *testing.T) {
	tests := []struct {
		typ      arrow.DataType
		typeName string
	}{
		{
			typ:      &arrow.StringType{},
			typeName: "utf8",
		},
		{
			typ:      &arrow.Date32Type{},
			typeName: "date32",
		},
		{
			typ:      &arrow.Date64Type{},
			typeName: "date64",
		},
		{
			typ:      &arrow.TimestampType{Unit: arrow.Second, TimeZone: "utc"},
			typeName: "timestamp[s, tz=utc]",
		},
		{
			typ:      &arrow.TimestampType{Unit: arrow.Millisecond},
			typeName: "timestamp[ms]",
		},
		{
			typ:      &arrow.Time32Type{Unit: arrow.Second},
			typeName: "time32[s]",
		},
		{
			typ:      &arrow.Time32Type{Unit: arrow.Microsecond},
			typeName: "time32[us]",
		},
		{
			typ:      &arrow.Time64Type{Unit: arrow.Second},
			typeName: "time64[s]",
		},
		{
			typ:      &arrow.Time64Type{Unit: arrow.Nanosecond},
			typeName: "time64[ns]",
		},
		{
			typ:      &arrow.DurationType{Unit: arrow.Nanosecond},
			typeName: "duration[ns]",
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d-%s", i, test.typeName), func(t *testing.T) {
			schema := arrow.NewSchema([]arrow.Field{{Type: test.typ}}, nil)
			reader, err := array.NewRecordReader(schema, nil)
			require.NoError(t, err)
			r := &rows{rdr: reader}
			assert.Equal(t, test.typeName, r.ColumnTypeDatabaseTypeName(0))
		})
	}
}
