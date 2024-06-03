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

package clickhouse

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouseDriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow/go/v17/arrow"
	"strings"
)

type connectionImpl struct {
	driverbase.ConnectionImplBase

	address  []string
	protocol clickhouse.Protocol
	database string
	username string
	password string
	table    string

	resultRecordBufferSize int
	prefetchConcurrency    int

	conn clickhouseDriver.Conn
}

// GetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *connectionImpl) GetCurrentCatalog() (string, error) {
	return c.database, nil
}

// GetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *connectionImpl) GetCurrentDbSchema() (string, error) {
	return c.table, nil
}

// SetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *connectionImpl) SetCurrentCatalog(value string) error {
	c.database = value
	return nil
}

// SetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *connectionImpl) SetCurrentDbSchema(value string) error {
	c.table = value
	return nil
}

// ListTableTypes implements driverbase.TableTypeLister.
func (c *connectionImpl) ListTableTypes(ctx context.Context) ([]string, error) {
	// todo: find clickhouse enums/get from connection for this
	return []string{"BASE TABLE", "TEMPORARY TABLE", "VIEW"}, nil
}

// SetAutocommit implements driverbase.AutocommitSetter.
func (c *connectionImpl) SetAutocommit(enabled bool) error {
	if enabled {
		return nil
	}
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "SetAutocommit to `false` is not yet implemented",
	}
}

// Commit commits any pending transactions on this connection, it should
// only be used if autocommit is disabled.
//
// Behavior is undefined if this is mixed with SQL transaction statements.
func (c *connectionImpl) Commit(_ context.Context) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "Commit not yet implemented for BigQuery driver",
	}
}

// Rollback rolls back any pending transactions. Only used if autocommit
// is disabled.
//
// Behavior is undefined if this is mixed with SQL transaction statements.
func (c *connectionImpl) Rollback(_ context.Context) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "Rollback not yet implemented for BigQuery driver",
	}
}

// Close closes this connection and releases any associated resources.
func (c *connectionImpl) Close() error {
	return c.conn.Close()
}

func (c *connectionImpl) newConnection(ctx context.Context) error {
	var err error
	if c.address == nil || len(c.address) == 0 {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "clickhouse address not given",
		}
	}
	connectionOptions := clickhouse.Options{
		Addr:     c.address,
		Protocol: c.protocol,
		Auth: clickhouse.Auth{
			Database: c.database,
			Username: c.username,
			Password: c.password,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Debugf: func(format string, v ...interface{}) {
			fmt.Printf(format, v)
		},
		TLS: &tls.Config{InsecureSkipVerify: true},
	}
	c.conn, err = clickhouse.Open(&connectionOptions)
	if err != nil {
		return adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  err.Error(),
		}
	}
	if err := c.conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			return adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  fmt.Sprintf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace),
			}
		}
		return adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  err.Error(),
		}
	}
	return err
}

// Metadata methods
// Generally these methods return an array.RecordReader that
// can be consumed to retrieve metadata about the database as Arrow
// data. The returned metadata has an expected schema given in the
// doc strings of the specific methods. Schema fields are nullable
// unless otherwise marked. While no Statement is used in these
// methods, the result set may count as an active statement to the
// driver for the purposes of concurrency management (e.g. if the
// driver has a limit on concurrent active statements and it must
// execute a SQL query internally in order to implement the metadata
// method).
//
// Some methods accept "search pattern" arguments, which are strings
// that can contain the special character "%" to match zero or more
// characters, or "_" to match exactly one character. (See the
// documentation of DatabaseMetaData in JDBC or "Pattern Value Arguments"
// in the ODBC documentation.) Escaping is not currently supported.
// GetObjects gets a hierarchical view of all catalogs, database schemas,
// tables, and columns.
//
// The result is an Arrow Dataset with the following schema:
//
//	Field Name                  | Field Type
//	----------------------------|----------------------------
//	catalog_name                | utf8
//	catalog_db_schemas          | list<DB_SCHEMA_SCHEMA>
//
// DB_SCHEMA_SCHEMA is a Struct with the fields:
//
//	Field Name                  | Field Type
//	----------------------------|----------------------------
//	db_schema_name              | utf8
//	db_schema_tables            | list<TABLE_SCHEMA>
//
// TABLE_SCHEMA is a Struct with the fields:
//
//	Field Name                  | Field Type
//	----------------------------|----------------------------
//	table_name                  | utf8 not null
//	table_type                  | utf8 not null
//	table_columns               | list<COLUMN_SCHEMA>
//	table_constraints           | list<CONSTRAINT_SCHEMA>
//
// COLUMN_SCHEMA is a Struct with the fields:
//
//	Field Name                  | Field Type          | Comments
//	----------------------------|---------------------|---------
//	column_name                 | utf8 not null       |
//	ordinal_position            | int32               | (1)
//	remarks                     | utf8                | (2)
//	xdbc_data_type              | int16               | (3)
//	xdbc_type_name              | utf8                | (3)
//	xdbc_column_size            | int32               | (3)
//	xdbc_decimal_digits         | int16               | (3)
//	xdbc_num_prec_radix         | int16               | (3)
//	xdbc_nullable               | int16               | (3)
//	xdbc_column_def             | utf8                | (3)
//	xdbc_sql_data_type          | int16               | (3)
//	xdbc_datetime_sub           | int16               | (3)
//	xdbc_char_octet_length      | int32               | (3)
//	xdbc_is_nullable            | utf8                | (3)
//	xdbc_scope_catalog          | utf8                | (3)
//	xdbc_scope_schema           | utf8                | (3)
//	xdbc_scope_table            | utf8                | (3)
//	xdbc_is_autoincrement       | bool                | (3)
//	xdbc_is_generatedcolumn     | utf8                | (3)
//
// 1. The column's ordinal position in the table (starting from 1).
// 2. Database-specific description of the column.
// 3. Optional Value. Should be null if not supported by the driver.
//    xdbc_values are meant to provide JDBC/ODBC-compatible metadata
//    in an agnostic manner.
//
// CONSTRAINT_SCHEMA is a Struct with the fields:
//
//	Field Name                  | Field Type          | Comments
//	----------------------------|---------------------|---------
//	constraint_name             | utf8                |
//	constraint_type             | utf8 not null       | (1)
//	constraint_column_names     | list<utf8> not null | (2)
//	constraint_column_usage     | list<USAGE_SCHEMA>  | (3)
//
// 1. One of 'CHECK', 'FOREIGN KEY', 'PRIMARY KEY', or 'UNIQUE'.
// 2. The columns on the current table that are constrained, in order.
// 3. For FOREIGN KEY only, the referenced table and columns.
//
// USAGE_SCHEMA is a Struct with fields:
//
//	Field Name                  | Field Type
//	----------------------------|----------------------------
//	fk_catalog                  | utf8
//	fk_db_schema                | utf8
//	fk_table                    | utf8 not null
//	fk_column_name              | utf8 not null
//
// For the parameters: If nil is passed, then that parameter will not
// be filtered by at all. If an empty string, then only objects without
// that property (ie: catalog or db schema) will be returned.
//
// tableName and columnName must be either nil (do not filter by
// table name or column name) or non-empty.
//
// All non-empty, non-nil strings should be a search pattern (as described
// earlier).

func (c *connectionImpl) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (*arrow.Schema, error) {
	return c.getTableSchemaWithFilter(ctx, catalog, dbSchema, tableName, nil)
}

// NewStatement initializes a new statement object tied to this connection
func (c *connectionImpl) NewStatement() (adbc.Statement, error) {
	return &statement{
		connectionImpl:         c,
		resultRecordBufferSize: c.resultRecordBufferSize,
		prefetchConcurrency:    c.prefetchConcurrency,
	}, nil
}

func (c *connectionImpl) GetOption(key string) (string, error) {
	switch key {
	case OptionStringProtocol:
		switch c.protocol {
		case clickhouse.HTTP:
			return OptionValueProtocolHTTP, nil
		case clickhouse.Native:
			return OptionValueProtocolNative, nil
		}
	default:
		return c.ConnectionImplBase.GetOption(key)
	}
	return "", nil
}

func (c *connectionImpl) SetOption(key string, value string) error {
	switch key {
	case OptionStringProtocol:
		switch strings.ToLower(value) {
		case OptionValueProtocolNative:
			c.protocol = clickhouse.Native
		case OptionValueProtocolHTTP:
			c.protocol = clickhouse.HTTP
		}
	default:
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown statement string type option `%s`", key),
		}
	}
	return nil
}

func (c *connectionImpl) GetOptionInt(key string) (int64, error) {
	switch key {
	case OptionIntQueryResultBufferSize:
		return int64(c.resultRecordBufferSize), nil
	case OptionIntQueryPrefetchConcurrency:
		return int64(c.prefetchConcurrency), nil
	default:
		return c.ConnectionImplBase.GetOptionInt(key)
	}
}

func (c *connectionImpl) SetOptionInt(key string, value int64) error {
	switch key {
	case OptionIntQueryResultBufferSize:
		c.resultRecordBufferSize = int(value)
		return nil
	case OptionIntQueryPrefetchConcurrency:
		c.prefetchConcurrency = int(value)
		return nil
	default:
		return c.ConnectionImplBase.SetOptionInt(key, value)
	}
}

func (c *connectionImpl) getTableSchemaWithFilter(ctx context.Context, catalog *string, dbSchema *string, tableName string, columnName *string) (*arrow.Schema, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "getTableSchemaWithFilter is not yet implemented",
	}
}
