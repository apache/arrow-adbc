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
	"context"
	"fmt"
	"regexp"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"google.golang.org/api/option"
)

type connectionImpl struct {
	driverbase.ConnectionImplBase

	authType    string
	credentials string
	projectID   string
	datasetID   string
	tableID     string

	catalog  string
	dbSchema string

	client *bigquery.Client
}

// GetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *connectionImpl) GetCurrentCatalog() (string, error) {
	return c.catalog, nil
}

// GetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *connectionImpl) GetCurrentDbSchema() (string, error) {
	return c.dbSchema, nil
}

// SetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *connectionImpl) SetCurrentCatalog(value string) error {
	sanitizedCatalog, err := sanitize(value)
	if err != nil {
		return err
	}
	c.catalog = sanitizedCatalog
	return nil
}

// SetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *connectionImpl) SetCurrentDbSchema(value string) error {
	sanitizedDbSchema, err := sanitize(value)
	if err != nil {
		return err
	}
	c.dbSchema = sanitizedDbSchema
	return nil
}

// ListTableTypes implements driverbase.TableTypeLister.
func (c *connectionImpl) ListTableTypes(ctx context.Context) ([]string, error) {
	return []string{"BASE TABLE", "VIEW"}, nil
}

// SetAutocommit implements driverbase.AutocommitSetter.
func (c *connectionImpl) SetAutocommit(enabled bool) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "SetAutocommit is not yet implemented",
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
	return nil
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
//		Field Name                  | Field Type          | Comments
//		----------------------------|---------------------|---------
//		column_name                 | utf8 not null       |
//		ordinal_position            | int32               | (1)
//		remarks                     | utf8                | (2)
//		xdbc_data_type              | int16               | (3)
//		xdbc_type_name              | utf8                | (3)
//		xdbc_column_size            | int32               | (3)
//		xdbc_decimal_digits         | int16               | (3)
//		xdbc_num_prec_radix         | int16               | (3)
//		xdbc_nullable               | int16               | (3)
//		xdbc_column_def             | utf8                | (3)
//		xdbc_sql_data_type          | int16               | (3)
//		xdbc_datetime_sub           | int16               | (3)
//		xdbc_char_octet_length      | int32               | (3)
//		xdbc_is_nullable            | utf8                | (3)
//		xdbc_scope_catalog          | utf8                | (3)
//		xdbc_scope_schema           | utf8                | (3)
//		xdbc_scope_table            | utf8                | (3)
//		xdbc_is_autoincrement       | bool                | (3)
//		xdbc_is_generatedcolumn     | utf8                | (3)
//
//	 1. The column's ordinal position in the table (starting from 1).
//	 2. Database-specific description of the column.
//	 3. Optional Value. Should be null if not supported by the driver.
//	    xdbc_values are meant to provide JDBC/ODBC-compatible metadata
//	    in an agnostic manner.
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

func (c *connectionImpl) GetObjects(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string, tableName *string, columnName *string, tableType []string) (array.RecordReader, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "GetObjects not yet implemented for BigQuery driver",
	}
}

func (c *connectionImpl) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (*arrow.Schema, error) {
	if catalog == nil {
		catalog = &c.catalog
	}
	sanitizedCatalog, err := sanitize(*catalog)
	if err != nil {
		return nil, err
	}

	if dbSchema == nil {
		dbSchema = &c.dbSchema
	}
	sanitizedDbSchema, err := sanitize(*dbSchema)
	if err != nil {
		return nil, err
	}

	sanitizedTableName, err := sanitize(tableName)
	if err != nil {
		return nil, err
	}

	queryString := fmt.Sprintf("SELECT * FROM `%s`.`%s`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '%s'", sanitizedCatalog, sanitizedDbSchema, sanitizedTableName)
	query := c.client.Query(queryString)
	reader, nColsInTable, err := newRecordReader(ctx, query, c.Alloc)
	if err != nil {
		return nil, err
	}

	metadataArray := make([]map[string]string, 0)
	fields := make([]arrow.Field, 0)
	columnIndex := 0

	for reader.Next() && ctx.Err() == nil {
		rec := reader.Record()
		numRows := int(rec.NumRows())
		for i := 0; i < numRows; i++ {
			metadata := make(map[string]string)
			metadata["PRIMARY_KEY"] = ""
			metadataArray = append(metadataArray, metadata)
			fields = append(fields, arrow.Field{
				Type: &arrow.NullType{},
			})
		}
		for i := 0; i < int(rec.NumCols()); i++ {
			col := rec.Column(i)
			name := strings.ToUpper(rec.ColumnName(i))

			switch name {
			case "IS_NULLABLE":
				for indexInTable := 0; indexInTable < numRows; indexInTable++ {
					field := &fields[columnIndex+indexInTable]
					field.Nullable = strings.ToUpper(col.ValueStr(indexInTable)) == "YES"
				}
			case "COLUMN_NAME":
				for indexInTable := 0; indexInTable < numRows; indexInTable++ {
					field := &fields[columnIndex+indexInTable]
					field.Name = col.ValueStr(indexInTable)
				}
			case "DATA_TYPE":
				for indexInTable := 0; indexInTable < numRows; indexInTable++ {
					field := &fields[columnIndex+indexInTable]
					typeString := col.ValueStr(indexInTable)
					switch typeString {
					case "INTEGER", "INT64":
						field.Type = &arrow.Int64Type{}
					case "FLOAT", "FLOAT64":
						field.Type = &arrow.Float64Type{}
					case "BOOL", "BOOLEAN":
						field.Type = &arrow.BooleanType{}
					case "STRING", "GEOGRAPHY", "JSON":
						field.Type = &arrow.StringType{}
					case "BYTES":
						field.Type = &arrow.BinaryType{}
					case "DATETIME", "TIMESTAMP":
						field.Type = &arrow.TimestampType{}
					case "TIME":
						field.Type = &arrow.Time64Type{}
					case "DATE":
						field.Type = &arrow.Date64Type{}
					default:
						field.Type = &arrow.NullType{}
					}
				}
			default:
				for indexInTable := 0; indexInTable < numRows; indexInTable++ {
					metadataArray[columnIndex+indexInTable][name] = col.ValueStr(indexInTable)
				}
			}
			col.Release()
		}
		columnIndex += numRows
		rec.Release()
	}
	for i := 0; i < int(nColsInTable); i++ {
		field := &fields[i]
		field.Metadata = arrow.MetadataFrom(metadataArray[i])
	}
	schema := arrow.NewSchema(fields, nil)
	return schema, nil
}

// NewStatement initializes a new statement object tied to this connection
func (c *connectionImpl) NewStatement() (adbc.Statement, error) {
	return &statement{
		connectionImpl: c,
		query:          c.client.Query(""),
	}, nil
}

func (c *connectionImpl) GetOption(key string) (string, error) {
	switch key {
	case OptionStringAuthType:
		return c.authType, nil
	case OptionStringCredentials:
		return c.credentials, nil
	case OptionStringProjectID:
		return c.projectID, nil
	case OptionStringDatasetID:
		return c.datasetID, nil
	case OptionStringTableID:
		return c.tableID, nil
	default:
		return c.ConnectionImplBase.GetOption(key)
	}
}

func (c *connectionImpl) newClient(ctx context.Context) error {
	if c.projectID == "" {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "ProjectID is empty",
		}
	}
	switch c.authType {
	case OptionValueAuthTypeCredentialsFile:
		client, err := bigquery.NewClient(ctx, c.projectID, option.WithCredentialsFile(c.credentials))
		if err != nil {
			return err
		}

		err = client.EnableStorageReadClient(ctx, option.WithCredentialsFile(c.credentials))
		if err != nil {
			return err
		}

		c.client = client
	default:
		client, err := bigquery.NewClient(ctx, c.projectID)
		if err != nil {
			return err
		}

		err = client.EnableStorageReadClient(ctx)
		if err != nil {
			return err
		}

		c.client = client
	}
	return nil
}

var (
	sanitizedInputRegex = regexp.MustCompile("^[a-zA-Z0-9_-]+")
)

func sanitize(value string) (string, error) {
	if value == "" {
		return value, nil
	} else {
		if sanitizedInputRegex.MatchString(value) {
			return value, nil
		} else {
			return "", adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("invalid characters in value `%s`", value),
			}
		}
	}
}
