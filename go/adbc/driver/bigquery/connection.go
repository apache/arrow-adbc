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
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow-go/v18/arrow"
	"golang.org/x/oauth2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type connectionImpl struct {
	driverbase.ConnectionImplBase

	authType              string
	accessToken           string
	credentials           string
	clientID              string
	clientSecret          string
	refreshToken          string
	accessTokenEndpoint   string
	accessTokenServerName string

	// catalog is the same as the project id in BigQuery
	catalog string
	// dbSchema is the same as the dataset id in BigQuery
	dbSchema string
	// tableID is the default table for statement
	tableID string

	resultRecordBufferSize int
	prefetchConcurrency    int

	client *bigquery.Client
}

// Helper for configuration of a connection for use with a job
func (c *connectionImpl) table(project, dataset, table string) *bigquery.Table {
	return c.client.DatasetInProject(project, dataset).Table(table)
}

func (c *connectionImpl) GetCatalogs(ctx context.Context, catalogFilter *string) ([]string, error) {
	catalogPattern, err := internal.PatternToRegexp(catalogFilter)
	if err != nil {
		return nil, err
	}
	if catalogPattern == nil {
		catalogPattern = internal.AcceptAll
	}

	// Connections to BQ are scoped to a particular Project, which corresponds to catalog-level namespacing.
	// TODO: Consider enumerating projects with ResourceManager API, but this may not be "idiomatic" usage.
	project := c.client.Project()
	res := make([]string, 0)
	if catalogPattern.MatchString(project) {
		res = append(res, project)
	}

	return res, nil
}

func (c *connectionImpl) GetDBSchemasForCatalog(ctx context.Context, catalog string, schemaFilter *string) ([]string, error) {
	schemaPattern, err := internal.PatternToRegexp(schemaFilter)
	if err != nil {
		return nil, err
	}
	if schemaPattern == nil {
		schemaPattern = internal.AcceptAll
	}

	it := c.client.Datasets(ctx)
	it.ProjectID = catalog

	res := make([]string, 0)
	for {
		ds, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		if schemaPattern.MatchString(ds.DatasetID) {
			res = append(res, ds.DatasetID)
		}

	}

	return res, nil
}

func (c *connectionImpl) GetTablesForDBSchema(ctx context.Context, catalog string, schema string, tableFilter *string, columnFilter *string, includeColumns bool) ([]driverbase.TableInfo, error) {
	tablePattern, err := internal.PatternToRegexp(tableFilter)
	if err != nil {
		return nil, err
	}
	if tablePattern == nil {
		tablePattern = internal.AcceptAll
	}

	it := c.client.DatasetInProject(catalog, schema).Tables(ctx)

	res := make([]driverbase.TableInfo, 0)
	for {
		table, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		if !tablePattern.MatchString(table.TableID) {
			continue
		}

		md, err := table.Metadata(ctx, bigquery.WithMetadataView(bigquery.BasicMetadataView))
		if err != nil {
			return nil, err
		}

		var constraints []driverbase.ConstraintInfo
		if md.TableConstraints != nil {
			constraints = make([]driverbase.ConstraintInfo, 0)
			if md.TableConstraints.PrimaryKey != nil {
				constraints = append(constraints, driverbase.ConstraintInfo{
					// BigQuery Primary Keys are unnamed
					ConstraintType:        internal.PrimaryKey,
					ConstraintColumnNames: driverbase.RequiredList(md.TableConstraints.PrimaryKey.Columns),
				})
			}

			for _, fk := range md.TableConstraints.ForeignKeys {
				var columnUsage []driverbase.ConstraintColumnUsage
				if len(fk.ColumnReferences) > 0 {
					columnUsage = make([]driverbase.ConstraintColumnUsage, len(fk.ColumnReferences))
				}
				for i, ref := range fk.ColumnReferences {
					columnUsage[i] = driverbase.ConstraintColumnUsage{
						ForeignKeyCatalog:  driverbase.Nullable(fk.ReferencedTable.ProjectID),
						ForeignKeyDbSchema: driverbase.Nullable(fk.ReferencedTable.DatasetID),
						ForeignKeyTable:    fk.ReferencedTable.TableID,
						ForeignKeyColumn:   ref.ReferencedColumn,
					}
				}
				constraints = append(constraints, driverbase.ConstraintInfo{
					ConstraintName:        driverbase.Nullable(fk.Name),
					ConstraintType:        internal.ForeignKey,
					ConstraintColumnUsage: columnUsage,
				})
			}
		}

		var columns []driverbase.ColumnInfo
		if includeColumns {
			columnPattern, err := internal.PatternToRegexp(columnFilter)
			if err != nil {
				return nil, err
			}
			if columnPattern == nil {
				columnPattern = internal.AcceptAll
			}

			columns = make([]driverbase.ColumnInfo, 0)
			for pos, fieldschema := range md.Schema {
				if columnPattern.MatchString(fieldschema.Name) {
					xdbcIsNullable := "YES"
					xdbcNullable := int16(1)
					if fieldschema.Required {
						xdbcIsNullable = "NO"
						xdbcNullable = 0
					}

					xdbcColumnSize := fieldschema.MaxLength
					if xdbcColumnSize == 0 {
						xdbcColumnSize = fieldschema.Precision
					}

					var xdbcCharOctetLength int32
					if fieldschema.Type == bigquery.BytesFieldType {
						xdbcCharOctetLength = int32(fieldschema.MaxLength)
					}

					field, err := buildField(fieldschema, 0)
					if err != nil {
						return nil, err
					}
					xdbcDataType := internal.ToXdbcDataType(field.Type)

					columns = append(columns, driverbase.ColumnInfo{
						ColumnName:          fieldschema.Name,
						OrdinalPosition:     driverbase.Nullable(int32(pos + 1)),
						Remarks:             driverbase.Nullable(fieldschema.Description),
						XdbcDataType:        driverbase.Nullable(int16(field.Type.ID())),
						XdbcTypeName:        driverbase.Nullable(string(fieldschema.Type)),
						XdbcNullable:        driverbase.Nullable(xdbcNullable),
						XdbcSqlDataType:     driverbase.Nullable(int16(xdbcDataType)),
						XdbcIsNullable:      driverbase.Nullable(xdbcIsNullable),
						XdbcDecimalDigits:   driverbase.Nullable(int16(fieldschema.Scale)),
						XdbcColumnSize:      driverbase.Nullable(int32(xdbcColumnSize)),
						XdbcCharOctetLength: driverbase.Nullable(xdbcCharOctetLength),
						XdbcScopeCatalog:    driverbase.Nullable(catalog),
						XdbcScopeSchema:     driverbase.Nullable(schema),
						XdbcScopeTable:      driverbase.Nullable(table.TableID),
					})
				}
			}
		}

		res = append(res, driverbase.TableInfo{
			TableName:        table.TableID,
			TableType:        string(md.Type),
			TableConstraints: constraints,
			TableColumns:     columns,
		})
	}

	return res, nil
}

type bigQueryTokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
	Scope       string `json:"scope"`
	TokenType   string `json:"token_type"`
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
	c.catalog = value
	return nil
}

// SetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *connectionImpl) SetCurrentDbSchema(value string) error {
	sanitized, err := sanitizeDataset(value)
	if err != nil {
		return err
	}
	c.dbSchema = sanitized
	return nil
}

// ListTableTypes implements driverbase.TableTypeLister.
func (c *connectionImpl) ListTableTypes(ctx context.Context) ([]string, error) {
	return []string{
		string(bigquery.RegularTable),
		string(bigquery.ViewTable),
		string(bigquery.ExternalTable),
		string(bigquery.MaterializedView),
		string(bigquery.Snapshot),
	}, nil
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
	return c.client.Close()
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
		alloc:                  c.Alloc,
		cnxn:                   c,
		parameterMode:          OptionValueQueryParameterModePositional,
		resultRecordBufferSize: c.resultRecordBufferSize,
		prefetchConcurrency:    c.prefetchConcurrency,
		queryConfig: bigquery.QueryConfig{
			DefaultProjectID: c.catalog,
			DefaultDatasetID: c.dbSchema,
		},
	}, nil
}

func (c *connectionImpl) GetOption(key string) (string, error) {
	switch key {
	case OptionStringAuthType:
		return c.authType, nil
	case OptionStringAuthCredentials:
		return c.credentials, nil
	case OptionStringAuthAccessToken:
		return c.accessToken, nil
	case OptionStringAuthClientID:
		return c.clientID, nil
	case OptionStringAuthClientSecret:
		return c.clientSecret, nil
	case OptionStringAuthRefreshToken:
		return c.refreshToken, nil
	case OptionStringProjectID:
		return c.catalog, nil
	case OptionStringDatasetID:
		return c.dbSchema, nil
	case OptionStringTableID:
		return c.tableID, nil
	default:
		return c.ConnectionImplBase.GetOption(key)
	}
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

func (c *connectionImpl) newClient(ctx context.Context) error {
	if c.catalog == "" {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "ProjectID is empty",
		}
	}
	switch c.authType {
	case OptionValueAuthTypeJSONCredentialFile,
		OptionValueAuthTypeJSONCredentialString,
		OptionValueAuthTypeUserAuthentication,
		OptionValueAuthTypeTemporaryAccessToken:
		var credentials option.ClientOption

		switch c.authType {
		case OptionValueAuthTypeJSONCredentialFile:
			credentials = option.WithCredentialsFile(c.credentials)
		case OptionValueAuthTypeJSONCredentialString:
			credentials = option.WithCredentialsJSON([]byte(c.credentials))
		case OptionValueAuthTypeUserAuthentication:
			if c.clientID == "" {
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("The `%s` parameter is empty", OptionStringAuthClientID),
				}
			}
			if c.clientSecret == "" {
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("The `%s` parameter is empty", OptionStringAuthClientSecret),
				}
			}
			if c.refreshToken == "" {
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("The `%s` parameter is empty", OptionStringAuthRefreshToken),
				}
			}
			if c.accessTokenEndpoint == "" || c.accessTokenEndpoint == DefaultAccessTokenEndpoint {
				c.accessTokenEndpoint = DefaultAccessTokenEndpoint
				if c.accessTokenServerName == "" {
					c.accessTokenServerName = DefaultAccessTokenServerName
				}
			}
			credentials = option.WithTokenSource(c)

		case OptionValueAuthTypeTemporaryAccessToken:
			if c.accessToken == "" {
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("The `%s` parameter is empty", OptionStringAuthAccessToken),
				}
			}
			credentials = option.WithTokenSource(
				oauth2.StaticTokenSource(&oauth2.Token{
					AccessToken: c.accessToken,
					TokenType:   "Bearer",
				}),
			)
		}

		client, err := bigquery.NewClient(ctx, c.catalog, credentials)
		if err != nil {
			return err
		}

		err = client.EnableStorageReadClient(ctx, credentials)
		if err != nil {
			return err
		}

		c.client = client

	default:
		client, err := bigquery.NewClient(ctx, c.catalog)
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
	// Dataset:
	//
	// https://cloud.google.com/bigquery/docs/datasets#dataset-naming
	//
	// When you create a dataset in BigQuery, the dataset name must be unique for each project.
	// The dataset name can contain the following:
	//   - Up to 1,024 characters.
	//   - Letters (uppercase or lowercase), numbers, and underscores.
	// Dataset names are case-sensitive by default. mydataset and MyDataset can coexist in the same project,
	// unless one of them has case-sensitivity turned off.
	// Dataset names cannot contain spaces or special characters such as -, &, @, or %.
	datasetRegex = regexp.MustCompile("^[a-zA-Z0-9_-]")
)

func sanitizeDataset(value string) (string, error) {
	if value == "" {
		return value, nil
	}

	if datasetRegex.MatchString(value) {
		if len(value) > 1024 {
			return "", adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  "Dataset name exceeds 1024 characters",
			}
		}
		return value, nil
	}

	return "", adbc.Error{
		Code: adbc.StatusInvalidArgument,
		Msg:  fmt.Sprintf("invalid characters in value `%s`", value),
	}
}

func (c *connectionImpl) getTableSchemaWithFilter(ctx context.Context, catalog *string, dbSchema *string, tableName string, columnName *string) (*arrow.Schema, error) {
	if catalog == nil {
		catalog = &c.catalog
	}

	if dbSchema == nil {
		dbSchema = &c.dbSchema
	}

	md, err := c.client.DatasetInProject(*catalog, *dbSchema).Table(tableName).Metadata(ctx)
	if err != nil {
		return nil, err
	}

	metadata := make(map[string]string)
	metadata["Name"] = md.Name
	metadata["Location"] = md.Location
	metadata["Description"] = md.Description
	if md.MaterializedView != nil {
		metadata["MaterializedView.EnableRefresh"] = strconv.FormatBool(md.MaterializedView.EnableRefresh)
		metadata["MaterializedView.LastRefreshTime"] = md.MaterializedView.LastRefreshTime.Format(time.RFC3339Nano)
		metadata["MaterializedView.Query"] = md.MaterializedView.Query
		metadata["MaterializedView.RefreshInterval"] = md.MaterializedView.RefreshInterval.String()
		metadata["MaterializedView.AllowNonIncrementalDefinition"] = strconv.FormatBool(md.MaterializedView.AllowNonIncrementalDefinition)
		if md.MaxStaleness != nil {
			metadata["MaterializedView.MaxStaleness"] = md.MaxStaleness.String()
		}
	}
	if md.TimePartitioning != nil {
		// "DAY", "HOUR", "MONTH", "YEAR"
		metadata["TimePartitioning.Type"] = string(md.TimePartitioning.Type)
		if md.TimePartitioning.Expiration != 0 {
			metadata["TimePartitioning.Expiration"] = md.TimePartitioning.Expiration.String()
		}
		if md.TimePartitioning.Field != "" {
			metadata["TimePartitioning.Field"] = md.TimePartitioning.Field
		}
	}
	if md.RangePartitioning != nil {
		if md.RangePartitioning.Field != "" {
			metadata["RangePartitioning.Field"] = md.RangePartitioning.Field
		}
		if md.RangePartitioning.Range != nil {
			metadata["RangePartitioning.Range.Start"] = strconv.FormatInt(md.RangePartitioning.Range.Start, 10)
			metadata["RangePartitioning.Range.End"] = strconv.FormatInt(md.RangePartitioning.Range.End, 10)
			metadata["RangePartitioning.Range.Interval"] = strconv.FormatInt(md.RangePartitioning.Range.Interval, 10)
		}
	}
	if md.RequirePartitionFilter {
		metadata["RequirePartitionFilter"] = strconv.FormatBool(md.RequirePartitionFilter)
	}
	labels := ""
	if len(md.Labels) > 0 {
		encodedLabel, err := json.Marshal(md.Labels)
		if err == nil {
			labels = string(encodedLabel)
		}
	}
	metadata["Labels"] = labels
	metadata["FullID"] = md.FullID
	metadata["Type"] = string(md.Type)
	metadata["CreationTime"] = md.CreationTime.Format(time.RFC3339Nano)
	metadata["LastModifiedTime"] = md.LastModifiedTime.Format(time.RFC3339Nano)
	metadata["NumBytes"] = strconv.FormatInt(md.NumBytes, 10)
	metadata["NumLongTermBytes"] = strconv.FormatInt(md.NumLongTermBytes, 10)
	metadata["NumRows"] = strconv.FormatUint(md.NumRows, 10)
	if md.SnapshotDefinition != nil {
		metadata["SnapshotDefinition.BaseTableReference"] = md.SnapshotDefinition.BaseTableReference.FullyQualifiedName()
		metadata["SnapshotDefinition.SnapshotTime"] = md.SnapshotDefinition.SnapshotTime.Format(time.RFC3339Nano)
	}
	if md.CloneDefinition != nil {
		metadata["CloneDefinition.BaseTableReference"] = md.CloneDefinition.BaseTableReference.FullyQualifiedName()
		metadata["CloneDefinition.CloneTime"] = md.CloneDefinition.CloneTime.Format(time.RFC3339Nano)
	}
	metadata["ETag"] = md.ETag
	metadata["DefaultCollation"] = md.DefaultCollation
	tableMetadata := arrow.MetadataFrom(metadata)

	fields := make([]arrow.Field, len(md.Schema))
	for i, schema := range md.Schema {
		f, err := buildField(schema, 0)
		if err != nil {
			return nil, err
		}
		fields[i] = f
	}
	schema := arrow.NewSchema(fields, &tableMetadata)

	return schema, nil
}

func buildField(schema *bigquery.FieldSchema, level uint) (arrow.Field, error) {
	field := arrow.Field{Name: schema.Name}
	metadata := make(map[string]string)
	metadata["Description"] = schema.Description
	metadata["Repeated"] = strconv.FormatBool(schema.Repeated)
	metadata["Required"] = strconv.FormatBool(schema.Required)
	field.Nullable = !schema.Required
	metadata["Type"] = string(schema.Type)

	if schema.PolicyTags != nil {
		policyTagList, err := json.Marshal(schema.PolicyTags)
		if err != nil {
			return arrow.Field{}, err
		}
		metadata["PolicyTags"] = string(policyTagList)
	}

	// https://cloud.google.com/bigquery/docs/reference/storage#arrow_schema_details
	switch schema.Type {
	case bigquery.StringFieldType:
		metadata["MaxLength"] = strconv.FormatInt(schema.MaxLength, 10)
		metadata["Collation"] = schema.Collation
		field.Type = arrow.BinaryTypes.String
	case bigquery.BytesFieldType:
		metadata["MaxLength"] = strconv.FormatInt(schema.MaxLength, 10)
		field.Type = arrow.BinaryTypes.Binary
	case bigquery.IntegerFieldType:
		field.Type = arrow.PrimitiveTypes.Int64
	case bigquery.FloatFieldType:
		field.Type = arrow.PrimitiveTypes.Float64
	case bigquery.BooleanFieldType:
		field.Type = arrow.FixedWidthTypes.Boolean
	case bigquery.TimestampFieldType:
		field.Type = arrow.FixedWidthTypes.Timestamp_ms
	case bigquery.RecordFieldType:
		if schema.Repeated {
			if len(schema.Schema) == 1 {
				arrayField, err := buildField(schema.Schema[0], level+1)
				if err != nil {
					return arrow.Field{}, err
				}
				field.Type = arrow.ListOf(arrayField.Type)
				field.Metadata = arrayField.Metadata
				field.Nullable = arrayField.Nullable
			} else {
				return arrow.Field{}, adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("Cannot create array schema for filed `%s`: len(schema.Schema) != 1", schema.Name),
				}
			}
		} else {
			nestedFields := make([]arrow.Field, len(schema.Schema))
			for i, nestedSchema := range schema.Schema {
				f, err := buildField(nestedSchema, level+1)
				if err != nil {
					return arrow.Field{}, err
				}
				nestedFields[i] = f
			}
			structType := arrow.StructOf(nestedFields...)
			if structType == nil {
				return arrow.Field{}, adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("Cannot create a struct schema for record `%s`", schema.Name),
				}
			}
			field.Type = structType
		}

	case bigquery.DateFieldType:
		field.Type = arrow.FixedWidthTypes.Date32
	case bigquery.TimeFieldType:
		field.Type = arrow.FixedWidthTypes.Time64us
	case bigquery.DateTimeFieldType:
		field.Type = arrow.FixedWidthTypes.Timestamp_us
	case bigquery.NumericFieldType:
		field.Type = &arrow.Decimal128Type{
			Precision: int32(schema.Precision),
			Scale:     int32(schema.Scale),
		}
	case bigquery.GeographyFieldType:
		// TODO: potentially we should consider using GeoArrow for this
		field.Type = arrow.BinaryTypes.String
	case bigquery.BigNumericFieldType:
		field.Type = &arrow.Decimal256Type{
			Precision: int32(schema.Precision),
			Scale:     int32(schema.Scale),
		}
	case bigquery.JSONFieldType:
		field.Type = arrow.BinaryTypes.String
	default:
		// TODO: unsupported ones are:
		// - bigquery.IntervalFieldType
		// - bigquery.RangeFieldType
		return arrow.Field{}, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("Google SQL type `%s` is not supported yet", schema.Type),
		}
	}

	if level == 0 {
		metadata["DefaultValueExpression"] = schema.DefaultValueExpression
	}
	field.Metadata = arrow.MetadataFrom(metadata)
	return field, nil
}

func (c *connectionImpl) Token() (*oauth2.Token, error) {
	token, err := c.getAccessToken()
	if err != nil {
		return nil, err
	}

	now := time.Now()
	return &oauth2.Token{
		AccessToken:  token.AccessToken,
		TokenType:    "Bearer",
		RefreshToken: c.refreshToken,
		Expiry:       now.Add(time.Second * time.Duration(token.ExpiresIn)),
	}, nil
}

func (c *connectionImpl) getAccessToken() (*bigQueryTokenResponse, error) {
	params := url.Values{}
	params.Add("grant_type", "refresh_token")
	params.Add("client_id", c.clientID)
	params.Add("client_secret", c.clientSecret)
	params.Add("refresh_token", c.refreshToken)
	req, err := http.NewRequest("POST", c.accessTokenEndpoint, bytes.NewBufferString(params.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{ServerName: c.accessTokenServerName},
	}
	client := &http.Client{
		Transport: tr,
	}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer func(Body io.ReadCloser) {
		bodyErr := Body.Close()
		if bodyErr != nil {
			err = bodyErr
		}
	}(resp.Body)

	contents, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}

	var tokenResponse bigQueryTokenResponse
	err = json.Unmarshal(contents, &tokenResponse)
	if err != nil {
		return nil, err
	}
	return &tokenResponse, nil
}
