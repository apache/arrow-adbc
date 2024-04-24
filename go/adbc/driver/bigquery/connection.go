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
	"strings"
	"time"

	"golang.org/x/oauth2"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow/go/v16/arrow"
	"google.golang.org/api/option"
)

type connectionImpl struct {
	driverbase.ConnectionImplBase

	authType     string
	credentials  string
	clientID     string
	clientSecret string
	refreshToken string
	projectID    string
	datasetID    string
	tableID      string

	catalog  string
	dbSchema string

	client *bigquery.Client
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

func (c *connectionImpl) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (*arrow.Schema, error) {
	return c.getTableSchemaWithFilter(ctx, catalog, dbSchema, tableName, nil)
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
	case OptionStringAuthCredentials:
		return c.credentials, nil
	case OptionStringAuthClientID:
		return c.clientID, nil
	case OptionStringAuthClientSecret:
		return c.clientSecret, nil
	case OptionStringAuthRefreshToken:
		return c.refreshToken, nil
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
	case OptionValueAuthTypeJSONCredentialFile, OptionValueAuthTypeJSONCredentialString, OptionValueAuthTypeUserAuthentication:
		var credentials option.ClientOption
		if c.authType == OptionValueAuthTypeJSONCredentialFile {
			credentials = option.WithCredentialsFile(c.credentials)
		} else if c.authType == OptionValueAuthTypeJSONCredentialString {
			credentials = option.WithCredentialsJSON([]byte(c.credentials))
		} else {
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
			credentials = option.WithTokenSource(c)
		}

		client, err := bigquery.NewClient(ctx, c.projectID, credentials)
		if err != nil {
			return err
		}

		err = client.EnableStorageReadClient(ctx, credentials)
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
	precisionScaleRegex = regexp.MustCompile(`^(?:BIG)?NUMERIC\((?P<precision>\d+)(?:,(?P<scale>\d+))?\)$`)
	simpleDataType      = map[string]arrow.DataType{
		"BOOL":      arrow.FixedWidthTypes.Boolean,
		"BOOLEAN":   arrow.FixedWidthTypes.Boolean,
		"FLOAT":     arrow.PrimitiveTypes.Float64,
		"FLOAT64":   arrow.PrimitiveTypes.Float64,
		"BYTES":     arrow.BinaryTypes.Binary,
		"STRING":    arrow.BinaryTypes.String,
		"GEOGRAPHY": arrow.BinaryTypes.String,
		"JSON":      arrow.BinaryTypes.String,
		"DATE":      arrow.FixedWidthTypes.Date32,
		"DATETIME":  &arrow.TimestampType{Unit: arrow.Microsecond},
		"TIMESTAMP": &arrow.TimestampType{Unit: arrow.Microsecond},
		"TIME":      arrow.FixedWidthTypes.Time64us,
	}
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

func buildSchemaField(name string, typeString string) (arrow.Field, error) {
	index := strings.Index(name, "(")
	if index == -1 {
		index = strings.Index(name, "<")
	} else {
		lIndex := strings.Index(name, "<")
		if index < lIndex {
			index = lIndex
		}
	}

	if index == -1 {
		return buildField(name, typeString, typeString, index)
	} else {
		dataType := typeString[:index]
		return buildField(name, typeString, dataType, index)
	}
}

func buildField(name, typeString, dataType string, index int) (arrow.Field, error) {
	// https://cloud.google.com/bigquery/docs/reference/storage#arrow_schema_details
	field := arrow.Field{
		Name: strings.Clone(name),
	}
	val, ok := simpleDataType[dataType]
	if ok {
		field.Type = val
		return field, nil
	}

	switch dataType {
	case "NUMERIC", "DECIMAL":
		precision, scale, err := parsePrecisionAndScale(name, typeString)
		if err != nil {
			return arrow.Field{}, err
		}
		field.Type = &arrow.Decimal128Type{
			Precision: precision,
			Scale:     scale,
		}
	case "BIGNUMERIC", "BIGDECIMAL":
		precision, scale, err := parsePrecisionAndScale(name, typeString)
		if err != nil {
			return arrow.Field{}, err
		}
		field.Type = &arrow.Decimal256Type{
			Precision: precision,
			Scale:     scale,
		}
	case "ARRAY":
		arrayType := strings.Replace(typeString[:len(dataType)], "<", "", 1)
		rIndex := strings.LastIndex(arrayType, ">")
		if rIndex == -1 {
			return arrow.Field{}, adbc.Error{
				Code: adbc.StatusInvalidData,
				Msg:  fmt.Sprintf("Cannot parse array type `%s` for field `%s`: cannot find `>`", typeString, name),
			}
		}
		arrayType = arrayType[:rIndex] + arrayType[rIndex+1:]
		arrayFieldType, err := buildField(name, typeString, arrayType, rIndex)
		if err != nil {
			return arrow.Field{}, err
		}
		field.Type = arrow.ListOf(arrayFieldType.Type)
		field.Metadata = arrayFieldType.Metadata
		field.Nullable = arrayFieldType.Nullable
	case "RECORD", "STRUCT":
		fieldRecords := typeString[index+1:]
		fieldRecords = fieldRecords[:len(fieldRecords)-1]
		nestedFields := make([]arrow.Field, 0)
		for _, record := range strings.Split(fieldRecords, ",") {
			fieldRecord := strings.TrimSpace(record)
			recordParts := strings.SplitN(fieldRecord, " ", 2)
			if len(recordParts) != 2 {
				return arrow.Field{}, adbc.Error{
					Code: adbc.StatusInvalidData,
					Msg:  fmt.Sprintf("invalid field record `%s` for type `%s`", fieldRecord, dataType),
				}
			}
			fieldName := recordParts[0]
			fieldType := recordParts[1]
			nestedField, err := buildSchemaField(fieldName, fieldType)
			if err != nil {
				return arrow.Field{}, err
			}
			nestedFields = append(nestedFields, nestedField)
		}
		structType := arrow.StructOf(nestedFields...)
		if structType == nil {
			return arrow.Field{}, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("Cannot create a struct schema for record `%s`", fieldRecords),
			}
		}
		field.Type = structType
	default:
		return arrow.Field{}, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("Cannot handle data type `%s`, type=`%s`", dataType, typeString),
		}
	}
	return field, nil
}

func parsePrecisionAndScale(name, typeString string) (int32, int32, error) {
	typeString = strings.TrimSpace(typeString)
	if len(typeString) == 0 {
		return 0, 0, adbc.Error{
			Code: adbc.StatusInvalidData,
			Msg:  fmt.Sprintf("Cannot parse data type `%s` for field `%s`", typeString, name),
		}
	}

	if typeString == "NUMERIC" {
		return 38, 9, nil
	} else if typeString == "BIGNUMERIC" {
		return 76, 38, nil
	}

	r := precisionScaleRegex.FindStringSubmatch(typeString)
	if len(r) != 3 {
		return 0, 0, adbc.Error{
			Code: adbc.StatusInvalidData,
			Msg:  fmt.Sprintf("Cannot parse data type `%s` for field `%s`", typeString, name),
		}
	}

	precisionString := r[precisionScaleRegex.SubexpIndex("precision")]
	precision, err := strconv.ParseInt(precisionString, 10, 32)
	if err != nil {
		return 0, 0, adbc.Error{
			Code: adbc.StatusInvalidData,
			Msg:  fmt.Sprintf("Cannot parse precision `%s` for field `%s`: %s", precisionString, name, err.Error()),
		}
	}

	scaleString := r[precisionScaleRegex.SubexpIndex("scale")]
	scale, err := strconv.ParseInt(scaleString, 10, 32)
	if err != nil {
		return 0, 0, adbc.Error{
			Code: adbc.StatusInvalidData,
			Msg:  fmt.Sprintf("Cannot parse scale `%s` for field `%s`: %s", scaleString, name, err.Error()),
		}
	}
	return int32(precision), int32(scale), nil
}

func (c *connectionImpl) getTableSchemaWithFilter(ctx context.Context, catalog *string, dbSchema *string, tableName string, columnName *string) (*arrow.Schema, error) {
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

	// sadly that query parameters cannot be used in place of table names
	queryString := fmt.Sprintf("SELECT * FROM `%s`.`%s`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = @tableName", sanitizedCatalog, sanitizedDbSchema)
	query := c.client.Query(queryString)
	query.Parameters = []bigquery.QueryParameter{
		{
			Name:  "tableName",
			Value: tableName,
		},
	}
	reader, _, err := newRecordReader(ctx, query, c.Alloc)
	if err != nil {
		return nil, err
	}

	fields := make([]arrow.Field, 0)
	var columns map[string]int = nil

	for reader.Next() && ctx.Err() == nil {
		rec := reader.Record()
		if columns == nil {
			columns = make(map[string]int)
			for i, f := range reader.Schema().Fields() {
				columns[strings.ToUpper(f.Name)] = i
			}
		}

		numRows := int(rec.NumRows())
		val, ok := columns["COLUMN_NAME"]
		if !ok {
			return nil, adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  "Column `COLUMN_NAME` does not exist in response",
			}
		}
		fieldName := rec.Column(val)

		val, ok = columns["IS_NULLABLE"]
		if !ok {
			return nil, adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  "Column `IS_NULLABLE` does not exist in response",
			}
		}
		fieldNullable := rec.Column(val)

		val, ok = columns["DATA_TYPE"]
		if !ok {
			return nil, adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  "Column `DATA_TYPE` does not exist in response",
			}
		}
		fieldType := rec.Column(val)

		for i := 0; i < numRows; i++ {
			metadata := make(map[string]string)
			metadata["PRIMARY_KEY"] = ""
			for j := 0; j < int(rec.NumCols()); j++ {
				col := rec.Column(j)
				columnName := strings.ToUpper(rec.ColumnName(j))
				switch columnName {
				case "COLUMN_NAME", "IS_NULLABLE":
					continue
				default:
					metadata[columnName] = strings.Clone(col.ValueStr(i))
				}
			}

			name := fieldName.ValueStr(i)
			nullable := strings.ToUpper(fieldNullable.ValueStr(i))
			dataType := fieldType.ValueStr(i)

			field, err := buildSchemaField(name, dataType)
			if err != nil {
				return nil, err
			}
			field.Nullable = nullable == "YES"
			field.Metadata = arrow.MetadataFrom(metadata)
			fields = append(fields, field)
		}
		fieldName.Release()
		fieldNullable.Release()
		fieldType.Release()
		rec.Release()
	}

	schema := arrow.NewSchema(fields, nil)
	return schema, nil
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
	req, err := http.NewRequest("POST", AccessTokenEndpoint, bytes.NewBufferString(params.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{ServerName: AccessTokenServerName},
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
