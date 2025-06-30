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
	"fmt"
	"runtime/debug"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const (
	OptionStringAuthType  = "adbc.bigquery.sql.auth_type"
	OptionStringProjectID = "adbc.bigquery.sql.project_id"
	OptionStringDatasetID = "adbc.bigquery.sql.dataset_id"
	OptionStringTableID   = "adbc.bigquery.sql.table_id"

	OptionValueAuthTypeDefault = "adbc.bigquery.sql.auth_type.auth_bigquery"

	OptionValueAuthTypeJSONCredentialFile   = "adbc.bigquery.sql.auth_type.json_credential_file"
	OptionValueAuthTypeJSONCredentialString = "adbc.bigquery.sql.auth_type.json_credential_string"
	OptionStringAuthCredentials             = "adbc.bigquery.sql.auth_credentials"

	OptionValueAuthTypeTemporaryAccessToken = "adbc.bigquery.sql.auth_type.temporary_access_token"
	OptionStringAuthAccessToken             = "adbc.bigquery.sql.auth.access_token"

	OptionValueAuthTypeUserAuthentication = "adbc.bigquery.sql.auth_type.user_authentication"
	OptionStringAuthClientID              = "adbc.bigquery.sql.auth.client_id"
	OptionStringAuthClientSecret          = "adbc.bigquery.sql.auth.client_secret"
	OptionStringAuthRefreshToken          = "adbc.bigquery.sql.auth.refresh_token"
	OptionStringAuthAccessTokenEndpoint   = "adbc.bigquery.sql.auth.access_token_endpoint"
	OptionStringAuthAccessTokenServerName = "adbc.bigquery.sql.auth.access_token_server_name"

	// OptionStringQueryParameterMode specifies if the query uses positional syntax ("?")
	// or the named syntax ("@p"). It is illegal to mix positional and named syntax.
	// Default is OptionValueQueryParameterModePositional.
	OptionStringQueryParameterMode          = "adbc.bigquery.sql.query.parameter_mode"
	OptionValueQueryParameterModeNamed      = "adbc.bigquery.sql.query.parameter_mode_named"
	OptionValueQueryParameterModePositional = "adbc.bigquery.sql.query.parameter_mode_positional"

	OptionStringQueryDestinationTable  = "adbc.bigquery.sql.query.destination_table"
	OptionStringQueryDefaultProjectID  = "adbc.bigquery.sql.query.default_project_id"
	OptionStringQueryDefaultDatasetID  = "adbc.bigquery.sql.query.default_dataset_id"
	OptionStringQueryCreateDisposition = "adbc.bigquery.sql.query.create_disposition"
	OptionStringQueryWriteDisposition  = "adbc.bigquery.sql.query.write_disposition"
	OptionBoolQueryDisableQueryCache   = "adbc.bigquery.sql.query.disable_query_cache"
	OptionBoolDisableFlattenedResults  = "adbc.bigquery.sql.query.disable_flattened_results"
	OptionBoolQueryAllowLargeResults   = "adbc.bigquery.sql.query.allow_large_results"
	OptionStringQueryPriority          = "adbc.bigquery.sql.query.priority"
	OptionIntQueryMaxBillingTier       = "adbc.bigquery.sql.query.max_billing_tier"
	OptionIntQueryMaxBytesBilled       = "adbc.bigquery.sql.query.max_bytes_billed"
	OptionBoolQueryUseLegacySQL        = "adbc.bigquery.sql.query.use_legacy_sql"
	OptionBoolQueryDryRun              = "adbc.bigquery.sql.query.dry_run"
	OptionBoolQueryCreateSession       = "adbc.bigquery.sql.query.create_session"
	OptionIntQueryJobTimeout           = "adbc.bigquery.sql.query.job_timeout"

	OptionIntQueryResultBufferSize    = "adbc.bigquery.sql.query.result_buffer_size"
	OptionIntQueryPrefetchConcurrency = "adbc.bigquery.sql.query.prefetch_concurrency"

	defaultQueryResultBufferSize    = 200
	defaultQueryPrefetchConcurrency = 10

	DefaultAccessTokenEndpoint   = "https://accounts.google.com/o/oauth2/token"
	DefaultAccessTokenServerName = "google.com"

	OptionStringIngestFileDelimiter = "adbc.bigquery.ingest.csv_delimiter"
	OptionStringIngestPath          = "adbc.bigquery.ingest.csv_filepath"
	OptionStringIngestSchema        = "adbc.bigquery.ingest.csv_schema"

	OptionJsonUpdateTableColumnsDescription = "adbc.bigquery.table.update.columns_description"
)

var (
	infoVendorVersion string
)

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			switch dep.Path {
			case "cloud.google.com/go/bigquery":
				infoVendorVersion = dep.Version
			}
		}
	}
}

type driverImpl struct {
	driverbase.DriverImplBase
}

// NewDriver creates a new BigQuery driver using the given Arrow allocator.
func NewDriver(alloc memory.Allocator) adbc.Driver {
	info := driverbase.DefaultDriverInfo("BigQuery")
	if infoVendorVersion != "" {
		if err := info.RegisterInfoCode(adbc.InfoVendorVersion, infoVendorVersion); err != nil {
			panic(err)
		}
	}
	return driverbase.NewDriver(&driverImpl{
		DriverImplBase: driverbase.NewDriverImplBase(info, alloc),
	})
}

func (d *driverImpl) NewDatabase(opts map[string]string) (adbc.Database, error) {
	db := &databaseImpl{
		DatabaseImplBase: driverbase.NewDatabaseImplBase(&d.DriverImplBase),
		authType:         OptionValueAuthTypeDefault,
	}
	if err := db.SetOptions(opts); err != nil {
		return nil, err
	}

	return driverbase.NewDatabase(db), nil
}

func parseParts(defaultProjectID, defaultDatasetID, value string) (string, string, string, error) {
	parts := strings.Split(value, ".")
	switch len(parts) {
	case 1:
		return defaultProjectID, defaultDatasetID, parts[0], nil
	case 2:
		return defaultProjectID, parts[0], parts[1], nil
	case 3:
		return parts[0], parts[1], parts[2], nil
	default:
		return "", "", "", adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("invalid table reference %q (want [[project.]dataset.]table)", value),
		}
	}
}

// This takes a partially qualified table string in format [project.][.dataset.]table
// and returns a bigquery.Table object.
// If project or dataset is not provided, the default project ID and dataset ID from the statement is used
// Returns an error if the format is invalid.
func stringToTable(st *statement, value string) (*bigquery.Table, error) {
	defaultProjectID := st.cnxn.catalog
	defaultDatasetID := st.cnxn.dbSchema
	projectID, datasetID, tableID, err := parseParts(defaultProjectID, defaultDatasetID, value)
	if err != nil {
		return nil, err
	}
	return st.cnxn.table(projectID, datasetID, tableID), nil
}

func stringToTableCreateDisposition(value string) (bigquery.TableCreateDisposition, error) {
	v := bigquery.TableCreateDisposition(value)
	switch v {
	case bigquery.CreateIfNeeded, bigquery.CreateNever:
		return v, nil
	default:
		return v, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown table create disposition value `%s`", v),
		}
	}
}

func stringToTableWriteDisposition(value string) (bigquery.TableWriteDisposition, error) {
	v := bigquery.TableWriteDisposition(value)
	switch v {
	case bigquery.WriteAppend, bigquery.WriteTruncate, bigquery.WriteEmpty:
		return v, nil
	default:
		return v, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown table write disposition value `%s`", v),
		}
	}
}

func stringToQueryPriority(value string) (bigquery.QueryPriority, error) {
	v := bigquery.QueryPriority(value)
	switch v {
	case bigquery.BatchPriority, bigquery.InteractivePriority:
		return v, nil
	default:
		return v, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown priority value `%s`", v),
		}
	}
}

func tableToString(value *bigquery.Table) string {
	if value == nil {
		return ""
	} else {
		return fmt.Sprintf("%s.%s.%s", value.ProjectID, value.DatasetID, value.TableID)
	}
}
