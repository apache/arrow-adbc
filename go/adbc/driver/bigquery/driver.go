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
	"runtime/debug"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const (
	OptionStringAuthType  = "adbc.bigquery.sql.auth_type"
	OptionStringLocation  = "adbc.bigquery.sql.location"
	OptionStringProjectID = "adbc.bigquery.sql.project_id"
	OptionStringDatasetID = "adbc.bigquery.sql.dataset_id"
	OptionStringTableID   = "adbc.bigquery.sql.table_id"

	OptionValueAuthTypeDefault = "adbc.bigquery.sql.auth_type.auth_bigquery"

	OptionValueAuthTypeJSONCredentialFile   = "adbc.bigquery.sql.auth_type.json_credential_file"
	OptionValueAuthTypeJSONCredentialString = "adbc.bigquery.sql.auth_type.json_credential_string"
	OptionStringAuthCredentials             = "adbc.bigquery.sql.auth_credentials"

	OptionValueAuthTypeUserAuthentication = "adbc.bigquery.sql.auth_type.user_authentication"
	OptionStringAuthClientID              = "adbc.bigquery.sql.auth.client_id"
	OptionStringAuthClientSecret          = "adbc.bigquery.sql.auth.client_secret"
	OptionStringAuthRefreshToken          = "adbc.bigquery.sql.auth.refresh_token"

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

	AccessTokenEndpoint   = "https://accounts.google.com/o/oauth2/token"
	AccessTokenServerName = "google.com"

	// WithAppDefaultCredentials instructs the driver to authenticate using
	// Application Default Credentials (ADC).
	OptionValueAuthTypeAppDefaultCredentials = "adbc.bigquery.sql.auth_type.app_default_credentials"

	// WithJSONCredentials instructs the driver to authenticate using the
	// given JSON credentials. The value should be a byte array representing
	// the JSON credentials.
	OptionValueAuthTypeJSONCredentials = "adbc.bigquery.sql.auth_type.json_credentials"

	// WithOAuthClientIDs instructs the driver to authenticate using the given
	// OAuth client ID and client secret. The value should be a string array
	// of length 2, where the first element is the client ID and the second
	// is the client secret.
	OptionValueAuthTypeOAuthClientIDs = "adbc.bigquery.sql.auth_type.oauth_client_ids"

	// OptionStringImpersonateTargetPrincipal instructs the driver to impersonate the
	// given service account email.
	OptionStringImpersonateTargetPrincipal = "adbc.bigquery.sql.impersonate.target_principal"

	// OptionStringImpersonateDelegates instructs the driver to impersonate using the
	// given comma-separated list of service account emails in the delegation
	// chain.
	OptionStringImpersonateDelegates = "adbc.bigquery.sql.impersonate.delegates"

	// OptionStringImpersonateScopes instructs the driver to impersonate using the
	// given comma-separated list of OAuth 2.0 scopes.
	OptionStringImpersonateScopes = "adbc.bigquery.sql.impersonate.scopes"

	// OptionStringImpersonateLifetime instructs the driver to impersonate for the
	// given duration (e.g. "3600s").
	OptionStringImpersonateLifetime = "adbc.bigquery.sql.impersonate.lifetime"
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
	return d.NewDatabaseWithContext(context.Background(), opts)
}

func (d *driverImpl) NewDatabaseWithContext(ctx context.Context, opts map[string]string) (adbc.Database, error) {
	dbBase, err := driverbase.NewDatabaseImplBase(ctx, &d.DriverImplBase)
	if err != nil {
		return nil, err
	}
	db := &databaseImpl{
		DatabaseImplBase: dbBase,
		authType:         OptionValueAuthTypeDefault,
	}
	if err := db.SetOptions(opts); err != nil {
		return nil, err
	}

	return driverbase.NewDatabase(db), nil
}

func stringToTable(defaultProjectID, defaultDatasetID, value string) (*bigquery.Table, error) {
	parts := strings.Split(value, ".")
	table := &bigquery.Table{
		ProjectID: defaultProjectID,
		DatasetID: defaultDatasetID,
	}
	switch len(parts) {
	case 1:
		table.TableID = parts[0]
	case 2:
		table.DatasetID = parts[0]
		table.TableID = parts[1]
	case 3:
		table.ProjectID = parts[0]
		table.DatasetID = parts[1]
		table.TableID = parts[2]
	default:
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("Invalid Table Reference format, expected `[[ProjectId.]DatasetId.]TableId`, got: `%s`", value),
		}
	}
	return table, nil
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
