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

package databricks

import (
	"runtime/debug"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const (
	// When multiple authentication attributes are available in the environment,
	// use the authentication type specified by this argument.
	OptionStringAuthType                 = "adbc.databricks.auth_type"
	OptionValueAuthTypeOAuthM2M          = "oauth-m2m"
	OptionValueAuthTypePAT               = "pat"
	OptionValueAuthTypeGoogleID          = "google-id"
	OptionValueAuthTypeGoogleCredentials = "google-credentials"
	OptionValueAuthTypeAzureCLI          = "azure-cli"
	OptionValueAuthTypeAzureMSI          = "azure-msi"
	OptionValueAuthTypeAzureClientSecret = "azure-client-secret"
	OptionValueAuthTypeExternalBrowser   = "external-browser"

	OptionStringCluster             = "adbc.databricks.cluster"
	OptionStringWarehouse           = "adbc.databricks.warehouse"
	OptionStringServerlessComputeID = "adbc.databricks.serverless_compute_id"

	// Optional default catalog and schema to use when executing SQL statements.
	OptionStringCatalog = "adbc.databricks.catalog"
	OptionStringSchema  = "adbc.databricks.schema"

	// URL of the metadata service that provides authentication credentials.
	OptionStringMetadataServiceURL = "adbc.databricks.metadata_service_url"

	// Databricks host (either of workspace endpoint or Accounts API endpoint)
	OptionStringHost  = "adbc.databricks.host"
	OptionStringToken = "adbc.databricks.token"
	// The Databricks account ID for the Databricks account endpoint.
	// Only has effect when the Databricks host is also set to
	// https://accounts.cloud.databricks.com.
	OptionStringAccountID = "adbc.databricks.account_id"
	OptionStringUsername  = "username"
	OptionStringPassword  = "password"

	// The Databricks service principal's client ID and secret.
	OptionStringClientID     = "adbc.databricks.client_id"
	OptionStringClientSecret = "adbc.databricks.client_secret"

	// Location of the Databricks CLI credentials file, that is created
	// by `databricks configure --token` command. By default, it is located
	// in ~/.databrickscfg.
	OptionStringConfigFile = "adbc.databricks.config_file"
	// The default named profile to use, other than DEFAULT.
	OptionStringProfile = "adbc.databricks.profile"

	OptionStringGoogleServiceAccount = "adbc.databricks.google_service_account"
	OptionStringGoogleCredentials    = "adbc.databricks.google_credentials"

	// Azure Resource Manager ID for Azure Databricks workspace, which is exhanged
	// for a Host.
	OptionStringAzureResourceID = "adbc.databricks.azure_workspace_resource_id"

	OptionStringAzureUseMSI       = "adbc.databricks.azure_use_msi"
	OptionStringAzureClientSecret = "adbc.databricks.azure_client_secret"
	OptionStringAzureClientID     = "adbc.databricks.azure_client_id"
	OptionStringAzureTenantID     = "adbc.databricks.azure_tenant_id"

	// Parameters to request Azure OIDC token on behalf of Github Actions.
	// Ref: https://docs.github.com/en/actions/deployment/security-hardening-your-deployments/configuring-openid-connect-in-cloud-providers
	OptionStringActionsIDTokenRequestURL   = "adbc.databricks.actions_id_token_request_url"
	OptionStringActionsIDTokenRequestToken = "adbc.databricks.actions_id_token_request_token"

	// AzureEnvironment (PUBLIC, USGOVERNMENT, CHINA) has specific set of API
	// endpoints. The environment is determined based on the workspace hostname,
	// if it's specified.
	OptionStringAzureEnvironment = "adbc.databricks.azure_environment"

	// Skip SSL certificate verification for HTTP calls.
	// Use at your own risk or for unit testing purposes.
	OptionBoolInsecureSkipVerify = "adbc.databricks.insecure_skip_verify"

	// Number of seconds for HTTP timeout. Default is 60 (1 minute).
	OptionIntHTTPTimeoutSeconds = "adbc.databricks.http_timeout_seconds"

	// Truncate JSON fields in JSON above this limit. Default is 96.
	OptionIntDebugTruncateBytes = "adbc.databricks.debug_truncate_bytes"

	// Debug HTTP headers of requests made by the provider. Default is false.
	OptionBoolDebugHeaders = "adbc.databricks.debug_headers"

	// Maximum number of requests per second made to Databricks REST API. Default is 15 RPS.
	OptionIntRateLimitPerSecond = "adbc.databricks.rate_limit_per_second"

	// Number of seconds to keep retrying HTTP requests. Default is 300 (5 minutes).
	// If negative, the client will retry on retriable errors indefinitely.
	OptionIntRetryTimeoutSeconds = "adbc.databricks.retry_timeout_seconds"
)

var (
	infoVendorVersion string
)

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			switch {
			case dep.Path == "github.com/databricks/databricks-sdk-go":
				infoVendorVersion = dep.Version
			}
		}
	}
}

type driverImpl struct {
	driverbase.DriverImplBase
}

// NewDriver creates a new Databricks driver using the given Arrow allocator.
func NewDriver(alloc memory.Allocator) adbc.Driver {
	info := driverbase.DefaultDriverInfo("Databricks")
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
		config:           nil,
	}
	if err := db.SetOptions(opts); err != nil {
		return nil, err
	}

	return driverbase.NewDatabase(db), nil
}
