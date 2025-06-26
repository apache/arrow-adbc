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
	"context"
	"strconv"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/databricks/databricks-sdk-go"
)

type databaseImpl struct {
	driverbase.DatabaseImplBase

	config *databricks.Config
	// Default Catalog name (optional)
	catalog string
	// Default Schema name (optional)
	dbSchema string
}

func (d *databaseImpl) Open(ctx context.Context) (adbc.Connection, error) {
	// Set up external browser credentials strategy if external-browser auth type is used
	if d.config.AuthType == OptionValueAuthTypeExternalBrowser {
		d.config.Credentials = &ExternalBrowserCredentials{
			Host:     d.config.Host,
			ClientID: d.config.ClientID,
		}
	}
	client, err := databricks.NewWorkspaceClient(d.config)
	if err != nil {
		if err == databricks.ErrNotWorkspaceClient {
			return nil, NewAdbcError(
				"[Databricks] "+err.Error(),
				adbc.StatusInvalidArgument,
			)
		}
		return nil, NewAdbcError(
			"[Databricks] "+err.Error(),
			adbc.StatusUnknown,
		)
	}
	mode := ""
	if d.config.WarehouseID != "" {
		mode = ModeWarehouse
	} else {
		mode = ModeCluster
	}

	conn := &connectionImpl{
		ConnectionImplBase: driverbase.NewConnectionImplBase(&d.DatabaseImplBase),
		client:             client,
		catalog:            d.catalog,
		dbSchema:           d.dbSchema,
		mode:               mode,
	}
	return driverbase.NewConnectionBuilder(conn).
		WithAutocommitSetter(conn).
		WithCurrentNamespacer(conn).
		WithTableTypeLister(conn).
		WithDbObjectsEnumerator(conn).
		Connection(), nil
}

func (d *databaseImpl) SetOptions(options map[string]string) error {
	if d.config == nil {
		d.config = &databricks.Config{}
	}
	for k, v := range options {
		err := d.SetOption(k, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *databaseImpl) SetOptionToConfig(key string, value string) error {
	config := d.config
	switch key {
	case OptionStringAuthType:
		config.AuthType = value
	case OptionStringCluster:
		config.ClusterID = value
	case OptionStringWarehouse:
		config.WarehouseID = value
	case OptionStringServerlessComputeID:
		config.ServerlessComputeID = value
	case OptionStringMetadataServiceURL:
		config.MetadataServiceURL = value
	case OptionStringHost:
		config.Host = value
	case OptionStringToken:
		config.Token = value
	case OptionStringAccountID:
		config.AccountID = value
	case OptionStringUsername:
		config.Username = value
	case OptionStringPassword:
		config.Password = value
	case OptionStringClientID:
		config.ClientID = value
	case OptionStringClientSecret:
		config.ClientSecret = value
	case OptionStringConfigFile:
		config.ConfigFile = value
	case OptionStringProfile:
		config.Profile = value
	case OptionStringGoogleServiceAccount:
		config.GoogleServiceAccount = value
	case OptionStringGoogleCredentials:
		config.GoogleCredentials = value
	case OptionStringAzureResourceID:
		config.AzureResourceID = value
	case OptionStringAzureUseMSI:
		val, err := strconv.ParseBool(value)
		if err == nil {
			config.AzureUseMSI = val
		} else {
			return err
		}
	case OptionStringAzureClientSecret:
		config.AzureClientSecret = value
	case OptionStringAzureClientID:
		config.AzureClientID = value
	case OptionStringAzureTenantID:
		config.AzureTenantID = value
	case OptionStringActionsIDTokenRequestURL:
		config.ActionsIDTokenRequestURL = value
	case OptionStringActionsIDTokenRequestToken:
		config.ActionsIDTokenRequestToken = value
	case OptionStringAzureEnvironment:
		config.AzureEnvironment = value
	case OptionBoolInsecureSkipVerify:
		val, err := strconv.ParseBool(value)
		if err == nil {
			config.InsecureSkipVerify = val
		} else {
			return err
		}
	case OptionIntHTTPTimeoutSeconds:
		val, err := strconv.ParseInt(value, 10, 32)
		if err == nil {
			config.HTTPTimeoutSeconds = int(val)
		} else {
			return err
		}
	case OptionIntDebugTruncateBytes:
		val, err := strconv.ParseInt(value, 10, 32)
		if err == nil {
			config.DebugTruncateBytes = int(val)
		} else {
			return err
		}
	case OptionBoolDebugHeaders:
		val, err := strconv.ParseBool(value)
		if err == nil {
			config.DebugHeaders = val
		} else {
			return err
		}
	case OptionIntRateLimitPerSecond:
		val, err := strconv.ParseInt(value, 10, 32)
		if err == nil {
			config.RateLimitPerSecond = int(val)
		} else {
			return err
		}
	case OptionIntRetryTimeoutSeconds:
		val, err := strconv.ParseInt(value, 10, 32)
		if err == nil {
			config.RetryTimeoutSeconds = int(val)
		} else {
			return err
		}
	default:
		return d.DatabaseImplBase.SetOption(key, value)
	}
	return nil
}

func (d *databaseImpl) GetOptionFromConfig(key string) (string, error) {
	config := d.config
	switch key {
	case OptionStringAuthType:
		return config.AuthType, nil
	case OptionStringCluster:
		return config.ClusterID, nil
	case OptionStringWarehouse:
		return config.WarehouseID, nil
	case OptionStringServerlessComputeID:
		return config.ServerlessComputeID, nil
	case OptionStringMetadataServiceURL:
		return config.MetadataServiceURL, nil
	case OptionStringHost:
		return config.Host, nil
	case OptionStringToken:
		return config.Token, nil
	case OptionStringAccountID:
		return config.AccountID, nil
	case OptionStringUsername:
		return config.Username, nil
	case OptionStringPassword:
		return config.Password, nil
	case OptionStringClientID:
		return config.ClientID, nil
	case OptionStringClientSecret:
		return config.ClientSecret, nil
	case OptionStringConfigFile:
		return config.ConfigFile, nil
	case OptionStringProfile:
		return config.Profile, nil
	case OptionStringGoogleServiceAccount:
		return config.GoogleServiceAccount, nil
	case OptionStringGoogleCredentials:
		return config.GoogleCredentials, nil
	case OptionStringAzureResourceID:
		return config.AzureResourceID, nil
	case OptionStringAzureUseMSI:
		return strconv.FormatBool(config.AzureUseMSI), nil
	case OptionStringAzureClientSecret:
		return config.AzureClientSecret, nil
	case OptionStringAzureClientID:
		return config.AzureClientID, nil
	case OptionStringAzureTenantID:
		return config.AzureTenantID, nil
	case OptionStringActionsIDTokenRequestURL:
		return config.ActionsIDTokenRequestURL, nil
	case OptionStringActionsIDTokenRequestToken:
		return config.ActionsIDTokenRequestToken, nil
	case OptionStringAzureEnvironment:
		return config.AzureEnvironment, nil
	case OptionBoolInsecureSkipVerify:
		return strconv.FormatBool(config.InsecureSkipVerify), nil
	case OptionIntHTTPTimeoutSeconds:
		return strconv.Itoa(config.HTTPTimeoutSeconds), nil
	case OptionIntDebugTruncateBytes:
		return strconv.Itoa(config.DebugTruncateBytes), nil
	case OptionBoolDebugHeaders:
		return strconv.FormatBool(config.DebugHeaders), nil
	case OptionIntRateLimitPerSecond:
		return strconv.Itoa(config.RateLimitPerSecond), nil
	case OptionIntRetryTimeoutSeconds:
		return strconv.Itoa(config.RetryTimeoutSeconds), nil
	default:
		return d.DatabaseImplBase.GetOption(key)
	}
}

func (d *databaseImpl) SetOption(k string, v string) error {
	switch k {
	case OptionStringCatalog:
		d.catalog = v
	case OptionStringSchema:
		d.dbSchema = v
	default:
		err := d.SetOptionToConfig(k, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *databaseImpl) GetOption(key string) (string, error) {
	switch key {
	case OptionStringCatalog:
		return d.catalog, nil
	case OptionStringSchema:
		return d.dbSchema, nil
	default:
		return d.GetOptionFromConfig(key)
	}
}

var (
	_ adbc.PostInitOptions = (*databaseImpl)(nil)
)
