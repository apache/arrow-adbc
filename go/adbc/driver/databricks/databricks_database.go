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
	client, err := databricks.NewWorkspaceClient(d.config)
	if err != nil {
		if err == databricks.ErrNotWorkspaceClient {
			return nil, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  "[Databricks] " + err.Error(),
			}
		}
		return nil, adbc.Error{
			Code: adbc.StatusUnknown,
			Msg:  "[Databricks] " + err.Error(),
		}
	}
	conn := &connectionImpl{
		ConnectionImplBase: driverbase.NewConnectionImplBase(&d.DatabaseImplBase),
		client:             client,
		catalog:     d.catalog,
		dbSchema:      d.dbSchema,
	}
	return driverbase.NewConnectionBuilder(conn).
		WithAutocommitSetter(conn).
		WithCurrentNamespacer(conn).
		WithTableTypeLister(conn).
		WithDbObjectsEnumerator(conn).
		Connection(), nil
}

func SetOptionToConfig(config *databricks.Config, key string, value string) error {
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
	}
	return nil
}

func (d *databaseImpl) SetOptions(options map[string]string) error {
	if d.config == nil {
		d.config = &databricks.Config{}
	}
	for k, v := range options {
		switch k {
		case OptionStringCatalog:
			d.catalog = v
		case OptionStringSchema:
			d.dbSchema = v
		default:
			err := SetOptionToConfig(d.config, k, v)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
