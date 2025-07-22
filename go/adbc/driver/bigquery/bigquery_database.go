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
	"strings"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
)

type databaseImpl struct {
	driverbase.DatabaseImplBase

	authType     string
	credentials  string
	clientID     string
	clientSecret string
	refreshToken string

	impersonateTargetPrincipal string
	impersonateDelegates       []string
	impersonateScopes          []string
	impersonateLifetime        time.Duration

	clientFactory      bigqueryClientFactory
	tokenSourceFactory impersonatedTokenSourceFactory

	// projectID is the catalog
	projectID string
	// datasetID is the schema
	datasetID string
	tableID   string
}

func (d *databaseImpl) Open(ctx context.Context) (adbc.Connection, error) {
	conn := &connectionImpl{
		ConnectionImplBase:         driverbase.NewConnectionImplBase(&d.DatabaseImplBase),
		authType:                   d.authType,
		credentials:                d.credentials,
		clientID:                   d.clientID,
		clientSecret:               d.clientSecret,
		refreshToken:               d.refreshToken,
		impersonateTargetPrincipal: d.impersonateTargetPrincipal,
		impersonateDelegates:       d.impersonateDelegates,
		impersonateScopes:          d.impersonateScopes,
		impersonateLifetime:        d.impersonateLifetime,
		clientFactory:              d.clientFactory,
		tokenSourceFactory:         d.tokenSourceFactory,
		tableID:                    d.tableID,
		catalog:                    d.projectID,
		dbSchema:                   d.datasetID,
		resultRecordBufferSize:     defaultQueryResultBufferSize,
		prefetchConcurrency:        defaultQueryPrefetchConcurrency,
	}

	err := conn.newClient(ctx)
	if err != nil {
		return nil, err
	}

	return driverbase.NewConnectionBuilder(conn).
		WithAutocommitSetter(conn).
		WithCurrentNamespacer(conn).
		WithTableTypeLister(conn).
		WithDbObjectsEnumerator(conn).
		Connection(), nil
}

func (d *databaseImpl) Close() error { return nil }

func (d *databaseImpl) GetOption(key string) (string, error) {
	switch key {
	case OptionStringAuthType:
		return d.authType, nil
	case OptionStringAuthCredentials:
		return d.credentials, nil
	case OptionStringAuthClientID:
		return d.clientID, nil
	case OptionStringAuthClientSecret:
		return d.clientSecret, nil
	case OptionStringAuthRefreshToken:
		return d.refreshToken, nil
	case OptionStringProjectID:
		return d.projectID, nil
	case OptionStringDatasetID:
		return d.datasetID, nil
	case OptionStringTableID:
		return d.tableID, nil
	case OptionStringImpersonateLifetime:
		if d.impersonateLifetime == 0 {
			// If no lifetime is set but impersonation is enabled, return the default
			if d.hasImpersonationOptions() {
				return (3600 * time.Second).String(), nil
			}
			return "", nil
		}
		return d.impersonateLifetime.String(), nil
	default:
		return d.DatabaseImplBase.GetOption(key)
	}
}

func (d *databaseImpl) SetOptions(options map[string]string) error {
	for k, v := range options {
		err := d.SetOption(k, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *databaseImpl) hasImpersonationOptions() bool {
	return d.impersonateTargetPrincipal != "" ||
		len(d.impersonateDelegates) > 0 ||
		len(d.impersonateScopes) > 0
}

func (d *databaseImpl) SetOption(key string, value string) error {
	switch key {
	case OptionStringAuthType:
		switch value {
		case OptionValueAuthTypeDefault,
			OptionValueAuthTypeJSONCredentialFile,
			OptionValueAuthTypeJSONCredentialString,
			OptionValueAuthTypeUserAuthentication,
			OptionValueAuthTypeAppDefaultCredentials:
			d.authType = value
		default:
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("unknown database auth type value `%s`", value),
			}
		}
	case OptionStringAuthCredentials:
		d.credentials = value
	case OptionStringAuthClientID:
		d.clientID = value
	case OptionStringAuthClientSecret:
		d.clientSecret = value
	case OptionStringAuthRefreshToken:
		d.refreshToken = value
	case OptionStringImpersonateTargetPrincipal:
		d.impersonateTargetPrincipal = value
	case OptionStringImpersonateDelegates:
		d.impersonateDelegates = strings.Split(value, ",")
	case OptionStringImpersonateScopes:
		d.impersonateScopes = strings.Split(value, ",")
	case OptionStringImpersonateLifetime:
		duration, err := time.ParseDuration(value)
		if err != nil {
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("invalid impersonate lifetime value `%s`: %v", value, err),
			}
		}
		d.impersonateLifetime = duration
	case OptionStringProjectID:
		d.projectID = value
	case OptionStringDatasetID:
		d.datasetID = value
	case OptionStringTableID:
		d.tableID = value
	default:
		return d.DatabaseImplBase.SetOption(key, value)
	}
	return nil
}
