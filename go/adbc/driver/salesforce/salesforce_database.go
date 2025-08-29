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

package salesforce

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
)

type databaseImpl struct {
	driverbase.DatabaseImplBase

	// Authentication settings
	authType string
	loginURL string
	version  string

	username     string
	clientId     string
	clientSecret string

	// JWT Bearer Flow
	jwtBearerPrivateKey string

	// Username password Flow
	password string

	// Connection settings
	instanceURL string

	// Query settings
	queryRowLimit string
	queryTimeout  string
}

func (d *databaseImpl) Open(ctx context.Context) (adbc.Connection, error) {
	conn := &connectionImpl{
		ConnectionImplBase: driverbase.NewConnectionImplBase(&d.DatabaseImplBase),

		// Copy authentication settings
		authType: d.authType,
		loginURL: d.loginURL,
		version:  d.version,

		username:     d.username,
		clientId:     d.clientId,
		clientSecret: d.clientSecret,

		jwtBearerPrivateKey: d.jwtBearerPrivateKey,

		instanceURL:   d.instanceURL,
		queryRowLimit: d.queryRowLimit,
		queryTimeout:  d.queryTimeout,
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
	case OptionStringLoginURL:
		return d.loginURL, nil
	case OptionStringVersion:
		return d.version, nil

	case OptionStringClientID:
		return d.clientId, nil
	case OptionStringClientSecret:
		return d.clientSecret, nil
	case OptionStringUsername:
		return d.username, nil

	case OptionStringJWTPrivateKey:
		return d.jwtBearerPrivateKey, nil

	case OptionStringPassword:
		return d.password, nil

	case OptionStringInstanceURL:
		return d.instanceURL, nil

	case OptionStringQueryRowLimit:
		return d.queryRowLimit, nil
	case OptionStringQueryTimeout:
		return d.queryTimeout, nil
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

func (d *databaseImpl) SetOption(key string, value string) error {
	switch key {
	case OptionStringAuthType:
		switch value {
		case OptionValueAuthTypeJwtBearer, OptionValueAuthTypeUsernamePassword:
			d.authType = value
		default:
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("unknown database auth type value `%s`", value),
			}
		}
	case OptionStringLoginURL:
		d.loginURL = value
	case OptionStringVersion:
		d.version = value

	case OptionStringClientID:
		d.clientId = value
	case OptionStringClientSecret:
		d.clientSecret = value
	case OptionStringUsername:
		d.username = value

	case OptionStringJWTPrivateKey:
		d.jwtBearerPrivateKey = value

	case OptionStringPassword:
		d.password = value

	case OptionStringInstanceURL:
		d.instanceURL = value

	case OptionStringQueryRowLimit:
		d.queryRowLimit = value
	case OptionStringQueryTimeout:
		d.queryTimeout = value
	default:
		return d.DatabaseImplBase.SetOption(key, value)
	}
	return nil
}
