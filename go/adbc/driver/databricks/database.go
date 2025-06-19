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
	"fmt"
	"strconv"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
)

type databaseImpl struct {
	driverbase.DatabaseImplBase

	// Connection parameters
	serverHostname string
	httpPath       string
	accessToken    string
	port           string
	catalog        string
	schema         string

	// Query options
	queryTimeout        time.Duration
	maxRows             int64
	queryRetryCount     int
	downloadThreadCount int

	// TLS/SSL options
	sslMode     string
	sslRootCert string

	// OAuth options (for future expansion)
	oauthClientID     string
	oauthClientSecret string
	oauthRefreshToken string
}

func (d *databaseImpl) Open(ctx context.Context) (adbc.Connection, error) {
	if d.serverHostname == "" {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "server hostname is required",
		}
	}

	if d.httpPath == "" {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "HTTP path is required",
		}
	}

	if d.accessToken == "" {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "access token is required",
		}
	}

	conn := &connectionImpl{
		ConnectionImplBase:  driverbase.NewConnectionImplBase(&d.DatabaseImplBase),
		serverHostname:      d.serverHostname,
		httpPath:            d.httpPath,
		accessToken:         d.accessToken,
		port:                d.port,
		catalog:             d.catalog,
		dbSchema:            d.schema,
		queryTimeout:        d.queryTimeout,
		maxRows:             d.maxRows,
		queryRetryCount:     d.queryRetryCount,
		downloadThreadCount: d.downloadThreadCount,
		sslMode:             d.sslMode,
		sslRootCert:         d.sslRootCert,
		oauthClientID:       d.oauthClientID,
		oauthClientSecret:   d.oauthClientSecret,
		oauthRefreshToken:   d.oauthRefreshToken,
	}

	err := conn.connect(ctx)
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

func (d *databaseImpl) Close() error {
	return nil
}

func (d *databaseImpl) GetOption(key string) (string, error) {
	switch key {
	case OptionServerHostname:
		return d.serverHostname, nil
	case OptionHTTPPath:
		return d.httpPath, nil
	case OptionAccessToken:
		return d.accessToken, nil
	case OptionPort:
		return d.port, nil
	case OptionCatalog:
		return d.catalog, nil
	case OptionSchema:
		return d.schema, nil
	case OptionQueryTimeout:
		if d.queryTimeout > 0 {
			return d.queryTimeout.String(), nil
		}
		return "", nil
	case OptionMaxRows:
		if d.maxRows > 0 {
			return strconv.FormatInt(d.maxRows, 10), nil
		}
		return "", nil
	case OptionQueryRetryCount:
		if d.queryRetryCount > 0 {
			return strconv.Itoa(d.queryRetryCount), nil
		}
		return "", nil
	case OptionDownloadThreadCount:
		if d.downloadThreadCount > 0 {
			return strconv.Itoa(d.downloadThreadCount), nil
		}
		return "", nil
	case OptionSSLMode:
		return d.sslMode, nil
	case OptionSSLRootCert:
		return d.sslRootCert, nil
	case OptionOAuthClientID:
		return d.oauthClientID, nil
	case OptionOAuthClientSecret:
		return d.oauthClientSecret, nil
	case OptionOAuthRefreshToken:
		return d.oauthRefreshToken, nil
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

func (d *databaseImpl) SetOption(key, value string) error {
	switch key {
	case OptionServerHostname:
		d.serverHostname = value
	case OptionHTTPPath:
		d.httpPath = value
	case OptionAccessToken:
		d.accessToken = value
	case OptionPort:
		d.port = value
	case OptionCatalog:
		d.catalog = value
	case OptionSchema:
		d.schema = value
	case OptionQueryTimeout:
		if value != "" {
			timeout, err := time.ParseDuration(value)
			if err != nil {
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("invalid query timeout: %v", err),
				}
			}
			d.queryTimeout = timeout
		}
	case OptionMaxRows:
		if value != "" {
			maxRows, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("invalid max rows: %v", err),
				}
			}
			d.maxRows = maxRows
		}
	case OptionQueryRetryCount:
		if value != "" {
			retryCount, err := strconv.Atoi(value)
			if err != nil {
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("invalid query retry count: %v", err),
				}
			}
			d.queryRetryCount = retryCount
		}
	case OptionDownloadThreadCount:
		if value != "" {
			threadCount, err := strconv.Atoi(value)
			if err != nil {
				return adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("invalid download thread count: %v", err),
				}
			}
			d.downloadThreadCount = threadCount
		}
	case OptionSSLMode:
		d.sslMode = value
	case OptionSSLRootCert:
		d.sslRootCert = value
	case OptionOAuthClientID:
		d.oauthClientID = value
	case OptionOAuthClientSecret:
		d.oauthClientSecret = value
	case OptionOAuthRefreshToken:
		d.oauthRefreshToken = value
	default:
		return d.DatabaseImplBase.SetOption(key, value)
	}
	return nil
}
