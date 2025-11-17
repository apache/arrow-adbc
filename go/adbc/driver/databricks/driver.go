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

// Package databricks is an ADBC Driver Implementation for Databricks
// SQL using databricks-sql-go as the underlying SQL driver.
//
// It can be used to register a driver for database/sql by importing
// github.com/apache/arrow-adbc/go/adbc/sqldriver and running:
//
//	sql.Register("databricks", sqldriver.Driver{databricks.Driver{}})
//
// You can then open a databricks connection with the database/sql
// standard package by using:
//
//	db, err := sql.Open("databricks", "token=<token>&hostname=<hostname>&port=<port>&httpPath=<path>")
package databricks

import (
	"context"
	"runtime/debug"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const (
	// Connection options
	OptionServerHostname = "databricks.server_hostname"
	OptionHTTPPath       = "databricks.http_path"
	OptionPort           = "databricks.port"
	OptionCatalog        = "databricks.catalog"
	OptionSchema         = "databricks.schema"

	// Query options
	OptionQueryTimeout        = "databricks.query.timeout"
	OptionMaxRows             = "databricks.query.max_rows"
	OptionQueryRetryCount     = "databricks.query.retry_count"
	OptionDownloadThreadCount = "databricks.download_thread_count"

	// TLS/SSL options
	OptionSSLMode     = "databricks.ssl_mode"
	OptionSSLRootCert = "databricks.ssl_root_cert"

	// Auth: Type
	OptionAuthType                     = "databricks.auth_type"
	OptionValueAuthTypeOAuthM2M        = "oauth-m2m"
	OptionValueAuthTypeExternalBrowser = "external-browser"
	OptionValueAuthTypePAT             = "pat"

	// Auth: OAuth
	OptionOAuthClientID          = "databricks.oauth.client_id"
	OptionOAuthClientSecret      = "databricks.oauth.client_secret"
	OptionOAuthRefreshToken      = "databricks.oauth.refresh_token"
	OptionExternalBrowserTimeout = "databricks.oauth.external_browser.timeout"

	// Auth: PAT
	OptionAccessToken = "databricks.access_token"

	// Default values
	DefaultPort                   = 443
	DefaultSSLMode                = "require"
	DefaultExternalBrowserTimeout = 1 * time.Minute
)

var (
	infoVendorVersion string
)

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			switch dep.Path {
			case "github.com/databricks/databricks-sql-go":
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
	return d.NewDatabaseWithContext(context.Background(), opts)
}

func (d *driverImpl) NewDatabaseWithContext(ctx context.Context, opts map[string]string) (adbc.Database, error) {
	dbBase, err := driverbase.NewDatabaseImplBase(ctx, &d.DriverImplBase)
	if err != nil {
		return nil, err
	}

	db := &databaseImpl{
		DatabaseImplBase: dbBase,
		port:             DefaultPort,
		sslMode:          DefaultSSLMode,
	}

	if err := db.SetOptions(opts); err != nil {
		return nil, err
	}

	return driverbase.NewDatabase(db), nil
}
