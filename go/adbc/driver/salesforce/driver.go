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
	"runtime/debug"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const (
	// Authentication options
	OptionStringAuthType                = "adbc.salesforce.dc.auth_type"
	OptionValueAuthTypeJwtBearer        = "adbc.salesforce.dc.auth_type.jwt_bearer"
	OptionValueAuthTypeUsernamePassword = "adbc.salesforce.dc.auth_type.username_password"

	OptionStringLoginURL     = "adbc.salesforce.dc.login_url"
	OptionStringUsername     = "adbc.salesforce.dc.username"
	OptionStringClientID     = "adbc.salesforce.dc.client_id"
	OptionStringClientSecret = "adbc.salesforce.dc.client_secret"

	// JWT Bearer Authentication options
	OptionStringJWTPrivateKey = "adbc.salesforce.dc.private_key"

	// Username password Authentication options
	OptionStringPassword = "adbc.salesforce.dc.password"

	// Connection options
	OptionStringInstanceURL = "adbc.salesforce.dc.instance_url"
	OptionStringVersion     = "adbc.salesforce.dc.version"

	// Query options
	OptionStringQueryRowLimit = "adbc.salesforce.dc.query.row_limit"
	OptionStringQueryTimeout  = "adbc.salesforce.dc.query.timeout"

	// Default values
	DefaultLoginURL = "https://login.salesforce.com"
	DefaultVersion  = "v64.0"
)

// Driver implements the ADBC Driver interface for Salesforce Data Cloud
type Driver struct {
	allocator memory.Allocator
}

var (
	infoVendorVersion string
)

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			switch {
			case dep.Path == "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce":
				infoVendorVersion = dep.Version
			}
		}
	}
}

type driverImpl struct {
	driverbase.DriverImplBase
}

// NewDriver creates a new Salesforce driver using the given Arrow allocator.
func NewDriver(alloc memory.Allocator) adbc.Driver {
	info := driverbase.DefaultDriverInfo("Salesforce")
	if infoVendorVersion != "" {
		if err := info.RegisterInfoCode(adbc.InfoVendorVersion, infoVendorVersion); err != nil {
			panic(err)
		}
	}
	return driverbase.NewDriver(&driverImpl{
		DriverImplBase: driverbase.NewDriverImplBase(info, alloc),
	})
}

// NewDatabase creates a new database connection for Salesforce Data Cloud
func (d *driverImpl) NewDatabase(opts map[string]string) (adbc.Database, error) {
	db := &databaseImpl{
		DatabaseImplBase: driverbase.NewDatabaseImplBase(&d.DriverImplBase),
		// Defaults to the JWT Bearer Flow
		authType: OptionValueAuthTypeJwtBearer,
		loginURL: DefaultLoginURL,
		version:  DefaultVersion,
	}
	if err := db.SetOptions(opts); err != nil {
		return nil, err
	}

	return driverbase.NewDatabase(db), nil
}
