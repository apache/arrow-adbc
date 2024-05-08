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

package snowflake

import (
	"errors"
	"maps"
	"runtime/debug"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/snowflakedb/gosnowflake"
)

const (
	OptionDatabase  = "adbc.snowflake.sql.db"
	OptionSchema    = "adbc.snowflake.sql.schema"
	OptionWarehouse = "adbc.snowflake.sql.warehouse"
	OptionRole      = "adbc.snowflake.sql.role"
	OptionRegion    = "adbc.snowflake.sql.region"
	OptionAccount   = "adbc.snowflake.sql.account"
	OptionProtocol  = "adbc.snowflake.sql.uri.protocol"
	OptionPort      = "adbc.snowflake.sql.uri.port"
	OptionHost      = "adbc.snowflake.sql.uri.host"
	// Specify auth type to use for snowflake connection based on
	// what is supported by the snowflake driver. Default is
	// "auth_snowflake" (use OptionValueAuth* consts to specify desired
	// authentication type).
	OptionAuthType = "adbc.snowflake.sql.auth_type"
	// Login retry timeout EXCLUDING network roundtrip and reading http response
	// use format like http://pkg.go.dev/time#ParseDuration such as
	// "300ms", "1.5s" or "1m30s". ParseDuration accepts negative values
	// but the absolute value will be used.
	OptionLoginTimeout = "adbc.snowflake.sql.client_option.login_timeout"
	// request retry timeout EXCLUDING network roundtrip and reading http response
	// use format like http://pkg.go.dev/time#ParseDuration such as
	// "300ms", "1.5s" or "1m30s". ParseDuration accepts negative values
	// but the absolute value will be used.
	OptionRequestTimeout = "adbc.snowflake.sql.client_option.request_timeout"
	// JWT expiration after timeout
	// use format like http://pkg.go.dev/time#ParseDuration such as
	// "300ms", "1.5s" or "1m30s". ParseDuration accepts negative values
	// but the absolute value will be used.
	OptionJwtExpireTimeout = "adbc.snowflake.sql.client_option.jwt_expire_timeout"
	// Timeout for network round trip + reading http response
	// use format like http://pkg.go.dev/time#ParseDuration such as
	// "300ms", "1.5s" or "1m30s". ParseDuration accepts negative values
	// but the absolute value will be used.
	OptionClientTimeout = "adbc.snowflake.sql.client_option.client_timeout"
	// OptionUseHighPrecision controls the data type used for NUMBER columns
	// using a FIXED size data type. By default, this is enabled and NUMBER
	// columns will be returned as Decimal128 types using the indicated
	// precision and scale of the type. If disabled, then fixed-point data
	// with a scale of 0 will be returned as Int64 columns, and a non-zero
	// scale will return a Float64 column.
	OptionUseHighPrecision = "adbc.snowflake.sql.client_option.use_high_precision"

	OptionApplicationName  = "adbc.snowflake.sql.client_option.app_name"
	OptionSSLSkipVerify    = "adbc.snowflake.sql.client_option.tls_skip_verify"
	OptionOCSPFailOpenMode = "adbc.snowflake.sql.client_option.ocsp_fail_open_mode"
	// specify the token to use for OAuth or other forms of authentication
	OptionAuthToken = "adbc.snowflake.sql.client_option.auth_token"
	// specify the OKTAUrl to use for OKTA Authentication
	OptionAuthOktaUrl = "adbc.snowflake.sql.client_option.okta_url"
	// enable the session to persist even after the connection is closed
	OptionKeepSessionAlive = "adbc.snowflake.sql.client_option.keep_session_alive"
	// specify the RSA private key to use to sign the JWT
	// this should point to a file containing a PKCS1 private key to be
	// loaded. Commonly encoded in PEM blocks of type "RSA PRIVATE KEY"
	OptionJwtPrivateKey = "adbc.snowflake.sql.client_option.jwt_private_key"
	// parses a private key in PKCS #8, ASN.1 DER form. Specify the private key
	// value without having to load it from the file system.
	OptionJwtPrivateKeyPkcs8Value = "adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_value"
	// a passcode to use with encrypted private keys for JWT authentication
	OptionJwtPrivateKeyPkcs8Password = "adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_password"
	OptionDisableTelemetry           = "adbc.snowflake.sql.client_option.disable_telemetry"
	// snowflake driver logging level
	OptionLogTracing = "adbc.snowflake.sql.client_option.tracing"
	// When true, the MFA token is cached in the credential manager. True by default
	// on Windows/OSX, false for Linux
	OptionClientRequestMFAToken = "adbc.snowflake.sql.client_option.cache_mfa_token"
	// When true, the ID token is cached in the credential manager. True by default
	// on Windows/OSX, false for Linux
	OptionClientStoreTempCred = "adbc.snowflake.sql.client_option.store_temp_creds"

	// auth types are implemented by the Snowflake driver in gosnowflake
	// general username password authentication
	OptionValueAuthSnowflake = "auth_snowflake"
	// use OAuth authentication for snowflake connection
	OptionValueAuthOAuth = "auth_oauth"
	// use an external browser to access a FED and perform SSO auth
	OptionValueAuthExternalBrowser = "auth_ext_browser"
	// use a native OKTA URL to perform SSO authentication on Okta
	OptionValueAuthOkta = "auth_okta"
	// use a JWT to perform authentication
	OptionValueAuthJwt = "auth_jwt"
	// use a username and password with mfa
	OptionValueAuthUserPassMFA = "auth_mfa"
)

var (
	infoVendorVersion string
)

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			switch {
			case dep.Path == "github.com/snowflakedb/gosnowflake":
				infoVendorVersion = dep.Version
			}
		}
	}
}

func errToAdbcErr(code adbc.Status, err error) error {
	if err == nil {
		return nil
	}

	var e adbc.Error
	if errors.As(err, &e) {
		e.Code = code
		return e
	}

	var sferr *gosnowflake.SnowflakeError
	if errors.As(err, &sferr) {
		var sqlstate [5]byte
		copy(sqlstate[:], []byte(sferr.SQLState))

		if sferr.SQLState == "42S02" {
			code = adbc.StatusNotFound
		}

		return adbc.Error{
			Code:       code,
			Msg:        sferr.Error(),
			VendorCode: int32(sferr.Number),
			SqlState:   sqlstate,
		}
	}

	return adbc.Error{
		Msg:  err.Error(),
		Code: code,
	}
}

func quoteTblName(name string) string {
	return "\"" + strings.ReplaceAll(name, "\"", "\"\"") + "\""
}

type driverImpl struct {
	driverbase.DriverImplBase
}

// NewDriver creates a new Snowflake driver using the given Arrow allocator.
func NewDriver(alloc memory.Allocator) adbc.Driver {
	info := driverbase.DefaultDriverInfo("Snowflake")
	if infoVendorVersion != "" {
		if err := info.RegisterInfoCode(adbc.InfoVendorVersion, infoVendorVersion); err != nil {
			panic(err)
		}
	}
	return driverbase.NewDriver(&driverImpl{DriverImplBase: driverbase.NewDriverImplBase(info, alloc)})
}

func (d *driverImpl) NewDatabase(opts map[string]string) (adbc.Database, error) {
	opts = maps.Clone(opts)
	db := &databaseImpl{
		DatabaseImplBase: driverbase.NewDatabaseImplBase(&d.DriverImplBase),
		useHighPrecision: true,
	}
	if err := db.SetOptions(opts); err != nil {
		return nil, err
	}

	return driverbase.NewDatabase(db), nil
}
