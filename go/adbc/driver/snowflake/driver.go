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
	"context"
	"crypto/x509"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/snowflakedb/gosnowflake"
	"golang.org/x/exp/maps"
)

const (
	infoDriverName = "ADBC Snowflake Driver - Go"
	infoVendorName = "Snowflake"

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
	OptionJwtPrivateKey    = "adbc.snowflake.sql.client_option.jwt_private_key"
	OptionDisableTelemetry = "adbc.snowflake.sql.client_option.disable_telemetry"
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
	infoDriverVersion      string
	infoDriverArrowVersion string
	infoSupportedCodes     []adbc.InfoCode
)

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			switch {
			case dep.Path == "github.com/apache/arrow-adbc/go/adbc/driver/snowflake":
				infoDriverVersion = dep.Version
			case strings.HasPrefix(dep.Path, "github.com/apache/arrow/go/"):
				infoDriverArrowVersion = dep.Version
			}
		}
	}
	// XXX: Deps not populated in tests
	// https://github.com/golang/go/issues/33976
	if infoDriverVersion == "" {
		infoDriverVersion = "(unknown or development build)"
	}
	if infoDriverArrowVersion == "" {
		infoDriverArrowVersion = "(unknown or development build)"
	}

	infoSupportedCodes = []adbc.InfoCode{
		adbc.InfoDriverName,
		adbc.InfoDriverVersion,
		adbc.InfoDriverArrowVersion,
		adbc.InfoVendorName,
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
		if len(sferr.SQLState) > 0 {
			copy(sqlstate[:], sferr.SQLState[:5])
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

type Driver struct {
	Alloc memory.Allocator
}

func (d Driver) NewDatabase(opts map[string]string) (adbc.Database, error) {
	db := &database{alloc: d.Alloc}

	opts = maps.Clone(opts)
	if db.alloc == nil {
		db.alloc = memory.DefaultAllocator
	}

	return db, db.SetOptions(opts)
}

var (
	drv         = gosnowflake.SnowflakeDriver{}
	authTypeMap = map[string]gosnowflake.AuthType{
		OptionValueAuthSnowflake:       gosnowflake.AuthTypeSnowflake,
		OptionValueAuthOAuth:           gosnowflake.AuthTypeOAuth,
		OptionValueAuthExternalBrowser: gosnowflake.AuthTypeExternalBrowser,
		OptionValueAuthOkta:            gosnowflake.AuthTypeOkta,
		OptionValueAuthJwt:             gosnowflake.AuthTypeJwt,
		OptionValueAuthUserPassMFA:     gosnowflake.AuthTypeUsernamePasswordMFA,
	}
)

type database struct {
	cfg   *gosnowflake.Config
	alloc memory.Allocator
}

func (d *database) SetOptions(cnOptions map[string]string) error {
	uri, ok := cnOptions[adbc.OptionKeyURI]
	if ok {
		cfg, err := gosnowflake.ParseDSN(uri)
		if err != nil {
			return errToAdbcErr(adbc.StatusInvalidArgument, err)
		}

		d.cfg = cfg
		delete(cnOptions, adbc.OptionKeyURI)
	} else {
		d.cfg = &gosnowflake.Config{}
	}

	var err error
	for k, v := range cnOptions {
		switch k {
		case adbc.OptionKeyUsername:
			d.cfg.User = v
		case adbc.OptionKeyPassword:
			d.cfg.Password = v
		case OptionDatabase:
			d.cfg.Database = v
		case OptionSchema:
			d.cfg.Schema = v
		case OptionWarehouse:
			d.cfg.Warehouse = v
		case OptionRole:
			d.cfg.Role = v
		case OptionRegion:
			d.cfg.Region = v
		case OptionAccount:
			d.cfg.Account = v
		case OptionProtocol:
			d.cfg.Protocol = v
		case OptionHost:
			d.cfg.Host = v
		case OptionPort:
			d.cfg.Port, err = strconv.Atoi(v)
			if err != nil {
				return adbc.Error{
					Msg:  "error encountered parsing Port option: " + err.Error(),
					Code: adbc.StatusInvalidArgument,
				}
			}
		case OptionAuthType:
			d.cfg.Authenticator, ok = authTypeMap[v]
			if !ok {
				return adbc.Error{
					Msg:  "invalid option value for " + OptionAuthType + ": '" + v + "'",
					Code: adbc.StatusInvalidArgument,
				}
			}
		case OptionLoginTimeout:
			dur, err := time.ParseDuration(v)
			if err != nil {
				return adbc.Error{
					Msg:  "could not parse duration for '" + OptionLoginTimeout + "': " + err.Error(),
					Code: adbc.StatusInvalidArgument,
				}
			}
			if dur < 0 {
				dur = -dur
			}
			d.cfg.LoginTimeout = dur
		case OptionRequestTimeout:
			dur, err := time.ParseDuration(v)
			if err != nil {
				return adbc.Error{
					Msg:  "could not parse duration for '" + OptionRequestTimeout + "': " + err.Error(),
					Code: adbc.StatusInvalidArgument,
				}
			}
			if dur < 0 {
				dur = -dur
			}
			d.cfg.RequestTimeout = dur
		case OptionJwtExpireTimeout:
			dur, err := time.ParseDuration(v)
			if err != nil {
				return adbc.Error{
					Msg:  "could not parse duration for '" + OptionJwtExpireTimeout + "': " + err.Error(),
					Code: adbc.StatusInvalidArgument,
				}
			}
			if dur < 0 {
				dur = -dur
			}
			d.cfg.JWTExpireTimeout = dur
		case OptionClientTimeout:
			dur, err := time.ParseDuration(v)
			if err != nil {
				return adbc.Error{
					Msg:  "could not parse duration for '" + OptionClientTimeout + "': " + err.Error(),
					Code: adbc.StatusInvalidArgument,
				}
			}
			if dur < 0 {
				dur = -dur
			}
			d.cfg.ClientTimeout = dur
		case OptionApplicationName:
			d.cfg.Application = v
		case OptionSSLSkipVerify:
			switch v {
			case adbc.OptionValueEnabled:
				d.cfg.InsecureMode = true
			case adbc.OptionValueDisabled:
				d.cfg.InsecureMode = false
			default:
				return adbc.Error{
					Msg:  fmt.Sprintf("Invalid value for database option '%s': '%s'", OptionSSLSkipVerify, v),
					Code: adbc.StatusInvalidArgument,
				}
			}
		case OptionOCSPFailOpenMode:
			switch v {
			case adbc.OptionValueEnabled:
				d.cfg.OCSPFailOpen = gosnowflake.OCSPFailOpenTrue
			case adbc.OptionValueDisabled:
				d.cfg.OCSPFailOpen = gosnowflake.OCSPFailOpenFalse
			default:
				return adbc.Error{
					Msg:  fmt.Sprintf("Invalid value for database option '%s': '%s'", OptionSSLSkipVerify, v),
					Code: adbc.StatusInvalidArgument,
				}
			}
		case OptionAuthToken:
			d.cfg.Token = v
		case OptionAuthOktaUrl:
			d.cfg.OktaURL, err = url.Parse(v)
			if err != nil {
				return adbc.Error{
					Msg:  fmt.Sprintf("error parsing URL for database option '%s': '%s'", k, v),
					Code: adbc.StatusInvalidArgument,
				}
			}
		case OptionKeepSessionAlive:
			switch v {
			case adbc.OptionValueEnabled:
				d.cfg.KeepSessionAlive = true
			case adbc.OptionValueDisabled:
				d.cfg.KeepSessionAlive = false
			default:
				return adbc.Error{
					Msg:  fmt.Sprintf("Invalid value for database option '%s': '%s'", OptionSSLSkipVerify, v),
					Code: adbc.StatusInvalidArgument,
				}
			}
		case OptionDisableTelemetry:
			switch v {
			case adbc.OptionValueEnabled:
				d.cfg.DisableTelemetry = true
			case adbc.OptionValueDisabled:
				d.cfg.DisableTelemetry = false
			default:
				return adbc.Error{
					Msg:  fmt.Sprintf("Invalid value for database option '%s': '%s'", OptionSSLSkipVerify, v),
					Code: adbc.StatusInvalidArgument,
				}
			}
		case OptionJwtPrivateKey:
			data, err := os.ReadFile(v)
			if err != nil {
				return adbc.Error{
					Msg:  "could not read private key file '" + v + "': " + err.Error(),
					Code: adbc.StatusInvalidArgument,
				}
			}

			d.cfg.PrivateKey, err = x509.ParsePKCS1PrivateKey(data)
			if err != nil {
				return adbc.Error{
					Msg:  "failed parsing private key file '" + v + "': " + err.Error(),
					Code: adbc.StatusInvalidArgument,
				}
			}
		case OptionClientRequestMFAToken:
			switch v {
			case adbc.OptionValueEnabled:
				d.cfg.ClientRequestMfaToken = gosnowflake.ConfigBoolTrue
			case adbc.OptionValueDisabled:
				d.cfg.ClientRequestMfaToken = gosnowflake.ConfigBoolFalse
			default:
				return adbc.Error{
					Msg:  fmt.Sprintf("Invalid value for database option '%s': '%s'", OptionSSLSkipVerify, v),
					Code: adbc.StatusInvalidArgument,
				}
			}
		case OptionClientStoreTempCred:
			switch v {
			case adbc.OptionValueEnabled:
				d.cfg.ClientStoreTemporaryCredential = gosnowflake.ConfigBoolTrue
			case adbc.OptionValueDisabled:
				d.cfg.ClientStoreTemporaryCredential = gosnowflake.ConfigBoolFalse
			default:
				return adbc.Error{
					Msg:  fmt.Sprintf("Invalid value for database option '%s': '%s'", OptionSSLSkipVerify, v),
					Code: adbc.StatusInvalidArgument,
				}
			}
		case OptionLogTracing:
			d.cfg.Tracing = v
		default:
			d.cfg.Params[k] = &v
		}
	}
	return nil
}

func (d *database) Open(ctx context.Context) (adbc.Connection, error) {
	connector := gosnowflake.NewConnector(drv, *d.cfg)

	ctx = gosnowflake.WithArrowAllocator(
		gosnowflake.WithArrowBatches(ctx), d.alloc)

	cn, err := connector.Connect(ctx)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err)
	}

	return &cnxn{cn: cn.(snowflakeConn), db: d, ctor: connector, sqldb: sql.OpenDB(connector)}, nil
}
