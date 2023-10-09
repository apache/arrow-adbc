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
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/driverbase"
	"github.com/snowflakedb/gosnowflake"
)

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

type databaseImpl struct {
	driverbase.DatabaseImplBase
	cfg *gosnowflake.Config

	useHighPrecision bool
}

func (d *databaseImpl) GetOption(key string) (string, error) {
	switch key {
	case adbc.OptionKeyUsername:
		return d.cfg.User, nil
	case adbc.OptionKeyPassword:
		return d.cfg.Password, nil
	case OptionDatabase:
		return d.cfg.Database, nil
	case OptionSchema:
		return d.cfg.Schema, nil
	case OptionWarehouse:
		return d.cfg.Warehouse, nil
	case OptionRole:
		return d.cfg.Role, nil
	case OptionRegion:
		return d.cfg.Region, nil
	case OptionAccount:
		return d.cfg.Account, nil
	case OptionProtocol:
		return d.cfg.Protocol, nil
	case OptionHost:
		return d.cfg.Host, nil
	case OptionPort:
		return strconv.Itoa(d.cfg.Port), nil
	case OptionAuthType:
		return d.cfg.Authenticator.String(), nil
	case OptionLoginTimeout:
		return strconv.FormatFloat(d.cfg.LoginTimeout.Seconds(), 'f', -1, 64), nil
	case OptionRequestTimeout:
		return strconv.FormatFloat(d.cfg.RequestTimeout.Seconds(), 'f', -1, 64), nil
	case OptionJwtExpireTimeout:
		return strconv.FormatFloat(d.cfg.JWTExpireTimeout.Seconds(), 'f', -1, 64), nil
	case OptionClientTimeout:
		return strconv.FormatFloat(d.cfg.ClientTimeout.Seconds(), 'f', -1, 64), nil
	case OptionApplicationName:
		return d.cfg.Application, nil
	case OptionSSLSkipVerify:
		if d.cfg.InsecureMode {
			return adbc.OptionValueEnabled, nil
		}
		return adbc.OptionValueDisabled, nil
	case OptionOCSPFailOpenMode:
		return strconv.FormatUint(uint64(d.cfg.OCSPFailOpen), 10), nil
	case OptionAuthToken:
		return d.cfg.Token, nil
	case OptionAuthOktaUrl:
		return d.cfg.OktaURL.String(), nil
	case OptionKeepSessionAlive:
		if d.cfg.KeepSessionAlive {
			return adbc.OptionValueEnabled, nil
		}
		return adbc.OptionValueDisabled, nil
	case OptionDisableTelemetry:
		if d.cfg.DisableTelemetry {
			return adbc.OptionValueEnabled, nil
		}
		return adbc.OptionValueDisabled, nil
	case OptionClientRequestMFAToken:
		if d.cfg.ClientRequestMfaToken == gosnowflake.ConfigBoolTrue {
			return adbc.OptionValueEnabled, nil
		}
		return adbc.OptionValueDisabled, nil
	case OptionClientStoreTempCred:
		if d.cfg.ClientStoreTemporaryCredential == gosnowflake.ConfigBoolTrue {
			return adbc.OptionValueEnabled, nil
		}
		return adbc.OptionValueDisabled, nil
	case OptionLogTracing:
		return d.cfg.Tracing, nil
	case OptionUseHighPrecision:
		if d.useHighPrecision {
			return adbc.OptionValueEnabled, nil
		}
		return adbc.OptionValueDisabled, nil
	default:
		val, ok := d.cfg.Params[key]
		if ok {
			return *val, nil
		}
	}
	return "", adbc.Error{
		Msg:  fmt.Sprintf("[Snowflake] Unknown database option '%s'", key),
		Code: adbc.StatusNotFound,
	}
}
func (d *databaseImpl) GetOptionBytes(key string) ([]byte, error) {
	return nil, adbc.Error{
		Msg:  fmt.Sprintf("[Snowflake] Unknown database option '%s'", key),
		Code: adbc.StatusNotFound,
	}
}
func (d *databaseImpl) GetOptionInt(key string) (int64, error) {
	return 0, adbc.Error{
		Msg:  fmt.Sprintf("[Snowflake] Unknown database option '%s'", key),
		Code: adbc.StatusNotFound,
	}
}
func (d *databaseImpl) GetOptionDouble(key string) (float64, error) {
	return 0, adbc.Error{
		Msg:  fmt.Sprintf("[Snowflake] Unknown database option '%s'", key),
		Code: adbc.StatusNotFound,
	}
}

func (d *databaseImpl) SetOptions(cnOptions map[string]string) error {
	uri, ok := cnOptions[adbc.OptionKeyURI]
	if ok {
		cfg, err := gosnowflake.ParseDSN(uri)
		if err != nil {
			return errToAdbcErr(adbc.StatusInvalidArgument, err)
		}

		d.cfg = cfg
		delete(cnOptions, adbc.OptionKeyURI)
	} else {
		d.cfg = &gosnowflake.Config{
			Params: make(map[string]*string),
		}
	}

	var err error
	for k, v := range cnOptions {
		v := v // copy into loop scope
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
		case OptionUseHighPrecision:
			switch v {
			case adbc.OptionValueEnabled:
				d.useHighPrecision = true
			case adbc.OptionValueDisabled:
				d.useHighPrecision = false
			default:
				return adbc.Error{
					Msg:  fmt.Sprintf("Invalid value for database option '%s': '%s'", OptionUseHighPrecision, v),
					Code: adbc.StatusInvalidArgument,
				}
			}
		default:
			d.cfg.Params[k] = &v
		}
	}
	return nil
}

func (d *databaseImpl) Open(ctx context.Context) (adbc.Connection, error) {
	connector := gosnowflake.NewConnector(drv, *d.cfg)

	ctx = gosnowflake.WithArrowAllocator(
		gosnowflake.WithArrowBatches(ctx), d.Alloc)

	cn, err := connector.Connect(ctx)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err)
	}

	return &cnxn{
		cn: cn.(snowflakeConn),
		db: d, ctor: connector,
		sqldb: sql.OpenDB(connector),
		// default enable high precision
		// SetOption(OptionUseHighPrecision, adbc.OptionValueDisabled) to
		// get Int64/Float64 instead
		useHighPrecision: d.useHighPrecision,
	}, nil
}
