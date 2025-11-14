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
	"crypto/rsa"
	"crypto/x509"
	"database/sql/driver"
	"encoding/pem"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/snowflakedb/gosnowflake"
	"github.com/youmark/pkcs8"
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
		OptionValueAuthPat:             gosnowflake.AuthTypePat,
		OptionValueAuthWIF:             gosnowflake.AuthTypeWorkloadIdentityFederation,
	}
)

type MaxTimestampPrecision uint8

const (
	// default precision
	Nanoseconds MaxTimestampPrecision = iota

	// use nanoseconds, but error if there is an overflow
	NanosecondsNoOverflow

	// use microseconds
	Microseconds
)

var (
	maxTimestampPrecisionMap = map[string]MaxTimestampPrecision{
		OptionValueNanoseconds:           Nanoseconds,
		OptionValueNanosecondsNoOverflow: NanosecondsNoOverflow,
		OptionValueMicroseconds:          Microseconds,
	}
)

type databaseImpl struct {
	driverbase.DatabaseImplBase
	cfg *gosnowflake.Config

	// Optional OAuth2 parameters
	clientId     string
	clientSecret string
	refreshToken string

	useHighPrecision      bool
	maxTimestampPrecision MaxTimestampPrecision
	defaultAppName        string
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
		if d.cfg.DisableOCSPChecks {
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
	case OptionClientConfigFile:
		return d.cfg.ClientConfigFile, nil
	case OptionClientId:
		return d.clientId, nil
	case OptionClientSecret:
		return d.clientSecret, nil
	case OptionRefreshToken:
		return d.refreshToken, nil
	case OptionUseHighPrecision:
		if d.useHighPrecision {
			return adbc.OptionValueEnabled, nil
		}
		return adbc.OptionValueDisabled, nil
	case OptionMaxTimestampPrecision:
		switch d.maxTimestampPrecision {
		case Microseconds:
			return OptionValueMicroseconds, nil
		case NanosecondsNoOverflow:
			return OptionValueNanosecondsNoOverflow, nil
		default:
			return OptionValueNanoseconds, nil
		}
	case adbc.OptionKeyTelemetryTraceParent:
		return d.GetTraceParent(), nil
	default:
		val, ok := d.cfg.Params[key]
		if ok {
			return *val, nil
		}
	}
	return d.DatabaseImplBase.GetOption(key)
}

func (d *databaseImpl) SetOption(key string, value string) error {
	return d.SetOptionInternal(key, value, nil)
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
	// XXX(https://github.com/apache/arrow-adbc/issues/2792): Snowflake
	// has a tendency to spam the log by default, so set the log level
	d.cfg.Tracing = "fatal"

	// set default application name to track
	// unless user overrides it
	d.cfg.Application = d.defaultAppName

	for k, v := range cnOptions {
		v := v // copy into loop scope
		err := d.SetOptionInternal(k, v, &cnOptions)
		if err != nil {
			return err
		}
	}
	return nil
}

// SetOptionInternal sets the option for the database.
//
// cnOptions is nil if the option is being set post-initialiation.
func (d *databaseImpl) SetOptionInternal(k string, v string, cnOptions *map[string]string) error {
	var err error
	var ok bool
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
		// Snowflake accepts both '_' and '-' in account identifiers, but only '-' works in hostnames.
		// This inconsistency is Snowflake's choice, even though it conflicts with TLS/SSL grammar:
		// https://docs.snowflake.com/en/user-guide/admin-account-identifier
		// https://datatracker.ietf.org/doc/html/rfc1035#section-2.3.1
		d.cfg.Account = strings.ReplaceAll(v, "_", "-")
	case OptionProtocol:
		d.cfg.Protocol = v
	case OptionHost:
		d.cfg.Host = v
	case OptionIdentityProvider:
		d.cfg.WorkloadIdentityProvider = v
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
			d.cfg.DisableOCSPChecks = true
		case adbc.OptionValueDisabled:
			d.cfg.DisableOCSPChecks = false
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

		var block []byte
		if strings.Contains(string(data), "PRIVATE KEY") {
			b, _ := pem.Decode(data)
			block = b.Bytes
		} else {
			block = data
		}

		var key *rsa.PrivateKey
		key, err = x509.ParsePKCS1PrivateKey(block)
		if err != nil && strings.Contains(err.Error(), "use ParsePKCS8PrivateKey instead") {
			var pkcs8Key any
			pkcs8Key, err = x509.ParsePKCS8PrivateKey(block)
			key, ok = pkcs8Key.(*rsa.PrivateKey)
			if !ok {
				err = errors.New("file does not contain an RSA private key")
			}
		}

		if err != nil {
			return adbc.Error{
				Msg:  "failed parsing private key file '" + v + "': " + err.Error(),
				Code: adbc.StatusInvalidArgument,
			}
		}

		d.cfg.PrivateKey = key
	case OptionJwtPrivateKeyPkcs8Value:
		block, _ := pem.Decode([]byte(v))

		if block == nil {
			return adbc.Error{
				Msg:  "Failed to parse PEM block containing the private key",
				Code: adbc.StatusInvalidArgument,
			}
		}

		var parsedKey any

		switch block.Type {
		case "ENCRYPTED PRIVATE KEY":
			if cnOptions == nil {
				return adbc.Error{
					Msg:  "[Snowflake] unable to set private key post initialization",
					Code: adbc.StatusInvalidArgument,
				}
			}
			passcode, ok := (*cnOptions)[OptionJwtPrivateKeyPkcs8Password]
			if ok {
				parsedKey, err = pkcs8.ParsePKCS8PrivateKey(block.Bytes, []byte(passcode))
			} else {
				return adbc.Error{
					Msg:  OptionJwtPrivateKeyPkcs8Password + " is not configured",
					Code: adbc.StatusInvalidArgument,
				}
			}
		case "PRIVATE KEY":
			parsedKey, err = pkcs8.ParsePKCS8PrivateKey(block.Bytes)
		default:
			return adbc.Error{
				Msg:  block.Type + " is not supported",
				Code: adbc.StatusInvalidArgument,
			}
		}

		if err != nil {
			return adbc.Error{
				Msg:  "[Snowflake] failed parsing PKCS8 private key: " + err.Error(),
				Code: adbc.StatusInvalidArgument,
			}
		}

		d.cfg.PrivateKey = parsedKey.(*rsa.PrivateKey)

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
	case OptionClientConfigFile:
		d.cfg.ClientConfigFile = v
	case OptionClientId:
		d.clientId = v
	case OptionClientSecret:
		d.clientSecret = v
	case OptionRefreshToken:
		d.refreshToken = v
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
	case OptionMaxTimestampPrecision:
		switch v {
		case OptionValueNanoseconds, OptionValueNanosecondsNoOverflow, OptionValueMicroseconds:
			d.maxTimestampPrecision = maxTimestampPrecisionMap[v]
		default:
			return adbc.Error{
				Msg:  fmt.Sprintf("Invalid value for database option '%s': '%s'", OptionMaxTimestampPrecision, v),
				Code: adbc.StatusInvalidArgument,
			}
		}
	case adbc.OptionKeyTelemetryTraceParent:
		d.SetTraceParent(v)
	default:
		d.cfg.Params[k] = &v
	}
	return nil
}

func (d *databaseImpl) Open(ctx context.Context) (adbcConnection adbc.Connection, err error) {
	ctx, span := internal.StartSpan(ctx, "databaseImpl.Open", d)
	defer internal.EndSpan(span, err)

	connector := gosnowflake.NewConnector(drv, *d.cfg)

	ctx = gosnowflake.WithArrowAllocator(
		gosnowflake.WithArrowBatches(ctx), d.Alloc)

	var cn driver.Conn
	cn, err = connector.Connect(ctx)
	if err != nil {
		err = errToAdbcErr(adbc.StatusIO, err)
		return nil, err
	}

	conn := &connectionImpl{
		cn: cn.(snowflakeConn),
		db: d, ctor: connector,
		// default enable high precision
		// SetOption(OptionUseHighPrecision, adbc.OptionValueDisabled) to
		// get Int64/Float64 instead
		useHighPrecision:      d.useHighPrecision,
		maxTimestampPrecision: d.maxTimestampPrecision,
		ConnectionImplBase:    driverbase.NewConnectionImplBase(&d.DatabaseImplBase),
	}

	adbcConnection = driverbase.NewConnectionBuilder(conn).
		WithAutocommitSetter(conn).
		WithCurrentNamespacer(conn).
		WithTableTypeLister(conn).
		WithDriverInfoPreparer(conn).
		Connection()

	driverbase.SetOTelDriverInfoAttributes(d.DriverInfo, span)
	return adbcConnection, err
}

func (d *databaseImpl) Close() error {
	return nil
}

var (
	_ adbc.PostInitOptions = (*databaseImpl)(nil)
)
