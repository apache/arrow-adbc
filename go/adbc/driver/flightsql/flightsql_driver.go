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

// Package flightsql is an ADBC Driver Implementation for Flight SQL
// natively in go.
//
// It can be used to register a driver for database/sql by importing
// github.com/apache/arrow-adbc/go/adbc/sqldriver and running:
//
//	sql.Register("flightsql", sqldriver.Driver{flightsql.Driver{}})
//
// You can then open a flightsql connection with the database/sql
// standard package by using:
//
//	db, err := sql.Open("flightsql", "uri=<flight sql db url>")
//
// The URI passed *must* contain a scheme, most likely "grpc+tcp://"
package flightsql

import (
	"context"
	"net/url"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	OptionAuthority                     = "adbc.flight.sql.client_option.authority"
	OptionMTLSCertChain                 = "adbc.flight.sql.client_option.mtls_cert_chain"
	OptionMTLSPrivateKey                = "adbc.flight.sql.client_option.mtls_private_key"
	OptionSSLOverrideHostname           = "adbc.flight.sql.client_option.tls_override_hostname"
	OptionSSLSkipVerify                 = "adbc.flight.sql.client_option.tls_skip_verify"
	OptionSSLRootCerts                  = "adbc.flight.sql.client_option.tls_root_certs"
	OptionWithBlock                     = "adbc.flight.sql.client_option.with_block"
	OptionWithMaxMsgSize                = "adbc.flight.sql.client_option.with_max_msg_size"
	OptionAuthorizationHeader           = "adbc.flight.sql.authorization_header"
	OptionTimeoutConnect                = "adbc.flight.sql.rpc.timeout_seconds.connect"
	OptionTimeoutFetch                  = "adbc.flight.sql.rpc.timeout_seconds.fetch"
	OptionTimeoutQuery                  = "adbc.flight.sql.rpc.timeout_seconds.query"
	OptionTimeoutUpdate                 = "adbc.flight.sql.rpc.timeout_seconds.update"
	OptionRPCCallHeaderPrefix           = "adbc.flight.sql.rpc.call_header."
	OptionCookieMiddleware              = "adbc.flight.sql.rpc.with_cookie_middleware"
	OptionSessionOptions                = "adbc.flight.sql.session.options"
	OptionSessionOptionPrefix           = "adbc.flight.sql.session.option."
	OptionEraseSessionOptionPrefix      = "adbc.flight.sql.session.optionerase."
	OptionBoolSessionOptionPrefix       = "adbc.flight.sql.session.optionbool."
	OptionStringListSessionOptionPrefix = "adbc.flight.sql.session.optionstringlist."
	OptionLastFlightInfo                = "adbc.flight.sql.statement.exec.last_flight_info"
	infoDriverName                      = "ADBC Flight SQL Driver - Go"

	// Oauth2 options
	OptionKeyOauthFlow        = "adbc.flight.sql.oauth.flow"
	OptionKeyAuthURI          = "adbc.flight.sql.oauth.auth_uri"
	OptionKeyTokenURI         = "adbc.flight.sql.oauth.token_uri"
	OptionKeyRedirectURI      = "adbc.flight.sql.oauth.redirect_uri"
	OptionKeyScope            = "adbc.flight.sql.oauth.scope"
	OptionKeyClientId         = "adbc.flight.sql.oauth.client_id"
	OptionKeyClientSecret     = "adbc.flight.sql.oauth.client_secret"
	OptionKeySubjectToken     = "adbc.flight.sql.oauth.exchange.subject_token"
	OptionKeySubjectTokenType = "adbc.flight.sql.oauth.exchange.subject_token_type"
	OptionKeyActorToken       = "adbc.flight.sql.oauth.exchange.actor_token"
	OptionKeyActorTokenType   = "adbc.flight.sql.oauth.exchange.actor_token_type"
	OptionKeyReqTokenType     = "adbc.flight.sql.oauth.exchange.requested_token_type"
	OptionKeyExchangeScope    = "adbc.flight.sql.oauth.exchange.scope"
	OptionKeyExchangeAud      = "adbc.flight.sql.oauth.exchange.aud"
	OptionKeyExchangeResource = "adbc.flight.sql.oauth.exchange.resource"
)

var errNoTransactionSupport = adbc.Error{
	Msg:  "[Flight SQL] server does not report transaction support",
	Code: adbc.StatusNotImplemented,
}

type driverImpl struct {
	driverbase.DriverImplBase
}

// Driver is the extended [adbc.Driver] interface for Flight SQL.
//
// It adds an additional method to create a database with grpc specific options that cannot be
// passed through the options map.
type Driver interface {
	adbc.Driver
	NewDatabaseWithOptions(map[string]string, ...grpc.DialOption) (adbc.Database, error)
	NewDatabaseWithOptionsContext(context.Context, map[string]string, ...grpc.DialOption) (adbc.Database, error)
}

// NewDriver creates a new Flight SQL driver using the given Arrow allocator.
func NewDriver(alloc memory.Allocator) Driver {
	info := driverbase.DefaultDriverInfo("Flight SQL")
	return &driverImpl{DriverImplBase: driverbase.NewDriverImplBase(info, alloc)}
}

// NewDatabase creates a new Flight SQL database using the given options.
//
// Additional grpc client options can can be passed as grpc.DialOption.
// This enables the use of additional grpc client options not directly exposed by the options map.
// such as grpc.WithStatsHandler() for enabling various telemetry handlers.
func (d *driverImpl) NewDatabaseWithOptions(opts map[string]string, userDialOpts ...grpc.DialOption) (adbc.Database, error) {
	return d.NewDatabaseWithOptionsContext(context.Background(), opts, userDialOpts...)
}

func (d *driverImpl) NewDatabaseWithOptionsContext(ctx context.Context, opts map[string]string, userDialOpts ...grpc.DialOption) (adbc.Database, error) {
	opts = maps.Clone(opts)
	uri, ok := opts[adbc.OptionKeyURI]
	if !ok {
		return nil, adbc.Error{
			Msg:  "URI required for a FlightSQL DB",
			Code: adbc.StatusInvalidArgument,
		}
	}
	delete(opts, adbc.OptionKeyURI)

	dbBase, err := driverbase.NewDatabaseImplBase(ctx, &d.DriverImplBase)
	if err != nil {
		return nil, err
	}
	db := &databaseImpl{
		DatabaseImplBase: dbBase,
		timeout: timeoutOption{
			// Match gRPC default
			connectTimeout: time.Second * 20,
		},
		hdrs:         make(metadata.MD),
		userDialOpts: userDialOpts,
	}

	if db.uri, err = url.Parse(uri); err != nil {
		return nil, adbc.Error{Msg: err.Error(), Code: adbc.StatusInvalidArgument}
	}

	// Use WithMaxMsgSize(16 MiB) since Flight services tend to send large messages
	db.dialOpts.maxMsgSize = 16 * 1024 * 1024

	db.options = make(map[string]string)

	if err := db.SetOptions(opts); err != nil {
		return nil, err
	}

	return driverbase.NewDatabase(db), nil
}

// NewDatabase creates a new Flight SQL database using the given options.
func (d *driverImpl) NewDatabase(opts map[string]string) (adbc.Database, error) {
	return d.NewDatabaseWithContext(context.Background(), opts)
}

func (d *driverImpl) NewDatabaseWithContext(ctx context.Context, opts map[string]string) (adbc.Database, error) {
	return d.NewDatabaseWithOptionsContext(ctx, opts)
}
