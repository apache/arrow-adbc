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
	"net/url"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"golang.org/x/exp/maps"
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
)

var errNoTransactionSupport = adbc.Error{
	Msg:  "[Flight SQL] server does not report transaction support",
	Code: adbc.StatusNotImplemented,
}

type driverImpl struct {
	driverbase.DriverImplBase
}

// NewDriver creates a new Flight SQL driver using the given Arrow allocator.
func NewDriver(alloc memory.Allocator) adbc.Driver {
	info := driverbase.DefaultDriverInfo("Flight SQL")
	return driverbase.NewDriver(&driverImpl{DriverImplBase: driverbase.NewDriverImplBase(info, alloc)})
}

func (d *driverImpl) NewDatabase(opts map[string]string) (adbc.Database, error) {
	opts = maps.Clone(opts)
	uri, ok := opts[adbc.OptionKeyURI]
	if !ok {
		return nil, adbc.Error{
			Msg:  "URI required for a FlightSQL DB",
			Code: adbc.StatusInvalidArgument,
		}
	}
	delete(opts, adbc.OptionKeyURI)

	db := &databaseImpl{
		DatabaseImplBase: driverbase.NewDatabaseImplBase(&d.DriverImplBase),
		timeout: timeoutOption{
			// Match gRPC default
			connectTimeout: time.Second * 20,
		},
		hdrs: make(metadata.MD),
	}

	var err error
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
