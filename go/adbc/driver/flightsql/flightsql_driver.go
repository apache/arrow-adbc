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
	"runtime/debug"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/driverbase"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc/metadata"
)

const (
	OptionAuthority           = "adbc.flight.sql.client_option.authority"
	OptionMTLSCertChain       = "adbc.flight.sql.client_option.mtls_cert_chain"
	OptionMTLSPrivateKey      = "adbc.flight.sql.client_option.mtls_private_key"
	OptionSSLOverrideHostname = "adbc.flight.sql.client_option.tls_override_hostname"
	OptionSSLSkipVerify       = "adbc.flight.sql.client_option.tls_skip_verify"
	OptionSSLRootCerts        = "adbc.flight.sql.client_option.tls_root_certs"
	OptionWithBlock           = "adbc.flight.sql.client_option.with_block"
	OptionWithMaxMsgSize      = "adbc.flight.sql.client_option.with_max_msg_size"
	OptionAuthorizationHeader = "adbc.flight.sql.authorization_header"
	OptionTimeoutFetch        = "adbc.flight.sql.rpc.timeout_seconds.fetch"
	OptionTimeoutQuery        = "adbc.flight.sql.rpc.timeout_seconds.query"
	OptionTimeoutUpdate       = "adbc.flight.sql.rpc.timeout_seconds.update"
	OptionRPCCallHeaderPrefix = "adbc.flight.sql.rpc.call_header."
	OptionCookieMiddleware    = "adbc.flight.sql.rpc.with_cookie_middleware"
	infoDriverName            = "ADBC Flight SQL Driver - Go"
)

var (
	infoDriverVersion      string
	infoDriverArrowVersion string
	infoSupportedCodes     []adbc.InfoCode
)

var errNoTransactionSupport = adbc.Error{
	Msg:  "[Flight SQL] server does not report transaction support",
	Code: adbc.StatusNotImplemented,
}

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			switch {
			case dep.Path == "github.com/apache/arrow-adbc/go/adbc/driver/flightsql":
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
		adbc.InfoDriverADBCVersion,
		adbc.InfoVendorName,
		adbc.InfoVendorVersion,
		adbc.InfoVendorArrowVersion,
	}
}

type driverImpl struct {
	driverbase.DriverImplBase
}

// NewDriver creates a new Flight SQL driver using the given Arrow allocator.
func NewDriver(alloc memory.Allocator) adbc.Driver {
	impl := driverImpl{DriverImplBase: driverbase.NewDriverImplBase("Flight SQL", alloc)}
	return driverbase.NewDriver(&impl)
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
		hdrs:             make(metadata.MD),
	}

	var err error
	if db.uri, err = url.Parse(uri); err != nil {
		return nil, adbc.Error{Msg: err.Error(), Code: adbc.StatusInvalidArgument}
	}

	// Do not set WithBlock since it converts some types of connection
	// errors to infinite hangs
	// Use WithMaxMsgSize(16 MiB) since Flight services tend to send large messages
	db.dialOpts.block = false
	db.dialOpts.maxMsgSize = 16 * 1024 * 1024

	db.options = make(map[string]string)

	if err := db.SetOptions(opts); err != nil {
		return nil, err
	}

	return driverbase.NewDatabase(db), nil
}
