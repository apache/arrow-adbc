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

package clickhouse

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"strings"
)

type databaseImpl struct {
	driverbase.DatabaseImplBase

	address  []string
	protocol clickhouse.Protocol
	database string
	username string
	password string
}

func (d *databaseImpl) Open(ctx context.Context) (adbc.Connection, error) {
	conn := &connectionImpl{
		ConnectionImplBase:     driverbase.NewConnectionImplBase(&d.DatabaseImplBase),
		address:                d.address,
		protocol:               d.protocol,
		database:               d.database,
		username:               d.username,
		password:               d.password,
		resultRecordBufferSize: defaultQueryResultBufferSize,
		prefetchConcurrency:    defaultQueryPrefetchConcurrency,
	}

	err := conn.newConnection(ctx)
	if err != nil {
		return nil, err
	}

	return driverbase.NewConnectionBuilder(conn).
		WithAutocommitSetter(conn).
		WithCurrentNamespacer(conn).
		WithTableTypeLister(conn).
		Connection(), nil
}

func (d *databaseImpl) Close() error { return nil }

func (d *databaseImpl) GetOption(key string) (string, error) {
	switch key {
	case OptionStringAddress:
		return strings.Join(d.address, ","), nil
	case OptionStringProtocol:
		switch d.protocol {
		case clickhouse.Native:
			return OptionValueProtocolNative, nil
		case clickhouse.HTTP:
			return OptionValueProtocolHTTP, nil
		}
	case OptionStringDataset:
		return d.database, nil
	case OptionStringUsername:
		return d.username, nil
	case OptionStringPassword:
		return d.password, nil
	}
	return d.DatabaseImplBase.GetOption(key)
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

func (d *databaseImpl) SetOption(key string, value string) error {
	switch key {
	case OptionStringAddress:
		addresses := strings.Split(value, ",")
		d.address = make([]string, len(addresses))
		for index, address := range addresses {
			d.address[index] = strings.TrimSpace(address)
		}
	case OptionStringProtocol:
		switch strings.ToLower(value) {
		case OptionValueProtocolNative:
			d.protocol = clickhouse.Native
		case OptionValueProtocolHTTP:
			d.protocol = clickhouse.HTTP
		}
	case OptionStringDataset:
		d.database = value
	case OptionStringUsername:
		d.username = value
	case OptionStringPassword:
		d.password = value
	default:
		return d.DatabaseImplBase.SetOption(key, value)
	}
	return nil
}
