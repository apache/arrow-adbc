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
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"runtime/debug"
)

const (
	// OptionStringAddress comma-separated addresses
	OptionStringAddress  = "adbc.clickhouse.address"
	OptionStringProtocol = "adbc.clickhouse.protocol"
	OptionStringDataset  = "adbc.clickhouse.sql.database"
	OptionStringUsername = "adbc.clickhouse.sql.username"
	OptionStringPassword = "adbc.clickhouse.sql.password"

	OptionValueProtocolHTTP   = "http"
	OptionValueProtocolNative = "native"

	OptionIntQueryResultBufferSize    = "adbc.clickhouse.sql.query.result_buffer_size"
	OptionIntQueryPrefetchConcurrency = "adbc.clickhouse.sql.query.prefetch_concurrency"

	defaultQueryResultBufferSize    = 200
	defaultQueryPrefetchConcurrency = 10
)

var (
	infoVendorVersion string
)

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			switch {
			case dep.Path == "github.com/ClickHouse/clickhouse-go/v2":
				infoVendorVersion = dep.Version
			}
		}
	}
}

type driverImpl struct {
	driverbase.DriverImplBase
}

// NewDriver creates a new BigQuery driver using the given Arrow allocator.
func NewDriver(alloc memory.Allocator) adbc.Driver {
	info := driverbase.DefaultDriverInfo("ClickHouse")
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
	db := &databaseImpl{
		DatabaseImplBase: driverbase.NewDatabaseImplBase(&d.DriverImplBase),
		protocol:         clickhouse.Native,
	}
	if err := db.SetOptions(opts); err != nil {
		return nil, err
	}

	return driverbase.NewDatabase(db), nil
}
