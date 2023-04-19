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
	"database/sql"
	"errors"
	"runtime/debug"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/snowflakedb/gosnowflake"
	"golang.org/x/exp/maps"
)

const (
	infoDriverName = "ADBC Snowflake Driver - Go"
	infoVendorName = "Snowflake"
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
		copy(sqlstate[:], sferr.SQLState[:5])
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
	uri, ok := opts[adbc.OptionKeyURI]
	if ok {
		cfg, err := gosnowflake.ParseDSN(uri)
		if err != nil {
			return nil, err
		}

		db.cfg = cfg
		delete(opts, adbc.OptionKeyURI)
	}

	if db.alloc == nil {
		db.alloc = memory.DefaultAllocator
	}

	return db, db.SetOptions(opts)
}

var drv = gosnowflake.SnowflakeDriver{}

type database struct {
	cfg   *gosnowflake.Config
	alloc memory.Allocator
}

func (d *database) SetOptions(cnOptions map[string]string) error {
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
