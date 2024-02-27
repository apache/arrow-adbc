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

package driverbase

import (
	"context"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"golang.org/x/exp/slog"
)

// DatabaseImpl is an interface that drivers implement to provide
// vendor-specific functionality.
type DatabaseImpl interface {
	adbc.GetSetOptions
	Base() *DatabaseImplBase
	Open(context.Context) (adbc.Connection, error)
	Close() error
	SetOptions(map[string]string) error
}

// DatabaseImplBase is a struct that provides default implementations of the
// DatabaseImpl interface. It is meant to be used as a composite struct for a
// driver's DatabaseImpl implementation.
type DatabaseImplBase struct {
	Alloc       memory.Allocator
	ErrorHelper ErrorHelper
	Logger      *slog.Logger
}

// NewDatabaseImplBase instantiates DatabaseImplBase. name is the driver's
// name and is used to construct error messages. alloc is an Arrow allocator
// to use.
func NewDatabaseImplBase(driver *DriverImplBase) DatabaseImplBase {
	return DatabaseImplBase{Alloc: driver.Alloc, ErrorHelper: driver.ErrorHelper, Logger: nilLogger()}
}

func (base *DatabaseImplBase) Base() *DatabaseImplBase {
	return base
}

func (base *DatabaseImplBase) GetOption(key string) (string, error) {
	return "", base.ErrorHelper.Errorf(adbc.StatusNotFound, "Unknown database option '%s'", key)
}

func (base *DatabaseImplBase) GetOptionBytes(key string) ([]byte, error) {
	return nil, base.ErrorHelper.Errorf(adbc.StatusNotFound, "Unknown database option '%s'", key)
}

func (base *DatabaseImplBase) GetOptionDouble(key string) (float64, error) {
	return 0, base.ErrorHelper.Errorf(adbc.StatusNotFound, "Unknown database option '%s'", key)
}

func (base *DatabaseImplBase) GetOptionInt(key string) (int64, error) {
	return 0, base.ErrorHelper.Errorf(adbc.StatusNotFound, "Unknown database option '%s'", key)
}

func (base *DatabaseImplBase) SetOption(key string, val string) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "Unknown database option '%s'", key)
}

func (base *DatabaseImplBase) SetOptionBytes(key string, val []byte) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "Unknown database option '%s'", key)
}

func (base *DatabaseImplBase) SetOptionDouble(key string, val float64) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "Unknown database option '%s'", key)
}

func (base *DatabaseImplBase) SetOptionInt(key string, val int64) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "Unknown database option '%s'", key)
}

// database is the implementation of adbc.Database.
type database struct {
	impl DatabaseImpl
}

// NewDatabase wraps a DatabaseImpl to create an adbc.Database.
func NewDatabase(impl DatabaseImpl) adbc.Database {
	return &database{
		impl: impl,
	}
}

func (db *database) GetOption(key string) (string, error) {
	return db.impl.GetOption(key)
}

func (db *database) GetOptionBytes(key string) ([]byte, error) {
	return db.impl.GetOptionBytes(key)
}

func (db *database) GetOptionDouble(key string) (float64, error) {
	return db.impl.GetOptionDouble(key)
}

func (db *database) GetOptionInt(key string) (int64, error) {
	return db.impl.GetOptionInt(key)
}

func (db *database) SetOption(key string, val string) error {
	return db.impl.SetOption(key, val)
}

func (db *database) SetOptionBytes(key string, val []byte) error {
	return db.impl.SetOptionBytes(key, val)
}

func (db *database) SetOptionDouble(key string, val float64) error {
	return db.impl.SetOptionDouble(key, val)
}

func (db *database) SetOptionInt(key string, val int64) error {
	return db.impl.SetOptionInt(key, val)
}

func (db *database) Open(ctx context.Context) (adbc.Connection, error) {
	return db.impl.Open(ctx)
}

func (db *database) Close() error {
	return db.impl.Close()
}

func (db *database) SetLogger(logger *slog.Logger) {
	if logger != nil {
		db.impl.Base().Logger = logger
	} else {
		db.impl.Base().Logger = nilLogger()
	}
}

func (db *database) SetOptions(opts map[string]string) error {
	return db.impl.SetOptions(opts)
}
