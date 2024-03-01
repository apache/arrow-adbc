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
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"golang.org/x/exp/slog"
)

const (
	DatabaseMessageOptionUnknown = "Unknown database option"
)

// DatabaseImpl is an interface that drivers implement to provide
// vendor-specific functionality.
type DatabaseImpl interface {
	adbc.Database
	adbc.GetSetOptions
	adbc.DatabaseLogging
	Base() *DatabaseImplBase
}

// DatabaseImplBase is a struct that provides default implementations of some of the
// methods defined in the DatabaseImpl interface. It is meant to be used as a composite
// struct for a driver's DatabaseImpl implementation.
//
// It is up to the driver implementor to understand the semantics of the default
// behavior provided. For example, in some cases the default implementation may provide
// a fallback value while in other cases it may provide a partial-result which must be
// merged with the driver-specific-result, if any.
type DatabaseImplBase struct {
	Alloc       memory.Allocator
	ErrorHelper ErrorHelper
	DriverInfo  *DriverInfo
	Logger      *slog.Logger
}

// NewDatabaseImplBase instantiates DatabaseImplBase.
//
//   - driver is a DriverImplBase containing the common resources from the parent
//     driver, allowing the Arrow allocator and error handler to be reused.
func NewDatabaseImplBase(driver *DriverImplBase) DatabaseImplBase {
	return DatabaseImplBase{Alloc: driver.Alloc, ErrorHelper: driver.ErrorHelper, DriverInfo: driver.DriverInfo, Logger: nilLogger()}
}

func (base *DatabaseImplBase) Base() *DatabaseImplBase {
	return base
}

func (base *DatabaseImplBase) SetLogger(logger *slog.Logger) {
	if logger != nil {
		base.Logger = logger
	} else {
		base.Logger = nilLogger()
	}
}

func (base *DatabaseImplBase) GetOption(key string) (string, error) {
	return "", base.ErrorHelper.Errorf(adbc.StatusNotFound, "%s '%s'", DatabaseMessageOptionUnknown, key)
}

func (base *DatabaseImplBase) GetOptionBytes(key string) ([]byte, error) {
	return nil, base.ErrorHelper.Errorf(adbc.StatusNotFound, "%s '%s'", DatabaseMessageOptionUnknown, key)
}

func (base *DatabaseImplBase) GetOptionDouble(key string) (float64, error) {
	return 0, base.ErrorHelper.Errorf(adbc.StatusNotFound, "%s '%s'", DatabaseMessageOptionUnknown, key)
}

func (base *DatabaseImplBase) GetOptionInt(key string) (int64, error) {
	return 0, base.ErrorHelper.Errorf(adbc.StatusNotFound, "%s '%s'", DatabaseMessageOptionUnknown, key)
}

func (base *DatabaseImplBase) SetOption(key string, val string) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", DatabaseMessageOptionUnknown, key)
}

func (base *DatabaseImplBase) SetOptionBytes(key string, val []byte) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", DatabaseMessageOptionUnknown, key)
}

func (base *DatabaseImplBase) SetOptionDouble(key string, val float64) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", DatabaseMessageOptionUnknown, key)
}

func (base *DatabaseImplBase) SetOptionInt(key string, val int64) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", DatabaseMessageOptionUnknown, key)
}
