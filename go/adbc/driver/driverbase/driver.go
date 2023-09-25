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

// Package driverbase provides a framework for implementing ADBC drivers in
// Go.  It intends to reduce boilerplate for common functionality and managing
// state transitions.
package driverbase

import (
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v13/arrow/memory"
)

// DriverImpl is an interface that drivers implement to provide
// vendor-specific functionality.
type DriverImpl interface {
	Base() *DriverImplBase
	NewDatabase(opts map[string]string) (adbc.Database, error)
}

// DatabaseImplBase is a struct that provides default implementations of the
// DriverImpl interface. It is meant to be used as a composite struct for a
// driver's DriverImpl implementation.
type DriverImplBase struct {
	Alloc       memory.Allocator
	ErrorHelper ErrorHelper
}

func NewDriverImplBase(name string, alloc memory.Allocator) DriverImplBase {
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	return DriverImplBase{Alloc: alloc, ErrorHelper: ErrorHelper{DriverName: name}}
}

func (base *DriverImplBase) Base() *DriverImplBase {
	return base
}

// driver is the actual implementation of adbc.Driver.
type driver struct {
	impl DriverImpl
}

// NewDatabase wraps a DriverImpl to create an adbc.Driver.
func NewDriver(impl DriverImpl) adbc.Driver {
	return &driver{impl}
}

func (drv *driver) NewDatabase(opts map[string]string) (adbc.Database, error) {
	return drv.impl.NewDatabase(opts)
}
