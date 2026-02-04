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
	"context"
	"runtime/debug"
	"strings"
	"sync"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var (
	infoDriverVersion      string
	infoDriverArrowVersion string
)

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			switch s.Key {
			case "vcs.modified":
				if s.Value == "true" {
					infoDriverVersion += "-dev"
				}
			}
		}
		for _, dep := range info.Deps {
			switch {
			case strings.HasPrefix(dep.Path, "github.com/apache/arrow-go/"):
				infoDriverArrowVersion = dep.Version
				return
			}
		}
	}
}

// DriverImpl is an interface that drivers implement to provide
// vendor-specific functionality.
type DriverImpl interface {
	adbc.Driver
	adbc.DriverWithContext
	Base() *DriverImplBase
}

// Driver is the interface satisfied by the result of the NewDriver constructor,
// given an input is provided satisfying the DriverImpl interface.
type Driver interface {
	adbc.Driver
}

// DriverImplBase is a struct that provides default implementations of the
// DriverImpl interface. It is meant to be used as a composite struct for a
// driver's DriverImpl implementation.
type DriverImplBase struct {
	Alloc       memory.Allocator
	ErrorHelper ErrorHelper
	DriverInfo  *DriverInfo
}

func (base *DriverImplBase) NewDatabase(opts map[string]string) (adbc.Database, error) {
	return nil, base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "NewDatabase")
}

func (base *DriverImplBase) NewDatabaseWithContext(ctx context.Context, opts map[string]string) (adbc.Database, error) {
	return nil, base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "NewDatabaseWithContext")
}

// NewDriverImplBase instantiates DriverImplBase.
//
//   - info contains build and vendor info, as well as the name to construct error messages.
//   - alloc is an Arrow allocator to use.
func NewDriverImplBase(info *DriverInfo, alloc memory.Allocator) DriverImplBase {
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}

	if infoDriverVersion != "" {
		if err := info.RegisterInfoCode(adbc.InfoDriverVersion, infoDriverVersion); err != nil {
			panic(err)
		}
	}

	if infoDriverArrowVersion != "" {
		if err := info.RegisterInfoCode(adbc.InfoDriverArrowVersion, infoDriverArrowVersion); err != nil {
			panic(err)
		}
	}
	registerExtensionTypes()
	return DriverImplBase{
		Alloc:       alloc,
		ErrorHelper: ErrorHelper{DriverName: info.GetName()},
		DriverInfo:  info,
	}
}

func (base *DriverImplBase) Base() *DriverImplBase {
	return base
}

type driver struct {
	DriverImpl
}

// NewDriver wraps a DriverImpl to create a Driver.
func NewDriver(impl DriverImpl) Driver {
	return &driver{DriverImpl: impl}
}

var _ DriverImpl = (*DriverImplBase)(nil)

// registerExtensionTypes ensures that canonical Arrow extension types are registered.
// This is called once during driver initialization to make sure extension types like
// UUID are available for use throughout the driver.
var registerExtensionTypes = sync.OnceFunc(func() {
	// The arrow/extensions package automatically registers canonical extension types
	// (UUID, Bool8, JSON, Opaque, Variant) in its init() function.
	// However, we explicitly ensure registration here in case the package wasn't
	// imported elsewhere, and to handle any registration errors gracefully.

	// List of canonical extension types to ensure are registered
	canonicalTypes := []arrow.ExtensionType{
		extensions.NewUUIDType(),
		extensions.NewBool8Type(),
		&extensions.JSONType{},
		&extensions.OpaqueType{},
		&extensions.VariantType{},
	}

	for _, extType := range canonicalTypes {
		// RegisterExtensionType is idempotent - it returns an error only if
		// a different type with the same name is already registered
		if err := arrow.RegisterExtensionType(extType); err != nil {
			// Log but don't fail - the type might already be registered
			// which is fine (the extensions package init() may have done it)
		}
	}
})
