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
	"fmt"
	"sort"

	"github.com/apache/arrow-adbc/go/adbc"
)

const (
	UnknownVersion               = "(unknown or development build)"
	DefaultInfoDriverADBCVersion = adbc.AdbcVersion1_1_0
)

var infoValueTypeCodeForInfoCode = map[adbc.InfoCode]adbc.InfoValueTypeCode{
	adbc.InfoVendorName:                adbc.InfoValueStringType,
	adbc.InfoVendorVersion:             adbc.InfoValueStringType,
	adbc.InfoVendorArrowVersion:        adbc.InfoValueStringType,
	adbc.InfoDriverName:                adbc.InfoValueStringType,
	adbc.InfoDriverVersion:             adbc.InfoValueStringType,
	adbc.InfoDriverArrowVersion:        adbc.InfoValueStringType,
	adbc.InfoDriverADBCVersion:         adbc.InfoValueInt64Type,
	adbc.InfoVendorSql:                 adbc.InfoValueBooleanType,
	adbc.InfoVendorSubstrait:           adbc.InfoValueBooleanType,
	adbc.InfoVendorSubstraitMinVersion: adbc.InfoValueStringType,
	adbc.InfoVendorSubstraitMaxVersion: adbc.InfoValueStringType,
}

func DefaultDriverInfo(name string) *DriverInfo {
	defaultInfoVendorName := name
	defaultInfoDriverName := fmt.Sprintf("ADBC %s Driver - Go", name)

	return &DriverInfo{
		name: name,
		info: map[adbc.InfoCode]any{
			adbc.InfoVendorName:         defaultInfoVendorName,
			adbc.InfoDriverName:         defaultInfoDriverName,
			adbc.InfoDriverVersion:      UnknownVersion,
			adbc.InfoDriverArrowVersion: UnknownVersion,
			adbc.InfoVendorVersion:      UnknownVersion,
			adbc.InfoVendorArrowVersion: UnknownVersion,
			adbc.InfoDriverADBCVersion:  DefaultInfoDriverADBCVersion,
		},
	}
}

type DriverInfo struct {
	name string
	info map[adbc.InfoCode]any
}

func (di *DriverInfo) GetName() string { return di.name }

func (di *DriverInfo) InfoSupportedCodes() []adbc.InfoCode {
	// The keys of the info map are used to determine which info codes are supported.
	// This means that any info codes the driver knows about should be set to some default
	// at init, even if we don't know the value yet.
	codes := make([]adbc.InfoCode, 0, len(di.info))
	for code := range di.info {
		codes = append(codes, code)
	}

	// Sorting info codes helps present them to the client in a consistent way.
	// It also helps add some determinism to internal tests.
	// The ordering is in no way part of the API contract and should not be relied upon.
	sort.SliceStable(codes, func(i, j int) bool {
		return codes[i] < codes[j]
	})
	return codes
}

func (di *DriverInfo) RegisterInfoCode(code adbc.InfoCode, value any) error {
	infoValueTypeCode, isStandardInfoCode := infoValueTypeCodeForInfoCode[code]
	if !isStandardInfoCode {
		di.info[code] = value
		return nil
	}

	// If it is a standard InfoCode, we make sure to validate its type on write
	var err error
	switch infoValueTypeCode {
	case adbc.InfoValueStringType:
		if val, ok := value.(string); !ok {
			err = fmt.Errorf("%s: expected info_value %v to be of type %T but found %T", code, value, val, value)
		}
	case adbc.InfoValueInt64Type:
		if val, ok := value.(int64); !ok {
			err = fmt.Errorf("%s: expected info_value %v to be of type %T but found %T", code, value, val, value)
		}
	case adbc.InfoValueBooleanType:
		if val, ok := value.(bool); !ok {
			err = fmt.Errorf("%s: expected info_value %v to be of type %T but found %T", code, value, val, value)
		}
	}

	if err == nil {
		di.info[code] = value
	}

	return err
}

func (di *DriverInfo) GetInfoForInfoCode(code adbc.InfoCode) (any, bool) {
	val, ok := di.info[code]
	return val, ok
}
