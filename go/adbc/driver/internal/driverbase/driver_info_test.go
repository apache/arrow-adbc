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

package driverbase_test

import (
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/stretchr/testify/require"
)

func TestDriverInfo(t *testing.T) {
	driverInfo := driverbase.DefaultDriverInfo("test")

	// The provided name is used for ErrorHelper, certain info code values, etc
	require.Equal(t, "test", driverInfo.GetName())

	// These are the info codes that are set for every driver
	expectedDefaultInfoCodes := []adbc.InfoCode{
		adbc.InfoVendorName,
		adbc.InfoVendorVersion,
		adbc.InfoVendorArrowVersion,
		adbc.InfoDriverName,
		adbc.InfoDriverVersion,
		adbc.InfoDriverArrowVersion,
		adbc.InfoDriverADBCVersion,
	}
	require.ElementsMatch(t, expectedDefaultInfoCodes, driverInfo.InfoSupportedCodes())

	// We get some formatted default values out of the box
	vendorName, ok := driverInfo.GetInfoForInfoCode(adbc.InfoVendorName)
	require.True(t, ok)
	require.Equal(t, "test", vendorName)

	driverName, ok := driverInfo.GetInfoForInfoCode(adbc.InfoDriverName)
	require.True(t, ok)
	require.Equal(t, "ADBC test Driver - Go", driverName)

	// We can register a string value to an info code that expects a string
	require.NoError(t, driverInfo.RegisterInfoCode(adbc.InfoDriverVersion, "string_value"))

	// We cannot register a non-string value to that same info code
	err := driverInfo.RegisterInfoCode(adbc.InfoDriverVersion, 123)
	require.Error(t, err)
	require.Equal(t, "DriverVersion: expected info_value 123 to be of type string but found int", err.Error())

	// We can also set vendor-specific info codes but they won't get type checked
	require.NoError(t, driverInfo.RegisterInfoCode(adbc.InfoCode(10_001), "string_value"))
	require.NoError(t, driverInfo.RegisterInfoCode(adbc.InfoCode(10_001), 123))

	// Once an info code has been registered, it is considered "supported" by the driver.
	// This means that it will be returned if GetInfo is called with no parameters.
	require.Contains(t, driverInfo.InfoSupportedCodes(), adbc.InfoCode(10_001))

	// We can retrieve arbitrary info codes, but the result's type must be asserted
	arrowVersion, ok := driverInfo.GetInfoForInfoCode(adbc.InfoDriverArrowVersion)
	require.True(t, ok)
	_, ok = arrowVersion.(string)
	require.True(t, ok)

	// We can check if info codes have been set or not
	_, ok = driverInfo.GetInfoForInfoCode(adbc.InfoCode(10_001))
	require.True(t, ok)
	_, ok = driverInfo.GetInfoForInfoCode(adbc.InfoCode(10_002))
	require.False(t, ok)
}
