package driverbase_test

import (
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/driverbase"
	"github.com/stretchr/testify/require"
)

func TestDriverInfo(t *testing.T) {
	driverInfo := driverbase.DefaultDriverInfo("test")

	// The provided name is used for ErrorHelper, certain info code values, etc
	require.Equal(t, "test", driverInfo.GetName())

	// These are the info codes that are set for every driver
	expectedDefaultInfoCodes := []adbc.InfoCode{
		adbc.InfoVendorName,
		adbc.InfoDriverName,
		adbc.InfoDriverVersion,
		adbc.InfoDriverArrowVersion,
		adbc.InfoDriverADBCVersion,
	}
	require.ElementsMatch(t, expectedDefaultInfoCodes, driverInfo.InfoSupportedCodes())

	// We get some formatted default values out of the box
	vendorName, ok := driverInfo.GetInfoVendorName()
	require.True(t, ok)
	require.Equal(t, "test", vendorName)

	driverName, ok := driverInfo.GetInfoDriverName()
	require.True(t, ok)
	require.Equal(t, "ADBC test Driver - Go", driverName)

	// We can register a string value to an info code that expects a string
	require.NoError(t, driverInfo.RegisterInfoCode(adbc.InfoDriverVersion, "string_value"))

	// We cannot register a non-string value to that same info code
	err := driverInfo.RegisterInfoCode(adbc.InfoDriverVersion, 123)
	require.Error(t, err)
	require.Equal(t, "info_code 101: expected info_value 123 to be of type string but found int", err.Error())

	// We can also set vendor-specific info codes but they won't get type checked
	require.NoError(t, driverInfo.RegisterInfoCode(adbc.InfoCode(10_001), "string_value"))
	require.NoError(t, driverInfo.RegisterInfoCode(adbc.InfoCode(10_001), 123))

	// Retrieving known info codes is type-safe
	driverVersion, ok := driverInfo.GetInfoDriverName()
	require.True(t, ok)
	require.NotEmpty(t, strings.Clone(driverVersion)) // do string stuff

	adbcVersion, ok := driverInfo.GetInfoDriverADBCVersion()
	require.True(t, ok)
	require.NotEmpty(t, adbcVersion+int64(123)) // do int64 stuff

	// We can also retrieve arbitrary info codes, but the result's type must be asserted
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