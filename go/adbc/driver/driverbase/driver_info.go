package driverbase

import (
	"fmt"
	"runtime/debug"
	"slices"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
)

const (
	unknownVersion               = "(unknown or development build)"
	defaultInfoDriverADBCVersion = adbc.AdbcVersion1_1_0
)

func DefaultDriverInfo(name string) *DriverInfo {
	defaultInfoVendorName := name
	defaultInfoDriverName := fmt.Sprintf("ADBC %s Driver - Go", name)

	return &DriverInfo{
		name: name,
		info: map[adbc.InfoCode]any{
			adbc.InfoVendorName:         defaultInfoVendorName,
			adbc.InfoDriverName:         defaultInfoDriverName,
			adbc.InfoDriverVersion:      unknownVersion,
			adbc.InfoDriverArrowVersion: unknownVersion,
			adbc.InfoDriverADBCVersion:  defaultInfoDriverADBCVersion,
		},
	}
}

type DriverInfo struct {
	name string
	info map[adbc.InfoCode]any
}

func (di *DriverInfo) Init(driverPath string) error {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			switch {
			case dep.Path == driverPath:
				if dep.Version != "" {
					di.RegisterInfoCode(adbc.InfoDriverVersion, dep.Version)
				}
			case strings.HasPrefix(dep.Path, "github.com/apache/arrow/go/"):
				if dep.Version != "" {
					di.RegisterInfoCode(adbc.InfoDriverArrowVersion, dep.Version)
				}
			}
		}
	}
	return nil
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
	slices.Sort(codes)
	return codes
}

func (di *DriverInfo) RegisterInfoCode(code adbc.InfoCode, value any) error {
	switch code {
	case adbc.InfoVendorName:
		if err := ensureType[string](value); err != nil {
			return fmt.Errorf("info_code %d: %w", code, err)
		}
	case adbc.InfoVendorVersion:
		if err := ensureType[string](value); err != nil {
			return fmt.Errorf("info_code %d: %w", code, err)
		}
	case adbc.InfoVendorArrowVersion:
		if err := ensureType[string](value); err != nil {
			return fmt.Errorf("info_code %d: %w", code, err)
		}
	case adbc.InfoDriverName:
		if err := ensureType[string](value); err != nil {
			return fmt.Errorf("info_code %d: %w", code, err)
		}
	case adbc.InfoDriverVersion:
		if err := ensureType[string](value); err != nil {
			return fmt.Errorf("info_code %d: %w", code, err)
		}
	case adbc.InfoDriverArrowVersion:
		if err := ensureType[string](value); err != nil {
			return fmt.Errorf("info_code %d: %w", code, err)
		}
	case adbc.InfoDriverADBCVersion:
		if err := ensureType[int64](value); err != nil {
			return fmt.Errorf("info_code %d: %w", code, err)
		}
	}

	di.info[code] = value
	return nil
}

func (di *DriverInfo) GetInfoForInfoCode(code adbc.InfoCode) (any, bool) {
	val, ok := di.info[code]
	return val, ok
}

func (di *DriverInfo) GetInfoVendorName() (string, bool) {
	return di.getStringInfoCode(adbc.InfoVendorName)
}

func (di *DriverInfo) GetInfoVendorVersion() (string, bool) {
	return di.getStringInfoCode(adbc.InfoVendorVersion)
}

func (di *DriverInfo) GetInfoVendorArrowVersion() (string, bool) {
	return di.getStringInfoCode(adbc.InfoVendorArrowVersion)
}

func (di *DriverInfo) GetInfoDriverName() (string, bool) {
	return di.getStringInfoCode(adbc.InfoDriverName)
}

func (di *DriverInfo) GetInfoDriverVersion() (string, bool) {
	return di.getStringInfoCode(adbc.InfoDriverVersion)
}

func (di *DriverInfo) GetInfoDriverArrowVersion() (string, bool) {
	return di.getStringInfoCode(adbc.InfoDriverArrowVersion)
}

func (di *DriverInfo) GetInfoDriverADBCVersion() (int64, bool) {
	return di.getInt64InfoCode(adbc.InfoDriverADBCVersion)
}

func (di *DriverInfo) getStringInfoCode(code adbc.InfoCode) (string, bool) {
	val, ok := di.GetInfoForInfoCode(code)
	if !ok {
		return "", false
	}

	if err := ensureType[string](val); err != nil {
		panic(err)
	}

	return val.(string), true
}

func (di *DriverInfo) getInt64InfoCode(code adbc.InfoCode) (int64, bool) {
	val, ok := di.GetInfoForInfoCode(code)
	if !ok {
		return int64(0), false
	}

	if err := ensureType[int64](val); err != nil {
		panic(err)
	}

	return val.(int64), true
}

func ensureType[T any](value any) error {
	typedVal, ok := value.(T)
	if !ok {
		return fmt.Errorf("expected info_value %v to be of type %T but found %T", value, typedVal, value)
	}
	return nil
}
