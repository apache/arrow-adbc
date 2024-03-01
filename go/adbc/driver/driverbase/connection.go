package driverbase

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"golang.org/x/exp/slog"
)

const (
	ConnectionMessageOptionUnknown  = "Unknown connection option"
	ConnectionMessageCannotCommit   = "Cannot commit when autocommit is enabled"
	ConnectionMessageCannotRollback = "Cannot rollback when autocommit is enabled"
)

// ConnectionImpl is an interface that drivers implement to provide
// vendor-specific functionality.
type ConnectionImpl interface {
	adbc.Connection
	adbc.GetSetOptions
	adbc.DatabaseLogging
	Base() *ConnectionImplBase
}

// ConnectionImplBase is a struct that provides default implementations of some of the
// methods defined in the ConnectionImpl interface. It is meant to be used as a composite
// struct for a driver's ConnectionImpl implementation.
//
// It is up to the driver implementor to understand the semantics of the default
// behavior provided. For example, in some cases the default implementation may provide
// a fallback value while in other cases it may provide a partial-result which must be
// merged with the driver-specific-result, if any.
type ConnectionImplBase struct {
	Alloc       memory.Allocator
	ErrorHelper ErrorHelper
	DriverInfo  *DriverInfo
	Logger      *slog.Logger
}

// NewConnectionImplBase instantiates ConnectionImplBase.
//
//   - database is a DatabaseImplBase containing the common resources from the parent
//     database, allowing the Arrow allocator, error handler, and logger to be reused.
func NewConnectionImplBase(database *DatabaseImplBase) ConnectionImplBase {
	return ConnectionImplBase{Alloc: database.Alloc, ErrorHelper: database.ErrorHelper, DriverInfo: database.DriverInfo, Logger: database.Logger}
}

func (base *ConnectionImplBase) Base() *ConnectionImplBase {
	return base
}

func (base *ConnectionImplBase) SetLogger(logger *slog.Logger) {
	if logger != nil {
		base.Logger = logger
	} else {
		base.Logger = nilLogger()
	}
}

func (base *ConnectionImplBase) Commit(ctx context.Context) error {
	return base.ErrorHelper.Errorf(adbc.StatusInvalidState, ConnectionMessageCannotCommit)
}

func (base *ConnectionImplBase) Rollback(context.Context) error {
	return base.ErrorHelper.Errorf(adbc.StatusInvalidState, ConnectionMessageCannotRollback)
}

func (base *ConnectionImplBase) GetInfo(ctx context.Context, infoCodes []adbc.InfoCode) (array.RecordReader, error) {

	if len(infoCodes) == 0 {
		infoCodes = base.DriverInfo.InfoSupportedCodes()
	}

	bldr := array.NewRecordBuilder(base.Alloc, adbc.GetInfoSchema)
	defer bldr.Release()
	bldr.Reserve(len(infoCodes))

	infoNameBldr := bldr.Field(0).(*array.Uint32Builder)
	infoValueBldr := bldr.Field(1).(*array.DenseUnionBuilder)
	strInfoBldr := infoValueBldr.Child(int(adbc.InfoValueStringType)).(*array.StringBuilder)
	intInfoBldr := infoValueBldr.Child(int(adbc.InfoValueInt64Type)).(*array.Int64Builder)

	for _, code := range infoCodes {
		switch code {
		case adbc.InfoDriverName:
			name, ok := base.DriverInfo.GetInfoDriverName()
			if !ok {
				continue
			}

			infoNameBldr.Append(uint32(code))
			infoValueBldr.Append(adbc.InfoValueStringType)
			strInfoBldr.Append(name)
		case adbc.InfoDriverVersion:
			version, ok := base.DriverInfo.GetInfoDriverVersion()
			if !ok {
				continue
			}

			infoNameBldr.Append(uint32(code))
			infoValueBldr.Append(adbc.InfoValueStringType)
			strInfoBldr.Append(version)
		case adbc.InfoDriverArrowVersion:
			arrowVersion, ok := base.DriverInfo.GetInfoDriverArrowVersion()
			if !ok {
				continue
			}

			infoNameBldr.Append(uint32(code))
			infoValueBldr.Append(adbc.InfoValueStringType)
			strInfoBldr.Append(arrowVersion)
		case adbc.InfoDriverADBCVersion:
			adbcVersion, ok := base.DriverInfo.GetInfoDriverADBCVersion()
			if !ok {
				continue
			}

			infoNameBldr.Append(uint32(code))
			infoValueBldr.Append(adbc.InfoValueInt64Type)
			intInfoBldr.Append(adbcVersion)
		case adbc.InfoVendorName:
			name, ok := base.DriverInfo.GetInfoVendorName()
			if !ok {
				continue
			}

			infoNameBldr.Append(uint32(code))
			infoValueBldr.Append(adbc.InfoValueStringType)
			strInfoBldr.Append(name)
		default:
			infoNameBldr.Append(uint32(code))
			value, ok := base.DriverInfo.GetInfoForInfoCode(code)
			if !ok {
				infoValueBldr.AppendNull()
				continue
			}

			// TODO: Handle other custom info types
			infoValueBldr.Append(adbc.InfoValueStringType)
			strInfoBldr.Append(fmt.Sprint(value))
		}
	}

	final := bldr.NewRecord()
	defer final.Release()
	return array.NewRecordReader(adbc.GetInfoSchema, []arrow.Record{final})
}

func (base *ConnectionImplBase) GetOption(key string) (string, error) {
	return "", base.ErrorHelper.Errorf(adbc.StatusNotFound, "%s '%s'", ConnectionMessageOptionUnknown, key)
}

func (base *ConnectionImplBase) GetOptionBytes(key string) ([]byte, error) {
	return nil, base.ErrorHelper.Errorf(adbc.StatusNotFound, "%s '%s'", ConnectionMessageOptionUnknown, key)
}

func (base *ConnectionImplBase) GetOptionDouble(key string) (float64, error) {
	return 0, base.ErrorHelper.Errorf(adbc.StatusNotFound, "%s '%s'", ConnectionMessageOptionUnknown, key)
}

func (base *ConnectionImplBase) GetOptionInt(key string) (int64, error) {
	return 0, base.ErrorHelper.Errorf(adbc.StatusNotFound, "%s '%s'", ConnectionMessageOptionUnknown, key)
}

func (base *ConnectionImplBase) SetOption(key string, val string) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", ConnectionMessageOptionUnknown, key)
}

func (base *ConnectionImplBase) SetOptionBytes(key string, val []byte) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", ConnectionMessageOptionUnknown, key)
}

func (base *ConnectionImplBase) SetOptionDouble(key string, val float64) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", ConnectionMessageOptionUnknown, key)
}

func (base *ConnectionImplBase) SetOptionInt(key string, val int64) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", ConnectionMessageOptionUnknown, key)
}
