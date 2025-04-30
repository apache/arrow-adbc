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
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

const driverNamespace = "apache.arrow.adbc"

type traceExporterType int

const (
	TraceExporterNone traceExporterType = iota
	TraceExporterOtlp
	TraceExporterConsole
	TraceExporterAdbcFile
)

var traceExporterNames = map[string]traceExporterType{
	string(adbc.TelemetryExporterNone):     TraceExporterNone,
	string(adbc.TelemetryExporterOtlp):     TraceExporterOtlp,
	string(adbc.TelemetryExporterConsole):  TraceExporterConsole,
	string(adbc.TelemetryExporterAdbcFile): TraceExporterAdbcFile,
}

func (te traceExporterType) String() string {
	return [...]string{
		string(adbc.TelemetryExporterNone),
		string(adbc.TelemetryExporterOtlp),
		string(adbc.TelemetryExporterConsole),
		string(adbc.TelemetryExporterAdbcFile),
	}[te]
}

func (te traceExporterType) EnumIndex() int {
	return int(te)
}

const (
	DatabaseMessageOptionUnknown = "Unknown database option"
)

// DatabaseImpl is an interface that drivers implement to provide
// vendor-specific functionality.
type DatabaseImpl interface {
	adbc.Database
	adbc.GetSetOptions
	Base() *DatabaseImplBase
}

// Database is the interface satisfied by the result of the NewDatabase constructor,
// given an input is provided satisfying the DatabaseImpl interface.
type Database interface {
	adbc.Database
	adbc.GetSetOptions
	adbc.DatabaseLogging
	adbc.OTelTracingInit
}

// DatabaseImplBase is a struct that provides default implementations of the
// DatabaseImpl interface. It is meant to be used as a composite struct for a
// driver's DatabaseImpl implementation.
type DatabaseImplBase struct {
	Alloc       memory.Allocator
	ErrorHelper ErrorHelper
	DriverInfo  *DriverInfo
	Logger      *slog.Logger
	Tracer      trace.Tracer

	tracerShutdownFunc func(context.Context) error
}

// NewDatabaseImplBase instantiates DatabaseImplBase.
//
//   - driver is a DriverImplBase containing the common resources from the parent
//     driver, allowing the Arrow allocator and error handler to be reused.
func NewDatabaseImplBase(driver *DriverImplBase) DatabaseImplBase {
	database := DatabaseImplBase{
		Alloc:       driver.Alloc,
		ErrorHelper: driver.ErrorHelper,
		DriverInfo:  driver.DriverInfo,
		Logger:      nilLogger(),
		Tracer:      nilTracer(),
	}
	database.InitTracing(driver.DriverInfo.GetName(), getDriverVersion(driver.DriverInfo))
	return database
}

func (base *DatabaseImplBase) Base() *DatabaseImplBase {
	return base
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

func (base *database) Close() error {
	return base.Base().Close()
}

func (base *DatabaseImplBase) Close() error {
	var err error = nil
	if base.Base().tracerShutdownFunc != nil {
		err = base.Base().tracerShutdownFunc(context.Background())
		base.Base().tracerShutdownFunc = nil
	}
	return err
}

func (base *DatabaseImplBase) Open(ctx context.Context) (adbc.Connection, error) {
	return nil, base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "Open")
}

func (base *DatabaseImplBase) SetOptions(options map[string]string) error {
	for key, val := range options {
		if err := base.SetOption(key, val); err != nil {
			return err
		}
	}
	return nil
}

// database is the implementation of adbc.Database.
type database struct {
	DatabaseImpl
}

// NewDatabase wraps a DatabaseImpl to create an adbc.Database.
func NewDatabase(impl DatabaseImpl) Database {
	return &database{
		DatabaseImpl: impl,
	}
}

func (db *database) SetLogger(logger *slog.Logger) {
	if logger != nil {
		db.Base().Logger = logger
	} else {
		db.Base().Logger = nilLogger()
	}
}

func (base *database) InitTracing(driverName string, driverVersion string) {
	base.Base().InitTracing(driverName, driverVersion)
}

func (base *DatabaseImplBase) InitTracing(driverName string, driverVersion string) {
	var exporter sdktrace.SpanExporter = nil
	var tracer trace.Tracer

	exporterName := os.Getenv("OTEL_TRACES_EXPORTER")
	exporterType, _ := tryParseTraceExporterType(exporterName)
	switch exporterType {
	case TraceExporterConsole:
		exporter, _ = newStdoutTraceExporter()
	case TraceExporterOtlp:
		exporter, _ = newOtlpTraceExporter(context.Background())
	case TraceExporterAdbcFile:
		exporter, _ = newAdbcFileExporter(driverName)
	}

	fullyQualifiedDriverName := driverNamespace + "." + driverName
	if exporter != nil {
		tracerProvider, err := newTracerProvider(exporter)
		if err != nil {
			panic(err)
		}
		base.Base().tracerShutdownFunc = tracerProvider.Shutdown
		tracer = tracerProvider.Tracer(
			fullyQualifiedDriverName,
			trace.WithInstrumentationVersion(driverVersion),
			trace.WithSchemaURL(semconv.SchemaURL),
		)
	} else {
		tracer = otel.Tracer(fullyQualifiedDriverName)
	}
	base.Base().Tracer = tracer
}

func tryParseTraceExporterType(value string) (traceExporterType, bool) {
	if te, ok := traceExporterNames[value]; ok {
		return te, true
	}
	return TraceExporterNone, false
}

func getDriverVersion(driverInfo *DriverInfo) string {
	var unknownDriverVersion = "unknown"
	value, ok := driverInfo.GetInfoForInfoCode(adbc.InfoDriverVersion)
	if !ok || value == nil {
		return unknownDriverVersion
	}
	if driverVersion, ok := value.(string); ok {
		return driverVersion
	}
	return unknownDriverVersion
}

func newOtlpTraceExporter(ctx context.Context) (*otlptrace.Exporter, error) {
	return otlptracegrpc.New(
		ctx,
		otlptracegrpc.WithEndpoint("localhost:4317"),
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithRetry(otlptracegrpc.RetryConfig{
			Enabled:         true,
			InitialInterval: 5 * time.Second,
			MaxInterval:     30 * time.Second,
			// MaxAttempts:     5,
		}),
	)
}

func newAdbcFileExporter(driverName string) (exporter *stdouttrace.Exporter, err error) {
	var fw io.Writer

	fullyQualifiedDriverName := strings.ToLower(driverNamespace + "." + driverName)
	if fw, err = NewRotatingFileWriter(WithLogNamePrefix(fullyQualifiedDriverName)); err != nil {
		return nil, err
	}
	if exporter, err = stdouttrace.New(stdouttrace.WithWriter(fw)); err != nil {
		return nil, err
	}
	return exporter, nil
}

func newStdoutTraceExporter() (exporter *stdouttrace.Exporter, err error) {
	if exporter, err = stdouttrace.New(); err != nil {
		return nil, err
	}
	return exporter, nil
}

func newTracerProvider(exporter sdktrace.SpanExporter) (*sdktrace.TracerProvider, error) {
	// Ensure default SDK resource and the required service name are set.
	mergedResource, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(driverNamespace),
		),
	)
	if err != nil {
		return nil, err
	}

	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(mergedResource),
	), nil
}

var _ DatabaseImpl = (*DatabaseImplBase)(nil)
