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
	"errors"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	driverNamespace    = "apache.arrow.adbc"
	otelTracesExporter = "OTEL_TRACES_EXPORTER"
)

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

const (
	DatabaseMessageOptionUnknown                   = "Unknown database option"
	DatabaseMessageOtelTracesExporterOptionUnknown = "Unknown " + otelTracesExporter + " option"
	DatabaseMessageNoOtelTracesExporters           = "No trace exporters added"
)

var getExporterName = sync.OnceValue(func() string {
	return os.Getenv(otelTracesExporter)
})

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
	traceParent        string
}

// NewDatabaseImplBase instantiates DatabaseImplBase.
//
//   - driver is a DriverImplBase containing the common resources from the parent
//     driver, allowing the Arrow allocator and error handler to be reused.
func NewDatabaseImplBase(ctx context.Context, driver *DriverImplBase) (DatabaseImplBase, error) {
	// Ensure extension types are registered before creating the database
	registerExtensionTypes()

	database := DatabaseImplBase{
		Alloc:       driver.Alloc,
		ErrorHelper: driver.ErrorHelper,
		DriverInfo:  driver.DriverInfo,
		Logger:      nilLogger(),
		Tracer:      nilTracer(),
	}
	err := database.InitTracing(ctx, driver.DriverInfo.GetName(), getDriverVersion(driver.DriverInfo))
	return database, err
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

func (base *DatabaseImplBase) Close() (err error) {
	if base.Base().tracerShutdownFunc != nil {
		err = base.Base().tracerShutdownFunc(context.Background())
		base.Base().tracerShutdownFunc = nil
	}
	return
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

func (d *DatabaseImplBase) GetInitialSpanAttributes() []attribute.KeyValue {
	return getInitialSpanAttributes(d.DriverInfo)
}

func (d *DatabaseImplBase) GetTraceParent() (traceParent string) {
	return d.traceParent
}

func (d *DatabaseImplBase) SetTraceParent(traceParent string) {
	d.traceParent = traceParent
}

func (d *DatabaseImplBase) StartSpan(
	ctx context.Context,
	spanName string,
	opts ...trace.SpanStartOption,
) (context.Context, trace.Span) {
	ctx, _ = maybeAddTraceParent(ctx, d, nil)
	return d.Tracer.Start(ctx, spanName, opts...)
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

func (base *database) InitTracing(ctx context.Context, driverName string, driverVersion string) error {
	return base.Base().InitTracing(ctx, driverName, driverVersion)
}

func (base *DatabaseImplBase) InitTracing(ctx context.Context, driverName string, driverVersion string) (err error) {
	fullyQualifiedDriverName := driverNamespace + "." + driverName

	exporterName := getExporterName()

	// Empty exporter
	if exporterName == "" {
		base.Tracer = otel.Tracer(fullyQualifiedDriverName)
		return
	}

	var (
		exporterType traceExporterType
		exporters    []sdktrace.SpanExporter
	)

	exporters, exporterType, err = getExporters(
		ctx,
		exporterName,
		base,
		driverName,
	)
	if err != nil {
		return
	}

	if len(exporters) < 1 {
		// This should not normally happen after a successful call to getExporters,
		// but here for completeness
		err = base.ErrorHelper.Errorf(
			adbc.StatusInvalidState,
			"%s '%s'",
			DatabaseMessageNoOtelTracesExporters,
			exporterType.String(),
		)
		return
	}

	base.Tracer, err = newTracer(exporters, base, fullyQualifiedDriverName, driverVersion)

	return
}

func getExporters(
	ctx context.Context,
	exporterName string,
	base *DatabaseImplBase,
	driverName string,
) (exporters []sdktrace.SpanExporter, exporterType traceExporterType, err error) {
	var exporter sdktrace.SpanExporter
	exporterType, ok := tryParseTraceExporterType(exporterName)
	if !ok {
		err = base.ErrorHelper.Errorf(
			adbc.StatusInvalidArgument,
			"%s '%s'",
			DatabaseMessageOtelTracesExporterOptionUnknown,
			exporterName,
		)
		return
	}
	switch exporterType {
	case TraceExporterNone:
		break
	case TraceExporterConsole:
		exporter, err = stdouttrace.New()
		if err != nil {
			return
		}
		exporters = append(exporters, exporter)
	case TraceExporterOtlp:
		exporters, err = newOtlpTraceExporters(ctx)
		if err != nil {
			return
		}
	case TraceExporterAdbcFile:
		exporter, err = newAdbcFileExporter(driverName)
		if err != nil {
			return
		}
		exporters = append(exporters, exporter)
	default:
		err = base.ErrorHelper.Errorf(
			adbc.StatusNotImplemented,
			"%s '%s'",
			DatabaseMessageOtelTracesExporterOptionUnknown,
			exporterType.String(),
		)
	}
	return
}

func newTracer(
	exporters []sdktrace.SpanExporter,
	base *DatabaseImplBase,
	fullyQualifiedDriverName string,
	driverVersion string,
) (tracer trace.Tracer, err error) {
	var tracerProvider *sdktrace.TracerProvider
	tracerProvider, err = newTracerProvider(exporters...)
	if err != nil {
		return
	}
	base.Base().tracerShutdownFunc = tracerProvider.Shutdown
	tracer = tracerProvider.Tracer(
		fullyQualifiedDriverName,
		trace.WithInstrumentationVersion(driverVersion),
		trace.WithSchemaURL(semconv.SchemaURL),
	)
	return
}

func tryParseTraceExporterType(value string) (traceExporterType, bool) {
	if te, ok := traceExporterNames[value]; ok {
		return te, true
	}
	return TraceExporterNone, false
}

func getDriverVersion(driverInfo *DriverInfo) string {
	const unknownDriverVersion = "unknown"
	value, ok := driverInfo.GetInfoForInfoCode(adbc.InfoDriverVersion)
	if !ok {
		return unknownDriverVersion
	}
	if driverVersion, ok := value.(string); ok {
		return driverVersion
	}
	return unknownDriverVersion
}

func newOtlpTraceExporters(ctx context.Context) ([]sdktrace.SpanExporter, error) {
	// Configure these exporters using environment variables
	// see: https://opentelemetry.io/docs/languages/sdk-configuration/otlp-exporter/
	// see: https://opentelemetry.io/docs/specs/otel/protocol/exporter/
	// see: https://pkg.go.dev/go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc
	// see: https://pkg.go.dev/go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp

	// Create the gRPC exporter
	grpcExporter, err := otlptracegrpc.New(
		ctx,
		otlptracegrpc.WithRetry(otlptracegrpc.RetryConfig{
			Enabled:         true,
			InitialInterval: 5 * time.Second,
			MaxInterval:     30 * time.Second,
		}),
	)
	if err != nil {
		return nil, err
	}
	// Create the http/protobuf exporter
	httpExporter, err := otlptracehttp.New(
		ctx,
		otlptracehttp.WithRetry(otlptracehttp.RetryConfig{
			Enabled:         true,
			InitialInterval: 5 * time.Second,
			MaxInterval:     30 * time.Second,
		}),
	)
	if err != nil {
		return nil, err
	}

	return []sdktrace.SpanExporter{grpcExporter, httpExporter}, nil
}

func newAdbcFileExporter(driverName string) (*stdouttrace.Exporter, error) {
	fullyQualifiedDriverName := strings.ToLower(driverNamespace + "." + driverName)
	fileWriter, err := NewRotatingFileWriter(WithLogNamePrefix(fullyQualifiedDriverName))
	if err != nil {
		return nil, err
	}
	return stdouttrace.New(stdouttrace.WithWriter(fileWriter))
}

func newTracerProvider(exporters ...sdktrace.SpanExporter) (*sdktrace.TracerProvider, error) {
	// Ensure default SDK resource and the required service name are set.
	tracerResource, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(driverNamespace),
		),
	)
	if err != nil {
		if errors.Is(err, resource.ErrSchemaURLConflict) {
			// If unable to merge with the default resource (conflicting ShhemaURL),
			// use just our resource
			tracerResource = resource.NewWithAttributes(
				semconv.SchemaURL,
				semconv.ServiceName(driverNamespace),
			)
		} else {
			return nil, err
		}
	}

	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(tracerResource),
	}
	for _, exporter := range exporters {
		opts = append(opts, sdktrace.WithBatcher(exporter))
	}
	return sdktrace.NewTracerProvider(
		opts...,
	), nil
}

var _ DatabaseImpl = (*DatabaseImplBase)(nil)
