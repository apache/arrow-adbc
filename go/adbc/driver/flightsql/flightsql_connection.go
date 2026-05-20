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

package flightsql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	flightproto "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/bluele/gcache"
	"google.golang.org/grpc"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type connectionImpl struct {
	driverbase.ConnectionImplBase

	cl *flightsql.Client

	db          *databaseImpl
	clientCache gcache.Cache
	hdrs        metadata.MD
	timeouts    timeoutOption
	txn         *flightsql.Txn
	supportInfo support

	// id is a short random identifier assigned at Open time. It is stamped
	// onto every log record emitted by this connection (via Logger.With)
	// and onto every ADBC error detail produced for operations executed on
	// it, allowing per-connection events to be correlated across logs and
	// host-application error reports.
	id string

	// openedAt is the wall-clock time at which Open() finished initializing
	// this connection. Recorded so Close() can log a "duration" attribute
	// (and therefore the lifetime of a problematic connection can be
	// summarized in a single log line without joining across two records).
	openedAt time.Time

	// peerAddr is the resolved network address of the FlightSQL server as
	// reported by gRPC's peer.FromContext on the first successful RPC.
	// Stored so subsequent log records (and especially failure records)
	// can be stamped with the actual server endpoint that was selected by
	// the gRPC name resolver / load balancer, which is frequently
	// different from the user-supplied URI when an L7 proxy is in front
	// of the server.
	peerAddr string

	// peerOnce guards the one-shot "peer resolved" log emission. Without
	// this gate every successful RPC would re-log the peer; the address
	// is invariant per gRPC ClientConn so logging it once is sufficient.
	peerOnce sync.Once
}

type flightSqlMetadata struct {
	internal.DefaultXdbcMetadataBuilder
	columnMetadata *flightsql.ColumnMetadata
}

func (md *flightSqlMetadata) SetMetadata(metadata arrow.Metadata) {
	md.columnMetadata = &flightsql.ColumnMetadata{Data: &metadata}
}

func (md *flightSqlMetadata) SetXdbcScopeCatalog(b *array.StringBuilder) {
	if v, ok := md.columnMetadata.CatalogName(); ok {
		b.Append(v)
	} else {
		md.DefaultXdbcMetadataBuilder.SetXdbcScopeCatalog(b)
	}
}

func (md *flightSqlMetadata) SetXdbcScopeSchema(b *array.StringBuilder) {
	if v, ok := md.columnMetadata.SchemaName(); ok {
		b.Append(v)
	} else {
		md.DefaultXdbcMetadataBuilder.SetXdbcScopeSchema(b)
	}
}

func (md *flightSqlMetadata) SetXdbcScopeTable(b *array.StringBuilder) {
	if v, ok := md.columnMetadata.TableName(); ok {
		b.Append(v)
	} else {
		md.DefaultXdbcMetadataBuilder.SetXdbcScopeTable(b)
	}
}

func (md *flightSqlMetadata) SetXdbcSqlDataType(columnType arrow.DataType, b *array.Int16Builder) {
	b.Append(int16(internal.ToXdbcDataType(columnType)))
}

func (md *flightSqlMetadata) SetXdbcTypeName(b *array.StringBuilder) {
	if v, ok := md.columnMetadata.TypeName(); ok {
		b.Append(v)
	} else {
		md.DefaultXdbcMetadataBuilder.SetXdbcTypeName(b)
	}
}

func (md *flightSqlMetadata) SetXdbcIsAutoincrement(builder *array.BooleanBuilder) {
	if v, ok := md.columnMetadata.IsAutoIncrement(); ok {
		builder.Append(v)
	} else {
		md.DefaultXdbcMetadataBuilder.SetXdbcIsAutoincrement(builder)
	}
}

func (c *connectionImpl) GetObjects(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string, tableName *string, columnName *string, tableType []string) (array.RecordReader, error) {
	// To avoid an N+1 query problem, we assume result sets here will fit in memory and build up a single response.
	g := internal.GetObjects{Ctx: ctx, Depth: depth, Catalog: catalog, DbSchema: dbSchema, TableName: tableName, ColumnName: columnName, TableType: tableType}
	if err := g.Init(c.Base().Alloc, c.GetObjectsDbSchemas, c.GetObjectsTables, &flightSqlMetadata{}); err != nil {
		return nil, err
	}
	defer g.Release()

	catalogs, err := c.GetObjectsCatalogs(ctx, catalog)
	if err != nil {
		return nil, err
	}

	foundCatalog := false
	for _, catalog := range catalogs {
		g.AppendCatalog(catalog)
		foundCatalog = true
	}

	// Implementations like Dremio report no catalogs, but still have schemas
	if !foundCatalog && depth != adbc.ObjectDepthCatalogs {
		g.AppendCatalog("")
	}
	return g.Finish()
}

// GetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *connectionImpl) GetCurrentCatalog() (string, error) {
	options, err := c.getSessionOptions(context.Background())
	if err != nil {
		return "", err
	}
	if catalog, ok := options["catalog"]; ok {
		if val, ok := catalog.(string); ok {
			return val, nil
		}
		return "", c.Base().ErrorHelper.Errorf(adbc.StatusInternal, "server returned non-string catalog %#v", catalog)
	}
	return "", c.Base().ErrorHelper.Errorf(adbc.StatusNotFound, "current catalog not supported")
}

// GetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *connectionImpl) GetCurrentDbSchema() (string, error) {
	options, err := c.getSessionOptions(context.Background())
	if err != nil {
		return "", err
	}
	if schema, ok := options["schema"]; ok {
		if val, ok := schema.(string); ok {
			return val, nil
		}
		return "", c.Base().ErrorHelper.Errorf(adbc.StatusInternal, "server returned non-string schema %#v", schema)
	}
	return "", c.Base().ErrorHelper.Errorf(adbc.StatusNotFound, "current schema not supported")
}

// SetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *connectionImpl) SetCurrentCatalog(value string) error {
	return c.setSessionOptions(context.Background(), "catalog", value)
}

// SetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *connectionImpl) SetCurrentDbSchema(value string) error {
	return c.setSessionOptions(context.Background(), "schema", value)
}

func (c *connectionImpl) SetAutocommit(enabled bool) error {
	if enabled && c.txn == nil {
		// no-op don't even error if the server didn't support transactions
		return nil
	}

	if !c.supportInfo.transactions {
		return errNoTransactionSupport
	}

	ctx := metadata.NewOutgoingContext(context.Background(), c.hdrs)
	var err error
	if c.txn != nil {
		if err = c.txn.Commit(ctx, c.timeouts); err != nil {
			return adbc.Error{
				Msg:  "[Flight SQL] failed to update autocommit: " + err.Error(),
				Code: adbc.StatusIO,
			}
		}
	}

	if enabled {
		c.txn = nil
		return nil
	}

	if c.txn, err = c.cl.BeginTransaction(ctx, c.timeouts); err != nil {
		return adbc.Error{
			Msg:  "[Flight SQL] failed to update autocommit: " + err.Error(),
			Code: adbc.StatusIO,
		}
	}

	return nil
}

var adbcToFlightSQLInfo = map[adbc.InfoCode]flightsql.SqlInfo{
	adbc.InfoVendorName:                flightsql.SqlInfoFlightSqlServerName,
	adbc.InfoVendorVersion:             flightsql.SqlInfoFlightSqlServerVersion,
	adbc.InfoVendorArrowVersion:        flightsql.SqlInfoFlightSqlServerArrowVersion,
	adbc.InfoVendorSql:                 flightsql.SqlInfoFlightSqlServerSql,
	adbc.InfoVendorSubstrait:           flightsql.SqlInfoFlightSqlServerSubstrait,
	adbc.InfoVendorSubstraitMinVersion: flightsql.SqlInfoFlightSqlServerSubstraitMinVersion,
	adbc.InfoVendorSubstraitMaxVersion: flightsql.SqlInfoFlightSqlServerSubstraitMaxVersion,
}

// doGetWithLogger is the logging-aware DoGet implementation used by every
// caller in the driver. Earlier revisions exposed a plain `doGet` wrapper
// that supplied a nil logger; it had no remaining callers and was removed
// to satisfy the unused-function linter — every call site now passes its
// own *slog.Logger (or relies on safeLogger to materialize a no-op one).
//
// When a logger is provided every endpoint resolution attempt is logged
// individually so that failures hopping between Flight locations can be
// traced. The function also accumulates per-location attempt errors and, if
// every attempt fails, joins them into a single returned error so that the
// caller can see *all* the locations that were tried (and how each one
// failed) rather than just the last one. This is critical for diagnosing
// "[FlightSQL] error reading from server: EOF" reports where the empty
// "endpoint N: []" indicates the Location list was actually empty: with the
// enhanced error the operator can see whether multiple alternate locations
// were attempted and which one ultimately served (or failed) the request.
func doGetWithLogger(ctx context.Context, cl *flightsql.Client, endpoint *flight.FlightEndpoint, clientCache gcache.Cache, logger *slog.Logger, opts ...grpc.CallOption) (rdr *flight.Reader, err error) {
	log := safeLogger(logger)
	if len(endpoint.Location) == 0 {
		log.DebugContext(ctx, "FlightSQL doGet",
			"phase", "noLocations",
		)
		start := time.Now()
		rdr, err = cl.DoGet(ctx, endpoint.Ticket, opts...)
		log.DebugContext(ctx, "FlightSQL doGet",
			"phase", "defaultClientResult",
			"duration", time.Since(start),
			"err", err,
		)
		return rdr, err
	}

	var (
		cc          interface{}
		hasFallback bool
		// attemptErrors collects every per-location attempt failure so that
		// the final error returned to the caller can describe all the
		// locations that were attempted (in order) rather than discarding
		// information when fallback also fails.
		attemptErrors []string
	)

	for _, loc := range endpoint.Location {
		if loc.Uri == flight.LocationReuseConnection {
			hasFallback = true
			continue
		}

		start := time.Now()
		cc, err = clientCache.Get(loc.Uri)
		if err != nil {
			attemptErrors = append(attemptErrors, fmt.Sprintf("clientCache.Get(%q): %s", loc.Uri, err.Error()))
			log.WarnContext(ctx, "FlightSQL doGet location attempt failed",
				"phase", "clientCacheGet",
				"location", loc.Uri,
				"duration", time.Since(start),
				"err", err,
			)
			continue
		}

		conn := cc.(*flightsql.Client)
		rdr, err = conn.DoGet(ctx, endpoint.Ticket, opts...)
		if err != nil {
			attemptErrors = append(attemptErrors, fmt.Sprintf("DoGet(%q): %s", loc.Uri, err.Error()))
			log.WarnContext(ctx, "FlightSQL doGet location attempt failed",
				"phase", "doGet",
				"location", loc.Uri,
				"duration", time.Since(start),
				"err", err,
			)
			continue
		}

		log.DebugContext(ctx, "FlightSQL doGet succeeded",
			"location", loc.Uri,
			"duration", time.Since(start),
		)
		return
	}

	if hasFallback {
		start := time.Now()
		rdr, err = cl.DoGet(ctx, endpoint.Ticket, opts...)
		if err != nil {
			attemptErrors = append(attemptErrors, fmt.Sprintf("DoGet(fallback to default client): %s", err.Error()))
			log.WarnContext(ctx, "FlightSQL doGet fallback to default client failed",
				"duration", time.Since(start),
				"err", err,
			)
			// Wrap the error with the full attempt history so the diagnostic
			// trail is preserved even if the caller does not log err.Error()
			// itself (it may only end up surfaced through the C ADBC layer).
			return nil, fmt.Errorf("all DoGet attempts failed: %s; final: %w", strings.Join(attemptErrors, "; "), err)
		}
		log.DebugContext(ctx, "FlightSQL doGet succeeded via default client fallback",
			"duration", time.Since(start),
		)
		return rdr, nil
	}

	if err != nil && len(attemptErrors) > 1 {
		err = fmt.Errorf("all %d DoGet location(s) failed: %s; final: %w",
			len(attemptErrors), strings.Join(attemptErrors, "; "), err)
	}

	return nil, err
}

func (c *connectionImpl) getSessionOptions(ctx context.Context) (map[string]interface{}, error) {
	ctx = metadata.NewOutgoingContext(ctx, c.hdrs)
	var header, trailer metadata.MD
	rawOptions, err := c.cl.GetSessionOptions(ctx, &flight.GetSessionOptionsRequest{}, grpc.Header(&header), grpc.Trailer(&trailer), c.timeouts)
	if err != nil {
		// We're going to make a bit of a concession to backwards compatibility
		// here and ignore UNIMPLEMENTED or INVALID_ARGUMENT
		grpcStatus := grpcstatus.Convert(err)
		if grpcStatus.Code() == grpccodes.InvalidArgument || grpcStatus.Code() == grpccodes.Unimplemented {
			return map[string]interface{}{}, nil
		}
		return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetSessionOptions")
	}

	options := make(map[string]interface{}, len(rawOptions.SessionOptions))
	for k, rawValue := range rawOptions.SessionOptions {
		switch v := rawValue.OptionValue.(type) {
		case *flightproto.SessionOptionValue_BoolValue:
			options[k] = v.BoolValue
		case *flightproto.SessionOptionValue_DoubleValue:
			options[k] = v.DoubleValue
		case *flightproto.SessionOptionValue_Int64Value:
			options[k] = v.Int64Value
		case *flightproto.SessionOptionValue_StringValue:
			options[k] = v.StringValue
		case *flightproto.SessionOptionValue_StringListValue_:
			if v.StringListValue.Values == nil {
				options[k] = make([]string, 0)
			} else {
				options[k] = v.StringListValue.Values
			}
		case nil:
			options[k] = nil
		default:
			return nil, adbc.Error{
				Code: adbc.StatusNotImplemented,
				Msg:  fmt.Sprintf("[FlightSQL] Unknown session option type %#v", rawValue),
			}
		}
	}
	return options, nil
}

func (c *connectionImpl) setSessionOptions(ctx context.Context, key string, val interface{}) error {
	req := flight.SetSessionOptionsRequest{}
	hdrs := make([]string, 0)
	for k, vv := range c.hdrs {
		for _, v := range vv {
			hdrs = append(hdrs, k, v)
		}
	}
	ctx = metadata.AppendToOutgoingContext(ctx, hdrs...)

	var err error
	req.SessionOptions, err = flight.NewSessionOptionValues(map[string]any{key: val})
	if err != nil {
		return adbc.Error{
			Msg:  fmt.Sprintf("[Flight SQL] Invalid session option %s=%#v: %s", key, val, err.Error()),
			Code: adbc.StatusInvalidArgument,
		}
	}

	var header, trailer metadata.MD
	errors, err := c.cl.SetSessionOptions(ctx, &req, grpc.Header(&header), grpc.Trailer(&trailer), c.timeouts)
	if err != nil {
		return adbcFromFlightStatusWithDetails(err, header, trailer, "SetSessionOptions")
	}
	if len(errors.Errors) > 0 {
		msg := strings.Builder{}
		fmt.Fprint(&msg, "[Flight SQL] Could not set option(s) ")

		first := true
		for k, v := range errors.Errors {
			if !first {
				fmt.Fprint(&msg, ", ")
			}
			first = false

			errmsg := "unknown error"
			switch v.Value {
			case flightproto.SetSessionOptionsResult_INVALID_NAME:
				errmsg = "invalid name"
			case flightproto.SetSessionOptionsResult_INVALID_VALUE:
				errmsg = "invalid value"
			case flightproto.SetSessionOptionsResult_ERROR:
				errmsg = "error setting option"
			}
			fmt.Fprintf(&msg, "'%s' (%s)", k, errmsg)
		}

		return adbc.Error{
			Msg:  msg.String(),
			Code: adbc.StatusInvalidArgument,
		}
	}
	return nil
}

func getSessionOption[T any](options map[string]interface{}, key string, defaultVal T, valueType string) (T, error) {
	rawValue, ok := options[key]
	if !ok {
		return defaultVal, adbc.Error{
			Msg:  fmt.Sprintf("[Flight SQL] unknown session option '%s'", key),
			Code: adbc.StatusNotFound,
		}
	}
	value, ok := rawValue.(T)
	if !ok {
		return defaultVal, adbc.Error{
			Msg:  fmt.Sprintf("[Flight SQL] session option %s=%#v is not %s value", key, rawValue, valueType),
			Code: adbc.StatusNotFound,
		}
	}
	return value, nil
}

func (c *connectionImpl) GetOption(key string) (string, error) {
	if strings.HasPrefix(key, OptionRPCCallHeaderPrefix) {
		name := strings.TrimPrefix(key, OptionRPCCallHeaderPrefix)
		headers := c.hdrs.Get(name)
		if len(headers) > 0 {
			return headers[0], nil
		}
		return "", adbc.Error{
			Msg:  "[Flight SQL] unknown header",
			Code: adbc.StatusNotFound,
		}
	}

	switch key {
	case OptionTimeoutFetch:
		return c.timeouts.fetchTimeout.String(), nil
	case OptionTimeoutQuery:
		return c.timeouts.queryTimeout.String(), nil
	case OptionTimeoutUpdate:
		return c.timeouts.updateTimeout.String(), nil
	case OptionSessionOptions:
		options, err := c.getSessionOptions(context.Background())
		if err != nil {
			return "", err
		}
		encoded, err := json.Marshal(options)
		if err != nil {
			return "", adbc.Error{
				Msg:  fmt.Sprintf("[Flight SQL] Could not encode option values: %s", err.Error()),
				Code: adbc.StatusInternal,
			}
		}
		return string(encoded), nil
	}
	switch {
	case strings.HasPrefix(key, OptionSessionOptionPrefix):
		options, err := c.getSessionOptions(context.Background())
		if err != nil {
			return "", err
		}
		name := key[len(OptionSessionOptionPrefix):]
		return getSessionOption(options, name, "", "a string")
	case strings.HasPrefix(key, OptionBoolSessionOptionPrefix):
		options, err := c.getSessionOptions(context.Background())
		if err != nil {
			return "", err
		}
		name := key[len(OptionBoolSessionOptionPrefix):]
		v, err := getSessionOption(options, name, false, "a boolean")
		if err != nil {
			return "", err
		}
		if v {
			return adbc.OptionValueEnabled, nil
		}
		return adbc.OptionValueDisabled, nil
	case strings.HasPrefix(key, OptionStringListSessionOptionPrefix):
		options, err := c.getSessionOptions(context.Background())
		if err != nil {
			return "", err
		}
		name := key[len(OptionStringListSessionOptionPrefix):]
		v, err := getSessionOption[[]string](options, name, nil, "a string list")
		if err != nil {
			return "", err
		}
		encoded, err := json.Marshal(v)
		if err != nil {
			return "", adbc.Error{
				Msg:  fmt.Sprintf("[Flight SQL] Could not encode option value: %s", err.Error()),
				Code: adbc.StatusInternal,
			}
		}
		return string(encoded), nil
	}

	return "", adbc.Error{
		Msg:  "[Flight SQL] unknown connection option",
		Code: adbc.StatusNotFound,
	}
}

func (c *connectionImpl) GetOptionBytes(key string) ([]byte, error) {
	switch key {
	case OptionSessionOptions:
		options, err := c.getSessionOptions(context.Background())
		if err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(options)
		if err != nil {
			return nil, adbc.Error{
				Msg:  fmt.Sprintf("[Flight SQL] Could not encode option values: %s", err.Error()),
				Code: adbc.StatusInternal,
			}
		}
		return encoded, nil
	}

	return nil, adbc.Error{
		Msg:  "[Flight SQL] unknown connection option",
		Code: adbc.StatusNotFound,
	}
}

func (c *connectionImpl) GetOptionInt(key string) (int64, error) {
	switch key {
	case OptionTimeoutFetch:
		fallthrough
	case OptionTimeoutQuery:
		fallthrough
	case OptionTimeoutUpdate:
		val, err := c.GetOptionDouble(key)
		if err != nil {
			return 0, err
		}
		return int64(val), nil
	}
	if strings.HasPrefix(key, OptionSessionOptionPrefix) {
		options, err := c.getSessionOptions(context.Background())
		if err != nil {
			return 0, err
		}
		name := key[len(OptionSessionOptionPrefix):]
		return getSessionOption(options, name, int64(0), "an integer")
	}

	return c.ConnectionImplBase.GetOptionInt(key)
}

func (c *connectionImpl) GetOptionDouble(key string) (float64, error) {
	switch key {
	case OptionTimeoutFetch:
		return c.timeouts.fetchTimeout.Seconds(), nil
	case OptionTimeoutQuery:
		return c.timeouts.queryTimeout.Seconds(), nil
	case OptionTimeoutUpdate:
		return c.timeouts.updateTimeout.Seconds(), nil
	}
	if strings.HasPrefix(key, OptionSessionOptionPrefix) {
		options, err := c.getSessionOptions(context.Background())
		if err != nil {
			return 0, err
		}
		name := key[len(OptionSessionOptionPrefix):]
		return getSessionOption(options, name, float64(0.0), "a floating-point")
	}

	return c.ConnectionImplBase.GetOptionDouble(key)
}

func (c *connectionImpl) SetOption(key, value string) error {
	if strings.HasPrefix(key, OptionRPCCallHeaderPrefix) {
		name := strings.TrimPrefix(key, OptionRPCCallHeaderPrefix)
		if value == "" {
			c.hdrs.Delete(name)
		} else {
			c.hdrs.Append(name, value)
		}
		return nil
	}

	switch key {
	case OptionTimeoutFetch, OptionTimeoutQuery, OptionTimeoutUpdate:
		return c.timeouts.setTimeoutString(key, value)
	}

	switch {
	case strings.HasPrefix(key, OptionSessionOptionPrefix):
		name := key[len(OptionSessionOptionPrefix):]
		return c.setSessionOptions(context.Background(), name, value)
	case strings.HasPrefix(key, OptionBoolSessionOptionPrefix):
		name := key[len(OptionBoolSessionOptionPrefix):]
		switch value {
		case adbc.OptionValueEnabled:
			return c.setSessionOptions(context.Background(), name, true)
		case adbc.OptionValueDisabled:
			return c.setSessionOptions(context.Background(), name, false)
		default:
			return adbc.Error{
				Msg:  fmt.Sprintf("[Flight SQL] invalid boolean session option value %s=%s", name, value),
				Code: adbc.StatusNotImplemented,
			}
		}
	case strings.HasPrefix(key, OptionStringListSessionOptionPrefix):
		name := key[len(OptionStringListSessionOptionPrefix):]
		stringlist := make([]string, 0)
		if err := json.Unmarshal([]byte(value), &stringlist); err != nil {
			return adbc.Error{
				Msg:  fmt.Sprintf("[Flight SQL] invalid string list session option value %s=%s: %s", name, value, err.Error()),
				Code: adbc.StatusNotImplemented,
			}
		}
		return c.setSessionOptions(context.Background(), name, stringlist)
	case strings.HasPrefix(key, OptionEraseSessionOptionPrefix):
		name := key[len(OptionEraseSessionOptionPrefix):]
		return c.setSessionOptions(context.Background(), name, nil)
	}

	return c.ConnectionImplBase.SetOption(key, value)
}

func (c *connectionImpl) SetOptionInt(key string, value int64) error {
	switch key {
	case OptionTimeoutFetch, OptionTimeoutQuery, OptionTimeoutUpdate:
		return c.timeouts.setTimeout(key, float64(value))
	}
	if strings.HasPrefix(key, OptionSessionOptionPrefix) {
		name := key[len(OptionSessionOptionPrefix):]
		return c.setSessionOptions(context.Background(), name, value)
	}

	return c.ConnectionImplBase.SetOptionInt(key, value)
}

func (c *connectionImpl) SetOptionDouble(key string, value float64) error {
	switch key {
	case OptionTimeoutFetch:
		fallthrough
	case OptionTimeoutQuery:
		fallthrough
	case OptionTimeoutUpdate:
		return c.timeouts.setTimeout(key, value)
	}
	if strings.HasPrefix(key, OptionSessionOptionPrefix) {
		name := key[len(OptionSessionOptionPrefix):]
		return c.setSessionOptions(context.Background(), name, value)
	}

	return c.ConnectionImplBase.SetOptionDouble(key, value)
}

func (c *connectionImpl) PrepareDriverInfo(ctx context.Context, infoCodes []adbc.InfoCode) error {
	driverInfo := c.DriverInfo

	if len(infoCodes) == 0 {
		infoCodes = driverInfo.InfoSupportedCodes()
	}

	translated := make([]flightsql.SqlInfo, 0, len(infoCodes))
	for _, code := range infoCodes {
		if t, ok := adbcToFlightSQLInfo[code]; ok {
			translated = append(translated, t)
		}
	}

	// None of the requested info codes are available on the server, so just return the local info
	if len(translated) == 0 {
		return nil
	}

	ctx = metadata.NewOutgoingContext(ctx, c.hdrs)
	var header, trailer metadata.MD
	info, err := c.cl.GetSqlInfo(ctx, translated, grpc.Header(&header), grpc.Trailer(&trailer), c.timeouts)

	// Just return local driver info if GetSqlInfo hasn't been implemented on the server
	if grpcstatus.Code(err) == grpccodes.Unimplemented {
		return nil
	}

	if err != nil {
		return adbcFromFlightStatus(err, "GetInfo(GetSqlInfo)")
	}

	// No error, go get the SqlInfo from the server
	for i, endpoint := range info.Endpoint {
		var header, trailer metadata.MD
		rdr, err := doGetWithLogger(ctx, c.cl, endpoint, c.clientCache, c.Logger, grpc.Header(&header), grpc.Trailer(&trailer), c.timeouts)
		if err != nil {
			return adbcFromFlightStatusWithDetails(err, header, trailer, "GetInfo(DoGet): endpoint %d: %s", i, endpoint.Location)
		}

		for rdr.Next() {
			rec := rdr.RecordBatch()
			field := rec.Column(0).(*array.Uint32)
			info := rec.Column(1).(*array.DenseUnion)

			var adbcInfoCode adbc.InfoCode
			for i := 0; i < int(rec.NumRows()); i++ {

				var found bool
				idx := int(info.ValueOffset(i))
				flightSqlInfoCode := flightsql.SqlInfo(field.Value(i))
				for infocode := range adbcToFlightSQLInfo {
					if adbcToFlightSQLInfo[infocode] == flightSqlInfoCode {
						adbcInfoCode = infocode
						found = true
						break
					}
				}

				// SqlInfo on the server that does not have an explicit mapping to ADBC is ignored
				if !found {
					continue
				}

				var v any
				switch arr := info.Field(info.ChildID(i)).(type) {
				case *array.String:
					v = strings.Clone(arr.Value(idx))
				case *array.Boolean:
					v = arr.Value(idx)
				default:
					return adbc.Error{
						Msg:  fmt.Sprintf("unsupported field_type %T for info_value", arr),
						Code: adbc.StatusInvalidArgument,
					}
				}

				if err := driverInfo.RegisterInfoCode(adbcInfoCode, v); err != nil {
					return err
				}
			}
		}

		if err := checkContext(rdr.Err(), ctx); err != nil {
			return adbcFromFlightStatusWithDetails(err, header, trailer, "GetInfo(DoGet): endpoint %d: %s", i, endpoint.Location)
		}
	}

	return nil
}

// Helper function to read and validate a metadata stream
func (c *connectionImpl) readInfo(ctx context.Context, expectedSchema *arrow.Schema, info *flight.FlightInfo, opts ...grpc.CallOption) (array.RecordReader, error) {
	// use a default queueSize for the reader
	rdr, err := newRecordReader(ctx, recordReaderConfig{
		alloc:       c.db.Alloc,
		cl:          c.cl,
		info:        info,
		clientCache: c.clientCache,
		bufferSize:  5,
		logger:      c.Logger,
	}, opts...)
	if err != nil {
		return nil, adbcFromFlightStatus(err, "DoGet")
	}

	if !rdr.Schema().Equal(expectedSchema) {
		rdr.Release()
		return nil, adbc.Error{
			Msg:  fmt.Sprintf("Invalid schema returned for: expected %s, got %s", expectedSchema.String(), rdr.Schema().String()),
			Code: adbc.StatusInternal,
		}
	}
	return rdr, nil
}

func (c *connectionImpl) GetObjectsCatalogs(ctx context.Context, catalog *string) ([]string, error) {
	var (
		header, trailer metadata.MD
		numCatalogs     int64
	)
	ctx = metadata.NewOutgoingContext(ctx, c.hdrs)
	// To avoid an N+1 query problem, we assume result sets here will fit in memory and build up a single response.
	info, err := c.cl.GetCatalogs(ctx, grpc.Header(&header), grpc.Trailer(&trailer), c.timeouts)
	if err != nil {
		return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetObjects(GetCatalogs)")
	}

	if info.TotalRecords > 0 {
		numCatalogs = info.TotalRecords
	}

	header = metadata.MD{}
	trailer = metadata.MD{}
	rdr, err := c.readInfo(ctx, schema_ref.Catalogs, info, c.timeouts, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetObjects(GetCatalogs)")
	}
	defer rdr.Release()

	catalogs := make([]string, 0, numCatalogs)
	for rdr.Next() {
		arr := rdr.RecordBatch().Column(0).(*array.String)
		for i := 0; i < arr.Len(); i++ {
			// XXX: force copy since accessor is unsafe
			catalogName := string([]byte(arr.Value(i)))
			catalogs = append(catalogs, catalogName)
		}
	}

	if err := checkContext(rdr.Err(), ctx); err != nil {
		return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetObjects(GetCatalogs)")
	}

	return catalogs, nil
}

// Helper function to build up a map of catalogs to DB schemas
func (c *connectionImpl) GetObjectsDbSchemas(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string) (result map[string][]string, err error) {
	if depth == adbc.ObjectDepthCatalogs {
		return
	}
	ctx = metadata.NewOutgoingContext(ctx, c.hdrs)
	result = make(map[string][]string)
	var header, trailer metadata.MD
	// Pre-populate the map of which schemas are in which catalogs
	info, err := c.cl.GetDBSchemas(ctx, &flightsql.GetDBSchemasOpts{DbSchemaFilterPattern: dbSchema}, grpc.Header(&header), grpc.Trailer(&trailer), c.timeouts)
	if err != nil {
		return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetObjects(GetDBSchemas)")
	}

	header = metadata.MD{}
	trailer = metadata.MD{}
	rdr, err := c.readInfo(ctx, schema_ref.DBSchemas, info, c.timeouts, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetObjects(GetDBSchemas)")
	}
	defer rdr.Release()

	for rdr.Next() {
		// Nullable
		catalog := rdr.RecordBatch().Column(0).(*array.String)
		// Non-nullable
		dbSchema := rdr.RecordBatch().Column(1).(*array.String)

		for i := 0; i < catalog.Len(); i++ {
			catalogName := ""
			if !catalog.IsNull(i) {
				catalogName = string([]byte(catalog.Value(i)))
			}
			result[catalogName] = append(result[catalogName], string([]byte(dbSchema.Value(i))))
		}
	}

	if err := checkContext(rdr.Err(), ctx); err != nil {
		return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetObjects(GetCatalogs)")
	}
	return
}

func (c *connectionImpl) GetObjectsTables(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string, tableName *string, columnName *string, tableType []string) (result internal.SchemaToTableInfo, err error) {
	if depth == adbc.ObjectDepthCatalogs || depth == adbc.ObjectDepthDBSchemas {
		return
	}
	ctx = metadata.NewOutgoingContext(ctx, c.hdrs)
	result = make(map[internal.CatalogAndSchema][]internal.TableInfo)

	// Pre-populate the map of which schemas are in which catalogs
	includeSchema := depth == adbc.ObjectDepthAll || depth == adbc.ObjectDepthColumns
	var header, trailer metadata.MD
	info, err := c.cl.GetTables(ctx, &flightsql.GetTablesOpts{
		DbSchemaFilterPattern:  dbSchema,
		TableNameFilterPattern: tableName,
		TableTypes:             tableType,
		IncludeSchema:          includeSchema,
	}, grpc.Header(&header), grpc.Trailer(&trailer), c.timeouts)
	if err != nil {
		return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetObjects(GetTables)")
	}

	expectedSchema := schema_ref.Tables
	if includeSchema {
		expectedSchema = schema_ref.TablesWithIncludedSchema
	}
	header = metadata.MD{}
	trailer = metadata.MD{}
	rdr, err := c.readInfo(ctx, expectedSchema, info, c.timeouts, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		return nil, adbcFromFlightStatus(err, "GetObjects(GetTables)")
	}
	defer rdr.Release()

	for rdr.Next() {
		// Nullable
		catalog := rdr.RecordBatch().Column(0).(*array.String)
		dbSchema := rdr.RecordBatch().Column(1).(*array.String)
		// Non-nullable
		tableName := rdr.RecordBatch().Column(2).(*array.String)
		tableType := rdr.RecordBatch().Column(3).(*array.String)

		for i := 0; i < catalog.Len(); i++ {
			catalogName := ""
			dbSchemaName := ""
			if !catalog.IsNull(i) {
				catalogName = string([]byte(catalog.Value(i)))
			}
			if !dbSchema.IsNull(i) {
				dbSchemaName = string([]byte(dbSchema.Value(i)))
			}
			key := internal.CatalogAndSchema{
				Catalog: catalogName,
				Schema:  dbSchemaName,
			}

			var schema *arrow.Schema
			if includeSchema {
				reader, err := ipc.NewReader(bytes.NewReader(rdr.RecordBatch().Column(4).(*array.Binary).Value(i)))
				if err != nil {
					return nil, adbc.Error{
						Msg:  err.Error(),
						Code: adbc.StatusInternal,
					}
				}
				schema = reader.Schema()
				reader.Release()
			}

			result[key] = append(result[key], internal.TableInfo{
				Name:      string([]byte(tableName.Value(i))),
				TableType: string([]byte(tableType.Value(i))),
				Schema:    schema,
			})
		}
	}

	if err := checkContext(rdr.Err(), ctx); err != nil {
		return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetObjects(GetTables)")
	}
	return
}

func (c *connectionImpl) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (*arrow.Schema, error) {
	opts := &flightsql.GetTablesOpts{
		Catalog:                catalog,
		DbSchemaFilterPattern:  dbSchema,
		TableNameFilterPattern: &tableName,
		IncludeSchema:          true,
	}

	ctx = metadata.NewOutgoingContext(ctx, c.hdrs)
	var header, trailer metadata.MD
	info, err := c.cl.GetTables(ctx, opts, c.timeouts, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetTableSchema(GetTables)")
	}

	header = metadata.MD{}
	trailer = metadata.MD{}
	rdr, err := doGetWithLogger(ctx, c.cl, info.Endpoint[0], c.clientCache, c.Logger, c.timeouts, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetTableSchema(DoGet)")
	}
	defer rdr.Release()

	rec, err := rdr.Read()
	if err != nil {
		if err == io.EOF {
			return nil, adbc.Error{
				Msg:  "No table found",
				Code: adbc.StatusNotFound,
			}
		}
		return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetTableSchema(DoGet)")
	}

	numRows := rec.NumRows()
	switch {
	case numRows == 0:
		return nil, adbc.Error{
			Code: adbc.StatusNotFound,
		}
	case numRows > math.MaxInt32:
		return nil, adbc.Error{
			Msg:  "[Flight SQL] GetTableSchema cannot handle tables with number of rows > 2^31 - 1",
			Code: adbc.StatusNotImplemented,
		}
	}

	var s *arrow.Schema
	for i := 0; i < int(numRows); i++ {
		currentTableName := rec.Column(2).(*array.String).Value(i)
		if currentTableName == tableName {
			// returned schema should be
			//    0: catalog_name: utf8
			//    1: db_schema_name: utf8
			//    2: table_name: utf8 not null
			//    3: table_type: utf8 not null
			//    4: table_schema: bytes not null
			schemaBytes := rec.Column(4).(*array.Binary).Value(i)
			s, err = flight.DeserializeSchema(schemaBytes, c.db.Alloc)
			if err != nil {
				return nil, adbcFromFlightStatus(err, "GetTableSchema")
			}
			return s, nil
		}
	}

	return s, adbc.Error{
		Msg:  "[Flight SQL] GetTableSchema could not find a table with a matching schema",
		Code: adbc.StatusNotFound,
	}
}

// GetTableTypes returns a list of the table types in the database.
//
// The result is an arrow dataset with the following schema:
//
//	Field Name			| Field Type
//	----------------|--------------
//	table_type			| utf8 not null
func (c *connectionImpl) GetTableTypes(ctx context.Context) (array.RecordReader, error) {
	ctx = metadata.NewOutgoingContext(ctx, c.hdrs)
	var header, trailer metadata.MD
	info, err := c.cl.GetTableTypes(ctx, c.timeouts, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetTableTypes")
	}

	return newRecordReader(ctx, recordReaderConfig{
		alloc:       c.db.Alloc,
		cl:          c.cl,
		info:        info,
		clientCache: c.clientCache,
		bufferSize:  5,
		logger:      c.Logger,
	})
}

// Commit commits any pending transactions on this connection, it should
// only be used if autocommit is disabled.
//
// Behavior is undefined if this is mixed with SQL transaction statements.
// When not supported, the convention is that it should act as if autocommit
// is enabled and return INVALID_STATE errors.
func (c *connectionImpl) Commit(ctx context.Context) error {
	ctx = metadata.NewOutgoingContext(ctx, c.hdrs)
	var header, trailer metadata.MD
	err := c.txn.Commit(ctx, c.timeouts, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		return adbcFromFlightStatusWithDetails(err, header, trailer, "Commit")
	}

	header = metadata.MD{}
	trailer = metadata.MD{}
	c.txn, err = c.cl.BeginTransaction(ctx, c.timeouts, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		return adbcFromFlightStatusWithDetails(err, header, trailer, "BeginTransaction")
	}
	return nil
}

// Rollback rolls back any pending transactions. Only used if autocommit
// is disabled.
//
// Behavior is undefined if this is mixed with SQL transaction statements.
// When not supported, the convention is that it should act as if autocommit
// is enabled and return INVALID_STATE errors.
func (c *connectionImpl) Rollback(ctx context.Context) error {
	ctx = metadata.NewOutgoingContext(ctx, c.hdrs)
	var header, trailer metadata.MD
	err := c.txn.Rollback(ctx, c.timeouts, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		return adbcFromFlightStatusWithDetails(err, header, trailer, "Rollback")
	}

	header = metadata.MD{}
	trailer = metadata.MD{}
	c.txn, err = c.cl.BeginTransaction(ctx, c.timeouts, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		return adbcFromFlightStatusWithDetails(err, header, trailer, "BeginTransaction")
	}
	return nil
}

// NewStatement initializes a new statement object tied to this connection
func (c *connectionImpl) NewStatement() (adbc.Statement, error) {
	id := newRandomID("stmt")
	// Build a statement-scoped logger so every record emitted for this
	// statement carries both connection_id (inherited from c.Logger via the
	// With() called in databaseImpl.Open) and statement_id. The discard
	// fallback in safeLogger keeps callers free of nil-checks if no logger
	// is wired up by the host.
	log := safeLogger(c.Logger).With("statement_id", id)
	return &statement{
		alloc:       c.db.Alloc,
		clientCache: c.clientCache,
		hdrs:        c.hdrs.Copy(),
		queueSize:   5,
		timeouts:    c.timeouts,
		cnxn:        c,
		id:          id,
		log:         log,
	}, nil
}

func (c *connectionImpl) execute(ctx context.Context, query string, opts ...grpc.CallOption) (*flight.FlightInfo, error) {
	if c.txn != nil {
		return c.txn.Execute(ctx, query, opts...)
	}

	return c.cl.Execute(ctx, query, opts...)
}

func (c *connectionImpl) executeSchema(ctx context.Context, query string, opts ...grpc.CallOption) (*flight.SchemaResult, error) {
	if c.txn != nil {
		return c.txn.GetExecuteSchema(ctx, query, opts...)
	}

	return c.cl.GetExecuteSchema(ctx, query, opts...)
}

func (c *connectionImpl) executeSubstrait(ctx context.Context, plan flightsql.SubstraitPlan, opts ...grpc.CallOption) (*flight.FlightInfo, error) {
	if c.txn != nil {
		return c.txn.ExecuteSubstrait(ctx, plan, opts...)
	}

	return c.cl.ExecuteSubstrait(ctx, plan, opts...)
}

func (c *connectionImpl) executeSubstraitSchema(ctx context.Context, plan flightsql.SubstraitPlan, opts ...grpc.CallOption) (*flight.SchemaResult, error) {
	if c.txn != nil {
		return c.txn.GetExecuteSubstraitSchema(ctx, plan, opts...)
	}

	return c.cl.GetExecuteSubstraitSchema(ctx, plan, opts...)
}

func (c *connectionImpl) executeUpdate(ctx context.Context, query string, opts ...grpc.CallOption) (n int64, err error) {
	if c.txn != nil {
		return c.txn.ExecuteUpdate(ctx, query, opts...)
	}

	return c.cl.ExecuteUpdate(ctx, query, opts...)
}

func (c *connectionImpl) executeSubstraitUpdate(ctx context.Context, plan flightsql.SubstraitPlan, opts ...grpc.CallOption) (n int64, err error) {
	if c.txn != nil {
		return c.txn.ExecuteSubstraitUpdate(ctx, plan, opts...)
	}

	return c.cl.ExecuteSubstraitUpdate(ctx, plan, opts...)
}

func (c *connectionImpl) poll(ctx context.Context, query string, retryDescriptor *flight.FlightDescriptor, opts ...grpc.CallOption) (*flight.PollInfo, error) {
	if c.txn != nil {
		return c.txn.ExecutePoll(ctx, query, retryDescriptor, opts...)
	}

	return c.cl.ExecutePoll(ctx, query, retryDescriptor, opts...)
}

func (c *connectionImpl) pollSubstrait(ctx context.Context, plan flightsql.SubstraitPlan, retryDescriptor *flight.FlightDescriptor, opts ...grpc.CallOption) (*flight.PollInfo, error) {
	if c.txn != nil {
		return c.txn.ExecuteSubstraitPoll(ctx, plan, retryDescriptor, opts...)
	}

	return c.cl.ExecuteSubstraitPoll(ctx, plan, retryDescriptor, opts...)
}

func (c *connectionImpl) prepare(ctx context.Context, query string, opts ...grpc.CallOption) (*flightsql.PreparedStatement, error) {
	if c.txn != nil {
		return c.txn.Prepare(ctx, query, opts...)
	}

	return c.cl.Prepare(ctx, query, opts...)
}

func (c *connectionImpl) prepareSubstrait(ctx context.Context, plan flightsql.SubstraitPlan, opts ...grpc.CallOption) (*flightsql.PreparedStatement, error) {
	if c.txn != nil {
		return c.txn.PrepareSubstrait(ctx, plan, opts...)
	}

	return c.cl.PrepareSubstrait(ctx, plan, opts...)
}

// Close closes this connection and releases any associated resources.
func (c *connectionImpl) Close() error {
	if c.cl == nil {
		return adbc.Error{
			Msg:  "[Flight SQL Connection] trying to close already closed connection",
			Code: adbc.StatusInvalidState,
		}
	}

	closeStart := time.Now()
	// Snapshot fields needed for the post-close log line before we
	// tear down c.cl. We deliberately log "starting" + "finished"
	// separately rather than a single "closed" line because the
	// underlying CloseSession can block on the server, and operators
	// triaging a hung shutdown need to see when the close began.
	logger := safeLogger(c.Logger)
	connID := c.id
	peerAddr := c.peerAddr
	openedAt := c.openedAt

	logger.Info("FlightSQL connection closing",
		"connection_id", connID,
		"peer_addr", peerAddr,
	)

	ctx := metadata.NewOutgoingContext(context.Background(), c.hdrs)
	var header, trailer metadata.MD
	_, err := c.cl.CloseSession(ctx, &flight.CloseSessionRequest{}, grpc.Header(&header), grpc.Trailer(&trailer), c.timeouts)
	if err != nil {
		grpcStatus := grpcstatus.Convert(err)
		// Ignore unimplemented
		if grpcStatus.Code() != grpccodes.Unimplemented {
			// Ignore the error since server may not support it and may not properly return UNIMPLEMENTED
			// TODO(https://github.com/apache/arrow-adbc/issues/1243): log a proper warning
			c.db.Logger.Debug("failed to close session", "error", err.Error())
		}
	}

	c.clientCache.Purge()
	err = c.cl.Close()
	c.cl = nil

	args := []any{
		"connection_id", connID,
		"peer_addr", peerAddr,
		"close_duration", time.Since(closeStart),
	}
	if !openedAt.IsZero() {
		args = append(args, "lifetime", time.Since(openedAt))
	}
	if err != nil {
		args = append(args, "err", err)
		args = append(args, grpcStatusAttrs(err)...)
		logger.Info("FlightSQL connection closed with error", args...)
	} else {
		logger.Info("FlightSQL connection closed", args...)
	}

	return adbcFromFlightStatus(err, "Close")
}

// recordPeer captures the resolved server peer address from a successful
// gRPC call's per-call grpc.Peer (or, equivalently, peer.FromContext on
// the call context). The first invocation per connection logs a
// structured "FlightSQL peer resolved" event so an operator can see at a
// glance which backend the gRPC name resolver selected; subsequent
// invocations are no-ops because the peer is invariant for a given
// gRPC ClientConn. The peer address is then attached to every Close,
// statement, and stream error log emitted by the connection.
func (c *connectionImpl) recordPeer(p *peer.Peer) {
	if c == nil || p == nil || p.Addr == nil {
		return
	}
	addr := p.Addr.String()
	if addr == "" {
		return
	}
	c.peerOnce.Do(func() {
		c.peerAddr = addr
		if c.Logger != nil {
			c.Logger.Info("FlightSQL peer resolved",
				"connection_id", c.id,
				"peer_addr", addr,
			)
		}
	})
}

// ReadPartition constructs a statement for a partition of a query. The
// results can then be read independently using the returned RecordReader.
//
// A partition can be retrieved by using ExecutePartitions on a statement.
func (c *connectionImpl) ReadPartition(ctx context.Context, serializedPartition []byte) (rdr array.RecordReader, err error) {
	var info flight.FlightInfo
	if err := proto.Unmarshal(serializedPartition, &info); err != nil {
		return nil, adbc.Error{
			Msg:  fmt.Sprintf("[flightsql] could not unmarshal partition as FlightInfo: %v", err),
			Code: adbc.StatusInvalidArgument,
		}
	}

	// The driver only ever returns one endpoint.
	if len(info.Endpoint) != 1 {
		return nil, adbc.Error{
			Msg:  fmt.Sprintf("[flightsql] invalid partition: expected 1 endpoint, got %d", len(info.Endpoint)),
			Code: adbc.StatusInvalidArgument,
		}
	}

	ctx = metadata.NewOutgoingContext(ctx, c.hdrs)
	rdr, err = doGetWithLogger(ctx, c.cl, info.Endpoint[0], c.clientCache, c.Logger, c.timeouts)
	if err != nil {
		return nil, adbcFromFlightStatus(err, "ReadPartition(DoGet)")
	}
	return rdr, nil
}

var (
	_ adbc.PostInitOptions = (*connectionImpl)(nil)
)
