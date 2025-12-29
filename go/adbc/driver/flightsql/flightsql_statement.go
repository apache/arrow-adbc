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
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/bluele/gcache"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

const (
	OptionStatementQueueSize = "adbc.rpc.result_queue_size"
	// Explicitly set substrait version for Flight SQL
	// substrait *does* include the version in the serialized plan
	// so this is not entirely necessary depending on the version
	// of substrait and the capabilities of the server.
	OptionStatementSubstraitVersion = "adbc.flight.sql.substrait.version"
)

func atomicLoadFloat64(x *float64) float64 {
	return math.Float64frombits(atomic.LoadUint64((*uint64)(unsafe.Pointer(x))))
}

func atomicStoreFloat64(x *float64, v float64) {
	atomic.StoreUint64((*uint64)(unsafe.Pointer(x)), math.Float64bits(v))
}

type sqlOrSubstrait struct {
	sqlQuery         string
	substraitPlan    []byte
	substraitVersion string
}

func (s *sqlOrSubstrait) setSqlQuery(query string) {
	s.sqlQuery = query
	s.substraitPlan = nil
}

func (s *sqlOrSubstrait) setSubstraitPlan(plan []byte) {
	s.sqlQuery = ""
	s.substraitPlan = plan
}

func (s *sqlOrSubstrait) execute(ctx context.Context, cnxn *connectionImpl, opts ...grpc.CallOption) (*flight.FlightInfo, error) {
	if s.sqlQuery != "" {
		return cnxn.execute(ctx, s.sqlQuery, opts...)
	} else if s.substraitPlan != nil {
		return cnxn.executeSubstrait(ctx, flightsql.SubstraitPlan{Plan: s.substraitPlan, Version: s.substraitVersion}, opts...)
	}

	return nil, adbc.Error{
		Code: adbc.StatusInvalidState,
		Msg:  "[Flight SQL Statement] cannot call ExecuteQuery without a query or prepared statement",
	}
}

func (s *sqlOrSubstrait) executeSchema(ctx context.Context, cnxn *connectionImpl, opts ...grpc.CallOption) (*arrow.Schema, error) {
	var (
		res *flight.SchemaResult
		err error
	)
	if s.sqlQuery != "" {
		res, err = cnxn.executeSchema(ctx, s.sqlQuery, opts...)
	} else if s.substraitPlan != nil {
		res, err = cnxn.executeSubstraitSchema(ctx, flightsql.SubstraitPlan{Plan: s.substraitPlan, Version: s.substraitVersion}, opts...)
	} else {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "[Flight SQL Statement] cannot call ExecuteQuery without a query or prepared statement",
		}
	}

	if err != nil {
		return nil, err
	}

	return flight.DeserializeSchema(res.Schema, cnxn.cl.Alloc)
}

func (s *sqlOrSubstrait) executeUpdate(ctx context.Context, cnxn *connectionImpl, opts ...grpc.CallOption) (int64, error) {
	if s.sqlQuery != "" {
		return cnxn.executeUpdate(ctx, s.sqlQuery, opts...)
	} else if s.substraitPlan != nil {
		return cnxn.executeSubstraitUpdate(ctx, flightsql.SubstraitPlan{Plan: s.substraitPlan, Version: s.substraitVersion}, opts...)
	}

	return -1, adbc.Error{
		Code: adbc.StatusInvalidState,
		Msg:  "[Flight SQL Statement] cannot call ExecuteUpdate without a query or prepared statement",
	}
}

func (s *sqlOrSubstrait) poll(ctx context.Context, cnxn *connectionImpl, retryDescriptor *flight.FlightDescriptor, opts ...grpc.CallOption) (*flight.PollInfo, error) {
	if s.sqlQuery != "" {
		return cnxn.poll(ctx, s.sqlQuery, retryDescriptor, opts...)
	} else if s.substraitPlan != nil {
		return cnxn.pollSubstrait(ctx, flightsql.SubstraitPlan{Plan: s.substraitPlan, Version: s.substraitVersion}, retryDescriptor, opts...)
	}

	return nil, adbc.Error{
		Code: adbc.StatusInvalidState,
		Msg:  "[Flight SQL] cannot call ExecuteQuery without a query or prepared statement",
	}
}

func (s *sqlOrSubstrait) prepare(ctx context.Context, cnxn *connectionImpl, opts ...grpc.CallOption) (*flightsql.PreparedStatement, error) {
	if s.sqlQuery != "" {
		return cnxn.prepare(ctx, s.sqlQuery, opts...)
	} else if s.substraitPlan != nil {
		return cnxn.prepareSubstrait(ctx, flightsql.SubstraitPlan{Plan: s.substraitPlan, Version: s.substraitVersion}, opts...)
	}

	return nil, adbc.Error{
		Code: adbc.StatusInvalidState,
		Msg:  "[FlightSQL Statement] must call SetSqlQuery before Prepare",
	}
}

type incrementalState struct {
	schema          *arrow.Schema
	previousInfo    *flight.FlightInfo
	retryDescriptor *flight.FlightDescriptor
	complete        bool
}

type statement struct {
	alloc       memory.Allocator
	cnxn        *connectionImpl
	clientCache gcache.Cache

	hdrs             metadata.MD
	query            sqlOrSubstrait
	prepared         *flightsql.PreparedStatement
	queueSize        int
	timeouts         timeoutOption
	incrementalState *incrementalState
	progress         float64
	// may seem redundant, but incrementalState isn't locked
	lastInfo atomic.Pointer[flight.FlightInfo]

	// Bulk ingest fields
	targetTable string
	ingestMode  string
	catalog     *string
	dbSchema    *string
	temporary   bool

	// Bound data for bulk ingest
	bound      arrow.RecordBatch
	streamBind array.RecordReader
}

func (s *statement) closePreparedStatement() error {
	var header, trailer metadata.MD
	err := s.prepared.Close(metadata.NewOutgoingContext(context.Background(), s.hdrs), grpc.Header(&header), grpc.Trailer(&trailer), s.timeouts)
	return adbcFromFlightStatusWithDetails(err, header, trailer, "ClosePreparedStatement")
}

func (s *statement) clearIncrementalQuery() error {
	// retryDescriptor != nil ==> query is in progress
	if s.incrementalState != nil {
		if !s.incrementalState.complete && s.incrementalState.retryDescriptor != nil {
			return adbc.Error{
				Code: adbc.StatusInvalidState,
				Msg:  "[Flight SQL] Cannot disable incremental execution while a query is in progress, finish execution first",
			}
		}
		s.incrementalState = &incrementalState{}
		s.lastInfo.Store(nil)
	}
	return nil
}

func (s *statement) poll(ctx context.Context, opts ...grpc.CallOption) (*flight.PollInfo, error) {
	if s.prepared != nil {
		return s.prepared.ExecutePoll(ctx, s.incrementalState.retryDescriptor, opts...)
	}
	return s.query.poll(ctx, s.cnxn, s.incrementalState.retryDescriptor, opts...)
}

// Close releases any relevant resources associated with this statement
// and closes it (particularly if it is a prepared statement).
//
// A statement instance should not be used after Close is called.
func (s *statement) Close() (err error) {
	if s.prepared != nil {
		err = s.closePreparedStatement()
		s.prepared = nil
	}

	if s.cnxn == nil {
		return adbc.Error{
			Msg:  "[Flight SQL Statement] cannot close already closed statement",
			Code: adbc.StatusInvalidState,
		}
	}

	if s.bound != nil {
		s.bound.Release()
		s.bound = nil
	}
	if s.streamBind != nil {
		s.streamBind.Release()
		s.streamBind = nil
	}

	s.clientCache = nil
	s.cnxn = nil

	return err
}

func (s *statement) GetOption(key string) (string, error) {
	switch key {
	case OptionStatementSubstraitVersion:
		return s.query.substraitVersion, nil
	case OptionTimeoutFetch:
		return s.timeouts.fetchTimeout.String(), nil
	case OptionTimeoutQuery:
		return s.timeouts.queryTimeout.String(), nil
	case OptionTimeoutUpdate:
		return s.timeouts.updateTimeout.String(), nil
	case adbc.OptionKeyIncremental:
		if s.incrementalState != nil {
			return adbc.OptionValueEnabled, nil
		}
		return adbc.OptionValueDisabled, nil
	}

	if strings.HasPrefix(key, OptionRPCCallHeaderPrefix) {
		name := strings.TrimPrefix(key, OptionRPCCallHeaderPrefix)
		values := s.hdrs.Get(name)
		if len(values) > 0 {
			return values[0], nil
		}
	}

	return "", adbc.Error{
		Msg:  fmt.Sprintf("[Flight SQL] Unknown statement option '%s'", key),
		Code: adbc.StatusNotFound,
	}
}
func (s *statement) GetOptionBytes(key string) ([]byte, error) {
	switch key {
	case OptionLastFlightInfo:
		info := s.lastInfo.Load()
		if info == nil {
			return []byte{}, nil
		}
		serialized, err := proto.Marshal(info)
		if err != nil {
			return nil, adbc.Error{
				Msg:  fmt.Sprintf("[Flight SQL] Could not serialize result for '%s': %s", key, err.Error()),
				Code: adbc.StatusInternal,
			}
		}
		return serialized, nil
	}
	return nil, adbc.Error{
		Msg:  fmt.Sprintf("[Flight SQL] Unknown statement option '%s'", key),
		Code: adbc.StatusNotFound,
	}
}
func (s *statement) GetOptionInt(key string) (int64, error) {
	switch key {
	case OptionTimeoutFetch:
		fallthrough
	case OptionTimeoutQuery:
		fallthrough
	case OptionTimeoutUpdate:
		val, err := s.GetOptionDouble(key)
		if err != nil {
			return 0, err
		}
		return int64(val), nil
	}

	return 0, adbc.Error{
		Msg:  fmt.Sprintf("[Flight SQL] Unknown statement option '%s'", key),
		Code: adbc.StatusNotFound,
	}
}
func (s *statement) GetOptionDouble(key string) (float64, error) {
	switch key {
	case OptionTimeoutFetch:
		return s.timeouts.fetchTimeout.Seconds(), nil
	case OptionTimeoutQuery:
		return s.timeouts.queryTimeout.Seconds(), nil
	case OptionTimeoutUpdate:
		return s.timeouts.updateTimeout.Seconds(), nil
	case adbc.OptionKeyProgress:
		return atomicLoadFloat64(&s.progress), nil
	case adbc.OptionKeyMaxProgress:
		return 1.0, nil
	}

	return 0, adbc.Error{
		Msg:  fmt.Sprintf("[Flight SQL] Unknown statement option '%s'", key),
		Code: adbc.StatusNotFound,
	}
}

// SetOption sets a string option on this statement
func (s *statement) SetOption(key string, val string) error {
	if strings.HasPrefix(key, OptionRPCCallHeaderPrefix) {
		name := strings.TrimPrefix(key, OptionRPCCallHeaderPrefix)
		if val == "" {
			s.hdrs.Delete(name)
		} else {
			s.hdrs.Append(name, val)
		}
		return nil
	}

	switch key {
	case OptionTimeoutFetch:
		fallthrough
	case OptionTimeoutQuery:
		fallthrough
	case OptionTimeoutUpdate:
		return s.timeouts.setTimeoutString(key, val)
	case OptionStatementQueueSize:
		var err error
		var size int
		if size, err = strconv.Atoi(val); err != nil {
			return adbc.Error{
				Msg:  fmt.Sprintf("Invalid value for statement option '%s': '%s' is not a positive integer", OptionStatementQueueSize, val),
				Code: adbc.StatusInvalidArgument,
			}
		}
		return s.SetOptionInt(key, int64(size))
	case OptionStatementSubstraitVersion:
		s.query.substraitVersion = val
	case adbc.OptionKeyIncremental:
		switch val {
		case adbc.OptionValueEnabled:
			if err := s.clearIncrementalQuery(); err != nil {
				return err
			}
			s.incrementalState = &incrementalState{}
		case adbc.OptionValueDisabled:
			if err := s.clearIncrementalQuery(); err != nil {
				return err
			}
			s.incrementalState = nil
		default:
			return adbc.Error{
				Msg:  fmt.Sprintf("[Flight SQL] Invalid statement option value %s=%s", key, val),
				Code: adbc.StatusInvalidArgument,
			}
		}
	case adbc.OptionKeyIngestTargetTable:
		s.prepared = nil
		s.query.sqlQuery = ""
		s.query.substraitPlan = nil
		s.targetTable = val
	case adbc.OptionKeyIngestMode:
		switch val {
		case adbc.OptionValueIngestModeCreate,
			adbc.OptionValueIngestModeAppend,
			adbc.OptionValueIngestModeReplace,
			adbc.OptionValueIngestModeCreateAppend:
			s.ingestMode = val
		default:
			return adbc.Error{
				Msg:  fmt.Sprintf("[Flight SQL] Invalid ingest mode '%s'", val),
				Code: adbc.StatusInvalidArgument,
			}
		}
	case adbc.OptionValueIngestTargetCatalog:
		if val == "" {
			s.catalog = nil
		} else {
			s.catalog = &val
		}
	case adbc.OptionValueIngestTargetDBSchema:
		if val == "" {
			s.dbSchema = nil
		} else {
			s.dbSchema = &val
		}
	case adbc.OptionValueIngestTemporary:
		switch val {
		case adbc.OptionValueEnabled:
			s.temporary = true
		case adbc.OptionValueDisabled:
			s.temporary = false
		default:
			return adbc.Error{
				Msg:  fmt.Sprintf("[Flight SQL] Invalid statement option value %s=%s", key, val),
				Code: adbc.StatusInvalidArgument,
			}
		}
	default:
		return adbc.Error{
			Msg:  "[Flight SQL] Unknown statement option '" + key + "'",
			Code: adbc.StatusNotImplemented,
		}
	}
	return nil
}

func (s *statement) SetOptionBytes(key string, value []byte) error {
	return adbc.Error{
		Msg:  fmt.Sprintf("[Flight SQL] Unknown statement option '%s'", key),
		Code: adbc.StatusNotImplemented,
	}
}

func (s *statement) SetOptionInt(key string, value int64) error {
	switch key {
	case OptionStatementQueueSize:
		if value <= 0 {
			return adbc.Error{
				Msg:  fmt.Sprintf("[Flight SQL] Invalid value for statement option '%s': '%d' is not a positive integer", OptionStatementQueueSize, value),
				Code: adbc.StatusInvalidArgument,
			}
		}
		s.queueSize = int(value)
		return nil
	}
	return s.SetOptionDouble(key, float64(value))
}

func (s *statement) SetOptionDouble(key string, value float64) error {
	switch key {
	case OptionTimeoutFetch:
		fallthrough
	case OptionTimeoutQuery:
		fallthrough
	case OptionTimeoutUpdate:
		return s.timeouts.setTimeout(key, value)
	}
	return adbc.Error{
		Msg:  fmt.Sprintf("[Flight SQL] Unknown statement option '%s'", key),
		Code: adbc.StatusNotImplemented,
	}
}

// SetSqlQuery sets the query string to be executed.
//
// The query can then be executed with any of the Execute methods.
// For queries expected to be executed repeatedly, Prepare should be
// called before execution.
func (s *statement) SetSqlQuery(query string) error {
	if s.prepared != nil {
		if err := s.closePreparedStatement(); err != nil {
			return err
		}
		s.prepared = nil
	}
	if err := s.clearIncrementalQuery(); err != nil {
		return err
	}
	s.targetTable = ""
	s.query.setSqlQuery(query)
	return nil
}

// ExecuteQuery executes the current query or prepared statement
// and returns a RecordReader for the results along with the number
// of rows affected if known, otherwise it will be -1.
//
// This invalidates any prior result sets on this statement.
func (s *statement) ExecuteQuery(ctx context.Context) (rdr array.RecordReader, nrec int64, err error) {
	if err := s.clearIncrementalQuery(); err != nil {
		return nil, -1, err
	}

	// Handle bulk ingest
	if s.targetTable != "" {
		nrec, err = s.executeIngest(ctx)
		return nil, nrec, err
	}

	ctx = metadata.NewOutgoingContext(ctx, s.hdrs)
	var info *flight.FlightInfo
	var header, trailer metadata.MD
	opts := append([]grpc.CallOption{}, grpc.Header(&header), grpc.Trailer(&trailer), s.timeouts)
	if s.prepared != nil {
		info, err = s.prepared.Execute(ctx, opts...)
	} else {
		info, err = s.query.execute(ctx, s.cnxn, opts...)
	}

	if err != nil {
		return nil, -1, adbcFromFlightStatusWithDetails(err, header, trailer, "ExecuteQuery")
	}

	nrec = info.TotalRecords
	rdr, err = newRecordReader(ctx, s.alloc, s.cnxn.cl, info, s.clientCache, s.queueSize, s.timeouts)
	return
}

// ExecuteUpdate executes a statement that does not generate a result
// set. It returns the number of rows affected if known, otherwise -1.
func (s *statement) ExecuteUpdate(ctx context.Context) (n int64, err error) {
	if err := s.clearIncrementalQuery(); err != nil {
		return -1, err
	}

	// Handle bulk ingest
	if s.targetTable != "" {
		return s.executeIngest(ctx)
	}

	ctx = metadata.NewOutgoingContext(ctx, s.hdrs)
	var header, trailer metadata.MD
	opts := append([]grpc.CallOption{}, grpc.Header(&header), grpc.Trailer(&trailer), s.timeouts)
	if s.prepared != nil {
		n, err = s.prepared.ExecuteUpdate(ctx, opts...)
	} else {
		n, err = s.query.executeUpdate(ctx, s.cnxn, opts...)
	}

	if err != nil {
		err = adbcFromFlightStatusWithDetails(err, header, trailer, "ExecuteQuery")
	}

	return
}

// Prepare turns this statement into a prepared statement to be executed
// multiple times. This invalidates any prior result sets.
func (s *statement) Prepare(ctx context.Context) error {
	ctx = metadata.NewOutgoingContext(ctx, s.hdrs)
	var header, trailer metadata.MD
	prep, err := s.query.prepare(ctx, s.cnxn, grpc.Header(&header), grpc.Trailer(&trailer), s.timeouts)
	if err != nil {
		return adbcFromFlightStatusWithDetails(err, header, trailer, "Prepare")
	}
	s.prepared = prep
	return nil
}

// SetSubstraitPlan allows setting a serialized Substrait execution
// plan into the query or for querying Substrait-related metadata.
//
// Drivers are not required to support both SQL and Substrait semantics.
// If they do, it may be via converting between representations internally.
//
// Like SetSqlQuery, after this is called the query can be executed
// using any of the Execute methods. If the query is expected to be
// executed repeatedly, Prepare should be called first on the statement.
func (s *statement) SetSubstraitPlan(plan []byte) error {
	if s.prepared != nil {
		if err := s.closePreparedStatement(); err != nil {
			return err
		}
		s.prepared = nil
	}
	if err := s.clearIncrementalQuery(); err != nil {
		return err
	}

	s.query.setSubstraitPlan(plan)
	return nil
}

// Bind uses an arrow record batch to bind parameters to the query.
//
// This can be used for bulk inserts or for prepared statements.
// The driver will call release on the passed in Record when it is done,
// but it may not do this until the statement is closed or another
// record is bound.
func (s *statement) Bind(_ context.Context, values arrow.RecordBatch) error {
	// For bulk ingest, bind to the statement
	if s.targetTable != "" {
		if s.streamBind != nil {
			s.streamBind.Release()
			s.streamBind = nil
		}
		if s.bound != nil {
			s.bound.Release()
		}
		s.bound = values
		if s.bound != nil {
			s.bound.Retain()
		}
		return nil
	}

	if s.prepared == nil {
		return adbc.Error{
			Msg:  "[Flight SQL Statement] must call Prepare before calling Bind",
			Code: adbc.StatusInvalidState}
	}

	// calls retain
	s.prepared.SetParameters(values)
	return nil
}

// BindStream uses a record batch stream to bind parameters for this
// query. This can be used for bulk inserts or prepared statements.
//
// The driver will call Release on the record reader, but may not do this
// until Close is called.
func (s *statement) BindStream(_ context.Context, stream array.RecordReader) error {
	// For bulk ingest, bind to the statement
	if s.targetTable != "" {
		if s.bound != nil {
			s.bound.Release()
			s.bound = nil
		}
		if s.streamBind != nil {
			s.streamBind.Release()
		}
		s.streamBind = stream
		if s.streamBind != nil {
			s.streamBind.Retain()
		}
		return nil
	}

	if s.prepared == nil {
		return adbc.Error{
			Msg:  "[Flight SQL Statement] must call Prepare before calling Bind",
			Code: adbc.StatusInvalidState}
	}

	// calls retain
	s.prepared.SetRecordReader(stream)
	return nil
}

// GetParameterSchema returns an Arrow schema representation of
// the expected parameters to be bound.
//
// This retrieves an Arrow Schema describing the number, names, and
// types of the parameters in a parameterized statement. The fields
// of the schema should be in order of the ordinal position of the
// parameters; named parameters should appear only once.
//
// If the parameter does not have a name, or a name cannot be determined,
// the name of the corresponding field in the schema will be an empty
// string. If the type cannot be determined, the type of the corresponding
// field will be NA (NullType).
//
// This should be called only after calling Prepare.
//
// This should return an error with StatusNotImplemented if the schema
// cannot be determined.
func (s *statement) GetParameterSchema() (*arrow.Schema, error) {
	if s.prepared == nil {
		return nil, adbc.Error{
			Msg:  "[Flight SQL Statement] must call Prepare before GetParameterSchema",
			Code: adbc.StatusInvalidState,
		}
	}

	ret := s.prepared.ParameterSchema()
	if ret == nil {
		return nil, adbc.Error{Code: adbc.StatusNotImplemented}
	}

	return ret, nil
}

// ExecutePartitions executes the current statement and gets the results
// as a partitioned result set.
//
// It returns the Schema of the result set (if available, nil otherwise),
// the collection of partition descriptors and the number of rows affected,
// if known. If unknown, the number of rows affected will be -1.
//
// If the driver does not support partitioned results, this will return
// an error with a StatusNotImplemented code.
func (s *statement) ExecutePartitions(ctx context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	ctx = metadata.NewOutgoingContext(ctx, s.hdrs)

	var (
		info *flight.FlightInfo
		poll *flight.PollInfo
		out  adbc.Partitions
		sc   *arrow.Schema
		err  error
	)

	var header, trailer metadata.MD
	if s.incrementalState != nil {
		if s.incrementalState.complete {
			schema := s.incrementalState.schema
			totalRecords := s.incrementalState.previousInfo.TotalRecords
			// Reset the statement for reuse
			s.incrementalState = &incrementalState{}
			atomicStoreFloat64(&s.progress, 0.0)
			s.lastInfo.Store(nil)
			return schema, adbc.Partitions{}, totalRecords, nil
		}

		backoff := 100 * time.Millisecond
		for {
			// Keep polling until the query completes or we get new partitions
			poll, err = s.poll(ctx, grpc.Header(&header), grpc.Trailer(&trailer), s.timeouts)
			if err != nil {
				break
			}
			info = poll.GetInfo()
			if info == nil {
				// The server is misbehaving
				// XXX: should we also issue a query cancellation?
				s.incrementalState = &incrementalState{}
				atomicStoreFloat64(&s.progress, 0.0)
				return nil, adbc.Partitions{}, -1, adbc.Error{
					Msg:  "[Flight SQL] Server returned a PollInfo with no FlightInfo",
					Code: adbc.StatusInternal,
				}
			}
			info = proto.Clone(info).(*flight.FlightInfo)
			// We only return the new endpoints each time
			if s.incrementalState.previousInfo != nil {
				offset := len(s.incrementalState.previousInfo.Endpoint)
				if offset >= len(info.Endpoint) {
					info.Endpoint = []*flight.FlightEndpoint{}
				} else {
					info.Endpoint = info.Endpoint[offset:]
				}
			}
			s.incrementalState.previousInfo = poll.GetInfo()
			s.incrementalState.retryDescriptor = poll.GetFlightDescriptor()
			atomicStoreFloat64(&s.progress, poll.GetProgress())
			s.lastInfo.Store(poll.GetInfo())

			if s.incrementalState.retryDescriptor == nil {
				// Query is finished
				s.incrementalState.complete = true
				break
			} else if len(info.Endpoint) > 0 {
				// Query made progress
				break
			}
			// Back off before next poll
			time.Sleep(backoff)
			backoff *= 2
			if backoff > 5000*time.Millisecond {
				backoff = 5000 * time.Millisecond
			}
		}

		// Special case: the query completed but there were no new endpoints. We
		// return 0 new partitions, and also reset the statement (because
		// returning 0 partitions implies completion)
		if s.incrementalState.complete && len(info.Endpoint) == 0 {
			s.incrementalState = &incrementalState{}
			atomicStoreFloat64(&s.progress, 0.0)
			s.lastInfo.Store(nil)
		}
	} else if s.prepared != nil {
		info, err = s.prepared.Execute(ctx, grpc.Header(&header), grpc.Trailer(&trailer), s.timeouts)
	} else {
		info, err = s.query.execute(ctx, s.cnxn, grpc.Header(&header), grpc.Trailer(&trailer), s.timeouts)
	}

	if err != nil {
		return nil, out, -1, adbcFromFlightStatusWithDetails(err, header, trailer, "ExecutePartitions")
	}

	if len(info.Schema) > 0 {
		sc, err = flight.DeserializeSchema(info.Schema, s.alloc)
		if err != nil {
			return nil, out, -1, adbcFromFlightStatus(err, "ExecutePartitions: could not deserialize FlightInfo schema:")
		}
	}

	if s.incrementalState != nil {
		s.incrementalState.schema = sc
	}

	out.NumPartitions = uint64(len(info.Endpoint))
	out.PartitionIDs = make([][]byte, out.NumPartitions)
	for i, e := range info.Endpoint {
		partition := proto.Clone(info).(*flight.FlightInfo)
		partition.Endpoint = []*flight.FlightEndpoint{e}
		data, err := proto.Marshal(partition)
		if err != nil {
			return sc, out, -1, adbc.Error{
				Msg:  err.Error(),
				Code: adbc.StatusInternal,
			}
		}

		out.PartitionIDs[i] = data
	}

	return sc, out, info.TotalRecords, nil
}

// ExecuteSchema gets the schema of the result set of a query without executing it.
func (s *statement) ExecuteSchema(ctx context.Context) (schema *arrow.Schema, err error) {
	ctx = metadata.NewOutgoingContext(ctx, s.hdrs)

	if s.prepared != nil {
		schema = s.prepared.DatasetSchema()
		if schema == nil {
			err = adbc.Error{
				Msg:  "[Flight SQL Statement] Database server did not provide schema for prepared statement",
				Code: adbc.StatusNotImplemented,
			}
		}
		return
	}

	var header, trailer metadata.MD
	schema, err = s.query.executeSchema(ctx, s.cnxn, grpc.Header(&header), grpc.Trailer(&trailer), s.timeouts)
	if err != nil {
		err = adbcFromFlightStatusWithDetails(err, header, trailer, "ExecuteSchema")
	}
	return
}
