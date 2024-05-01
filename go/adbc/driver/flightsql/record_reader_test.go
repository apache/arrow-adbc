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
	"net/url"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/apache/arrow/go/v17/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bluele/gcache"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func orderingSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "epIndex", Type: arrow.PrimitiveTypes.Int8},
		{Name: "batchIndex", Type: arrow.PrimitiveTypes.Int8},
	}, nil)
}

type testFlightService struct {
	flight.BaseFlightServer
	alloc        memory.Allocator
	failureCount int
}

func (f *testFlightService) DoGet(request *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	// Crude way to make requests fail until retried enough times
	if f.failureCount > 0 {
		f.failureCount--
		return fmt.Errorf("Failed request")
	}

	schema := orderingSchema()
	wr := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
	defer wr.Close()

	builder := array.NewRecordBuilder(f.alloc, schema)
	defer builder.Release()

	epIndex := builder.Field(0).(*array.Int8Builder)
	batchIndex := builder.Field(1).(*array.Int8Builder)

	for idx := int8(0); idx < 4; idx++ {
		epIndex.Append(int8(request.Ticket[0]))
		batchIndex.Append(idx)

		rec := builder.NewRecord()
		defer rec.Release()
		if err := wr.Write(rec); err != nil {
			return err
		}
	}

	return nil
}

func getFlightClientTest(ctx context.Context, loc string) (*flightsql.Client, error) {
	uri, err := url.Parse(loc)
	if err != nil {
		return nil, err
	}

	return flightsql.NewClient(uri.Host, nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

type RecordReaderTests struct {
	suite.Suite

	alloc   *memory.CheckedAllocator
	server  flight.Server
	service *testFlightService
	cl      *flightsql.Client
	clCache gcache.Cache
}

func (suite *RecordReaderTests) SetupSuite() {
	suite.alloc = memory.NewCheckedAllocator(memory.DefaultAllocator)

	suite.server = flight.NewServerWithMiddleware(nil)
	suite.NoError(suite.server.Init("localhost:0"))
	suite.service = &testFlightService{alloc: suite.alloc}
	suite.server.RegisterFlightService(suite.service)

	go func() {
		// Explicitly ignore error
		_ = suite.server.Serve()
	}()

	var err error
	suite.cl, err = flightsql.NewClient(suite.server.Addr().String(), nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	suite.NoError(err)

	suite.clCache = gcache.New(20).LRU().
		LoaderFunc(func(loc interface{}) (interface{}, error) {
			uri, ok := loc.(string)
			if !ok {
				return nil, adbc.Error{Code: adbc.StatusInternal}
			}

			cl, err := getFlightClientTest(context.Background(), uri)
			if err != nil {
				return nil, err
			}

			cl.Alloc = suite.alloc
			return cl, nil
		}).
		EvictedFunc(func(_, client interface{}) {
			conn := client.(*flightsql.Client)
			conn.Close()
		}).Build()
}

func (suite *RecordReaderTests) TearDownSuite() {
	suite.cl.Close()
	suite.clCache.Purge()
	suite.server.Shutdown()
	suite.alloc.AssertSize(suite.T(), 0)
}

func (suite *RecordReaderTests) TestFallbackFailedConnection() {
	goodLocation := "grpc://" + suite.server.Addr().String()
	badLocation := "grpc://127.0.0.2:1234"
	info := flight.FlightInfo{
		Schema: flight.SerializeSchema(orderingSchema(), suite.alloc),
		Endpoint: []*flight.FlightEndpoint{
			{
				Ticket:   &flight.Ticket{Ticket: []byte{0}},
				Location: []*flight.Location{{Uri: badLocation}, {Uri: goodLocation}},
			},
		},
	}

	reader, err := newRecordReader(context.Background(), suite.alloc, suite.cl, &info, suite.clCache, 3)
	suite.NoError(err)
	defer reader.Release()

	suite.True(reader.Schema().Equal(orderingSchema()))
	suite.True(reader.Next())
	suite.True(reader.Next())
	suite.True(reader.Next())
	suite.True(reader.Next())
	suite.False(reader.Next())
	suite.NoError(reader.Err())
}

func (suite *RecordReaderTests) TestFallbackFailedDoGet() {
	defer func() {
		suite.service.failureCount = 0
	}()

	suite.service.failureCount = 2
	goodLocation := "grpc://" + suite.server.Addr().String()
	info := flight.FlightInfo{
		Schema: flight.SerializeSchema(orderingSchema(), suite.alloc),
		Endpoint: []*flight.FlightEndpoint{
			{
				Ticket:   &flight.Ticket{Ticket: []byte{0}},
				Location: []*flight.Location{{Uri: goodLocation}, {Uri: goodLocation}, {Uri: goodLocation}},
			},
		},
	}

	reader, err := newRecordReader(context.Background(), suite.alloc, suite.cl, &info, suite.clCache, 3)
	suite.NoError(err)
	defer reader.Release()

	suite.True(reader.Schema().Equal(orderingSchema()))
	suite.True(reader.Next())
	suite.True(reader.Next())
	suite.True(reader.Next())
	suite.True(reader.Next())
	suite.False(reader.Next())
	suite.NoError(reader.Err())

	// Not enough retries
	suite.service.failureCount = 4
	reader, err = newRecordReader(context.Background(), suite.alloc, suite.cl, &info, suite.clCache, 3)
	suite.NoError(err)
	defer reader.Release()
	suite.False(reader.Next())
	suite.Error(reader.Err())
}

func (suite *RecordReaderTests) TestFallbackFailed() {
	badLocation := "grpc://127.0.0.2:1234"
	info := flight.FlightInfo{
		Schema: flight.SerializeSchema(orderingSchema(), suite.alloc),
		Endpoint: []*flight.FlightEndpoint{
			{
				Ticket:   &flight.Ticket{Ticket: []byte{0}},
				Location: []*flight.Location{{Uri: badLocation}, {Uri: badLocation}},
			},
		},
	}

	reader, err := newRecordReader(context.Background(), suite.alloc, suite.cl, &info, suite.clCache, 3)
	suite.NoError(err)
	defer reader.Release()

	suite.False(reader.Next())
	suite.Error(reader.Err())
}

func (suite *RecordReaderTests) TestNoEndpoints() {
	info := flight.FlightInfo{
		Schema: flight.SerializeSchema(orderingSchema(), suite.alloc),
	}

	reader, err := newRecordReader(context.Background(), suite.alloc, suite.cl, &info, suite.clCache, 3)
	suite.NoError(err)
	defer reader.Release()

	suite.True(reader.Schema().Equal(orderingSchema()))
	suite.False(reader.Next())
	suite.NoError(reader.Err())
}

func (suite *RecordReaderTests) TestNoEndpointsNoSchema() {
	info := flight.FlightInfo{}

	_, err := newRecordReader(context.Background(), suite.alloc, suite.cl, &info, suite.clCache, 3)
	suite.ErrorContains(err, "Server returned FlightInfo with no schema and no endpoints, cannot read stream")
}

func (suite *RecordReaderTests) TestNoEndpointsInvalidSchema() {
	info := flight.FlightInfo{
		Schema: []byte("f"),
	}

	_, err := newRecordReader(context.Background(), suite.alloc, suite.cl, &info, suite.clCache, 3)
	suite.ErrorContains(err, "Server returned FlightInfo with invalid schema and no endpoints, cannot read stream")
}

func (suite *RecordReaderTests) TestNoSchema() {
	location := "grpc://" + suite.server.Addr().String()
	info := flight.FlightInfo{
		Endpoint: []*flight.FlightEndpoint{
			{
				Ticket:   &flight.Ticket{Ticket: []byte{0}},
				Location: []*flight.Location{{Uri: location}},
			},
		},
	}

	reader, err := newRecordReader(context.Background(), suite.alloc, suite.cl, &info, suite.clCache, 3)
	suite.NoError(err)
	defer reader.Release()

	suite.True(reader.Schema().Equal(orderingSchema()))
	suite.True(reader.Next())
	suite.True(reader.Next())
	suite.True(reader.Next())
	suite.True(reader.Next())
	suite.False(reader.Next())
	suite.NoError(reader.Err())
}

func (suite *RecordReaderTests) TestSchemaEndpointMismatch() {
	location := "grpc://" + suite.server.Addr().String()
	badSchema := arrow.NewSchema([]arrow.Field{
		{Name: "epIndex", Type: arrow.PrimitiveTypes.Int32},
		{Name: "batchIndex", Type: arrow.PrimitiveTypes.Int32},
	}, nil)
	info := flight.FlightInfo{
		Schema: flight.SerializeSchema(badSchema, suite.alloc),
		Endpoint: []*flight.FlightEndpoint{
			{
				Ticket:   &flight.Ticket{Ticket: []byte{0}},
				Location: []*flight.Location{{Uri: location}},
			},
			{
				Ticket:   &flight.Ticket{Ticket: []byte{1}},
				Location: []*flight.Location{{Uri: location}},
			},
		},
	}

	reader, err := newRecordReader(context.Background(), suite.alloc, suite.cl, &info, suite.clCache, 3)
	suite.NoError(err)
	defer reader.Release()

	suite.True(reader.Schema().Equal(badSchema))
	suite.False(reader.Next())
	suite.ErrorContains(reader.Err(), "returned inconsistent schema: expected schema:")
}

func (suite *RecordReaderTests) TestOrdering() {
	// Info with a ton of endpoints; we want to make sure data comes back in order
	location := "grpc://" + suite.server.Addr().String()
	info := flight.FlightInfo{
		Schema: flight.SerializeSchema(orderingSchema(), suite.alloc),
		Endpoint: []*flight.FlightEndpoint{
			{
				Ticket:   &flight.Ticket{Ticket: []byte{0}},
				Location: []*flight.Location{{Uri: location}},
			},
			{
				Ticket:   &flight.Ticket{Ticket: []byte{1}},
				Location: []*flight.Location{{Uri: location}},
			},
			{
				Ticket:   &flight.Ticket{Ticket: []byte{2}},
				Location: []*flight.Location{{Uri: location}},
			},
			{
				Ticket:   &flight.Ticket{Ticket: []byte{3}},
				Location: []*flight.Location{{Uri: location}},
			},
		},
	}

	reader, err := newRecordReader(context.Background(), suite.alloc, suite.cl, &info, suite.clCache, 3)
	suite.NoError(err)
	defer reader.Release()

	for epIdx := int8(0); epIdx < 4; epIdx++ {
		for batchIdx := int8(0); batchIdx < 4; batchIdx++ {
			suite.True(reader.Next())
			rec := reader.Record()
			// don't need to manually release this record because we never
			// call retain. Each call to Next releases the previous record

			suite.True(rec.Schema().Equal(orderingSchema()))
			suite.Equal(int64(1), rec.NumRows())

			epIndices := rec.Column(0).(*array.Int8)
			batchIndices := rec.Column(1).(*array.Int8)
			suite.True(epIndices.IsValid(0))
			suite.True(batchIndices.IsValid(0))
			suite.Equal(epIdx, epIndices.Value(0))
			suite.Equal(batchIdx, batchIndices.Value(0))
		}
	}
	suite.False(reader.Next())
	suite.NoError(reader.Err())
}

func TestRecordReader(t *testing.T) {
	suite.Run(t, &RecordReaderTests{})
}
