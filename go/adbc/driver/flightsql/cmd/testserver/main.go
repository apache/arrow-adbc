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

// A server intended specifically for testing the Flight SQL driver.  Unlike
// the upstream SQLite example, which tries to be functional, this server
// tries to be useful.

package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/flight"
	"github.com/apache/arrow/go/v13/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ExampleServer struct {
	flightsql.BaseServer
}

func (srv *ExampleServer) ClosePreparedStatement(ctx context.Context, request flightsql.ActionClosePreparedStatementRequest) error {
	return nil
}

func (srv *ExampleServer) CreatePreparedStatement(ctx context.Context, req flightsql.ActionCreatePreparedStatementRequest) (result flightsql.ActionCreatePreparedStatementResult, err error) {
	result.Handle = []byte(req.GetQuery())
	return
}

func (srv *ExampleServer) GetFlightInfoPreparedStatement(_ context.Context, cmd flightsql.PreparedStatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if bytes.Equal(cmd.GetPreparedStatementHandle(), []byte("error_do_get")) || bytes.Equal(cmd.GetPreparedStatementHandle(), []byte("error_do_get_stream")) {
		schema := arrow.NewSchema([]arrow.Field{{Name: "ints", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)
		return &flight.FlightInfo{
			Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: desc.Cmd}}},
			FlightDescriptor: desc,
			TotalRecords:     -1,
			TotalBytes:       -1,
			Schema:           flight.SerializeSchema(schema, srv.Alloc),
		}, nil
	}

	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: desc.Cmd}}},
		FlightDescriptor: desc,
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

func (srv *ExampleServer) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	ticket, err := flightsql.CreateStatementQueryTicket(desc.Cmd)
	if err != nil {
		return nil, err
	}

	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: ticket}}},
		FlightDescriptor: desc,
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

func (srv *ExampleServer) DoGetPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery) (schema *arrow.Schema, out <-chan flight.StreamChunk, err error) {
	log.Printf("DoGetPreparedStatement: %v", cmd.GetPreparedStatementHandle())
	if bytes.Equal(cmd.GetPreparedStatementHandle(), []byte("error_do_get")) {
		err = status.Error(codes.InvalidArgument, "expected error")
		return
	}

	schema = arrow.NewSchema([]arrow.Field{{Name: "ints", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)
	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(`[{"a": 5}]`))

	ch := make(chan flight.StreamChunk)
	go func() {
		defer close(ch)
		ch <- flight.StreamChunk{
			Data: rec,
			Desc: nil,
			Err:  nil,
		}
		if bytes.Equal(cmd.GetPreparedStatementHandle(), []byte("error_do_get_stream")) {
			ch <- flight.StreamChunk{
				Data: nil,
				Desc: nil,
				Err:  status.Error(codes.InvalidArgument, "expected error"),
			}
		}
	}()
	out = ch
	return
}

func (srv *ExampleServer) DoGetStatement(ctx context.Context, cmd flightsql.StatementQueryTicket) (schema *arrow.Schema, out <-chan flight.StreamChunk, err error) {
	schema = arrow.NewSchema([]arrow.Field{{Name: "ints", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)
	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(`[{"ints": 5}]`))

	ch := make(chan flight.StreamChunk)
	go func() {
		defer close(ch)
		ch <- flight.StreamChunk{
			Data: rec,
			Desc: nil,
			Err:  nil,
		}
	}()
	out = ch
	return
}

func main() {
	var (
		host = flag.String("host", "localhost", "hostname to bind to")
		port = flag.Int("port", 0, "port to bind to")
	)

	flag.Parse()

	srv := &ExampleServer{}
	srv.Alloc = memory.DefaultAllocator

	server := flight.NewServerWithMiddleware(nil)
	server.RegisterFlightService(flightsql.NewFlightServer(srv))
	server.Init(net.JoinHostPort(*host, strconv.Itoa(*port)))
	server.SetShutdownOnSignals(os.Interrupt, os.Kill)

	fmt.Println("Starting testing Flight SQL Server on", server.Addr(), "...")

	if err := server.Serve(); err != nil {
		log.Fatal(err)
	}
}
