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
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/apache/arrow/go/v17/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type ExampleServer struct {
	flightsql.BaseServer

	mu            sync.Mutex
	pollingStatus map[string]int
}

func StatusWithDetail(code codes.Code, message string, details ...proto.Message) error {
	p := status.New(code, message).Proto()
	// Have to do this by hand because gRPC uses deprecated proto import
	for _, detail := range details {
		any, err := anypb.New(detail)
		if err != nil {
			panic(err)
		}
		p.Details = append(p.Details, any)
	}
	return status.FromProto(p).Err()
}

func (srv *ExampleServer) ClosePreparedStatement(ctx context.Context, request flightsql.ActionClosePreparedStatementRequest) error {
	return nil
}

func (srv *ExampleServer) CreatePreparedStatement(ctx context.Context, req flightsql.ActionCreatePreparedStatementRequest) (result flightsql.ActionCreatePreparedStatementResult, err error) {
	switch req.GetQuery() {
	case "error_create_prepared_statement":
		err = status.Error(codes.InvalidArgument, "expected error (DoAction)")
		return
	case "error_create_prepared_statement_detail":
		detail1 := wrapperspb.String("detail1")
		detail2 := wrapperspb.String("detail2")
		err = StatusWithDetail(codes.InvalidArgument, "expected error (DoAction)", detail1, detail2)
		return
	}
	result.Handle = []byte(req.GetQuery())
	return
}

func (srv *ExampleServer) GetFlightInfoPreparedStatement(_ context.Context, cmd flightsql.PreparedStatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	switch string(cmd.GetPreparedStatementHandle()) {
	case "error_do_get", "error_do_get_stream", "error_do_get_detail", "error_do_get_stream_detail", "forever":
		schema := arrow.NewSchema([]arrow.Field{{Name: "ints", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)
		return &flight.FlightInfo{
			Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: desc.Cmd}}},
			FlightDescriptor: desc,
			TotalRecords:     -1,
			TotalBytes:       -1,
			Schema:           flight.SerializeSchema(schema, srv.Alloc),
		}, nil
	case "error_get_flight_info":
		return nil, status.Error(codes.InvalidArgument, "expected error (GetFlightInfo)")
	case "error_get_flight_info_detail":
		detail1 := wrapperspb.String("detail1")
		detail2 := wrapperspb.String("detail2")
		return nil, StatusWithDetail(codes.InvalidArgument, "expected error (GetFlightInfo)", detail1, detail2)
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

func (srv *ExampleServer) PollFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.PollInfo, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	var val wrapperspb.StringValue
	var err error
	if err = proto.Unmarshal(desc.Cmd, &val); err != nil {
		return nil, err
	}

	ticket, err := flightsql.CreateStatementQueryTicket([]byte(val.Value))
	if err != nil {
		return nil, err
	}

	if val.Value == "forever" {
		srv.pollingStatus[val.Value]++
		return &flight.PollInfo{
			Info: &flight.FlightInfo{
				Schema:           flight.SerializeSchema(arrow.NewSchema([]arrow.Field{{Name: "ints", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil), srv.Alloc),
				Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: ticket}}},
				FlightDescriptor: desc,
				TotalRecords:     -1,
				TotalBytes:       -1,
				AppMetadata:      []byte("app metadata"),
			},
			FlightDescriptor: desc,
			Progress:         proto.Float64(float64(srv.pollingStatus[val.Value]) / 100.0),
		}, nil
	}

	srv.pollingStatus[val.Value]--
	progress := srv.pollingStatus[val.Value]

	numEndpoints := 5 - progress
	endpoints := make([]*flight.FlightEndpoint, numEndpoints)
	for i := range endpoints {
		endpoints[i] = &flight.FlightEndpoint{Ticket: &flight.Ticket{Ticket: ticket}}
	}

	var schema []byte
	if progress < 3 {
		schema = flight.SerializeSchema(arrow.NewSchema([]arrow.Field{{Name: "ints", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil), srv.Alloc)
	}
	if progress == 0 {
		desc = nil
	}

	if val.Value == "error_poll_later" && progress == 3 {
		return nil, StatusWithDetail(codes.Unavailable, "expected error (PollFlightInfo)")
	}

	return &flight.PollInfo{
		Info: &flight.FlightInfo{
			Schema:           schema,
			Endpoint:         endpoints,
			FlightDescriptor: desc,
			TotalRecords:     -1,
			TotalBytes:       -1,
		},
		FlightDescriptor: desc,
		Progress:         proto.Float64(1.0 - (float64(progress) / 5.0)),
	}, nil
}

func (srv *ExampleServer) PollFlightInfoPreparedStatement(ctx context.Context, query flightsql.PreparedStatementQuery, desc *flight.FlightDescriptor) (*flight.PollInfo, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	switch string(query.GetPreparedStatementHandle()) {
	case "error_poll":
		detail1 := wrapperspb.String("detail1")
		detail2 := wrapperspb.String("detail2")
		return nil, StatusWithDetail(codes.InvalidArgument, "expected error (PollFlightInfo)", detail1, detail2)
	case "finish_immediately":
		ticket, err := flightsql.CreateStatementQueryTicket(query.GetPreparedStatementHandle())
		if err != nil {
			return nil, err
		}
		return &flight.PollInfo{
			Info: &flight.FlightInfo{
				Schema:           flight.SerializeSchema(arrow.NewSchema([]arrow.Field{{Name: "ints", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil), srv.Alloc),
				Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: ticket}}},
				FlightDescriptor: desc,
				TotalRecords:     -1,
				TotalBytes:       -1,
			},
			FlightDescriptor: nil,
			Progress:         proto.Float64(1.0),
		}, nil
	}

	descriptor, err := proto.Marshal(&wrapperspb.StringValue{Value: string(query.GetPreparedStatementHandle())})
	if err != nil {
		return nil, err
	}

	srv.pollingStatus[string(query.GetPreparedStatementHandle())] = 5
	return &flight.PollInfo{
		Info: &flight.FlightInfo{
			Schema:           nil,
			Endpoint:         []*flight.FlightEndpoint{},
			FlightDescriptor: desc,
			TotalRecords:     -1,
			TotalBytes:       -1,
		},
		FlightDescriptor: &flight.FlightDescriptor{
			Type: flight.DescriptorCMD,
			Cmd:  descriptor,
		},
		Progress: proto.Float64(0.0),
	}, nil
}

func (srv *ExampleServer) DoGetPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery) (schema *arrow.Schema, out <-chan flight.StreamChunk, err error) {
	log.Printf("DoGetPreparedStatement: %v", cmd.GetPreparedStatementHandle())
	switch string(cmd.GetPreparedStatementHandle()) {
	case "error_do_get":
		err = status.Error(codes.InvalidArgument, "expected error (DoGet)")
		return
	case "error_do_get_detail":
		detail1 := wrapperspb.String("detail1")
		detail2 := wrapperspb.String("detail2")
		err = StatusWithDetail(codes.InvalidArgument, "expected error (DoGet)", detail1, detail2)
		return
	case "forever":
		ch := make(chan flight.StreamChunk)
		schema = arrow.NewSchema([]arrow.Field{{Name: "ints", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)
		var rec arrow.Record
		rec, _, err = array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(`[{"a": 5}]`))
		go func() {
			// wait for client cancel
			<-ctx.Done()
			defer close(ch)

			// arrow-go crashes if we don't give this
			ch <- flight.StreamChunk{
				Data: rec,
				Desc: nil,
				Err:  nil,
			}
		}()
		out = ch
		return
	case "stateless_prepared_statement":
		err = status.Error(codes.InvalidArgument, "client didn't use the updated handle")
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
		switch string(cmd.GetPreparedStatementHandle()) {
		case "error_do_get_stream":
			ch <- flight.StreamChunk{
				Data: nil,
				Desc: nil,
				Err:  status.Error(codes.InvalidArgument, "expected stream error (DoGet)"),
			}
		case "error_do_get_stream_detail":
			detail1 := wrapperspb.String("detail1")
			detail2 := wrapperspb.String("detail2")
			ch <- flight.StreamChunk{
				Data: nil,
				Desc: nil,
				Err:  StatusWithDetail(codes.InvalidArgument, "expected stream error (DoGet)", detail1, detail2),
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

func (srv *ExampleServer) DoPutPreparedStatementQuery(ctx context.Context, cmd flightsql.PreparedStatementQuery, reader flight.MessageReader, writer flight.MetadataWriter) ([]byte, error) {
	switch string(cmd.GetPreparedStatementHandle()) {
	case "error_do_put":
		return nil, status.Error(codes.Unknown, "expected error (DoPut)")
	case "error_do_put_detail":
		detail1 := wrapperspb.String("detail1")
		detail2 := wrapperspb.String("detail2")
		return nil, StatusWithDetail(codes.Unknown, "expected error (DoPut)", detail1, detail2)
	case "stateless_prepared_statement":
		return []byte("expected prepared statement handle"), nil
	}

	return nil, status.Error(codes.Unimplemented, fmt.Sprintf("DoPutPreparedStatementQuery not implemented: %s", string(cmd.GetPreparedStatementHandle())))
}

func (srv *ExampleServer) DoPutPreparedStatementUpdate(context.Context, flightsql.PreparedStatementUpdate, flight.MessageReader) (int64, error) {
	return 0, status.Error(codes.Unimplemented, "DoPutPreparedStatementUpdate not implemented")
}

func main() {
	var (
		host = flag.String("host", "localhost", "hostname to bind to")
		port = flag.Int("port", 0, "port to bind to")
	)

	flag.Parse()

	srv := &ExampleServer{pollingStatus: make(map[string]int)}
	srv.Alloc = memory.DefaultAllocator

	server := flight.NewServerWithMiddleware(nil)
	server.RegisterFlightService(flightsql.NewFlightServer(srv))
	if err := server.Init(net.JoinHostPort(*host, strconv.Itoa(*port))); err != nil {
		log.Fatal(err)
	}
	server.SetShutdownOnSignals(os.Interrupt, os.Kill)

	fmt.Println("Starting testing Flight SQL Server on", server.Addr(), "...")

	if err := server.Serve(); err != nil {
		log.Fatal(err)
	}
}
