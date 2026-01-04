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
//
// Supports optional OAuth authentication and TLS for testing OAuth flows.

package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type RecordedHeader struct {
	method string
	header string
	value  string
}

type ExampleServer struct {
	flightsql.BaseServer

	mu            sync.Mutex
	pollingStatus map[string]int
	headers       []RecordedHeader
}

var recordedHeadersSchema = arrow.NewSchema([]arrow.Field{
	{Name: "method", Type: arrow.BinaryTypes.String, Nullable: false},
	{Name: "header", Type: arrow.BinaryTypes.String, Nullable: false},
	{Name: "value", Type: arrow.BinaryTypes.String, Nullable: false},
}, nil)

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

func (srv *ExampleServer) recordHeaders(ctx context.Context, method string) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		panic("Misuse of recordHeaders")
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()
	for k, vv := range md {
		for _, v := range vv {
			log.Printf("Header: %s: %s = %s\n", method, k, v)
			srv.headers = append(srv.headers, RecordedHeader{
				method: method, header: k, value: v,
			})
		}
	}
}

func (srv *ExampleServer) BeginTransaction(ctx context.Context, req flightsql.ActionBeginTransactionRequest) ([]byte, error) {
	srv.recordHeaders(ctx, "BeginTransaction")
	return []byte("foo"), nil
}

func (srv *ExampleServer) EndTransaction(ctx context.Context, req flightsql.ActionEndTransactionRequest) error {
	srv.recordHeaders(ctx, "EndTransaction")
	return nil
}

func (srv *ExampleServer) ClosePreparedStatement(ctx context.Context, request flightsql.ActionClosePreparedStatementRequest) error {
	srv.recordHeaders(ctx, "ClosePreparedStatement")
	return nil
}

func (srv *ExampleServer) CreatePreparedStatement(ctx context.Context, req flightsql.ActionCreatePreparedStatementRequest) (result flightsql.ActionCreatePreparedStatementResult, err error) {
	srv.recordHeaders(ctx, "CreatePreparedStatement")
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

func (srv *ExampleServer) GetFlightInfoPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	srv.recordHeaders(ctx, "GetFlightInfoPreparedStatement")
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
	srv.recordHeaders(ctx, "GetFlightInfoStatement")
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
	srv.recordHeaders(ctx, "DoGetPreparedStatement")
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
		var rec arrow.RecordBatch
		rec, _, err = array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(`[{"ints": 5}]`))
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
	case "recorded_headers":
		schema = recordedHeadersSchema
		ch := make(chan flight.StreamChunk)

		methods := array.NewStringBuilder(srv.Alloc)
		headers := array.NewStringBuilder(srv.Alloc)
		values := array.NewStringBuilder(srv.Alloc)
		defer methods.Release()
		defer headers.Release()
		defer values.Release()

		srv.mu.Lock()
		defer srv.mu.Unlock()

		count := int64(0)
		for _, recorded := range srv.headers {
			count++
			methods.AppendString(recorded.method)
			headers.AppendString(recorded.header)
			values.AppendString(recorded.value)
		}
		srv.headers = make([]RecordedHeader, 0)

		rec := array.NewRecordBatch(recordedHeadersSchema, []arrow.Array{
			methods.NewArray(),
			headers.NewArray(),
			values.NewArray(),
		}, count)

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
	srv.recordHeaders(ctx, "DoPutPreparedStatementQuery")
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

func (srv *ExampleServer) GetFlightInfoCatalogs(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	srv.recordHeaders(ctx, "GetFlightInfoCatalogs")
	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: desc.Cmd}}},
		FlightDescriptor: desc,
		Schema:           flight.SerializeSchema(schema_ref.Catalogs, srv.Alloc),
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

func (srv *ExampleServer) DoGetCatalogs(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	srv.recordHeaders(ctx, "DoGetCatalogs")

	// Just return some dummy data
	schema := schema_ref.Catalogs
	ch := make(chan flight.StreamChunk, 1)
	catalogs, _, err := array.FromJSON(srv.Alloc, arrow.BinaryTypes.String, strings.NewReader(`["catalog"]`))
	if err != nil {
		return nil, nil, err
	}
	defer catalogs.Release()

	batch := array.NewRecordBatch(schema, []arrow.Array{catalogs}, 1)
	ch <- flight.StreamChunk{Data: batch}
	close(ch)
	return schema, ch, nil
}

func (srv *ExampleServer) GetFlightInfoSchemas(ctx context.Context, req flightsql.GetDBSchemas, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	srv.recordHeaders(ctx, "GetFlightInfoDBSchemas")
	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: desc.Cmd}}},
		FlightDescriptor: desc,
		Schema:           flight.SerializeSchema(schema_ref.DBSchemas, srv.Alloc),
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

func (srv *ExampleServer) DoGetDBSchemas(ctx context.Context, req flightsql.GetDBSchemas) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	srv.recordHeaders(ctx, "DoGetDBSchemas")

	// Just return some dummy data
	schema := schema_ref.DBSchemas
	ch := make(chan flight.StreamChunk, 1)
	// Not really a proper match, but good enough
	if req.GetDBSchemaFilterPattern() == nil || *req.GetDBSchemaFilterPattern() == "" || *req.GetDBSchemaFilterPattern() == "main" {
		catalogs, _, err := array.FromJSON(srv.Alloc, arrow.BinaryTypes.String, strings.NewReader(`["main"]`))
		if err != nil {
			return nil, nil, err
		}
		defer catalogs.Release()

		dbSchemas, _, err := array.FromJSON(srv.Alloc, arrow.BinaryTypes.String, strings.NewReader(`[""]`))
		if err != nil {
			return nil, nil, err
		}
		defer dbSchemas.Release()

		batch := array.NewRecordBatch(schema, []arrow.Array{catalogs, dbSchemas}, 1)
		ch <- flight.StreamChunk{Data: batch}
	}
	close(ch)
	return schema, ch, nil
}

func (srv *ExampleServer) GetFlightInfoTables(ctx context.Context, req flightsql.GetTables, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	srv.recordHeaders(ctx, "GetFlightInfoTables")
	schema := schema_ref.Tables
	if req.GetIncludeSchema() {
		schema = schema_ref.TablesWithIncludedSchema
	}
	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: desc.Cmd}}},
		FlightDescriptor: desc,
		Schema:           flight.SerializeSchema(schema, srv.Alloc),
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

func (srv *ExampleServer) DoGetTables(ctx context.Context, req flightsql.GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	srv.recordHeaders(ctx, "DoGetTables")
	// Just return some dummy data
	schema := schema_ref.Tables
	if req.GetIncludeSchema() {
		schema = schema_ref.TablesWithIncludedSchema
	}
	ch := make(chan flight.StreamChunk, 1)
	close(ch)
	return schema, ch, nil
}

func (srv *ExampleServer) SetSessionOptions(ctx context.Context, req *flight.SetSessionOptionsRequest) (*flight.SetSessionOptionsResult, error) {
	srv.recordHeaders(ctx, "SetSessionOptions")
	return &flight.SetSessionOptionsResult{}, nil
}

func (srv *ExampleServer) GetSessionOptions(ctx context.Context, req *flight.GetSessionOptionsRequest) (*flight.GetSessionOptionsResult, error) {
	srv.recordHeaders(ctx, "GetSessionOptions")
	return &flight.GetSessionOptionsResult{}, nil
}

func (srv *ExampleServer) CloseSession(ctx context.Context, req *flight.CloseSessionRequest) (*flight.CloseSessionResult, error) {
	srv.recordHeaders(ctx, "CloseSession")
	return &flight.CloseSessionResult{}, nil
}

// Hardcoded test credentials for Basic authentication
const (
	testBasicUsername = "user"
	testBasicPassword = "password"
)

// createAuthMiddleware creates gRPC interceptors that validate Bearer tokens or Basic auth.
// If tokenPrefix is empty, no validation is performed (authentication disabled).
// Supports both:
//   - Bearer tokens: validated against the tokenPrefix
//   - Basic auth: validated against hardcoded test credentials (user:password)
func createAuthMiddleware(tokenPrefix string) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	validateAuth := func(ctx context.Context) error {
		if tokenPrefix == "" {
			return nil // No authentication required
		}

		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return status.Error(codes.InvalidArgument, "missing metadata")
		}

		auth := md.Get("authorization")
		if len(auth) == 0 {
			return status.Error(codes.Unauthenticated, "missing authorization header")
		}

		authHeader := auth[0]

		// Check for Basic authentication
		if strings.HasPrefix(authHeader, "Basic ") {
			encoded := strings.TrimPrefix(authHeader, "Basic ")
			decoded, err := base64.StdEncoding.DecodeString(encoded)
			if err != nil {
				log.Printf("Basic auth decode failed: %v", err)
				return status.Error(codes.Unauthenticated, "invalid basic auth encoding")
			}

			credentials := string(decoded)
			parts := strings.SplitN(credentials, ":", 2)
			if len(parts) != 2 {
				return status.Error(codes.Unauthenticated, "invalid basic auth format")
			}

			username, password := parts[0], parts[1]
			if username == testBasicUsername && password == testBasicPassword {
				log.Printf("Basic auth validated for user: %s", username)
				return nil
			}
			log.Printf("Basic auth failed: invalid credentials for user: %s", username)
			return status.Error(codes.Unauthenticated, "invalid credentials")
		}

		// Check for Bearer token authentication
		if strings.HasPrefix(authHeader, "Bearer ") {
			bearerToken := strings.TrimPrefix(authHeader, "Bearer ")
			if !strings.HasPrefix(bearerToken, tokenPrefix) {
				log.Printf("Token validation failed: token=%s, expected prefix=%s", bearerToken, tokenPrefix)
				return status.Error(codes.Unauthenticated, "invalid token")
			}

			log.Printf("Token validated: %s", bearerToken[:min(len(bearerToken), 20)]+"...")
			return nil
		}

		return status.Error(codes.Unauthenticated, "invalid authorization format, expected 'Bearer <token>' or 'Basic <credentials>'")
	}

	unary := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if err := validateAuth(ctx); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}

	stream := func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := validateAuth(ss.Context()); err != nil {
			return err
		}
		return handler(srv, ss)
	}

	return unary, stream
}

// generateSelfSignedCert generates a self-signed TLS certificate for testing
func generateSelfSignedCert() (tls.Certificate, []byte, error) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("failed to generate private key: %w", err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"ADBC Test Server"},
			CommonName:   "localhost",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("0.0.0.0")},
		DNSNames:              []string{"localhost"},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("failed to create certificate: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("failed to create key pair: %w", err)
	}

	return cert, certPEM, nil
}

func main() {
	var (
		host        = flag.String("host", "localhost", "hostname to bind to")
		port        = flag.Int("port", 0, "port to bind to")
		useTLS      = flag.Bool("tls", false, "Enable TLS with self-signed certificate")
		tokenPrefix = flag.String("token-prefix", "", "Required prefix for valid Bearer tokens (empty = no auth)")
		certFile    = flag.String("cert-file", "", "Path to write the PEM certificate (for client verification)")
	)

	flag.Parse()

	srv := &ExampleServer{pollingStatus: make(map[string]int)}
	srv.Alloc = memory.DefaultAllocator
	if err := srv.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerTransaction, int32(flightsql.SqlTransactionTransaction)); err != nil {
		log.Fatal(err)
	}

	// Create middleware (OAuth validation if token-prefix is set)
	var middleware []flight.ServerMiddleware
	if *tokenPrefix != "" {
		unary, stream := createAuthMiddleware(*tokenPrefix)
		middleware = append(middleware, flight.ServerMiddleware{Unary: unary, Stream: stream})
	}

	addr := net.JoinHostPort(*host, strconv.Itoa(*port))
	var server flight.Server

	if *useTLS {
		cert, certPEM, err := generateSelfSignedCert()
		if err != nil {
			log.Fatalf("Failed to generate TLS certificate: %v", err)
		}

		if *certFile != "" {
			if err := os.WriteFile(*certFile, certPEM, 0644); err != nil {
				log.Fatalf("Failed to write certificate file: %v", err)
			}
			log.Printf("Certificate written to %s", *certFile)
		}

		tlsConfig := &tls.Config{Certificates: []tls.Certificate{cert}}
		server = flight.NewServerWithMiddleware(middleware, grpc.Creds(credentials.NewTLS(tlsConfig)))
	} else {
		server = flight.NewServerWithMiddleware(middleware)
	}

	server.RegisterFlightService(flightsql.NewFlightServer(srv))
	if err := server.Init(addr); err != nil {
		log.Fatal(err)
	}
	server.SetShutdownOnSignals(os.Interrupt, os.Kill)

	// Build descriptive startup message
	features := []string{}
	if *useTLS {
		features = append(features, "TLS")
	}
	if *tokenPrefix != "" {
		features = append(features, fmt.Sprintf("OAuth(prefix=%s)", *tokenPrefix))
	}
	if len(features) > 0 {
		fmt.Printf("Starting testing Flight SQL Server on %s with %s...\n", server.Addr(), strings.Join(features, ", "))
	} else {
		fmt.Println("Starting testing Flight SQL Server on", server.Addr(), "...")
	}

	if err := server.Serve(); err != nil {
		log.Fatal(err)
	}
}
