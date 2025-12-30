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

// Tests that use custom server implementations.

package flightsql_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/apache/arrow-adbc/go/adbc"
	driver "github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal"
	"github.com/apache/arrow-adbc/go/adbc/validation"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	flightproto "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/stretchr/testify/suite"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// ---- Common Infra --------------------

type ServerBasedTests struct {
	suite.Suite

	s    flight.Server
	db   adbc.Database
	cnxn adbc.Connection
}

func (suite *ServerBasedTests) DoSetupSuite(srv flightsql.Server, srvMiddleware []flight.ServerMiddleware, dbArgs map[string]string, dialOpts ...grpc.DialOption) {
	suite.setupFlightServer(srv, srvMiddleware)

	suite.setupDatabase(dbArgs, dialOpts...)
}

func (suite *ServerBasedTests) setupDatabase(dbArgs map[string]string, dialOpts ...grpc.DialOption) {
	var err error
	uri := "grpc+tcp://" + suite.s.Addr().String()

	args := map[string]string{
		"uri": uri,
	}
	maps.Copy(args, dbArgs)
	suite.db, err = (driver.NewDriver(memory.DefaultAllocator)).NewDatabaseWithOptions(args, dialOpts...)
	suite.Require().NoError(err)
}

func (suite *ServerBasedTests) setupFlightServer(srv flightsql.Server, srvMiddleware []flight.ServerMiddleware, srvOpts ...grpc.ServerOption) {
	suite.s = flight.NewServerWithMiddleware(srvMiddleware, srvOpts...)
	suite.s.RegisterFlightService(flightsql.NewFlightServer(srv))
	suite.Require().NoError(suite.s.Init("localhost:0"))
	suite.s.SetShutdownOnSignals(os.Interrupt, os.Kill)
	go func() {
		_ = suite.s.Serve()
	}()
}

func (suite *ServerBasedTests) SetupTest() {
	var err error
	suite.cnxn, err = suite.db.Open(context.Background())
	suite.Require().NoError(err)
}

func (suite *ServerBasedTests) TearDownTest() {
	suite.Require().NoError(suite.cnxn.Close())
}

func (suite *ServerBasedTests) TearDownSuite() {
	suite.NoError(suite.db.Close())
	suite.db = nil
	suite.s.Shutdown()
}

func (suite *ServerBasedTests) generateCertOption() (*tls.Config, string) {
	// Generate a self-signed certificate in-process for testing
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	suite.Require().NoError(err)
	certTemplate := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Unit Tests Incorporated"},
		},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	certDer, err := x509.CreateCertificate(rand.Reader, &certTemplate, &certTemplate, &privKey.PublicKey, privKey)
	suite.Require().NoError(err)
	buffer := &bytes.Buffer{}
	suite.Require().NoError(pem.Encode(buffer, &pem.Block{Type: "CERTIFICATE", Bytes: certDer}))
	certBytes := make([]byte, buffer.Len())
	copy(certBytes, buffer.Bytes())
	buffer.Reset()
	suite.Require().NoError(pem.Encode(buffer, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privKey)}))
	keyBytes := make([]byte, buffer.Len())
	copy(keyBytes, buffer.Bytes())

	cert, err := tls.X509KeyPair(certBytes, keyBytes)
	suite.Require().NoError(err)

	suite.Require().NoError(err)
	tlsConfig := &tls.Config{Certificates: []tls.Certificate{cert}}
	return tlsConfig, string(certBytes)
}

func (suite *ServerBasedTests) openAndExecuteQuery(query string) {
	var err error
	suite.cnxn, err = suite.db.Open(context.Background())
	suite.Require().NoError(err)
	defer validation.CheckedClose(suite.T(), suite.cnxn)

	stmt, err := suite.cnxn.NewStatement()
	suite.Require().NoError(err)
	defer validation.CheckedClose(suite.T(), stmt)

	suite.Require().NoError(stmt.SetSqlQuery(query))
	reader, _, err := stmt.ExecuteQuery(context.Background())
	suite.NoError(err)
	defer reader.Release()
}

// ---- Tests --------------------

func TestAuthn(t *testing.T) {
	suite.Run(t, &AuthnTests{})
}

func TestGrpcDialerOptions(t *testing.T) {
	suite.Run(t, &DialerOptionsTests{})
}

func TestErrorDetails(t *testing.T) {
	suite.Run(t, &ErrorDetailsTests{})
}

func TestExecuteSchema(t *testing.T) {
	suite.Run(t, &ExecuteSchemaTests{})
}

func TestIncrementalPoll(t *testing.T) {
	suite.Run(t, &IncrementalPollTests{})
}

func TestTimeout(t *testing.T) {
	suite.Run(t, &TimeoutTests{})
}

func TestCookies(t *testing.T) {
	suite.Run(t, &CookieTests{})
}

func TestDataType(t *testing.T) {
	suite.Run(t, &DataTypeTests{})
}

func TestMultiTable(t *testing.T) {
	suite.Run(t, &MultiTableTests{})
}

func TestSessionOptions(t *testing.T) {
	suite.Run(t, &SessionOptionTests{})
}

func TestGetObjects(t *testing.T) {
	suite.Run(t, &GetObjectsTests{})
}

func TestBulkIngest(t *testing.T) {
	suite.Run(t, &BulkIngestTests{})
}

func TestOauth(t *testing.T) {
	suite.Run(t, &OAuthTests{})
}

// ---- AuthN Tests --------------------

type AuthnTestServer struct {
	flightsql.BaseServer
}

func (server *AuthnTestServer) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	md := metadata.MD{}
	md.Set("authorization", "Bearer final")
	if err := grpc.SendHeader(ctx, md); err != nil {
		return nil, err
	}
	tkt, _ := flightsql.CreateStatementQueryTicket([]byte{})
	info := &flight.FlightInfo{
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{
			{Ticket: &flight.Ticket{Ticket: tkt}},
		},
		TotalRecords: -1,
		TotalBytes:   -1,
	}
	return info, nil
}

func (server *AuthnTestServer) DoGetStatement(ctx context.Context, tkt flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	sc := arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)
	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, sc, strings.NewReader(`[{"a": 5}]`))
	if err != nil {
		return nil, nil, err
	}

	ch := make(chan flight.StreamChunk)
	go func() {
		defer close(ch)
		ch <- flight.StreamChunk{
			Data: rec,
			Desc: nil,
			Err:  nil,
		}
	}()
	return sc, ch, nil
}

func authnTestUnary(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "Could not get metadata")
	}
	auth := md.Get("authorization")
	if len(auth) == 0 {
		return nil, status.Error(codes.Unauthenticated, "No token")
	} else if auth[0] != "Bearer initial" {
		return nil, status.Error(codes.Unauthenticated, "Invalid token for unary call: "+auth[0])
	}

	md.Set("authorization", "Bearer final")
	ctx = metadata.NewOutgoingContext(ctx, md)
	return handler(ctx, req)
}

func authnTestStream(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	md, ok := metadata.FromIncomingContext(ss.Context())
	if !ok {
		return status.Error(codes.InvalidArgument, "Could not get metadata")
	}
	auth := md.Get("authorization")
	if len(auth) == 0 {
		return status.Error(codes.Unauthenticated, "No token")
	} else if auth[0] != "Bearer final" {
		return status.Error(codes.Unauthenticated, "Invalid token for stream call: "+auth[0])
	}

	return handler(srv, ss)
}

type AuthnTests struct {
	ServerBasedTests
}

func (suite *AuthnTests) SetupSuite() {
	suite.setupFlightServer(&AuthnTestServer{}, []flight.ServerMiddleware{
		{Stream: authnTestStream, Unary: authnTestUnary},
	})
}

func (suite *AuthnTests) SetupTest() {
	suite.setupDatabase(map[string]string{
		"uri": "grpc+tcp://" + suite.s.Addr().String(),
	})
}

func (suite *AuthnTests) TearDownTest() {
	suite.NoError(suite.db.Close())
	suite.db = nil
}

func (suite *AuthnTests) TearDownSuite() {
	suite.s.Shutdown()
}

func (suite *AuthnTests) TestBearerTokenUpdated() {
	err := suite.db.SetOptions(map[string]string{
		driver.OptionAuthorizationHeader: "Bearer initial",
	})
	suite.Require().NoError(err)

	// apache/arrow-adbc#584: when setting the auth header directly, the client should use any updated token value from the server if given

	suite.openAndExecuteQuery("a-query")
}

type OAuthTests struct {
	ServerBasedTests

	oauthServer     *httptest.Server
	mockOAuthServer *MockOAuthServer
	pemCert         string
}

// MockOAuthServer simulates an OAuth 2.0 server for testing
type MockOAuthServer struct {
	// Track calls to validate server behavior
	clientCredentialsCalls int
	tokenExchangeCalls     int
}

func (m *MockOAuthServer) handleTokenRequest(w http.ResponseWriter, r *http.Request) {
	// Parse the form to get the request parameters
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	grantType := r.FormValue("grant_type")

	switch grantType {
	case "client_credentials":
		m.clientCredentialsCalls++
		// Validate client credentials
		clientID := r.FormValue("client_id")
		clientSecret := r.FormValue("client_secret")

		if clientID == "test-client" && clientSecret == "test-secret" {
			// Return a valid token response
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"access_token": "test-client-token",
				"token_type": "bearer",
				"expires_in": 3600
			}`))

			return
		}

	case "urn:ietf:params:oauth:grant-type:token-exchange":
		m.tokenExchangeCalls++
		// Validate token exchange parameters
		subjectToken := r.FormValue("subject_token")
		subjectTokenType := r.FormValue("subject_token_type")

		if subjectToken == "test-subject-token" &&
			subjectTokenType == "urn:ietf:params:oauth:token-type:jwt" {
			// Return a valid token response
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"access_token": "test-exchanged-token",
				"token_type": "bearer",
				"expires_in": 3600
			}`))
			return
		}
	}

	// Default: return error for invalid request
	http.Error(w, "Invalid request", http.StatusBadRequest)
}

func oauthTestUnary(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "Could not get metadata")
	}
	auth := md.Get("authorization")
	if len(auth) == 0 {
		return nil, status.Error(codes.Unauthenticated, "No token")
	} else if auth[0] != "Bearer test-exchanged-token" && auth[0] != "Bearer test-client-token" {
		return nil, status.Error(codes.Unauthenticated, "Invalid token for unary call: "+auth[0])
	}

	md.Set("authorization", "Bearer final")
	ctx = metadata.NewOutgoingContext(ctx, md)
	return handler(ctx, req)
}

func (suite *OAuthTests) SetupSuite() {

	tlsConfig, pemCertString := suite.generateCertOption()
	suite.pemCert = pemCertString

	suite.mockOAuthServer = &MockOAuthServer{}
	suite.oauthServer = httptest.NewUnstartedServer(http.HandlerFunc(suite.mockOAuthServer.handleTokenRequest))
	suite.oauthServer.TLS = tlsConfig
	suite.oauthServer.StartTLS()

	suite.setupFlightServer(&AuthnTestServer{}, []flight.ServerMiddleware{
		{Unary: oauthTestUnary},
	}, grpc.Creds(credentials.NewTLS(tlsConfig)))
}

func (suite *OAuthTests) TearDownSuite() {
	suite.oauthServer.Close()
	suite.s.Shutdown()
}

func (suite *OAuthTests) SetupTest() {
	suite.setupDatabase(map[string]string{
		"uri": "grpc+tls://" + suite.s.Addr().String(),
	})
}

func (suite *OAuthTests) TearDownTest() {
	suite.NoError(suite.db.Close())
	suite.db = nil
}

func (suite *OAuthTests) TestTokenExchangeFlow() {
	err := suite.db.SetOptions(map[string]string{
		driver.OptionKeyOauthFlow:        driver.TokenExchange,
		driver.OptionKeySubjectToken:     "test-subject-token",
		driver.OptionKeySubjectTokenType: "urn:ietf:params:oauth:token-type:jwt",
		driver.OptionKeyTokenURI:         suite.oauthServer.URL,
		driver.OptionSSLRootCerts:        suite.pemCert,
	})
	suite.Require().NoError(err)

	suite.openAndExecuteQuery("a-query")
	suite.Equal(1, suite.mockOAuthServer.tokenExchangeCalls, "Token exchange flow should be called once")
}

func (suite *OAuthTests) TestClientCredentialsFlow() {
	err := suite.db.SetOptions(map[string]string{
		driver.OptionKeyOauthFlow:    driver.ClientCredentials,
		driver.OptionKeyClientId:     "test-client",
		driver.OptionKeyClientSecret: "test-secret",
		driver.OptionKeyTokenURI:     suite.oauthServer.URL,
		driver.OptionSSLRootCerts:    suite.pemCert,
	})
	suite.Require().NoError(err)

	suite.cnxn, err = suite.db.Open(context.Background())
	suite.Require().NoError(err)
	defer validation.CheckedClose(suite.T(), suite.cnxn)

	suite.openAndExecuteQuery("a-query")
	// golang/oauth2 tries to call the token endpoint sending the client credentials in the authentication header,
	// if it fails, it retries sending the client credentials in the request body.
	// See https://code.google.com/p/goauth2/issues/detail?id=31 for background.
	suite.Equal(2, suite.mockOAuthServer.clientCredentialsCalls, "Client credentials flow should be called once")
}

func (suite *OAuthTests) TestFailOauthWithTokenSet() {
	err := suite.db.SetOptions(map[string]string{
		driver.OptionAuthorizationHeader: "Bearer test-client-token",
		driver.OptionKeyOauthFlow:        driver.ClientCredentials,
		driver.OptionKeyClientId:         "test-client",
		driver.OptionKeyClientSecret:     "test-secret",
		driver.OptionKeyTokenURI:         suite.oauthServer.URL,
	})
	suite.Error(err, "Expected error for missing parameters")
	suite.Contains(err.Error(), "Authentication conflict: Use either Authorization header OR username/password parameter")
}

func (suite *OAuthTests) TestMissingRequiredParamsTokenExchange() {
	testCases := []struct {
		name             string
		options          map[string]string
		expectedErrorMsg string
	}{
		{
			name: "Missing token",
			options: map[string]string{
				driver.OptionKeyOauthFlow:        driver.TokenExchange,
				driver.OptionKeySubjectTokenType: "urn:ietf:params:oauth:token-type:jwt",
				driver.OptionKeyTokenURI:         suite.oauthServer.URL,
			},
			expectedErrorMsg: "token exchange grant requires adbc.flight.sql.oauth.exchange.subject_token",
		},
		{
			name: "Missing subject token type",
			options: map[string]string{
				driver.OptionKeyOauthFlow:    driver.TokenExchange,
				driver.OptionKeySubjectToken: "test-subject-token",
				driver.OptionKeyTokenURI:     suite.oauthServer.URL,
			},
			expectedErrorMsg: "token exchange grant requires adbc.flight.sql.oauth.exchange.subject_token_type",
		},
		{
			name: "Missing token URI",
			options: map[string]string{
				driver.OptionKeyOauthFlow:        driver.TokenExchange,
				driver.OptionKeySubjectToken:     "test-subject-token",
				driver.OptionKeySubjectTokenType: "urn:ietf:params:oauth:token-type:jwt",
			},
			expectedErrorMsg: "token exchange grant requires adbc.flight.sql.oauth.token_uri",
		},
	}

	for _, tc := range testCases {
		suite.Run(tc.name, func() {
			// We need to set options with the driver's SetOptions method
			err := suite.db.SetOptions(tc.options)
			suite.Error(err, "Expected error for missing parameters")
			suite.Contains(err.Error(), tc.expectedErrorMsg)
		})
	}
}
func (suite *OAuthTests) TestMissingRequiredParamsClientCredentials() {
	testCases := []struct {
		name             string
		options          map[string]string
		expectedErrorMsg string
	}{
		{
			name: "Missing client ID",
			options: map[string]string{
				driver.OptionKeyOauthFlow:    driver.ClientCredentials,
				driver.OptionKeyClientSecret: "test-secret",
				driver.OptionKeyTokenURI:     suite.oauthServer.URL,
			},
			expectedErrorMsg: "client credentials grant requires adbc.flight.sql.oauth.client_id",
		},
		{
			name: "Missing client secret",
			options: map[string]string{
				driver.OptionKeyOauthFlow: driver.ClientCredentials,
				driver.OptionKeyClientId:  "test-client",
				driver.OptionKeyTokenURI:  suite.oauthServer.URL,
			},
			expectedErrorMsg: "client credentials grant requires adbc.flight.sql.oauth.client_secret",
		},
		{
			name: "Missing token URI",
			options: map[string]string{
				driver.OptionKeyOauthFlow:    driver.ClientCredentials,
				driver.OptionKeyClientId:     "test-client",
				driver.OptionKeyClientSecret: "test-secret",
			},
			expectedErrorMsg: "client credentials grant requires adbc.flight.sql.oauth.token_uri",
		},
	}

	for _, tc := range testCases {
		suite.Run(tc.name, func() {
			// We need to set options with the driver's SetOptions method
			err := suite.db.SetOptions(tc.options)
			suite.Error(err, "Expected error for missing parameters")
			suite.Contains(err.Error(), tc.expectedErrorMsg)
		})
	}
}

func (suite *OAuthTests) TestInvalidOAuthFlow() {
	err := suite.db.SetOptions(map[string]string{
		driver.OptionKeyOauthFlow:    "invalid-flow",
		driver.OptionKeySubjectToken: "test-token",
	})

	suite.Error(err, "Expected error for invalid OAuth flow")
	suite.Contains(err.Error(), "Not Implemented: oauth flow not implemented: invalid-flow")
}

// ---- Grpc Dialer Options Tests --------------

type DialerOptionsTests struct {
	ServerBasedTests
	statsHandler *dialerOptionsGrpcStatsHandler
}

type dialerOptionsGrpcStatsHandler struct {
	connectionsHandled int
	rpcsHandled        int
	connectionsTagged  int
	rpcsTagged         int
}

func (d *dialerOptionsGrpcStatsHandler) HandleConn(ctx context.Context, stat stats.ConnStats) {
	d.connectionsHandled++
}
func (d *dialerOptionsGrpcStatsHandler) HandleRPC(ctx context.Context, stat stats.RPCStats) {
	d.rpcsHandled++
}
func (d *dialerOptionsGrpcStatsHandler) TagConn(ctx context.Context, stat *stats.ConnTagInfo) context.Context {
	d.connectionsTagged++
	return ctx
}
func (d *dialerOptionsGrpcStatsHandler) TagRPC(ctx context.Context, stat *stats.RPCTagInfo) context.Context {
	d.rpcsTagged++
	return ctx
}

func (suite *DialerOptionsTests) SetupSuite() {
	suite.statsHandler = &dialerOptionsGrpcStatsHandler{}
	suite.DoSetupSuite(&AuthnTestServer{}, nil, nil, grpc.WithStatsHandler(suite.statsHandler))
}

// TestGrpcObserved validates that the grpc stats handler that was passed through correctly to the underlying grpc client.
func (suite *DialerOptionsTests) TestGrpcObserved() {
	stmt, err := suite.cnxn.NewStatement()
	suite.Require().NoError(err)
	defer validation.CheckedClose(suite.T(), stmt)

	suite.Require().NoError(stmt.SetSqlQuery("timeout"))
	reader, _, err := stmt.ExecuteQuery(context.Background())
	suite.NoError(err)
	defer reader.Release()

	suite.Less(0, suite.statsHandler.connectionsTagged)
	suite.Less(0, suite.statsHandler.connectionsHandled)
	suite.Less(0, suite.statsHandler.rpcsTagged)
	suite.Less(0, suite.statsHandler.rpcsHandled)
}

// ---- Error Details Tests --------------------

type ErrorDetailsTestServer struct {
	flightsql.BaseServer
}

func (srv *ErrorDetailsTestServer) GetFlightInfoStatement(ctx context.Context, query flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if query.GetQuery() == "details" {
		detail := wrapperspb.Int32Value{Value: 42}
		st, err := status.New(codes.Unknown, "details").WithDetails(&detail)
		if err != nil {
			return nil, err
		}
		return nil, st.Err()
	} else if query.GetQuery() == "query" {
		tkt, err := flightsql.CreateStatementQueryTicket([]byte("fetch"))
		if err != nil {
			panic(err)
		}
		return &flight.FlightInfo{Endpoint: []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: tkt}}}}, nil
	} else if query.GetQuery() == "vendorcode" {
		return nil, status.Errorf(codes.ResourceExhausted, "Resource exhausted")
	} else if query.GetQuery() == "binaryheader" {
		if err := grpc.SendHeader(ctx, metadata.Pairs("x-header-bin", string([]byte{0, 110}))); err != nil {
			return nil, err
		}
		if err := grpc.SetTrailer(ctx, metadata.Pairs("x-trailer-bin", string([]byte{111, 0, 112}))); err != nil {
			return nil, err
		}
		return nil, status.Errorf(codes.FailedPrecondition, "Resource exhausted")
	}
	return nil, status.Errorf(codes.Unimplemented, "GetSchemaStatement not implemented")
}

func (ts *ErrorDetailsTestServer) DoGetStatement(ctx context.Context, tkt flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	sc := arrow.NewSchema([]arrow.Field{}, nil)
	detail := wrapperspb.Int32Value{Value: 42}
	st, err := status.New(codes.Unknown, "details").WithDetails(&detail)
	if err != nil {
		return nil, nil, err
	}

	ch := make(chan flight.StreamChunk)
	go func() {
		defer close(ch)
		ch <- flight.StreamChunk{
			Data: nil,
			Desc: nil,
			Err:  st.Err(),
		}
	}()
	return sc, ch, nil
}

type ErrorDetailsTests struct {
	ServerBasedTests
}

func (suite *ErrorDetailsTests) SetupSuite() {
	srv := ErrorDetailsTestServer{}
	srv.Alloc = memory.DefaultAllocator
	suite.DoSetupSuite(&srv, nil, nil)
}

func (ts *ErrorDetailsTests) TestBinaryDetails() {
	stmt, err := ts.cnxn.NewStatement()
	ts.NoError(err)
	defer validation.CheckedClose(ts.T(), stmt)

	ts.NoError(stmt.SetSqlQuery("binaryheader"))

	_, _, err = stmt.ExecuteQuery(context.Background())
	var adbcErr adbc.Error
	ts.ErrorAs(err, &adbcErr)

	ts.Equal(int32(codes.FailedPrecondition), adbcErr.VendorCode)

	ts.Equal(2, len(adbcErr.Details))

	headerFound := false
	trailerFound := false
	for _, wrapper := range adbcErr.Details {
		switch wrapper.Key() {
		case "x-header-bin":
			val, err := wrapper.Serialize()
			ts.NoError(err)
			ts.Equal([]byte{0, 110}, val)
			headerFound = true
		case "x-trailer-bin":
			val, err := wrapper.Serialize()
			ts.NoError(err)
			ts.Equal([]byte{111, 0, 112}, val)
			trailerFound = true
		default:
			ts.Failf("Unexpected detail key: %s", wrapper.Key())
		}
	}
	ts.Truef(headerFound, "Did not find x-header-bin")
	ts.Truef(trailerFound, "Did not find x-trailer-bin")
}

func (ts *ErrorDetailsTests) TestGetFlightInfo() {
	stmt, err := ts.cnxn.NewStatement()
	ts.NoError(err)
	defer validation.CheckedClose(ts.T(), stmt)

	ts.NoError(stmt.SetSqlQuery("details"))

	_, _, err = stmt.ExecuteQuery(context.Background())
	var adbcErr adbc.Error
	ts.ErrorAs(err, &adbcErr)

	ts.Equal(int32(codes.Unknown), adbcErr.VendorCode)

	ts.Equal(1, len(adbcErr.Details))

	wrapper := adbcErr.Details[0]
	ts.Equal("grpc-status-details-bin", wrapper.Key())

	raw, err := wrapper.Serialize()
	ts.NoError(err)
	any := anypb.Any{}
	ts.NoError(proto.Unmarshal(raw, &any))
	message := wrappers.Int32Value{}
	ts.NoError(any.UnmarshalTo(&message))
	ts.Equal(int32(42), message.Value)
}

func (ts *ErrorDetailsTests) TestDoGet() {
	stmt, err := ts.cnxn.NewStatement()
	ts.NoError(err)
	defer validation.CheckedClose(ts.T(), stmt)

	ts.NoError(stmt.SetSqlQuery("query"))

	reader, _, err := stmt.ExecuteQuery(context.Background())
	ts.NoError(err)

	defer reader.Release()

	for reader.Next() {
	}
	err = reader.Err()

	ts.Error(err)

	var adbcErr adbc.Error
	ts.ErrorAs(err, &adbcErr, "Error was: %#v", err)

	ts.Equal(1, len(adbcErr.Details))

	wrapper := adbcErr.Details[0]
	ts.Equal("grpc-status-details-bin", wrapper.Key())

	raw, err := wrapper.Serialize()
	ts.NoError(err)
	any := anypb.Any{}
	ts.NoError(proto.Unmarshal(raw, &any))
	message := wrappers.Int32Value{}
	ts.NoError(any.UnmarshalTo(&message))
	ts.Equal(int32(42), message.Value)
}

func (ts *ErrorDetailsTests) TestVendorCode() {
	stmt, err := ts.cnxn.NewStatement()
	ts.NoError(err)
	defer validation.CheckedClose(ts.T(), stmt)

	ts.NoError(stmt.SetSqlQuery("vendorcode"))

	_, _, err = stmt.ExecuteQuery(context.Background())
	var adbcErr adbc.Error
	ts.ErrorAs(err, &adbcErr)

	ts.Equal(int32(codes.ResourceExhausted), adbcErr.VendorCode)
}

// ---- ExecuteSchema Tests --------------------

type ExecuteSchemaTestServer struct {
	flightsql.BaseServer
}

func (srv *ExecuteSchemaTestServer) GetSchemaStatement(ctx context.Context, query flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	if query.GetQuery() == "sample query" {
		return &flight.SchemaResult{
			Schema: flight.SerializeSchema(arrow.NewSchema([]arrow.Field{
				{Name: "ints", Type: arrow.PrimitiveTypes.Int32},
			}, nil), srv.Alloc),
		}, nil
	}
	return nil, status.Errorf(codes.Unimplemented, "GetSchemaStatement not implemented")
}

func (srv *ExecuteSchemaTestServer) CreatePreparedStatement(ctx context.Context, req flightsql.ActionCreatePreparedStatementRequest) (res flightsql.ActionCreatePreparedStatementResult, err error) {
	if req.GetQuery() == "sample query" {
		return flightsql.ActionCreatePreparedStatementResult{
			DatasetSchema: arrow.NewSchema([]arrow.Field{
				{Name: "ints", Type: arrow.PrimitiveTypes.Int32},
			}, nil),
		}, nil
	}
	return flightsql.ActionCreatePreparedStatementResult{}, status.Error(codes.Unimplemented, "CreatePreparedStatement not implemented")
}

func (srv *ExecuteSchemaTestServer) ClosePreparedStatement(ctx context.Context, req flightsql.ActionClosePreparedStatementRequest) error {
	return nil
}

type ExecuteSchemaTests struct {
	ServerBasedTests
}

func (suite *ExecuteSchemaTests) SetupSuite() {
	srv := ExecuteSchemaTestServer{}
	srv.Alloc = memory.DefaultAllocator
	suite.DoSetupSuite(&srv, nil, nil)
}

func (ts *ExecuteSchemaTests) TestNoQuery() {
	stmt, err := ts.cnxn.NewStatement()
	ts.NoError(err)
	defer validation.CheckedClose(ts.T(), stmt)

	es := stmt.(adbc.StatementExecuteSchema)
	_, err = es.ExecuteSchema(context.Background())

	var adbcErr adbc.Error
	ts.ErrorAs(err, &adbcErr)
	ts.Equal(adbc.StatusInvalidState, adbcErr.Code, adbcErr.Error())
}

func (ts *ExecuteSchemaTests) TestPreparedQuery() {
	stmt, err := ts.cnxn.NewStatement()
	ts.NoError(err)
	defer validation.CheckedClose(ts.T(), stmt)

	ts.NoError(stmt.SetSqlQuery("sample query"))
	ts.NoError(stmt.Prepare(context.Background()))

	es := stmt.(adbc.StatementExecuteSchema)
	schema, err := es.ExecuteSchema(context.Background())
	ts.NoError(err)
	ts.NotNil(schema)

	expectedSchema := arrow.NewSchema([]arrow.Field{
		{Name: "ints", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	ts.True(expectedSchema.Equal(schema), schema.String())
}

func (ts *ExecuteSchemaTests) TestQuery() {
	stmt, err := ts.cnxn.NewStatement()
	ts.NoError(err)
	defer validation.CheckedClose(ts.T(), stmt)

	ts.NoError(stmt.SetSqlQuery("sample query"))

	es := stmt.(adbc.StatementExecuteSchema)
	schema, err := es.ExecuteSchema(context.Background())
	ts.NoError(err)
	ts.NotNil(schema)

	expectedSchema := arrow.NewSchema([]arrow.Field{
		{Name: "ints", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	ts.True(expectedSchema.Equal(schema), schema.String())
}

// ---- IncrementalPoll Tests --------------------

type IncrementalQuery struct {
	query     string
	nextIndex int
	// if set, then return an error in the next poll and unset
	// for testing the client's error handling
	unavailable bool
}

type IncrementalPollTestServer struct {
	flightsql.BaseServer
	mu        sync.Mutex
	queries   map[string]*IncrementalQuery
	testCases map[string]IncrementalPollTestCase
}

var unavailableCase = IncrementalPollTestCase{
	progress: []int{1, 1},
}

func (srv *IncrementalPollTestServer) PollFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.PollInfo, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	var val wrapperspb.StringValue
	var err error
	if err = proto.Unmarshal(desc.Cmd, &val); err != nil {
		return nil, err
	}
	queryId := val.Value
	progress := int64(0)
	if strings.Contains(queryId, ";") {
		parts := strings.SplitN(queryId, ";", 2)
		queryId = parts[0]
		progress, err = strconv.ParseInt(parts[1], 10, 32)
		if err != nil {
			return nil, err
		}
	}

	query, ok := srv.queries[queryId]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Query ID not found")
	}

	if query.query == "infinite" {
		query.nextIndex++

		descriptor, err := proto.Marshal(&wrapperspb.StringValue{Value: queryId})
		if err != nil {
			return nil, err
		}
		return &flight.PollInfo{
			Info: &flight.FlightInfo{
				Schema: nil,
				Endpoint: []*flight.FlightEndpoint{{
					Ticket: &flight.Ticket{
						Ticket: []byte{},
					},
				}},
				AppMetadata: []byte("app metadata"),
			},
			FlightDescriptor: &flight.FlightDescriptor{
				Type: flight.DescriptorCMD,
				Cmd:  descriptor,
			},
			// always makes a bit of progress, never gets anywhere
			Progress: proto.Float64(float64(query.nextIndex) / 100.0),
		}, nil
	}

	testCase, ok := srv.testCases[query.query]
	if !ok {
		if query.query == "unavailable" {
			testCase = unavailableCase
		} else {
			return nil, status.Errorf(codes.Unimplemented, "Invalid case %s", query.query)
		}
	}

	if testCase.differentRetryDescriptor && progress != int64(query.nextIndex) {
		return nil, status.Errorf(codes.InvalidArgument, "Used wrong retry descriptor, expected %d but got %d", query.nextIndex, progress)
	}

	if query.unavailable {
		query.unavailable = false
		return nil, status.Errorf(codes.Unavailable, "Server temporarily unavailable")
	}

	return srv.MakePollInfo(&testCase, query, queryId)
}

func (srv *IncrementalPollTestServer) PollFlightInfoStatement(ctx context.Context, query flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.PollInfo, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	queryId := uuid.New().String()

	if query.GetQuery() == "unavailable" {
		srv.queries[queryId] = &IncrementalQuery{
			query:       query.GetQuery(),
			nextIndex:   0,
			unavailable: true,
		}

		return srv.MakePollInfo(&unavailableCase, srv.queries[queryId], queryId)
	} else if query.GetQuery() == "infinite" {
		srv.queries[queryId] = &IncrementalQuery{
			query:     query.GetQuery(),
			nextIndex: 0,
		}

		descriptor, err := proto.Marshal(&wrapperspb.StringValue{Value: queryId})
		if err != nil {
			return nil, err
		}
		return &flight.PollInfo{
			Info: &flight.FlightInfo{
				Schema: nil,
				Endpoint: []*flight.FlightEndpoint{{
					Ticket: &flight.Ticket{
						Ticket: []byte{},
					},
				}},
				AppMetadata: []byte("app metadata"),
			},
			FlightDescriptor: &flight.FlightDescriptor{
				Type: flight.DescriptorCMD,
				Cmd:  descriptor,
			},
			Progress: proto.Float64(0),
		}, nil
	}

	testCase, ok := srv.testCases[query.GetQuery()]
	if !ok {
		return nil, status.Errorf(codes.Unimplemented, "Invalid case %s", query.GetQuery())
	}

	srv.queries[queryId] = &IncrementalQuery{
		query:     query.GetQuery(),
		nextIndex: 0,
	}

	return srv.MakePollInfo(&testCase, srv.queries[queryId], queryId)
}

func (srv *IncrementalPollTestServer) PollFlightInfoPreparedStatement(ctx context.Context, query flightsql.PreparedStatementQuery, desc *flight.FlightDescriptor) (*flight.PollInfo, error) {
	queryId := uuid.New().String()
	req := string(query.GetPreparedStatementHandle())

	testCase, ok := srv.testCases[req]
	if !ok {
		return nil, status.Errorf(codes.Unimplemented, "Invalid case %s", req)
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()

	srv.queries[queryId] = &IncrementalQuery{
		query:     req,
		nextIndex: 0,
	}

	return srv.MakePollInfo(&testCase, srv.queries[queryId], queryId)
}

func (srv *IncrementalPollTestServer) BeginTransaction(context.Context, flightsql.ActionBeginTransactionRequest) (id []byte, err error) {
	return []byte("txn"), nil
}

func (srv *IncrementalPollTestServer) EndTransaction(context.Context, flightsql.ActionEndTransactionRequest) error {
	return nil
}

func (srv *IncrementalPollTestServer) CreatePreparedStatement(ctx context.Context, req flightsql.ActionCreatePreparedStatementRequest) (res flightsql.ActionCreatePreparedStatementResult, err error) {
	return flightsql.ActionCreatePreparedStatementResult{
		Handle: []byte(req.GetQuery()),
		DatasetSchema: arrow.NewSchema([]arrow.Field{
			{Name: "ints", Type: arrow.PrimitiveTypes.Int32},
		}, nil),
	}, nil
}

func (srv *IncrementalPollTestServer) ClosePreparedStatement(ctx context.Context, req flightsql.ActionClosePreparedStatementRequest) error {
	return nil
}

func (srv *IncrementalPollTestServer) MakePollInfo(testCase *IncrementalPollTestCase, query *IncrementalQuery, queryId string) (*flight.PollInfo, error) {
	schema := flight.SerializeSchema(arrow.NewSchema([]arrow.Field{
		{Name: "ints", Type: arrow.PrimitiveTypes.Int32},
	}, nil), srv.Alloc)

	pb := wrapperspb.StringValue{Value: queryId}
	if testCase.differentRetryDescriptor {
		pb.Value = queryId + ";" + strconv.Itoa(query.nextIndex+1)
	}
	descriptor, err := proto.Marshal(&pb)
	if err != nil {
		return nil, err
	}

	numEndpoints := 0
	for i := 0; i <= query.nextIndex; i++ {
		if i >= len(testCase.progress) {
			break
		}
		numEndpoints += testCase.progress[i]
	}
	endpoints := make([]*flight.FlightEndpoint, numEndpoints)
	for i := range endpoints {
		endpoints[i] = &flight.FlightEndpoint{
			Ticket: &flight.Ticket{
				Ticket: []byte{},
			},
		}
	}

	query.nextIndex++
	pollInfo := flight.PollInfo{
		Info: &flight.FlightInfo{
			Schema:   schema,
			Endpoint: endpoints,
		},
		FlightDescriptor: &flight.FlightDescriptor{
			Type: flight.DescriptorCMD,
			Cmd:  descriptor,
		},
		Progress: proto.Float64(float64(query.nextIndex) / float64(len(testCase.progress))),
	}

	if query.nextIndex >= len(testCase.progress) {
		if testCase.completeLazily {
			if query.nextIndex == len(testCase.progress) {
				// Make the client poll one more time
			} else {
				pollInfo.FlightDescriptor = nil
				delete(srv.queries, queryId)
			}

		} else {
			pollInfo.FlightDescriptor = nil
			delete(srv.queries, queryId)
		}
	}

	return &pollInfo, nil
}

type IncrementalPollTestCase struct {
	// on each poll (including the first), this many new endpoints complete
	// making 0 progress is allowed, but not recommended (allow clients to 'long poll')
	progress []int

	// use a different retry descriptor for each poll
	differentRetryDescriptor bool

	// require one extra poll to get completion (i.e. the last poll will have a nil FlightInfo)
	completeLazily bool
}

type IncrementalPollTests struct {
	ServerBasedTests
	testCases map[string]IncrementalPollTestCase
}

func (suite *IncrementalPollTests) SetupSuite() {
	suite.testCases = map[string]IncrementalPollTestCase{
		"basic": {
			progress: []int{1, 1, 1, 1},
		},
		"basic 2": {
			progress: []int{2, 3, 4, 5},
		},
		"basic 3": {
			progress: []int{2},
		},
		"descriptor changes": {
			progress:                 []int{1, 1, 1, 1},
			differentRetryDescriptor: true,
		},
		"lazy": {
			progress:       []int{1, 1, 1, 1},
			completeLazily: true,
		},
		"lazy 2": {
			progress:       []int{1, 1, 1, 0},
			completeLazily: true,
		},
		"no progress": {
			progress: []int{0, 1, 1, 1},
		},
		"no progress 2": {
			progress: []int{0, 0, 1, 1},
		},
		"no progress 3": {
			progress: []int{0, 0, 1, 0},
		},
	}

	srv := IncrementalPollTestServer{
		queries:   make(map[string]*IncrementalQuery),
		testCases: suite.testCases,
	}
	suite.NoError(srv.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerTransaction, int32(flightsql.SqlTransactionTransaction)))
	srv.Alloc = memory.DefaultAllocator
	suite.DoSetupSuite(&srv, nil, nil)
}

func (ts *IncrementalPollTests) TestMaxProgress() {
	stmt, err := ts.cnxn.NewStatement()
	ts.NoError(err)
	defer validation.CheckedClose(ts.T(), stmt)
	opts := stmt.(adbc.GetSetOptions)

	val, err := opts.GetOptionDouble(adbc.OptionKeyMaxProgress)
	ts.NoError(err)
	ts.Equal(1.0, val)
}

func (ts *IncrementalPollTests) TestOptionValue() {
	stmt, err := ts.cnxn.NewStatement()
	ts.NoError(err)
	defer validation.CheckedClose(ts.T(), stmt)
	opts := stmt.(adbc.GetSetOptions)

	val, err := opts.GetOption(adbc.OptionKeyIncremental)
	ts.NoError(err)
	ts.Equal(adbc.OptionValueDisabled, val)

	ts.NoError(stmt.SetOption(adbc.OptionKeyIncremental, adbc.OptionValueEnabled))

	val, err = opts.GetOption(adbc.OptionKeyIncremental)
	ts.NoError(err)
	ts.Equal(adbc.OptionValueEnabled, val)

	var adbcErr adbc.Error
	ts.ErrorAs(stmt.SetOption(adbc.OptionKeyIncremental, "foobar"), &adbcErr)
	ts.Equal(adbc.StatusInvalidArgument, adbcErr.Code)
}

func (ts *IncrementalPollTests) TestAppMetadata() {
	ctx, cancel := context.WithCancel(context.Background())
	stmt, err := ts.cnxn.NewStatement()
	ts.NoError(err)
	defer validation.CheckedClose(ts.T(), stmt)

	ts.NoError(stmt.SetOption(adbc.OptionKeyIncremental, adbc.OptionValueEnabled))

	ts.NoError(stmt.SetSqlQuery("infinite"))
	_, partitions, _, err := stmt.ExecutePartitions(ctx)
	ts.NoError(err)
	ts.Equalf(uint64(1), partitions.NumPartitions, "%#v", partitions)

	progress := 0.0
	go func() {
		var err error
		var info []byte
		for {
			// While the below is stuck, we should be able to get the app metadata and progress
			progress, err = stmt.(adbc.GetSetOptions).GetOptionDouble(adbc.OptionKeyProgress)
			ts.NoError(err)

			info, err = stmt.(adbc.GetSetOptions).GetOptionBytes(driver.OptionLastFlightInfo)
			ts.NoError(err)
			var flightInfo flight.FlightInfo
			ts.NoError(proto.Unmarshal(info, &flightInfo))
			ts.Equal([]byte("app metadata"), flightInfo.AppMetadata)

			if progress > 0.03 {
				break
			}
		}
		cancel()
	}()

	// will get stuck forever, but will "make progress"
	_, _, _, err = stmt.ExecutePartitions(ctx)
	var adbcErr adbc.Error
	ts.ErrorAs(err, &adbcErr)
	ts.Equal(adbc.StatusCancelled, adbcErr.Code)
}

func (ts *IncrementalPollTests) TestUnavailable() {
	// An error from the server should not tear down all the state.  We
	// should be able to retry the request.
	ctx := context.Background()
	stmt, err := ts.cnxn.NewStatement()
	ts.NoError(err)
	defer validation.CheckedClose(ts.T(), stmt)

	ts.NoError(stmt.SetOption(adbc.OptionKeyIncremental, adbc.OptionValueEnabled))

	ts.NoError(stmt.SetSqlQuery("unavailable"))
	_, partitions, _, err := stmt.ExecutePartitions(ctx)
	ts.NoError(err)
	ts.Equalf(uint64(1), partitions.NumPartitions, "%#v", partitions)

	_, partitions, _, err = stmt.ExecutePartitions(ctx)
	ts.ErrorContains(err, "Server temporarily unavailable")
	ts.Equal(uint64(0), partitions.NumPartitions)

	_, partitions, _, err = stmt.ExecutePartitions(ctx)
	ts.NoError(err)
	ts.Equalf(uint64(1), partitions.NumPartitions, "%#v", partitions)

	_, partitions, _, err = stmt.ExecutePartitions(ctx)
	ts.NoError(err)
	ts.Equal(uint64(0), partitions.NumPartitions)
}

func (ts *IncrementalPollTests) RunOneTestCase(ctx context.Context, stmt adbc.Statement, name string, testCase *IncrementalPollTestCase) {
	opts := stmt.(adbc.GetSetOptions)

	for idx, progress := range testCase.progress {
		if progress == 0 {
			// the driver hides this from us
			continue
		}

		_, partitions, _, err := stmt.ExecutePartitions(ctx)
		ts.NoError(err)

		ts.Equal(uint64(progress), partitions.NumPartitions)

		val, err := opts.GetOptionDouble(adbc.OptionKeyProgress)
		ts.NoError(err)
		ts.Equal(float64(idx+1)/float64(len(testCase.progress)), val)
	}

	// Query completed, but we find out by getting no partitions in this call
	_, partitions, _, err := stmt.ExecutePartitions(ctx)
	ts.NoError(err)

	ts.Equal(uint64(0), partitions.NumPartitions)
}

func (ts *IncrementalPollTests) TestQuery() {
	ctx := context.Background()
	for name, testCase := range ts.testCases {
		ts.Run(name, func() {
			stmt, err := ts.cnxn.NewStatement()
			ts.NoError(err)
			defer validation.CheckedClose(ts.T(), stmt)

			ts.NoError(stmt.SetOption(adbc.OptionKeyIncremental, adbc.OptionValueEnabled))

			// Run the query multiple times (we should be able to reuse the statement)
			for i := 0; i < 2; i++ {
				ts.NoError(stmt.SetSqlQuery(name))
				ts.RunOneTestCase(ctx, stmt, name, &testCase)
			}
		})
	}
}

func (ts *IncrementalPollTests) TestQueryPrepared() {
	ctx := context.Background()
	for name, testCase := range ts.testCases {
		ts.Run(name, func() {
			stmt, err := ts.cnxn.NewStatement()
			ts.NoError(err)
			defer validation.CheckedClose(ts.T(), stmt)

			ts.NoError(stmt.SetOption(adbc.OptionKeyIncremental, adbc.OptionValueEnabled))

			// Run the query multiple times (we should be able to reuse the statement)
			for i := 0; i < 2; i++ {
				ts.NoError(stmt.SetSqlQuery(name))
				ts.NoError(stmt.Prepare(ctx))
				ts.RunOneTestCase(ctx, stmt, name, &testCase)
			}
		})
	}
}

func (ts *IncrementalPollTests) TestQueryPreparedTransaction() {
	ctx := context.Background()
	for name, testCase := range ts.testCases {
		ts.Run(name, func() {
			ts.NoError(ts.cnxn.(adbc.PostInitOptions).SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled))
			stmt, err := ts.cnxn.NewStatement()
			ts.NoError(err)
			defer validation.CheckedClose(ts.T(), stmt)

			ts.NoError(stmt.SetOption(adbc.OptionKeyIncremental, adbc.OptionValueEnabled))

			// Run the query multiple times (we should be able to reuse the statement)
			for i := 0; i < 2; i++ {
				ts.NoError(stmt.SetSqlQuery(name))
				ts.NoError(stmt.Prepare(ctx))
				ts.RunOneTestCase(ctx, stmt, name, &testCase)
			}
		})
	}
}

func (ts *IncrementalPollTests) TestQueryTransaction() {
	ctx := context.Background()
	for name, testCase := range ts.testCases {
		ts.Run(name, func() {
			ts.NoError(ts.cnxn.(adbc.PostInitOptions).SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled))
			stmt, err := ts.cnxn.NewStatement()
			ts.NoError(err)
			defer validation.CheckedClose(ts.T(), stmt)

			ts.NoError(stmt.SetOption(adbc.OptionKeyIncremental, adbc.OptionValueEnabled))

			// Run the query multiple times (we should be able to reuse the statement)
			for i := 0; i < 2; i++ {
				ts.NoError(stmt.SetSqlQuery(name))
				ts.RunOneTestCase(ctx, stmt, name, &testCase)
			}
		})
	}
}

// ---- Timeout Tests --------------------

type TimeoutTestServer struct {
	flightsql.BaseServer
	badPort  int
	goodPort int
}

func (ts *TimeoutTestServer) DoGetStatement(ctx context.Context, tkt flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	ticket := string(tkt.GetStatementHandle())
	if ticket == "sleep and succeed" {
		time.Sleep(1 * time.Second)
	}

	switch ticket {
	case "bad endpoint", "sleep and succeed":
		sc := arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)
		rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, sc, strings.NewReader(`[{"a": 5}]`))
		if err != nil {
			return nil, nil, err
		}

		ch := make(chan flight.StreamChunk)
		go func() {
			defer close(ch)
			ch <- flight.StreamChunk{
				Data: rec,
				Desc: nil,
				Err:  nil,
			}
		}()
		return sc, ch, nil
	}

	// wait till the context is cancelled
	<-ctx.Done()
	return nil, nil, ctx.Err()
}

func (ts *TimeoutTestServer) DoPutCommandStatementUpdate(ctx context.Context, cmd flightsql.StatementUpdate) (int64, error) {
	if cmd.GetQuery() == "timeout" {
		<-ctx.Done()
		return -1, ctx.Err()
	}
	return -1, arrow.ErrNotImplemented
}

func (ts *TimeoutTestServer) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	switch cmd.GetQuery() {
	case "timeout":
		<-ctx.Done()
	case "bad endpoint":
		tkt, _ := flightsql.CreateStatementQueryTicket([]byte("bad endpoint"))
		info := &flight.FlightInfo{
			FlightDescriptor: desc,
			Endpoint: []*flight.FlightEndpoint{
				{
					Ticket: &flight.Ticket{Ticket: tkt},
					Location: []*flight.Location{
						{Uri: fmt.Sprintf("grpc://localhost:%d", ts.badPort)},
						{Uri: fmt.Sprintf("grpc://localhost:%d", ts.goodPort)},
					},
				},
			},
			TotalRecords: -1,
			TotalBytes:   -1,
		}
		return info, nil
	case "fetch":
		tkt, _ := flightsql.CreateStatementQueryTicket([]byte("fetch"))
		info := &flight.FlightInfo{
			FlightDescriptor: desc,
			Endpoint: []*flight.FlightEndpoint{
				{Ticket: &flight.Ticket{Ticket: tkt}},
			},
			TotalRecords: -1,
			TotalBytes:   -1,
		}
		return info, nil
	case "notimeout":
		time.Sleep(1 * time.Second)
		tkt, _ := flightsql.CreateStatementQueryTicket([]byte("sleep and succeed"))
		info := &flight.FlightInfo{
			FlightDescriptor: desc,
			Endpoint: []*flight.FlightEndpoint{
				{Ticket: &flight.Ticket{Ticket: tkt}},
			},
			TotalRecords: -1,
			TotalBytes:   -1,
		}
		return info, nil
	}
	return nil, ctx.Err()
}

func (ts *TimeoutTestServer) CreatePreparedStatement(ctx context.Context, req flightsql.ActionCreatePreparedStatementRequest) (result flightsql.ActionCreatePreparedStatementResult, err error) {
	<-ctx.Done()
	return result, ctx.Err()
}

type TimeoutTests struct {
	ServerBasedTests
	server net.Listener
}

func (suite *TimeoutTests) SetupSuite() {
	var err error
	suite.server, err = net.Listen("tcp", "localhost:0")
	suite.NoError(err)

	badPort := suite.server.Addr().(*net.TCPAddr).Port
	server := &TimeoutTestServer{badPort: badPort}
	suite.DoSetupSuite(server, nil, nil)
	server.goodPort = suite.s.Addr().(*net.TCPAddr).Port
}

func (suite *TimeoutTests) TearDownSuite() {
	suite.ServerBasedTests.TearDownSuite()
	suite.NoError(suite.server.Close())
}

func (ts *TimeoutTests) TestInvalidValues() {
	keys := []string{
		"adbc.flight.sql.rpc.timeout_seconds.fetch",
		"adbc.flight.sql.rpc.timeout_seconds.query",
		"adbc.flight.sql.rpc.timeout_seconds.update",
	}
	values := []string{"1.1f", "asdf", "inf", "NaN", "-1"}

	for _, k := range keys {
		for _, v := range values {
			ts.Run("key="+k+",val="+v, func() {
				err := ts.cnxn.(adbc.PostInitOptions).SetOption(k, v)
				var adbcErr adbc.Error
				ts.ErrorAs(err, &adbcErr)
				ts.Equal(adbc.StatusInvalidArgument, adbcErr.Code)
				ts.ErrorContains(err, "invalid timeout option value")
			})
		}
	}
}

func (ts *TimeoutTests) TestRemoveTimeout() {
	keys := []string{
		"adbc.flight.sql.rpc.timeout_seconds.fetch",
		"adbc.flight.sql.rpc.timeout_seconds.query",
		"adbc.flight.sql.rpc.timeout_seconds.update",
	}
	for _, k := range keys {
		ts.Run(k, func() {
			ts.NoError(ts.cnxn.(adbc.PostInitOptions).SetOption(k, "1.0"))
			ts.NoError(ts.cnxn.(adbc.PostInitOptions).SetOption(k, "0"))
		})
	}
}

func (ts *TimeoutTests) TestGetSet() {
	keys := []string{
		"adbc.flight.sql.rpc.timeout_seconds.fetch",
		"adbc.flight.sql.rpc.timeout_seconds.query",
		"adbc.flight.sql.rpc.timeout_seconds.update",
	}
	stmt, err := ts.cnxn.NewStatement()
	ts.Require().NoError(err)
	defer validation.CheckedClose(ts.T(), stmt)

	for _, v := range []interface{}{ts.db, ts.cnxn, stmt} {
		getset := v.(adbc.GetSetOptions)

		for _, k := range keys {
			strval, err := getset.GetOption(k)
			ts.NoError(err)
			ts.Equal("0s", strval)

			intval, err := getset.GetOptionInt(k)
			ts.NoError(err)
			ts.Equal(int64(0), intval)

			floatval, err := getset.GetOptionDouble(k)
			ts.NoError(err)
			ts.Equal(0.0, floatval)

			err = getset.SetOptionInt(k, 1)
			ts.NoError(err)

			strval, err = getset.GetOption(k)
			ts.NoError(err)
			ts.Equal("1s", strval)

			intval, err = getset.GetOptionInt(k)
			ts.NoError(err)
			ts.Equal(int64(1), intval)

			floatval, err = getset.GetOptionDouble(k)
			ts.NoError(err)
			ts.Equal(1.0, floatval)

			err = getset.SetOptionDouble(k, 0.1)
			ts.NoError(err)

			strval, err = getset.GetOption(k)
			ts.NoError(err)
			ts.Equal("100ms", strval)

			intval, err = getset.GetOptionInt(k)
			ts.NoError(err)
			// truncated
			ts.Equal(int64(0), intval)

			floatval, err = getset.GetOptionDouble(k)
			ts.NoError(err)
			ts.Equal(0.1, floatval)
		}
	}

}

func (ts *TimeoutTests) TestDoActionTimeout() {
	ts.NoError(ts.cnxn.(adbc.PostInitOptions).
		SetOption("adbc.flight.sql.rpc.timeout_seconds.update", "0.1"))

	stmt, err := ts.cnxn.NewStatement()
	ts.Require().NoError(err)
	defer validation.CheckedClose(ts.T(), stmt)

	ts.Require().NoError(stmt.SetSqlQuery("fetch"))
	var adbcErr adbc.Error
	ts.ErrorAs(stmt.Prepare(context.Background()), &adbcErr)
	ts.Equal(adbc.StatusTimeout, adbcErr.Code, adbcErr.Error())
	// It seems gRPC isn't stable about the error message, unfortunately
}

func (ts *TimeoutTests) TestDoGetTimeout() {
	ts.NoError(ts.cnxn.(adbc.PostInitOptions).
		SetOption("adbc.flight.sql.rpc.timeout_seconds.fetch", "0.1"))

	stmt, err := ts.cnxn.NewStatement()
	ts.Require().NoError(err)
	defer validation.CheckedClose(ts.T(), stmt)

	ts.Require().NoError(stmt.SetSqlQuery("fetch"))
	var adbcErr adbc.Error
	_, _, err = stmt.ExecuteQuery(context.Background())
	ts.ErrorAs(err, &adbcErr)
	ts.Equal(adbc.StatusTimeout, adbcErr.Code, adbcErr.Error())
}

func (ts *TimeoutTests) TestDoPutTimeout() {
	ts.NoError(ts.cnxn.(adbc.PostInitOptions).
		SetOption("adbc.flight.sql.rpc.timeout_seconds.update", "1.1"))

	stmt, err := ts.cnxn.NewStatement()
	ts.Require().NoError(err)
	defer validation.CheckedClose(ts.T(), stmt)

	ts.Require().NoError(stmt.SetSqlQuery("timeout"))
	var adbcErr adbc.Error
	_, err = stmt.ExecuteUpdate(context.Background())
	ts.ErrorAs(err, &adbcErr)
	ts.Equal(adbc.StatusTimeout, adbcErr.Code, adbcErr.Error())
}

func (ts *TimeoutTests) TestGetFlightInfoTimeout() {
	ts.NoError(ts.cnxn.(adbc.PostInitOptions).
		SetOption("adbc.flight.sql.rpc.timeout_seconds.query", "0.1"))

	stmt, err := ts.cnxn.NewStatement()
	ts.Require().NoError(err)
	defer validation.CheckedClose(ts.T(), stmt)

	ts.Require().NoError(stmt.SetSqlQuery("timeout"))
	var adbcErr adbc.Error
	_, _, err = stmt.ExecuteQuery(context.Background())
	ts.ErrorAs(err, &adbcErr)
	ts.NotEqual(adbc.StatusNotImplemented, adbcErr.Code, adbcErr.Error())
}

func (ts *TimeoutTests) TestDontTimeout() {
	ts.NoError(ts.cnxn.(adbc.PostInitOptions).
		SetOption("adbc.flight.sql.rpc.timeout_seconds.fetch", "2.0"))
	ts.NoError(ts.cnxn.(adbc.PostInitOptions).
		SetOption("adbc.flight.sql.rpc.timeout_seconds.query", "2.0"))

	stmt, err := ts.cnxn.NewStatement()
	ts.Require().NoError(err)
	defer validation.CheckedClose(ts.T(), stmt)

	ts.Require().NoError(stmt.SetSqlQuery("notimeout"))
	// GetFlightInfo will sleep for one second and DoGet will also
	// sleep for one second. But our timeout is 2 seconds, which is
	// per-operation. So we shouldn't time out and all should succeed.
	rr, _, err := stmt.ExecuteQuery(context.Background())
	ts.Require().NoError(err)
	defer rr.Release()

	ts.True(rr.Next())
	rec := rr.RecordBatch()

	sc := arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)
	expected, _, err := array.RecordFromJSON(memory.DefaultAllocator, sc, strings.NewReader(`[{"a": 5}]`))
	ts.Require().NoError(err)
	defer expected.Release()
	ts.Truef(array.RecordEqual(rec, expected), "expected: %s\nactual: %s", expected, rec)
}

func (ts *TimeoutTests) TestBadAddress() {
	stmt, err := ts.cnxn.NewStatement()
	ts.Require().NoError(err)
	defer validation.CheckedClose(ts.T(), stmt)
	ts.Require().NoError(stmt.SetSqlQuery("bad endpoint"))

	ts.Require().NoError(ts.db.(adbc.GetSetOptions).SetOptionDouble(driver.OptionTimeoutConnect, 5))

	rr, _, err := stmt.ExecuteQuery(context.Background())
	ts.Require().NoError(err)
	defer rr.Release()

	rr, _, err = stmt.ExecuteQuery(context.Background())
	ts.Require().NoError(err)
	defer rr.Release()

	rr, _, err = stmt.ExecuteQuery(context.Background())
	ts.Require().NoError(err)
	defer rr.Release()
}

// ---- Cookie Tests --------------------
type CookieTestServer struct {
	flightsql.BaseServer

	cur  time.Time
	addr string
}

func (server *CookieTestServer) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	md := metadata.MD{}
	md.Append("set-cookie", "foo=bar")
	md.Append("set-cookie", "bar=baz; Max-Age=1")
	server.cur = time.Now()

	if err := grpc.SendHeader(ctx, md); err != nil {
		return nil, err
	}

	tkt, _ := flightsql.CreateStatementQueryTicket([]byte{})
	info := &flight.FlightInfo{
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{
			{
				Ticket: &flight.Ticket{Ticket: tkt},
				// passing a non-empty location uri so that the test client
				// creates a sub-client and we test that the cookies are
				// preserved and copied over.
				Location: []*flight.Location{{Uri: server.addr}},
			},
		},
		TotalRecords: -1,
		TotalBytes:   -1,
	}

	return info, nil
}

func (server *CookieTestServer) DoGetStatement(ctx context.Context, tkt flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	var (
		foundFoo, foundBar bool
	)

	cookies := metadata.ValueFromIncomingContext(ctx, "cookie")
	for _, line := range cookies {
		line = textproto.TrimString(line)

		var part string
		for len(line) > 0 {
			part, line, _ = strings.Cut(line, ";")
			part = textproto.TrimString(part)
			if part == "" {
				continue
			}

			name, val, _ := strings.Cut(part, "=")
			name = textproto.TrimString(name)
			if len(val) > 1 && val[0] == '"' && val[len(val)-1] == '"' {
				val = val[1 : len(val)-1]
			}

			switch name {
			case "foo":
				if val == "bar" {
					foundFoo = true
				}
			case "bar":
				if val == "baz" {
					foundBar = true
				}
			default:
				return nil, nil, fmt.Errorf("found unexpected cookie '%s' = '%s'", name, val)
			}
		}
	}

	if !foundFoo {
		return nil, nil, errors.New("missing cookie 'foo'='bar'")
	}

	if !foundBar && time.Now().Before(server.cur.Add(1*time.Second)) {
		return nil, nil, errors.New("missing cookie 'bar'='baz'")
	}

	sc := arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)
	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, sc, strings.NewReader(`[{"a": 5}]`))
	if err != nil {
		return nil, nil, err
	}

	ch := make(chan flight.StreamChunk)
	go func() {
		defer close(ch)
		ch <- flight.StreamChunk{
			Data: rec,
			Desc: nil,
			Err:  nil,
		}
	}()
	return sc, ch, nil
}

type CookieTests struct {
	ServerBasedTests
}

func (suite *CookieTests) SetupSuite() {
	ts := &CookieTestServer{}
	suite.DoSetupSuite(ts, nil, map[string]string{
		driver.OptionCookieMiddleware: adbc.OptionValueEnabled,
	})
	ts.addr = "grpc://" + suite.s.Addr().String()
}

func (suite *CookieTests) TestCookieUsage() {
	stmt, err := suite.cnxn.NewStatement()
	suite.Require().NoError(err)
	defer validation.CheckedClose(suite.T(), stmt)

	suite.Require().NoError(stmt.SetSqlQuery("timeout"))
	reader, _, err := stmt.ExecuteQuery(context.Background())
	suite.Require().NoError(err)
	defer reader.Release()
}

// ---- Data Type Tests --------------------
type DataTypeTestServer struct {
	flightsql.BaseServer
}

func (server *DataTypeTestServer) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	tkt, _ := flightsql.CreateStatementQueryTicket([]byte(cmd.GetQuery()))
	info := &flight.FlightInfo{
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{
			{Ticket: &flight.Ticket{Ticket: tkt}},
		},
		TotalRecords: -1,
		TotalBytes:   -1,
	}

	return info, nil
}

var (
	SchemaListInt3     = arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.FixedSizeListOf(3, arrow.PrimitiveTypes.Int32), Nullable: true}}, nil)
	SchemaListInt      = arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true}}, nil)
	SchemaLargeListInt = arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.LargeListOf(arrow.PrimitiveTypes.Int32), Nullable: true}}, nil)
	SchemaMapIntInt    = arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.MapOf(arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int32), Nullable: true}}, nil)
)

func (server *DataTypeTestServer) DoGetStatement(ctx context.Context, tkt flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	var schema *arrow.Schema
	var record arrow.RecordBatch
	var err error

	cmd := string(tkt.GetStatementHandle())
	switch cmd {
	case "list[int, 3]":
		schema = SchemaListInt3
		record, _, err = array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(`[{"a": [1, 2, 3]}]`))
	case "list[int]":
		schema = SchemaListInt
		record, _, err = array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(`[{"a": [1]}]`))
	case "large_list[int]":
		schema = SchemaLargeListInt
		record, _, err = array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(`[{"a": [1]}]`))
	case "map[int]int":
		schema = SchemaMapIntInt
		record, _, err = array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(`[{"a": null}]`))
	default:
		return nil, nil, fmt.Errorf("Unknown command: '%s'", cmd)
	}

	if err != nil {
		return nil, nil, err
	}

	ch := make(chan flight.StreamChunk)
	go func() {
		defer close(ch)
		ch <- flight.StreamChunk{
			Data: record,
		}
	}()
	return schema, ch, nil
}

type DataTypeTests struct {
	ServerBasedTests
}

func (suite *DataTypeTests) SetupSuite() {
	suite.DoSetupSuite(&DataTypeTestServer{}, nil, map[string]string{})
}

func (suite *DataTypeTests) DoTestCase(name string, schema *arrow.Schema) {
	stmt, err := suite.cnxn.NewStatement()
	suite.NoError(err)
	defer validation.CheckedClose(suite.T(), stmt)

	suite.NoError(stmt.SetSqlQuery(name))
	reader, _, err := stmt.ExecuteQuery(context.Background())
	suite.NoError(err)
	suite.Equal(reader.Schema(), schema)
	defer reader.Release()
}

func (suite *DataTypeTests) TestListInt3() {
	suite.DoTestCase("list[int, 3]", SchemaListInt3)
}

func (suite *DataTypeTests) TestLargeListInt() {
	suite.DoTestCase("large_list[int]", SchemaLargeListInt)
}

func (suite *DataTypeTests) TestListInt() {
	suite.DoTestCase("list[int]", SchemaListInt)
}

func (suite *DataTypeTests) TestMapIntInt() {
	suite.DoTestCase("map[int]int", SchemaMapIntInt)
}

// ---- Multi Table Tests --------------------

type MultiTableTestServer struct {
	flightsql.BaseServer
}

func (server *MultiTableTestServer) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	query := cmd.GetQuery()
	tkt, err := flightsql.CreateStatementQueryTicket([]byte(query))
	if err != nil {
		return nil, err
	}

	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: tkt}}},
		FlightDescriptor: desc,
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

func (server *MultiTableTestServer) GetFlightInfoTables(ctx context.Context, cmd flightsql.GetTables, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	schema := schema_ref.Tables
	if cmd.GetIncludeSchema() {
		schema = schema_ref.TablesWithIncludedSchema
	}
	server.Alloc = memory.NewCheckedAllocator(memory.DefaultAllocator)
	info := &flight.FlightInfo{
		Endpoint: []*flight.FlightEndpoint{
			{Ticket: &flight.Ticket{Ticket: desc.Cmd}, Location: []*flight.Location{{Uri: flight.LocationReuseConnection}}},
		},
		FlightDescriptor: desc,
		Schema:           flight.SerializeSchema(schema, server.Alloc),
		TotalRecords:     -1,
		TotalBytes:       -1,
	}

	return info, nil
}

func (server *MultiTableTestServer) DoGetTables(ctx context.Context, cmd flightsql.GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	bldr := array.NewRecordBuilder(server.Alloc, adbc.GetTableSchemaSchema)

	bldr.Field(0).(*array.StringBuilder).AppendValues([]string{"", ""}, nil)
	bldr.Field(1).(*array.StringBuilder).AppendValues([]string{"", ""}, nil)
	bldr.Field(2).(*array.StringBuilder).AppendValues([]string{"tbl1", "tbl2"}, nil)
	bldr.Field(3).(*array.StringBuilder).AppendValues([]string{"", ""}, nil)

	sc1 := arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)
	sc2 := arrow.NewSchema([]arrow.Field{{Name: "b", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)
	buf1 := flight.SerializeSchema(sc1, server.Alloc)
	buf2 := flight.SerializeSchema(sc2, server.Alloc)

	bldr.Field(4).(*array.BinaryBuilder).AppendValues([][]byte{buf1, buf2}, nil)
	defer bldr.Release()

	rec := bldr.NewRecordBatch()

	ch := make(chan flight.StreamChunk)
	go func() {
		defer close(ch)
		ch <- flight.StreamChunk{
			Data: rec,
			Desc: nil,
			Err:  nil,
		}
	}()
	return adbc.GetTableSchemaSchema, ch, nil
}

type MultiTableTests struct {
	ServerBasedTests
}

func (suite *MultiTableTests) SetupSuite() {
	suite.DoSetupSuite(&MultiTableTestServer{}, nil, map[string]string{})
}

// Regression test for https://github.com/apache/arrow-adbc/issues/934
func (suite *MultiTableTests) TestGetTableSchema() {
	actualSchema, err := suite.cnxn.GetTableSchema(context.Background(), nil, nil, "tbl2")
	suite.NoError(err)

	expectedSchema := arrow.NewSchema([]arrow.Field{{Name: "b", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)
	suite.Equal(expectedSchema, actualSchema)
}

// ---- Session Option Tests --------------------

type SessionOptionTestServer struct {
	flightsql.BaseServer
	options map[string]interface{}
}

func (server *SessionOptionTestServer) GetSessionOptions(ctx context.Context, req *flight.GetSessionOptionsRequest) (*flight.GetSessionOptionsResult, error) {
	options := make(map[string]*flight.SessionOptionValue)
	for k, v := range server.options {
		switch s := v.(type) {
		case bool:
			options[k] = &flight.SessionOptionValue{OptionValue: &flightproto.SessionOptionValue_BoolValue{BoolValue: s}}
		case float64:
			options[k] = &flight.SessionOptionValue{OptionValue: &flightproto.SessionOptionValue_DoubleValue{DoubleValue: s}}
		case int64:
			options[k] = &flight.SessionOptionValue{OptionValue: &flightproto.SessionOptionValue_Int64Value{Int64Value: s}}
		case string:
			options[k] = &flight.SessionOptionValue{OptionValue: &flightproto.SessionOptionValue_StringValue{StringValue: s}}
		case []string:
			options[k] = &flight.SessionOptionValue{OptionValue: &flightproto.SessionOptionValue_StringListValue_{StringListValue: &flightproto.SessionOptionValue_StringListValue{Values: s}}}
		case nil:
			options[k] = &flight.SessionOptionValue{}
		default:
			panic("not implemented")
		}
	}
	return &flight.GetSessionOptionsResult{
		SessionOptions: options,
	}, nil
}

func (server *SessionOptionTestServer) SetSessionOptions(ctx context.Context, req *flight.SetSessionOptionsRequest) (*flight.SetSessionOptionsResult, error) {
	errors := map[string]*flightproto.SetSessionOptionsResult_Error{}
	for k, v := range req.SessionOptions {
		switch k {
		case "bad name":
			errors[k] = &flightproto.SetSessionOptionsResult_Error{Value: flightproto.SetSessionOptionsResult_INVALID_NAME}
			continue
		case "bad value":
			errors[k] = &flightproto.SetSessionOptionsResult_Error{Value: flightproto.SetSessionOptionsResult_INVALID_VALUE}
			continue
		case "error":
			errors[k] = &flightproto.SetSessionOptionsResult_Error{Value: flightproto.SetSessionOptionsResult_ERROR}
			continue
		}
		switch s := v.GetOptionValue().(type) {
		case *flightproto.SessionOptionValue_BoolValue:
			server.options[k] = s.BoolValue
		case *flightproto.SessionOptionValue_DoubleValue:
			server.options[k] = s.DoubleValue
		case *flightproto.SessionOptionValue_Int64Value:
			server.options[k] = s.Int64Value
		case *flightproto.SessionOptionValue_StringValue:
			server.options[k] = s.StringValue
		case *flightproto.SessionOptionValue_StringListValue_:
			server.options[k] = s.StringListValue.Values
		case nil:
			delete(server.options, k)
		default:
			return nil, status.Error(codes.InvalidArgument, "invalid option type")
		}
	}
	return &flight.SetSessionOptionsResult{Errors: errors}, nil
}

func (server *SessionOptionTestServer) CloseSession(ctx context.Context, req *flight.CloseSessionRequest) (*flight.CloseSessionResult, error) {
	return &flight.CloseSessionResult{
		Status: flight.CloseSessionResultClosed,
	}, nil
}

type SessionOptionTests struct {
	ServerBasedTests
}

func (suite *SessionOptionTests) SetupSuite() {
	suite.DoSetupSuite(&SessionOptionTestServer{
		options: map[string]interface{}{
			"string":     "expected",
			"bool":       true,
			"float64":    float64(1.5),
			"int64":      int64(20),
			"catalog":    "main",
			"schema":     "session",
			"stringlist": []string{"a", "b", "c"},
			"nilopt":     nil,
		},
	}, nil, map[string]string{})
}

func (suite *SessionOptionTests) TestGetAllOptions() {
	val, err := suite.cnxn.(adbc.GetSetOptions).GetOption(driver.OptionSessionOptions)
	suite.NoError(err)

	options := make(map[string]interface{})
	suite.NoError(json.Unmarshal([]byte(val), &options))
	// XXX: because Go decodes ints to strings by default. Should we use
	// an alternate representation? What happens to int64max?
	suite.Equal(float64(20), options["int64"])
	suite.Equal("expected", options["string"])
	// Bit of a hack, but lets servers send "this option exists, but is
	// not set" by returning a nil/unset value
	suite.Nil(options["nilopt"])
}

func (suite *SessionOptionTests) TestGetAllOptionsByte() {
	val, err := suite.cnxn.(adbc.GetSetOptions).GetOptionBytes(driver.OptionSessionOptions)
	suite.NoError(err)

	options := make(map[string]interface{})
	// XXX: maybe we can return the underlying proto repr here?
	suite.NoError(json.Unmarshal(val, &options))
	suite.Equal(float64(20), options["int64"])
	suite.Equal("expected", options["string"])
}

func (suite *SessionOptionTests) TestGetSetCatalog() {
	val, err := suite.cnxn.(adbc.GetSetOptions).GetOption(adbc.OptionKeyCurrentCatalog)
	suite.NoError(err)
	suite.Equal("main", val)

	suite.NoError(suite.cnxn.(adbc.GetSetOptions).SetOption(adbc.OptionKeyCurrentCatalog, "postgres"))
	val, err = suite.cnxn.(adbc.GetSetOptions).GetOption(adbc.OptionKeyCurrentCatalog)
	suite.NoError(err)
	suite.Equal("postgres", val)
}

func (suite *SessionOptionTests) TestGetSetSchema() {
	val, err := suite.cnxn.(adbc.GetSetOptions).GetOption(adbc.OptionKeyCurrentDbSchema)
	suite.NoError(err)
	suite.Equal("session", val)

	suite.NoError(suite.cnxn.(adbc.GetSetOptions).SetOption(adbc.OptionKeyCurrentDbSchema, "public"))
	val, err = suite.cnxn.(adbc.GetSetOptions).GetOption(adbc.OptionKeyCurrentDbSchema)
	suite.NoError(err)
	suite.Equal("public", val)
}

func (suite *SessionOptionTests) TestGetSetBool() {
	o := suite.cnxn.(adbc.GetSetOptions)
	val, err := o.GetOption(driver.OptionBoolSessionOptionPrefix + "bool")
	suite.NoError(err)
	suite.Equal("true", val)

	suite.NoError(o.SetOption(driver.OptionBoolSessionOptionPrefix+"bool", "false"))
	val, err = o.GetOption(driver.OptionBoolSessionOptionPrefix + "bool")
	suite.NoError(err)
	suite.Equal("false", val)
}

func (suite *SessionOptionTests) TestGetSetFloat64() {
	o := suite.cnxn.(adbc.GetSetOptions)
	val, err := o.GetOptionDouble(driver.OptionSessionOptionPrefix + "float64")
	suite.NoError(err)
	suite.Equal(1.5, val)

	suite.NoError(o.SetOptionDouble(driver.OptionSessionOptionPrefix+"float64", -42.0))
	val, err = o.GetOptionDouble(driver.OptionSessionOptionPrefix + "float64")
	suite.NoError(err)
	suite.Equal(-42.0, val)
}

func (suite *SessionOptionTests) TestGetSetInt64() {
	o := suite.cnxn.(adbc.GetSetOptions)
	val, err := o.GetOptionInt(driver.OptionSessionOptionPrefix + "int64")
	suite.NoError(err)
	suite.Equal(int64(20), val)

	suite.NoError(o.SetOptionInt(driver.OptionSessionOptionPrefix+"int64", 128))
	val, err = o.GetOptionInt(driver.OptionSessionOptionPrefix + "int64")
	suite.NoError(err)
	suite.Equal(int64(128), val)
}

func (suite *SessionOptionTests) TestGetSetString() {
	o := suite.cnxn.(adbc.GetSetOptions)
	_, err := o.GetOption(driver.OptionSessionOptionPrefix + "unknown")
	suite.ErrorContains(err, "unknown session option 'unknown'")

	suite.NoError(o.SetOption(driver.OptionSessionOptionPrefix+"unknown", "42"))
	val, err := o.GetOption(driver.OptionSessionOptionPrefix + "unknown")
	suite.NoError(err)
	suite.Equal("42", val)

	suite.NoError(o.SetOption(driver.OptionEraseSessionOptionPrefix+"unknown", ""))
	_, err = o.GetOption(driver.OptionSessionOptionPrefix + "unknown")
	suite.ErrorContains(err, "unknown session option 'unknown'")

	suite.ErrorContains(o.SetOption(driver.OptionSessionOptionPrefix+"bad name", ""), "Could not set option(s) 'bad name' (invalid name)")
	suite.ErrorContains(o.SetOption(driver.OptionSessionOptionPrefix+"bad value", ""), "Could not set option(s) 'bad value' (invalid value)")
	suite.ErrorContains(o.SetOption(driver.OptionSessionOptionPrefix+"error", ""), "Could not set option(s) 'error' (error setting option)")
}

func (suite *SessionOptionTests) TestGetSetStringList() {
	o := suite.cnxn.(adbc.GetSetOptions)
	val, err := o.GetOption(driver.OptionStringListSessionOptionPrefix + "stringlist")
	suite.NoError(err)
	suite.Equal(`["a","b","c"]`, val)

	suite.NoError(o.SetOption(driver.OptionStringListSessionOptionPrefix+"stringlist", `["foo", "bar"]`))
	val, err = o.GetOption(driver.OptionStringListSessionOptionPrefix + "stringlist")
	suite.NoError(err)
	suite.Equal(`["foo","bar"]`, val)

	suite.NoError(o.SetOption(driver.OptionStringListSessionOptionPrefix+"stringlist", `[]`))
	val, err = o.GetOption(driver.OptionStringListSessionOptionPrefix + "stringlist")
	suite.NoError(err)
	suite.Equal(`[]`, val)
}

// ---- GetObjects Tests --------------------

type GetObjectsTestServer struct {
	flightsql.BaseServer
	catalogName string
	schemaName  string
	tableName   string
	testData    map[string][]string
}

func (srv *GetObjectsTestServer) GetFlightInfoCatalogs(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return srv.flightInfoForSchema(schema_ref.Catalogs, desc), nil
}

func (srv *GetObjectsTestServer) GetFlightInfoSchemas(ctx context.Context, cmd flightsql.GetDBSchemas, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return srv.flightInfoForSchema(schema_ref.DBSchemas, desc), nil
}

func (srv *GetObjectsTestServer) GetFlightInfoTables(ctx context.Context, cmd flightsql.GetTables, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return srv.flightInfoForSchema(schema_ref.TablesWithIncludedSchema, desc), nil
}

func (srv *GetObjectsTestServer) flightInfoForSchema(sc *arrow.Schema, desc *flight.FlightDescriptor) *flight.FlightInfo {
	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: desc.Cmd}}},
		FlightDescriptor: desc,
		Schema:           flight.SerializeSchema(sc, srv.Alloc),
		TotalRecords:     -1,
		TotalBytes:       -1,
	}
}

func (srv *GetObjectsTestServer) DoGetCatalogs(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	// no catalogs
	schema := schema_ref.Catalogs
	ch := make(chan flight.StreamChunk, 1)
	defer close(ch)
	return schema, ch, nil
}

func (srv *GetObjectsTestServer) DoGetTables(ctx context.Context, cmd flightsql.GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	schemaBldr := array.NewBinaryBuilder(srv.Alloc, arrow.BinaryTypes.Binary)

	columnFields := make([]arrow.Field, 0)
	for key, val := range srv.testData {

		bldr := flightsql.NewColumnMetadataBuilder()
		defer bldr.Clear()

		bldr.CatalogName(srv.catalogName)
		bldr.SchemaName(srv.schemaName)
		bldr.TableName(srv.tableName)
		bldr.TypeName(val[0])
		if val, err := strconv.ParseInt(val[2], 10, 32); err != nil {
			panic(err)
		} else {
			bldr.Precision(int32(val))
		}
		if val, err := strconv.ParseInt(val[3], 10, 32); err != nil {
			panic(err)
		} else {
			bldr.Scale(int32(val))
		}
		bldr.IsAutoIncrement(val[4] == "true")
		bldr.IsCaseSensitive(val[5] == "true")
		bldr.IsReadOnly(val[6] == "true")
		bldr.IsSearchable(val[7] == "true")

		colType, err := strconv.ParseInt(val[1], 10, 32)
		if err != nil {
			panic(err)
		}
		var fieldType arrow.DataType
		switch colType {
		case int64(arrow.PrimitiveTypes.Int32.ID()):
			fieldType = arrow.PrimitiveTypes.Int32
		case int64(arrow.PrimitiveTypes.Float32.ID()):
			fieldType = arrow.PrimitiveTypes.Float32
		case int64(arrow.PrimitiveTypes.Float64.ID()):
			fieldType = arrow.PrimitiveTypes.Float64
		default:
			panic(fmt.Errorf("unknown column type %d", colType))
		}

		columnFields = append(columnFields, arrow.Field{
			Name:     key,
			Type:     fieldType,
			Nullable: false,
			Metadata: bldr.Metadata(),
		})

	}

	schemaBldr.Append(flight.SerializeSchema(arrow.NewSchema(columnFields, nil), srv.Alloc))
	schemaCol := schemaBldr.NewArray()
	defer schemaCol.Release()

	jsonStr := fmt.Sprintf(`[{"catalog_name": "%s", "db_schema_name": "%s", "table_name": "%s", "table_type": "TABLE"}]`,
		srv.catalogName, // variable for catalog_name
		srv.schemaName,  // variable for db_schema_name
		srv.tableName)   // variable for table_type
	tablesRecord, _, _ := array.RecordFromJSON(srv.Alloc, schema_ref.Tables, strings.NewReader(jsonStr))
	defer tablesRecord.Release()

	tablesRecordWithSchema := array.NewRecordBatch(schema_ref.TablesWithIncludedSchema, append(tablesRecord.Columns(), schemaCol), tablesRecord.NumRows())
	defer tablesRecordWithSchema.Release()

	ch := make(chan flight.StreamChunk)

	rdr, err := array.NewRecordReader(schema_ref.TablesWithIncludedSchema, []arrow.RecordBatch{tablesRecordWithSchema})
	go flight.StreamChunksFromReader(rdr, ch)
	return schema_ref.TablesWithIncludedSchema, ch, err
}

func (srv *GetObjectsTestServer) DoGetDBSchemas(ctx context.Context, cmd flightsql.GetDBSchemas) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	schema := schema_ref.DBSchemas
	ch := make(chan flight.StreamChunk, 1)
	// Not really a proper match, but good enough
	catalogs, _, err := array.FromJSON(srv.Alloc, arrow.BinaryTypes.String, strings.NewReader(fmt.Sprintf(`["%s"]`, srv.catalogName)))
	if err != nil {
		return nil, nil, err
	}
	defer catalogs.Release()

	dbSchemas, _, err := array.FromJSON(srv.Alloc, arrow.BinaryTypes.String, strings.NewReader(fmt.Sprintf(`["%s"]`, srv.schemaName)))
	if err != nil {
		return nil, nil, err
	}
	defer dbSchemas.Release()

	batch := array.NewRecordBatch(schema, []arrow.Array{catalogs, dbSchemas}, 1)
	ch <- flight.StreamChunk{Data: batch}
	close(ch)
	return schema, ch, nil
}

type GetObjectsTests struct {
	ServerBasedTests

	catalogName string
	schemaName  string
	tableName   string
}

func (suite *GetObjectsTests) SetupSuite() {
	srv := &GetObjectsTestServer{}
	suite.catalogName = ""
	suite.schemaName = "test_schema"
	suite.tableName = "test_table"
	srv.catalogName = suite.catalogName
	srv.schemaName = suite.schemaName
	srv.tableName = suite.tableName
	srv.testData = map[string][]string{
		"intcols": {
			arrow.PrimitiveTypes.Int32.Name(),                  // TYPE_NAME
			strconv.Itoa(int(arrow.PrimitiveTypes.Int32.ID())), // FieldType
			strconv.Itoa(10),          // PRECISION
			strconv.Itoa(15),          // SCALE
			strconv.FormatBool(true),  // IS_AUTO_INCREMENT
			strconv.FormatBool(false), // IS_CASE_SENSITIVE
			strconv.FormatBool(true),  // IS_READ_ONLY
			strconv.FormatBool(true),  // IS_SEARCHABLE
		},
		"floatcols": {
			arrow.PrimitiveTypes.Float32.Name(),                  // TYPE_NAME
			strconv.Itoa(int(arrow.PrimitiveTypes.Float32.ID())), // FieldType
			strconv.Itoa(15),          // PRECISION
			strconv.Itoa(15),          // SCALE
			strconv.FormatBool(false), // IS_AUTO_INCREMENT
			strconv.FormatBool(false), // IS_CASE_SENSITIVE
			strconv.FormatBool(false), // IS_READ_ONLY
			strconv.FormatBool(false), // IS_SEARCHABLE
		},
		"currencycol": {
			"CURRENCY", // TYPE_NAME
			strconv.Itoa(int(arrow.PrimitiveTypes.Float64.ID())), // FieldType
			strconv.Itoa(15),          // PRECISION
			strconv.Itoa(15),          // SCALE
			strconv.FormatBool(false), // IS_AUTO_INCREMENT
			strconv.FormatBool(false), // IS_CASE_SENSITIVE
			strconv.FormatBool(false), // IS_READ_ONLY
			strconv.FormatBool(false), // IS_SEARCHABLE
		},
	}
	srv.Alloc = memory.NewCheckedAllocator(memory.DefaultAllocator)
	suite.DoSetupSuite(srv, nil, nil)
}

// Testing metadata from flight driver is converted to xdbc metadata.
// Ordering is being ignored to avoid flakiness as the order of the columns is not guaranteed.
func (suite *GetObjectsTests) TestMetadataGetObjectsColumnsXdbc() {
	tests := []struct {
		name       string
		columnName []string
		//ordinalPosition           []string
		remarks                   []string
		xdbcDataType              []string
		xdbcTypeName              []string
		xdbcColumnSize            []string
		xdbcDecimalDigits         []string
		xdbcNumPrecRadix          []string
		xdbcNullable              []string
		xdbcColumnDef             []string
		xdbcSqlDataType           []string
		xdbcDatetimeSub           []string
		xdbcCharOctetLength       []string
		xdbcIsNullable            []string
		xdbcScopeCatalog          []string
		xdbcScopeSchema           []string
		xdbcScopeTable            []string
		xdbcIsAutoincrement       []string
		xdbcIsAutogeneratedColumn []string
	}{
		{
			fmt.Sprintf("%s.%s.%s", suite.catalogName, suite.schemaName, suite.tableName),
			[]string{"currencycol", "floatcols", "intcols"}, //columnName
			//[]string{"1", "2", "3"}, //ordinalPosition
			[]string{"currencycol_", "floatcols_", "intcols_"}, //remarks
			[]string{ //xdbcDataType
				"currencycol_" + strconv.Itoa(int(internal.ToXdbcDataType(arrow.PrimitiveTypes.Float64))),
				"floatcols_" + strconv.Itoa(int(internal.ToXdbcDataType(arrow.PrimitiveTypes.Float32))),
				"intcols_" + strconv.Itoa(int(internal.ToXdbcDataType(arrow.PrimitiveTypes.Int32))),
			},
			[]string{ //xdbcTypeName
				"currencycol_CURRENCY",
				"floatcols_" + arrow.PrimitiveTypes.Float32.Name(),
				"intcols_" + arrow.PrimitiveTypes.Int32.Name(),
			},
			[]string{"currencycol_0", "floatcols_0", "intcols_0"}, //xdbcColumnSize
			[]string{"currencycol_0", "floatcols_0", "intcols_0"}, //xdbcDecimalDigits
			[]string{"currencycol_0", "floatcols_0", "intcols_0"}, //xdbcNumPrecRadix
			[]string{"currencycol_0", "floatcols_0", "intcols_0"}, //xdbcNullable
			[]string{"currencycol_", "floatcols_", "intcols_"},    //xdbcColumnDef
			[]string{ //xdbcSqlDataType
				"currencycol_" + strconv.Itoa(int(internal.ToXdbcDataType(arrow.PrimitiveTypes.Float64))),
				"floatcols_" + strconv.Itoa(int(internal.ToXdbcDataType(arrow.PrimitiveTypes.Float32))),
				"intcols_" + strconv.Itoa(int(internal.ToXdbcDataType(arrow.PrimitiveTypes.Int32))),
			},
			[]string{"currencycol_0", "floatcols_0", "intcols_0"}, //xdbcDatetimeSub
			[]string{"currencycol_0", "floatcols_0", "intcols_0"}, //xdbcCharOctetLength
			[]string{"currencycol_", "floatcols_", "intcols_"},    //xdbcIsNullable
			[]string{ //xdbcScopeCatalog
				"currencycol_" + suite.catalogName,
				"floatcols_" + suite.catalogName,
				"intcols_" + suite.catalogName,
			},
			[]string{ //xdbcScopeSchema
				"currencycol_" + suite.schemaName,
				"floatcols_" + suite.schemaName,
				"intcols_" + suite.schemaName,
			},
			[]string{ //xdbcScopeTable
				"currencycol_" + suite.tableName,
				"floatcols_" + suite.tableName,
				"intcols_" + suite.tableName,
			},
			[]string{ //xdbcIsAutoincrement
				"currencycol_false",
				"floatcols_false",
				"intcols_true",
			},
			[]string{ //xdbcIsAutogeneratedColumn
				"currencycol_false",
				"floatcols_false",
				"intcols_false",
			},
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			rdr, err := suite.cnxn.GetObjects(context.Background(), adbc.ObjectDepthColumns, nil, nil, nil, nil, nil)
			suite.Require().NoError(err)
			defer rdr.Release()

			suite.Truef(adbc.GetObjectsSchema.Equal(rdr.Schema()), "expected: %s\ngot: %s", adbc.GetObjectsSchema, rdr.Schema())
			suite.True(rdr.Next())
			rec := rdr.RecordBatch()
			suite.Greater(rec.NumRows(), int64(0))
			var (
				foundExpected        = false
				catalogDbSchemasList = rec.Column(1).(*array.List)
				catalogDbSchemas     = catalogDbSchemasList.ListValues().(*array.Struct)
				dbSchemaNames        = catalogDbSchemas.Field(0).(*array.String)
				dbSchemaTablesList   = catalogDbSchemas.Field(1).(*array.List)
				dbSchemaTables       = dbSchemaTablesList.ListValues().(*array.Struct)
				tableColumnsList     = dbSchemaTables.Field(2).(*array.List)
				tableColumns         = tableColumnsList.ListValues().(*array.Struct)

				columnName = make([]string, 0)
				//ordinalPosition           = make([]string, 0)
				remarks                   = make([]string, 0)
				xdbcDataType              = make([]string, 0)
				xdbcTypeName              = make([]string, 0)
				xdbcColumnSize            = make([]string, 0)
				xdbcDecimalDigits         = make([]string, 0)
				xdbcNumPrecRadix          = make([]string, 0)
				xdbcNullable              = make([]string, 0)
				xdbcColumnDef             = make([]string, 0)
				xdbcSqlDataType           = make([]string, 0)
				xdbcDatetimeSub           = make([]string, 0)
				xdbcCharOctetLength       = make([]string, 0)
				xdbcIsNullable            = make([]string, 0)
				xdbcScopeCatalog          = make([]string, 0)
				xdbcScopeSchema           = make([]string, 0)
				xdbcScopeTable            = make([]string, 0)
				xdbcIsAutoincrement       = make([]string, 0)
				xdbcIsAutogeneratedColumn = make([]string, 0)
			)
			for row := 0; row < int(rec.NumRows()); row++ {
				dbSchemaIdxStart, dbSchemaIdxEnd := catalogDbSchemasList.ValueOffsets(row)
				for dbSchemaIdx := dbSchemaIdxStart; dbSchemaIdx < dbSchemaIdxEnd; dbSchemaIdx++ {
					schemaName := dbSchemaNames.Value(int(dbSchemaIdx))
					tblIdxStart, tblIdxEnd := dbSchemaTablesList.ValueOffsets(int(dbSchemaIdx))
					for tblIdx := tblIdxStart; tblIdx < tblIdxEnd; tblIdx++ {
						tableName := dbSchemaTables.Field(0).(*array.String).Value(int(tblIdx))

						if strings.EqualFold(schemaName, suite.schemaName) && strings.EqualFold(suite.tableName, tableName) {
							foundExpected = true

							colIdxStart, colIdxEnd := tableColumnsList.ValueOffsets(int(tblIdx))
							for colIdx := colIdxStart; colIdx < colIdxEnd; colIdx++ {
								name := tableColumns.Field(0).(*array.String).Value(int(colIdx))
								columnName = append(columnName, name)

								// pos := tableColumns.Field(1).(*array.Int32).Value(int(colIdx))
								// ordinalPosition = append(ordinalPosition, strconv.Itoa(int(pos)))

								rm := tableColumns.Field(2).(*array.String).Value(int(colIdx))
								remarks = append(remarks, name+"_"+rm)

								xdt := tableColumns.Field(3).(*array.Int16).Value(int(colIdx))
								xdbcDataType = append(xdbcDataType, name+"_"+strconv.Itoa(int(xdt)))

								dataType := tableColumns.Field(4).(*array.String).Value(int(colIdx))
								xdbcTypeName = append(xdbcTypeName, name+"_"+dataType)

								columnSize := tableColumns.Field(5).(*array.Int32).Value(int(colIdx))
								xdbcColumnSize = append(xdbcColumnSize, name+"_"+strconv.Itoa(int(columnSize)))

								decimalDigits := tableColumns.Field(6).(*array.Int16).Value(int(colIdx))
								xdbcDecimalDigits = append(xdbcDecimalDigits, name+"_"+strconv.Itoa(int(decimalDigits)))

								numPrecRadix := tableColumns.Field(7).(*array.Int16).Value(int(colIdx))
								xdbcNumPrecRadix = append(xdbcNumPrecRadix, name+"_"+strconv.Itoa(int(numPrecRadix)))

								nullable := tableColumns.Field(8).(*array.Int16).Value(int(colIdx))
								xdbcNullable = append(xdbcNullable, name+"_"+strconv.Itoa(int(nullable)))

								columnDef := tableColumns.Field(9).(*array.String).Value(int(colIdx))
								xdbcColumnDef = append(xdbcColumnDef, name+"_"+columnDef)

								sqlType := tableColumns.Field(10).(*array.Int16).Value(int(colIdx))
								xdbcSqlDataType = append(xdbcSqlDataType, name+"_"+strconv.Itoa(int(sqlType)))

								dtPrec := tableColumns.Field(11).(*array.Int16).Value(int(colIdx))
								xdbcDatetimeSub = append(xdbcDatetimeSub, name+"_"+strconv.Itoa(int(dtPrec)))

								charOctetLen := tableColumns.Field(12).(*array.Int32).Value(int(colIdx))
								xdbcCharOctetLength = append(xdbcCharOctetLength, name+"_"+strconv.Itoa(int(charOctetLen)))

								isNullable := tableColumns.Field(13).(*array.String).Value(int(colIdx))
								xdbcIsNullable = append(xdbcIsNullable, name+"_"+isNullable)

								scopeCatalog := tableColumns.Field(14).(*array.String).Value(int(colIdx))
								xdbcScopeCatalog = append(xdbcScopeCatalog, name+"_"+scopeCatalog)

								scopeSchema := tableColumns.Field(15).(*array.String).Value(int(colIdx))
								xdbcScopeSchema = append(xdbcScopeSchema, name+"_"+scopeSchema)

								scopeTable := tableColumns.Field(16).(*array.String).Value(int(colIdx))
								xdbcScopeTable = append(xdbcScopeTable, name+"_"+scopeTable)

								isAutoIncrement := tableColumns.Field(17).(*array.Boolean).Value(int(colIdx))
								xdbcIsAutoincrement = append(xdbcIsAutoincrement, name+"_"+strconv.FormatBool(isAutoIncrement))

								isAutoGenerated := tableColumns.Field(18).(*array.Boolean).Value(int(colIdx))
								xdbcIsAutogeneratedColumn = append(xdbcIsAutogeneratedColumn, name+"_"+strconv.FormatBool(isAutoGenerated))
							}
						}
					}
				}
			}

			suite.False(rdr.Next())
			suite.True(foundExpected)

			suite.ElementsMatch(tt.columnName, columnName, "columnName")
			//suite.Equal(tt.ordinalPosition, ordinalPosition, "ordinalPosition")
			suite.ElementsMatch(tt.remarks, remarks, "remarks")
			suite.ElementsMatch(tt.xdbcDataType, xdbcDataType, "xdbcDataType")
			suite.ElementsMatch(tt.xdbcTypeName, xdbcTypeName, "xdbcTypeName")
			suite.ElementsMatch(tt.xdbcColumnSize, xdbcColumnSize, "xdbcColumnSize")
			suite.ElementsMatch(tt.xdbcDecimalDigits, xdbcDecimalDigits, "xdbcDecimalDigits")
			suite.ElementsMatch(tt.xdbcNumPrecRadix, xdbcNumPrecRadix, "xdbcNumPrecRadix")
			suite.ElementsMatch(tt.xdbcNullable, xdbcNullable, "xdbcNullable")
			suite.ElementsMatch(tt.xdbcColumnDef, xdbcColumnDef, "xdbcColumnDef")
			suite.ElementsMatch(tt.xdbcSqlDataType, xdbcSqlDataType, "xdbcSqlDataType")
			suite.ElementsMatch(tt.xdbcDatetimeSub, xdbcDatetimeSub, "xdbcDatetimeSub")
			suite.ElementsMatch(tt.xdbcCharOctetLength, xdbcCharOctetLength, "xdbcCharOctetLength")
			suite.ElementsMatch(tt.xdbcIsNullable, xdbcIsNullable, "xdbcIsNullable")
			suite.ElementsMatch(tt.xdbcScopeCatalog, xdbcScopeCatalog, "xdbcScopeCatalog")
			suite.ElementsMatch(tt.xdbcScopeSchema, xdbcScopeSchema, "xdbcScopeSchema")
			suite.ElementsMatch(tt.xdbcScopeTable, xdbcScopeTable, "xdbcScopeTable")
			suite.ElementsMatch(tt.xdbcIsAutoincrement, xdbcIsAutoincrement, "xdbcIsAutoincrement")
			suite.ElementsMatch(tt.xdbcIsAutogeneratedColumn, xdbcIsAutogeneratedColumn, "xdbcIsAutogeneratedColumn")
		})
	}
}

// ---- Bulk Ingest Tests --------------------

// BulkIngestTestServer implements a FlightSQL server that supports bulk ingestion
type BulkIngestTestServer struct {
	flightsql.BaseServer

	mu             sync.Mutex
	ingestedData   []arrow.RecordBatch
	ingestRequests []flightsql.StatementIngest
}

func (srv *BulkIngestTestServer) DoPutCommandStatementIngest(ctx context.Context, cmd flightsql.StatementIngest, rdr flight.MessageReader) (int64, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	// Store the ingest request for validation
	srv.ingestRequests = append(srv.ingestRequests, cmd)

	var totalRows int64
	for rdr.Next() {
		rec := rdr.RecordBatch()
		rec.Retain()
		srv.ingestedData = append(srv.ingestedData, rec)
		totalRows += rec.NumRows()
	}

	if err := rdr.Err(); err != nil {
		return -1, err
	}

	return totalRows, nil
}

func (srv *BulkIngestTestServer) GetIngestedData() []arrow.RecordBatch {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	result := make([]arrow.RecordBatch, len(srv.ingestedData))
	copy(result, srv.ingestedData)
	return result
}

func (srv *BulkIngestTestServer) GetIngestRequests() []flightsql.StatementIngest {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	result := make([]flightsql.StatementIngest, len(srv.ingestRequests))
	copy(result, srv.ingestRequests)
	return result
}

func (srv *BulkIngestTestServer) Clear() {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	for _, rec := range srv.ingestedData {
		rec.Release()
	}
	srv.ingestedData = nil
	srv.ingestRequests = nil
}

type BulkIngestTests struct {
	ServerBasedTests
	server *BulkIngestTestServer
}

func (suite *BulkIngestTests) SetupSuite() {
	suite.server = &BulkIngestTestServer{}
	suite.DoSetupSuite(suite.server, nil, nil)
}

func (suite *BulkIngestTests) TearDownTest() {
	suite.server.Clear()
	suite.ServerBasedTests.TearDownTest()
}

func (suite *BulkIngestTests) TestBulkIngestCreate() {
	stmt, err := suite.cnxn.NewStatement()
	suite.Require().NoError(err)
	defer validation.CheckedClose(suite.T(), stmt)

	// Set up for bulk ingest
	suite.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestTargetTable, "test_table"))
	suite.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreate))

	// Create sample data
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	bldr.Field(1).(*array.StringBuilder).AppendValues([]string{"Alice", "Bob", "Charlie"}, nil)

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	// Bind the data
	suite.Require().NoError(stmt.Bind(context.Background(), rec))

	// Execute the ingest
	nRows, err := stmt.ExecuteUpdate(context.Background())
	suite.Require().NoError(err)
	suite.Equal(int64(3), nRows)

	// Verify data was ingested
	ingestedData := suite.server.GetIngestedData()
	suite.Require().Len(ingestedData, 1)
	suite.Equal(int64(3), ingestedData[0].NumRows())

	// Verify request parameters
	requests := suite.server.GetIngestRequests()
	suite.Require().Len(requests, 1)
	suite.Equal("test_table", requests[0].GetTable())
}

func (suite *BulkIngestTests) TestBulkIngestAppend() {
	stmt, err := suite.cnxn.NewStatement()
	suite.Require().NoError(err)
	defer validation.CheckedClose(suite.T(), stmt)

	// Set up for bulk ingest with append mode
	suite.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestTargetTable, "existing_table"))
	suite.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeAppend))

	// Create sample data
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
	}, nil)

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bldr.Release()

	bldr.Field(0).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2, 3.3, 4.4}, nil)

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	// Bind and execute
	suite.Require().NoError(stmt.Bind(context.Background(), rec))
	nRows, err := stmt.ExecuteUpdate(context.Background())
	suite.Require().NoError(err)
	suite.Equal(int64(4), nRows)

	// Verify request parameters
	requests := suite.server.GetIngestRequests()
	suite.Require().Len(requests, 1)
	suite.Equal("existing_table", requests[0].GetTable())

	tableDefOpts := requests[0].GetTableDefinitionOptions()
	suite.Require().NotNil(tableDefOpts)
	// Append mode: fail if table doesn't exist, append if it does
	suite.Equal(flightproto.CommandStatementIngest_TableDefinitionOptions_TABLE_NOT_EXIST_OPTION_FAIL, tableDefOpts.IfNotExist)
	suite.Equal(flightproto.CommandStatementIngest_TableDefinitionOptions_TABLE_EXISTS_OPTION_APPEND, tableDefOpts.IfExists)
}

func (suite *BulkIngestTests) TestBulkIngestReplace() {
	stmt, err := suite.cnxn.NewStatement()
	suite.Require().NoError(err)
	defer validation.CheckedClose(suite.T(), stmt)

	// Set up for bulk ingest with replace mode
	suite.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestTargetTable, "replace_table"))
	suite.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeReplace))

	// Create sample data
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "col1", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bldr.Release()

	bldr.Field(0).(*array.Int32Builder).AppendValues([]int32{10, 20}, nil)

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	// Bind and execute
	suite.Require().NoError(stmt.Bind(context.Background(), rec))
	nRows, err := stmt.ExecuteUpdate(context.Background())
	suite.Require().NoError(err)
	suite.Equal(int64(2), nRows)

	// Verify request parameters
	requests := suite.server.GetIngestRequests()
	suite.Require().Len(requests, 1)

	tableDefOpts := requests[0].GetTableDefinitionOptions()
	suite.Require().NotNil(tableDefOpts)
	// Replace mode: create if doesn't exist, replace if it does
	suite.Equal(flightproto.CommandStatementIngest_TableDefinitionOptions_TABLE_NOT_EXIST_OPTION_CREATE, tableDefOpts.IfNotExist)
	suite.Equal(flightproto.CommandStatementIngest_TableDefinitionOptions_TABLE_EXISTS_OPTION_REPLACE, tableDefOpts.IfExists)
}

func (suite *BulkIngestTests) TestBulkIngestCreateAppend() {
	stmt, err := suite.cnxn.NewStatement()
	suite.Require().NoError(err)
	defer validation.CheckedClose(suite.T(), stmt)

	// Set up for bulk ingest with create_append mode
	suite.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestTargetTable, "create_append_table"))
	suite.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreateAppend))

	// Create sample data
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bldr.Release()

	bldr.Field(0).(*array.StringBuilder).AppendValues([]string{"test1", "test2"}, nil)

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	// Bind and execute
	suite.Require().NoError(stmt.Bind(context.Background(), rec))
	nRows, err := stmt.ExecuteUpdate(context.Background())
	suite.Require().NoError(err)
	suite.Equal(int64(2), nRows)

	// Verify request parameters
	requests := suite.server.GetIngestRequests()
	suite.Require().Len(requests, 1)

	tableDefOpts := requests[0].GetTableDefinitionOptions()
	suite.Require().NotNil(tableDefOpts)
	// CreateAppend mode: create if doesn't exist, append if it does
	suite.Equal(flightproto.CommandStatementIngest_TableDefinitionOptions_TABLE_NOT_EXIST_OPTION_CREATE, tableDefOpts.IfNotExist)
	suite.Equal(flightproto.CommandStatementIngest_TableDefinitionOptions_TABLE_EXISTS_OPTION_APPEND, tableDefOpts.IfExists)
}

func (suite *BulkIngestTests) TestBulkIngestWithCatalogAndSchema() {
	stmt, err := suite.cnxn.NewStatement()
	suite.Require().NoError(err)
	defer validation.CheckedClose(suite.T(), stmt)

	// Set up for bulk ingest with catalog and schema
	suite.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestTargetTable, "qualified_table"))
	suite.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreate))
	suite.Require().NoError(stmt.SetOption(adbc.OptionValueIngestTargetCatalog, "my_catalog"))
	suite.Require().NoError(stmt.SetOption(adbc.OptionValueIngestTargetDBSchema, "my_schema"))

	// Create sample data
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{100}, nil)

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	// Bind and execute
	suite.Require().NoError(stmt.Bind(context.Background(), rec))
	nRows, err := stmt.ExecuteUpdate(context.Background())
	suite.Require().NoError(err)
	suite.Equal(int64(1), nRows)

	// Verify request parameters
	requests := suite.server.GetIngestRequests()
	suite.Require().Len(requests, 1)
	suite.Equal("qualified_table", requests[0].GetTable())
	suite.Equal("my_catalog", requests[0].GetCatalog())
	suite.Equal("my_schema", requests[0].GetSchema())
}

func (suite *BulkIngestTests) TestBulkIngestTemporary() {
	stmt, err := suite.cnxn.NewStatement()
	suite.Require().NoError(err)
	defer validation.CheckedClose(suite.T(), stmt)

	// Set up for temporary table ingest
	suite.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestTargetTable, "temp_table"))
	suite.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreate))
	suite.Require().NoError(stmt.SetOption(adbc.OptionValueIngestTemporary, adbc.OptionValueEnabled))

	// Create sample data
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "temp_col", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bldr.Release()

	bldr.Field(0).(*array.Int32Builder).AppendValues([]int32{1}, nil)

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	// Bind and execute
	suite.Require().NoError(stmt.Bind(context.Background(), rec))
	nRows, err := stmt.ExecuteUpdate(context.Background())
	suite.Require().NoError(err)
	suite.Equal(int64(1), nRows)

	// Verify request parameters
	requests := suite.server.GetIngestRequests()
	suite.Require().Len(requests, 1)
	suite.True(requests[0].GetTemporary())
}

func (suite *BulkIngestTests) TestBulkIngestWithStream() {
	stmt, err := suite.cnxn.NewStatement()
	suite.Require().NoError(err)
	defer validation.CheckedClose(suite.T(), stmt)

	// Set up for bulk ingest
	suite.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestTargetTable, "stream_table"))
	suite.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreate))

	// Create sample data with multiple batches
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "batch_id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)

	bldr.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
	rec1 := bldr.NewRecordBatch()

	bldr.Field(0).(*array.Int32Builder).AppendValues([]int32{4, 5}, nil)
	rec2 := bldr.NewRecordBatch()

	bldr.Release()

	// Create record reader from multiple batches
	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec1, rec2})
	suite.Require().NoError(err)

	// Bind stream and execute
	suite.Require().NoError(stmt.BindStream(context.Background(), rdr))
	nRows, err := stmt.ExecuteUpdate(context.Background())
	suite.Require().NoError(err)
	suite.Equal(int64(5), nRows)

	// Verify data was ingested
	ingestedData := suite.server.GetIngestedData()
	suite.Require().Len(ingestedData, 2)

	var totalRows int64
	for _, rec := range ingestedData {
		totalRows += rec.NumRows()
	}
	suite.Equal(int64(5), totalRows)
}

func (suite *BulkIngestTests) TestBulkIngestWithoutBind() {
	stmt, err := suite.cnxn.NewStatement()
	suite.Require().NoError(err)
	defer validation.CheckedClose(suite.T(), stmt)

	// Set up for bulk ingest without binding data
	suite.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestTargetTable, "no_data_table"))
	suite.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreate))

	// Try to execute without binding - should fail
	_, err = stmt.ExecuteUpdate(context.Background())
	suite.Require().Error(err)
	suite.Contains(err.Error(), "must call Bind before bulk ingestion")
}

func (suite *BulkIngestTests) TestBulkIngestViaExecuteQuery() {
	stmt, err := suite.cnxn.NewStatement()
	suite.Require().NoError(err)
	defer validation.CheckedClose(suite.T(), stmt)

	// Set up for bulk ingest
	suite.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestTargetTable, "query_table"))
	suite.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreate))

	// Create sample data
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	// Bind the data
	suite.Require().NoError(stmt.Bind(context.Background(), rec))

	// Execute via ExecuteQuery (should work for ingest too)
	rdr, nRows, err := stmt.ExecuteQuery(context.Background())
	suite.Require().NoError(err)
	suite.Nil(rdr) // No result set for ingest
	suite.Equal(int64(2), nRows)

	// Verify data was ingested
	ingestedData := suite.server.GetIngestedData()
	suite.Require().Len(ingestedData, 1)
	suite.Equal(int64(2), ingestedData[0].NumRows())
}
