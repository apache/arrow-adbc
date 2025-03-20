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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/bluele/gcache"
	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type dbDialOpts struct {
	opts       []grpc.DialOption
	maxMsgSize int
	authority  string
}

func (d *dbDialOpts) rebuild() {
	d.opts = []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(d.maxMsgSize),
			grpc.MaxCallSendMsgSize(d.maxMsgSize)),
	}
	if d.authority != "" {
		d.opts = append(d.opts, grpc.WithAuthority(d.authority))
	}
}

type databaseImpl struct {
	driverbase.DatabaseImplBase

	uri           *url.URL
	creds         credentials.TransportCredentials
	token         string
	user, pass    string
	hdrs          metadata.MD
	timeout       timeoutOption
	dialOpts      dbDialOpts
	enableCookies bool
	options       map[string]string
	userDialOpts  []grpc.DialOption
	tokenExchange *tokenExchangeFlow
}

type OauthFlow int32

const (
	Anonymous     OauthFlow = iota
	AuthPKCE      OauthFlow = 1
	Confidential  OauthFlow = 2
	TokenExchange OauthFlow = 5
)

type clientCredentials struct {
	conf  *oauth2.Config
	token *oauth2.Token
}

func newClientCredentials(clientId string, clientSecret string, endpoint string) *clientCredentials {
	return &clientCredentials{
		conf: &oauth2.Config{
			ClientID:     clientId,
			ClientSecret: clientSecret,
			Endpoint: oauth2.Endpoint{
				TokenURL: fmt.Sprintf("%s/token", endpoint),
			},
		},
	}
}

type confidentialFlow struct {
	conf  *oauth2.Config
	token *oauth2.Token
}

func newConfidentialFlow(clientId string, clientSecret string, endpoint string) *confidentialFlow {
	return &confidentialFlow{
		conf: &oauth2.Config{
			ClientID:     clientId,
			ClientSecret: clientSecret,
			Endpoint: oauth2.Endpoint{
				TokenURL: fmt.Sprintf("%s/token", endpoint),
			},
		},
	}
}

func (c *confidentialFlow) GetToken(ctx context.Context) (string, error) {
	token, err := c.conf.TokenSource(ctx, c.token).Token()
	if err != nil {
		return "", err
	}
	return token.AccessToken, nil
}

type tokenExchangeFlow struct {
	conf      *oauth2.Config
	origToken *oauth2.Token
	token     *oauth2.Token
}

func newTokenExchangeFlow(origToken *oauth2.Token, endpoint string, scopes []string) *tokenExchangeFlow {
	return &tokenExchangeFlow{
		origToken: origToken,
		conf: &oauth2.Config{
			Endpoint: oauth2.Endpoint{
				TokenURL: fmt.Sprintf("%s/oauth/token", endpoint),
			},
		},
	}
}

func (f *tokenExchangeFlow) GetToken(ctx context.Context) (string, error) {

	if f.token == nil {
		if f.origToken == nil {
			return "", fmt.Errorf("no token to exchange")
		}

		options := []oauth2.AuthCodeOption{
			oauth2.SetAuthURLParam("subject_token", f.origToken.AccessToken),
			oauth2.SetAuthURLParam("subject_token_type", "urn:ietf:params:oauth:token-type:jwt"),
			oauth2.SetAuthURLParam("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange"),
		}

		options = append(options, oauth2.SetAuthURLParam("scope", "dremio.all"))

		tok, err := f.conf.Exchange(ctx, "", options...)
		if err != nil {
			return "", err
		}

		f.token = tok
		return tok.AccessToken, nil
	}

	token, err := f.conf.TokenSource(ctx, f.token).Token()
	if err != nil {
		return "", err
	}
	return token.AccessToken, nil
}

// conf2 := &oauth2.Config{
// 	// ClientID: "dremio-backend",
// 	// ClientSecret:
// 	// RedirectURL: "http://localhost:8555/",
// 	Scopes: scopes,
// 	Endpoint: oauth2.Endpoint{
// 		AuthURL:  "http://localhost:9047/oauth/auth",
// 		TokenURL: "http://localhost:9047/oauth/token",
// 	},
// }

// options := []oauth2.AuthCodeOption{
// 	oauth2.SetAuthURLParam("subject_token", access_token.AccessToken),
// 	oauth2.SetAuthURLParam("subject_token_type", "urn:ietf:params:oauth:token-type:jwt"),
// 	oauth2.SetAuthURLParam("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange"),
// }
// if len(scopes) > 0 {
// 	options = append(options, oauth2.SetAuthURLParam("scope", "dremio.all")) // offline_access"))
// }

// tok, err := conf2.Exchange(ctx, "", options...)

// type authPKCEAuthenticator struct {
// 	token *oauth2.Token
// }

// func newTokenAuthenticator() *authPKCEAuthenticator {
// 	conf := &oauth2.Config{
// 		ClientID: "dremio-backend",
// 		// ClientSecret:
// 		RedirectURL: "http://localhost:8555/",
// 		// Scopes:
// 		Endpoint: oauth2.Endpoint{
// 			TokenURL:      "http://localhost:8080/realms/dremio-realm/protocol/openid-connect/token",
// 			AuthURL:       "http://localhost:8080/realms/dremio-realm/protocol/openid-connect/auth",
// 			DeviceAuthURL: "http://localhost:8080/realms/dremio-realm/protocol/openid-connect/auth/device",
// 			// AuthStyle:
// 		},
// 	}

// 	// use PKCE to protect against CSRF attacks
// 	// https://www.ietf.org/archive/id/draft-ietf-oauth-security-topics-22.html#name-countermeasures-6
// 	verifier := oauth2.GenerateVerifier()

// 	// Redirect user to consent page to ask for permission
// 	// for the scopes specified above.
// 	url := conf.AuthCodeURL("state", oauth2.AccessTypeOffline, oauth2.S256ChallengeOption(verifier))
// 	fmt.Printf("Visit the URL for the auth dialog: %v", url)

// 	http.HandleFunc("/", redirectHandler)

// 	// Start the HTTP server on port 8080
// 	srv := &http.Server{Addr: ":8555"}

// 	// Start the server in a goroutine
// 	go func() {
// 		fmt.Println("Starting server on port 8080")
// 		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
// 			fmt.Printf("Error starting server: %v\n", err)
// 		}
// 	}()

// 	// Wait for the stop signal
// 	code := <-stopChan

// 	// Create a context with a timeout to allow the server to shut down gracefully
// 	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
// 	defer cancel()

// 	// Shutdown the server
// 	if err := srv.Shutdown(ctx); err != nil {
// 		fmt.Printf("Server forced to shutdown: %v\n", err)
// 	}

// 	fmt.Println("Server stopped")

// 	return &authPKCEAuthenticator{
// 		token: &oauth2.Token{},
// 	}
// }

// func (t *authPKCEAuthenticator) GetToken(ctx context.Context) (string, error) {
// 	//Check if the token is expired
// 	return t.token.AccessToken, nil
// }

func (d *databaseImpl) SetOptions(cnOptions map[string]string) error {
	var tlsConfig tls.Config

	for k, v := range cnOptions {
		d.options[k] = v
	}

	if authority, ok := cnOptions[OptionAuthority]; ok {
		d.dialOpts.authority = authority
		delete(cnOptions, OptionAuthority)
	}

	mtlsCert := cnOptions[OptionMTLSCertChain]
	mtlsKey := cnOptions[OptionMTLSPrivateKey]
	switch {
	case mtlsCert != "" && mtlsKey != "":
		cert, err := tls.X509KeyPair([]byte(mtlsCert), []byte(mtlsKey))
		if err != nil {
			return adbc.Error{
				Msg:  fmt.Sprintf("Invalid mTLS certificate: %#v", err),
				Code: adbc.StatusInvalidArgument,
			}
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
		delete(cnOptions, OptionMTLSCertChain)
		delete(cnOptions, OptionMTLSPrivateKey)
	case mtlsCert != "":
		return adbc.Error{
			Msg:  fmt.Sprintf("Must provide both '%s' and '%s', only provided '%s'", OptionMTLSCertChain, OptionMTLSPrivateKey, OptionMTLSCertChain),
			Code: adbc.StatusInvalidArgument,
		}
	case mtlsKey != "":
		return adbc.Error{
			Msg:  fmt.Sprintf("Must provide both '%s' and '%s', only provided '%s'", OptionMTLSCertChain, OptionMTLSPrivateKey, OptionMTLSPrivateKey),
			Code: adbc.StatusInvalidArgument,
		}
	}

	if hostname, ok := cnOptions[OptionSSLOverrideHostname]; ok {
		tlsConfig.ServerName = hostname
		delete(cnOptions, OptionSSLOverrideHostname)
	}

	if val, ok := cnOptions[OptionSSLSkipVerify]; ok {
		if val == adbc.OptionValueEnabled {
			tlsConfig.InsecureSkipVerify = true
		} else if val == adbc.OptionValueDisabled {
			tlsConfig.InsecureSkipVerify = false
		} else {
			return adbc.Error{
				Msg:  fmt.Sprintf("Invalid value for database option '%s': '%s'", OptionSSLSkipVerify, val),
				Code: adbc.StatusInvalidArgument,
			}
		}
		delete(cnOptions, OptionSSLSkipVerify)
	}

	if cert, ok := cnOptions[OptionSSLRootCerts]; ok {
		cp := x509.NewCertPool()
		if !cp.AppendCertsFromPEM([]byte(cert)) {
			return adbc.Error{
				Msg:  fmt.Sprintf("Invalid value for database option '%s': failed to append certificates", OptionSSLRootCerts),
				Code: adbc.StatusInvalidArgument,
			}
		}
		tlsConfig.RootCAs = cp
		delete(cnOptions, OptionSSLRootCerts)
	}

	d.creds = credentials.NewTLS(&tlsConfig)

	if auth, ok := cnOptions[OptionAuthorizationHeader]; ok {
		d.hdrs.Set("authorization", auth)
		delete(cnOptions, OptionAuthorizationHeader)
	}

	if u, ok := cnOptions[adbc.OptionKeyUsername]; ok {
		if d.hdrs.Len() > 0 {
			return adbc.Error{
				Msg:  "Authentication conflict: Use either Authorization header OR username/password parameter",
				Code: adbc.StatusInvalidArgument,
			}
		}
		d.user = u
		delete(cnOptions, adbc.OptionKeyUsername)
	}

	if p, ok := cnOptions[adbc.OptionKeyPassword]; ok {
		if d.hdrs.Len() > 0 {
			return adbc.Error{
				Msg:  "Authentication conflict: Use either Authorization header OR username/password parameter",
				Code: adbc.StatusInvalidArgument,
			}
		}
		d.pass = p
		delete(cnOptions, adbc.OptionKeyPassword)
	}

	if p, ok := cnOptions[adbc.OptionKeyToken]; ok {
		if d.hdrs.Len() > 0 {
			return adbc.Error{
				Msg:  "Authentication conflict: Use either Authorization header OR token parameter",
				Code: adbc.StatusInvalidArgument,
			}
		}

		// d.tokenExchange = newTokenExchangeFlow(&oauth2.Token{AccessToken: p}, "http://autorelease.drem.io:9047", []string{"dremio.all"})
		d.tokenExchange = newTokenExchangeFlow(&oauth2.Token{AccessToken: p}, "http://localhost:9047", []string{"dremio.all"})
		// d.token = p
		delete(cnOptions, adbc.OptionKeyToken)
	}

	var err error
	if tv, ok := cnOptions[OptionTimeoutFetch]; ok {
		if err = d.timeout.setTimeoutString(OptionTimeoutFetch, tv); err != nil {
			return err
		}
		delete(cnOptions, OptionTimeoutFetch)
	}

	if tv, ok := cnOptions[OptionTimeoutQuery]; ok {
		if err = d.timeout.setTimeoutString(OptionTimeoutQuery, tv); err != nil {
			return err
		}
		delete(cnOptions, OptionTimeoutQuery)
	}

	if tv, ok := cnOptions[OptionTimeoutUpdate]; ok {
		if err = d.timeout.setTimeoutString(OptionTimeoutUpdate, tv); err != nil {
			return err
		}
		delete(cnOptions, OptionTimeoutUpdate)
	}

	if tv, ok := cnOptions[OptionTimeoutConnect]; ok {
		if err = d.timeout.setTimeoutString(OptionTimeoutConnect, tv); err != nil {
			return err
		}
		delete(cnOptions, OptionTimeoutConnect)
	}

	// gRPC deprecated this and explicitly recommends against it
	delete(cnOptions, OptionWithBlock)

	if val, ok := cnOptions[OptionWithMaxMsgSize]; ok {
		var err error
		var size int
		if size, err = strconv.Atoi(val); err != nil {
			return adbc.Error{
				Msg:  fmt.Sprintf("Invalid value for database option '%s': '%s' is not a positive integer", OptionWithMaxMsgSize, val),
				Code: adbc.StatusInvalidArgument,
			}
		} else if size <= 0 {
			return adbc.Error{
				Msg:  fmt.Sprintf("Invalid value for database option '%s': '%s' is not a positive integer", OptionWithMaxMsgSize, val),
				Code: adbc.StatusInvalidArgument,
			}
		}
		d.dialOpts.maxMsgSize = size
		delete(cnOptions, OptionWithMaxMsgSize)
	}
	d.dialOpts.rebuild()

	if val, ok := cnOptions[OptionCookieMiddleware]; ok {
		if val == adbc.OptionValueEnabled {
			d.enableCookies = true
		} else if val == adbc.OptionValueDisabled {
			d.enableCookies = false
		} else {
			return d.ErrorHelper.Errorf(adbc.StatusInvalidArgument, "Invalid value for database option '%s': '%s'", OptionCookieMiddleware, val)
		}
		delete(cnOptions, OptionCookieMiddleware)
	}

	for key, val := range cnOptions {
		if strings.HasPrefix(key, OptionRPCCallHeaderPrefix) {
			d.hdrs.Append(strings.TrimPrefix(key, OptionRPCCallHeaderPrefix), val)
			continue
		}
		return d.ErrorHelper.Errorf(adbc.StatusInvalidArgument, "[Flight SQL] Unknown database option '%s'", key)
	}

	return nil
}

func (d *databaseImpl) GetOption(key string) (string, error) {
	switch key {
	case OptionTimeoutFetch:
		return d.timeout.fetchTimeout.String(), nil
	case OptionTimeoutQuery:
		return d.timeout.queryTimeout.String(), nil
	case OptionTimeoutUpdate:
		return d.timeout.updateTimeout.String(), nil
	case OptionTimeoutConnect:
		return d.timeout.connectTimeout.String(), nil
	}
	if val, ok := d.options[key]; ok {
		return val, nil
	}
	return d.DatabaseImplBase.GetOption(key)
}

func (d *databaseImpl) GetOptionInt(key string) (int64, error) {
	switch key {
	case OptionTimeoutFetch:
		fallthrough
	case OptionTimeoutQuery:
		fallthrough
	case OptionTimeoutUpdate:
		fallthrough
	case OptionTimeoutConnect:
		val, err := d.GetOptionDouble(key)
		if err != nil {
			return 0, err
		}
		return int64(val), nil
	}

	return d.DatabaseImplBase.GetOptionInt(key)
}

func (d *databaseImpl) GetOptionDouble(key string) (float64, error) {
	switch key {
	case OptionTimeoutFetch:
		return d.timeout.fetchTimeout.Seconds(), nil
	case OptionTimeoutQuery:
		return d.timeout.queryTimeout.Seconds(), nil
	case OptionTimeoutUpdate:
		return d.timeout.updateTimeout.Seconds(), nil
	case OptionTimeoutConnect:
		return d.timeout.connectTimeout.Seconds(), nil
	}

	return d.DatabaseImplBase.GetOptionDouble(key)
}

func (d *databaseImpl) SetOption(key, value string) error {
	// We can't change most options post-init
	switch key {
	case OptionTimeoutFetch, OptionTimeoutQuery, OptionTimeoutUpdate, OptionTimeoutConnect:
		return d.timeout.setTimeoutString(key, value)
	}
	if strings.HasPrefix(key, OptionRPCCallHeaderPrefix) {
		d.hdrs.Set(strings.TrimPrefix(key, OptionRPCCallHeaderPrefix), value)
	}
	return d.DatabaseImplBase.SetOption(key, value)
}

func (d *databaseImpl) SetOptionInt(key string, value int64) error {
	switch key {
	case OptionTimeoutFetch:
		fallthrough
	case OptionTimeoutQuery:
		fallthrough
	case OptionTimeoutUpdate:
		fallthrough
	case OptionTimeoutConnect:
		return d.timeout.setTimeout(key, float64(value))
	}

	return d.DatabaseImplBase.SetOptionInt(key, value)
}

func (d *databaseImpl) SetOptionDouble(key string, value float64) error {
	switch key {
	case OptionTimeoutFetch:
		fallthrough
	case OptionTimeoutQuery:
		fallthrough
	case OptionTimeoutUpdate:
		fallthrough
	case OptionTimeoutConnect:
		return d.timeout.setTimeout(key, value)
	}

	return d.DatabaseImplBase.SetOptionDouble(key, value)
}

func (d *databaseImpl) Close() error {
	return nil
}

func redirectHandler(w http.ResponseWriter, r *http.Request) {
	// Extract query parameters from the redirect URI
	code := r.URL.Query().Get("code")
	state := r.URL.Query().Get("state")

	// Print the received code and state
	fmt.Printf("Received code: %s\n", code)
	fmt.Printf("Received state: %s\n", state)

	// Respond to the client
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Authorization code received. You can close this window."))
	stopChan <- code
}

var stopChan = make(chan string)

func getRefreshToken(ctx context.Context) (string, error) {

	conf := &oauth2.Config{
		ClientID: "dremio-backend",
		// ClientSecret:
		RedirectURL: "http://localhost:8555/",
		// Scopes:
		Endpoint: oauth2.Endpoint{
			TokenURL:      "http://localhost:8080/realms/dremio-realm/protocol/openid-connect/token",
			AuthURL:       "http://localhost:8080/realms/dremio-realm/protocol/openid-connect/auth",
			DeviceAuthURL: "http://localhost:8080/realms/dremio-realm/protocol/openid-connect/auth/device",
			// AuthStyle:
		},
	}

	// use PKCE to protect against CSRF attacks
	// https://www.ietf.org/archive/id/draft-ietf-oauth-security-topics-22.html#name-countermeasures-6
	verifier := oauth2.GenerateVerifier()

	// Redirect user to consent page to ask for permission
	// for the scopes specified above.
	url := conf.AuthCodeURL("state", oauth2.AccessTypeOffline, oauth2.S256ChallengeOption(verifier))
	fmt.Printf("Visit the URL for the auth dialog: %v", url)

	http.HandleFunc("/", redirectHandler)

	// Start the HTTP server on port 8080
	srv := &http.Server{Addr: ":8555"}

	// Start the server in a goroutine
	go func() {
		fmt.Println("Starting server on port 8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("Error starting server: %v\n", err)
		}
	}()

	// Wait for the stop signal
	code := <-stopChan

	// Create a context with a timeout to allow the server to shut down gracefully
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Shutdown the server
	if err := srv.Shutdown(ctx); err != nil {
		fmt.Printf("Server forced to shutdown: %v\n", err)
	}

	fmt.Println("Server stopped")

	// Use the authorization code that is pushed to the redirect
	// URL. Exchange will do the handshake to retrieve the
	// initial access token. The HTTP Client returned by
	// conf.Client will refresh the token as necessary.
	// if _, err := fmt.Scan(&code); err != nil {
	// 	fmt.Errorf("%s", err)
	// }

	// options := []oauth2.AuthCodeOption {
	//     oauth2.SetAuthURLParam("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange"),
	//     oauth2.SetAuthURLParam("subject_token", "a-token-jwt"),
	//     oauth2.SetAuthURLParam("subject_token_type", "urn:ietf:params:oauth:token-type:jwt"),
	// }

	// https://github.com/apache/arrow/issues/38565
	// https://github.com/apache/arrow/issues/41919

	access_token, err := conf.Exchange(ctx, code, oauth2.VerifierOption(verifier))

	var scopes = []string{"dremio.all", "offline_access"}

	conf2 := &oauth2.Config{
		// ClientID: "dremio-backend",
		// ClientSecret:
		// RedirectURL: "http://localhost:8555/",
		Scopes: scopes,
		Endpoint: oauth2.Endpoint{
			AuthURL:  "http://localhost:9047/oauth/auth",
			TokenURL: "http://localhost:9047/oauth/token",
		},
	}

	options := []oauth2.AuthCodeOption{
		oauth2.SetAuthURLParam("subject_token", access_token.AccessToken),
		oauth2.SetAuthURLParam("subject_token_type", "urn:ietf:params:oauth:token-type:jwt"),
		oauth2.SetAuthURLParam("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange"),
	}
	if len(scopes) > 0 {
		options = append(options, oauth2.SetAuthURLParam("scope", "dremio.all")) // offline_access"))
	}

	tok, err := conf2.Exchange(ctx, "", options...)
	if err != nil {
		return "", err
	}

	return tok.AccessToken, nil
}

func getFlightClient(ctx context.Context, loc string, d *databaseImpl, authMiddle *bearerAuthMiddleware, cookies flight.CookieMiddleware) (*flightsql.Client, error) {
	middleware := []flight.ClientMiddleware{
		{
			Unary:  makeUnaryLoggingInterceptor(d.Logger),
			Stream: makeStreamLoggingInterceptor(d.Logger),
		},
		flight.CreateClientMiddleware(authMiddle),
		{
			Unary:  unaryTimeoutInterceptor,
			Stream: streamTimeoutInterceptor,
		},
	}

	if d.enableCookies {
		middleware = append(middleware, flight.CreateClientMiddleware(cookies))
	}

	uri, err := url.Parse(loc)
	if err != nil {
		return nil, adbc.Error{Msg: fmt.Sprintf("Invalid URI '%s': %s", loc, err), Code: adbc.StatusInvalidArgument}
	}
	creds := d.creds

	target := uri.Host
	if uri.Scheme == "grpc" || uri.Scheme == "grpc+tcp" {
		creds = insecure.NewCredentials()
	} else if uri.Scheme == "grpc+unix" {
		creds = insecure.NewCredentials()
		target = "unix:" + uri.Path
	}

	dv, _ := d.DatabaseImplBase.DriverInfo.GetInfoForInfoCode(adbc.InfoDriverVersion)
	driverVersion := dv.(string)
	dialOpts := append(d.dialOpts.opts, grpc.WithConnectParams(d.timeout.connectParams()), grpc.WithTransportCredentials(creds), grpc.WithUserAgent("ADBC Flight SQL Driver "+driverVersion))
	dialOpts = append(dialOpts, d.userDialOpts...)

	d.Logger.DebugContext(ctx, "new client", "location", loc)
	// cl, err := flightsql.NewClient(target, &clientAuth{}, middleware, dialOpts...)
	cl, err := flightsql.NewClient(target, nil, middleware, dialOpts...)
	if err != nil {
		return nil, adbc.Error{
			Msg:  err.Error(),
			Code: adbc.StatusIO,
		}
	}

	cl.Alloc = d.Alloc
	if len(authMiddle.hdrs.Get("authorization")) > 0 {
		d.Logger.DebugContext(ctx, "reusing auth token", "location", loc)
	} else {
		if d.tokenExchange != nil {
			token, err := d.tokenExchange.GetToken(ctx)
			if err != nil {
				return nil, adbcFromFlightStatusWithDetails(err, nil, nil, "Authenticate TokenExchange")
			}
			authMiddle.hdrs.Set("authorization", "Bearer "+token)
		} else if d.token != "" {
			token, err := getRefreshToken(ctx)
			if err != nil {
				return nil, adbcFromFlightStatusWithDetails(err, nil, nil, "Authenticate")
			}
			authMiddle.hdrs.Set("authorization", "Bearer "+token)
		} else if d.user != "" || d.pass != "" {
			var header, trailer metadata.MD
			ctx, err = cl.Client.AuthenticateBasicToken(ctx, d.user, d.pass, grpc.Header(&header), grpc.Trailer(&trailer), d.timeout)
			if err != nil {
				return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "AuthenticateBasicToken")
			}

			if md, ok := metadata.FromOutgoingContext(ctx); ok {
				authMiddle.mutex.Lock()
				defer authMiddle.mutex.Unlock()
				authMiddle.hdrs.Set("authorization", md.Get("Authorization")[0])
			}
		}
	}

	return cl, nil
}

type support struct {
	transactions bool
}

func (d *databaseImpl) Open(ctx context.Context) (adbc.Connection, error) {
	authMiddle := &bearerAuthMiddleware{hdrs: d.hdrs.Copy()}
	var cookies flight.CookieMiddleware
	if d.enableCookies {
		cookies = flight.NewCookieMiddleware()
	}

	cl, err := getFlightClient(ctx, d.uri.String(), d, authMiddle, cookies)
	if err != nil {
		return nil, err
	}

	cache := gcache.New(20).LRU().
		Expiration(5 * time.Minute).
		LoaderFunc(func(loc interface{}) (interface{}, error) {
			uri, ok := loc.(string)
			if !ok {
				return nil, adbc.Error{Msg: fmt.Sprintf("Location must be a string, got %#v",
					uri), Code: adbc.StatusInternal}
			}

			var cookieMiddleware flight.CookieMiddleware
			// if cookies are enabled, start by cloning the existing cookies
			if d.enableCookies {
				cookieMiddleware = cookies.Clone()
			}
			// use the existing auth token if there is one
			cl, err := getFlightClient(context.Background(), uri, d,
				&bearerAuthMiddleware{hdrs: authMiddle.hdrs.Copy()}, cookieMiddleware)
			if err != nil {
				return nil, err
			}

			cl.Alloc = d.Alloc
			return cl, nil
		}).
		EvictedFunc(func(_, client interface{}) {
			conn := client.(*flightsql.Client)
			conn.Close()
		}).Build()

	var cnxnSupport support

	info, err := cl.GetSqlInfo(ctx, []flightsql.SqlInfo{flightsql.SqlInfoFlightSqlServerTransaction}, d.timeout)
	// ignore this if it fails
	if err == nil {
		const int32code = 3

		for _, endpoint := range info.Endpoint {
			rdr, err := doGet(ctx, cl, endpoint, cache, d.timeout)
			if err != nil {
				continue
			}
			defer rdr.Release()

			for rdr.Next() {
				rec := rdr.Record()
				codes := rec.Column(0).(*array.Uint32)
				values := rec.Column(1).(*array.DenseUnion)
				int32Value := values.Field(int32code).(*array.Int32)

				for i := 0; i < int(rec.NumRows()); i++ {
					switch codes.Value(i) {
					case uint32(flightsql.SqlInfoFlightSqlServerTransaction):
						if values.TypeCode(i) != int32code {
							continue
						}

						idx := values.ValueOffset(i)
						if !int32Value.IsValid(int(idx)) {
							continue
						}

						value := int32Value.Value(int(idx))
						cnxnSupport.transactions =
							value == int32(flightsql.SqlTransactionTransaction) ||
								value == int32(flightsql.SqlTransactionSavepoint)
					}
				}
			}
		}
	}

	conn := &connectionImpl{
		cl: cl, db: d, clientCache: cache,
		hdrs: make(metadata.MD), timeouts: d.timeout, supportInfo: cnxnSupport,
		ConnectionImplBase: driverbase.NewConnectionImplBase(&d.DatabaseImplBase),
	}

	return driverbase.NewConnectionBuilder(conn).
		WithDriverInfoPreparer(conn).
		WithAutocommitSetter(conn).
		WithCurrentNamespacer(conn).
		Connection(), nil
}

type bearerAuthMiddleware struct {
	mutex sync.RWMutex
	hdrs  metadata.MD
}

func (b *bearerAuthMiddleware) StartCall(ctx context.Context) context.Context {
	md, _ := metadata.FromOutgoingContext(ctx)
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	return metadata.NewOutgoingContext(ctx, metadata.Join(md, b.hdrs))
}

func (b *bearerAuthMiddleware) HeadersReceived(ctx context.Context, md metadata.MD) {
	// apache/arrow-adbc#584
	headers := md.Get("authorization")
	if len(headers) > 0 {
		b.mutex.Lock()
		defer b.mutex.Unlock()
		b.hdrs.Set("authorization", headers...)
	}
}
