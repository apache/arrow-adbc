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
	"fmt"
	"net/http"

	"golang.org/x/oauth2"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
)

const (
	ClientCredentials = "client_credentials"
	TokenExchange     = "token_exchange"
)

type oAuthOption struct {
	isRequired bool
	oAuthKey   string
}

var (
	clientCredentialsParams = map[string]oAuthOption{
		OptionKeyClientId:     {true, "client_id"},
		OptionKeyClientSecret: {true, "client_secret"},
		OptionKeyTokenURI:     {true, "token_uri"},
		OptionKeyScope:        {false, "scope"},
	}

	tokenExchangParams = map[string]oAuthOption{
		OptionKeySubjectToken:     {true, "subject_token"},
		OptionKeySubjectTokenType: {true, "subject_token_type"},
		OptionKeyReqTokenType:     {false, "requested_token_type"},
		OptionKeyExchangeAud:      {false, "audience"},
		OptionKeyExchangeResource: {false, "resource"},
		OptionKeyExchangeScope:    {false, "scope"},
	}
)

func parseOAuthOptions(options map[string]string, paramMap map[string]oAuthOption, flowName string) (map[string]string, error) {
	params := map[string]string{}

	for key, param := range paramMap {
		if value, ok := options[key]; ok {
			params[key] = value
			delete(options, key)
		} else if param.isRequired {
			return nil, fmt.Errorf("%s grant requires %s", flowName, key)
		}
	}

	return params, nil
}

func createOAuthContext(tlsConfig *tls.Config) context.Context {
	ctx := context.Background()

	if tlsConfig == nil {
		return ctx
	}

	// Create a custom HTTP client with TLS config to use for oauth calls
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}

	return context.WithValue(ctx, oauth2.HTTPClient, httpClient)
}

func exchangeToken(ctx context.Context, conf *oauth2.Config, codeOptions []oauth2.AuthCodeOption) (credentials.PerRPCCredentials, error) {
	tok, err := conf.Exchange(ctx, "", codeOptions...)
	if err != nil {
		return nil, err
	}
	return &oauth.TokenSource{TokenSource: conf.TokenSource(ctx, tok)}, nil
}

func newClientCredentials(options map[string]string, tlsConfig *tls.Config) (credentials.PerRPCCredentials, error) {
	ctx := createOAuthContext(tlsConfig)

	codeOptions := []oauth2.AuthCodeOption{
		// Required value for client credentials requests as specified in https://datatracker.ietf.org/doc/html/rfc6749#section-4.4.2
		oauth2.SetAuthURLParam("grant_type", "client_credentials"),
	}

	params, err := parseOAuthOptions(options, clientCredentialsParams, "client credentials")
	if err != nil {
		return nil, err
	}

	conf := &oauth2.Config{
		ClientID:     params[OptionKeyClientId],
		ClientSecret: params[OptionKeyClientSecret],
		Endpoint: oauth2.Endpoint{
			TokenURL: params[OptionKeyTokenURI],
		},
	}

	if scopes, ok := params[OptionKeyScope]; ok {
		conf.Scopes = []string{scopes}
	}

	return exchangeToken(ctx, conf, codeOptions)
}

func newTokenExchangeFlow(options map[string]string, tlsConfig *tls.Config) (credentials.PerRPCCredentials, error) {
	ctx := createOAuthContext(tlsConfig)

	tokenURI, ok := options[OptionKeyTokenURI]
	if !ok {
		return nil, fmt.Errorf("token exchange grant requires %s", OptionKeyTokenURI)
	}
	delete(options, OptionKeyTokenURI)

	conf := &oauth2.Config{
		Endpoint: oauth2.Endpoint{
			TokenURL: tokenURI,
		},
	}

	codeOptions := []oauth2.AuthCodeOption{
		// Required value for token exchange requests as specified in https://datatracker.ietf.org/doc/html/rfc8693#name-request
		oauth2.SetAuthURLParam("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange"),
	}

	params, err := parseOAuthOptions(options, tokenExchangParams, "token exchange")
	if err != nil {
		return nil, err
	}

	for key, param := range tokenExchangParams {
		if value, ok := params[key]; ok {
			codeOptions = append(codeOptions, oauth2.SetAuthURLParam(param.oAuthKey, value))
		}
	}

	// actor token and actor token type are optional
	// but if one is present, the other must be present
	if actor, ok := options[OptionKeyActorToken]; ok {
		codeOptions = append(codeOptions, oauth2.SetAuthURLParam("actor_token", actor))
		delete(options, OptionKeyActorToken)
		if actorTokenType, ok := options[OptionKeyActorTokenType]; ok {
			codeOptions = append(codeOptions, oauth2.SetAuthURLParam("actor_token_type", actorTokenType))
			delete(options, OptionKeyActorTokenType)
		} else {
			return nil, fmt.Errorf("token exchange grant requires %s when %s is provided",
				OptionKeyActorTokenType, OptionKeyActorToken)
		}
	}

	return exchangeToken(ctx, conf, codeOptions)
}
