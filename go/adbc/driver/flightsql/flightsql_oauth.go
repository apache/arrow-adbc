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

// TokenSource supplies PerRPCCredentials from an oauth2.TokenSource.
type FlightTokenSource struct {
	oauth2.TokenSource
}

// GetRequestMetadata gets the request metadata as a map from a TokenSource.
func (ts FlightTokenSource) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	token, err := ts.Token()
	if err != nil {
		return nil, err
	}
	// ri, _ := credentials.RequestInfoFromContext(ctx)
	// if err = credentials.CheckSecurityLevel(ri.AuthInfo, credentials.PrivacyAndIntegrity); err != nil {
	// 	return nil, fmt.Errorf("unable to transfer TokenSource PerRPCCredentials: %v", err)
	// }
	return map[string]string{
		"authorization": token.Type() + " " + token.AccessToken,
	}, nil
}

// RequireTransportSecurity indicates whether the credentials requires transport security.
func (ts FlightTokenSource) RequireTransportSecurity() bool {
	return false
}

// Bit flags for different OAuth authentication methods. Enables multiple authentication methods to be
// specified simultaneaously if needed
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

const (
	ttPrefix              = "urn:ietf:params:oauth:token-type:"
	TokenTypeAccessToken  = ttPrefix + "access_token"
	TokenTypeRefreshToken = ttPrefix + "refresh_token"
	TokenTypeIdToken      = ttPrefix + "id_token"
	TokenTypeSaml1        = ttPrefix + "saml1"
	TokenTypeSaml2        = ttPrefix + "saml2"
	TokenTypeJWT          = ttPrefix + "jwt"
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

func exchangeToken(conf *oauth2.Config, codeOptions []oauth2.AuthCodeOption, tlsConfig *tls.Config) (credentials.PerRPCCredentials, error) {
	ctx := context.Background()

	if tlsConfig != nil {
		// Set the HTTP client with custom TLS config in the context
		httpClient := &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: tlsConfig,
			},
		}

		ctx = context.WithValue(ctx, oauth2.HTTPClient, httpClient)
	}

	tok, err := conf.Exchange(ctx, "", codeOptions...)
	if err != nil {
		return nil, err
	}
	// return &FlightTokenSource{TokenSource: conf.TokenSource(ctx, tok)}, nil
	return &oauth.TokenSource{TokenSource: conf.TokenSource(ctx, tok)}, nil
}

func newClientCredentials(options map[string]string, tlsConfig *tls.Config) (credentials.PerRPCCredentials, error) {
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

	return exchangeToken(conf, codeOptions, tlsConfig)
}

func newTokenExchangeFlow(options map[string]string, tlsConfig *tls.Config) (credentials.PerRPCCredentials, error) {
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

	return exchangeToken(conf, codeOptions, tlsConfig)
}
