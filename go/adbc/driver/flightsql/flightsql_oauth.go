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

	"golang.org/x/oauth2"
)

type OauthAuthFlow interface {
	GetToken(ctx context.Context) (*oauth2.Token, error)
}

const (
	AuthPKCE = 1 << iota
	ClientCredentials
	TokenExchange
)

type oAuthOption struct {
	isRequired bool
	oAuthKey   string
}

type oAuthConfig struct {
	conf        *oauth2.Config
	flowOptions []oauth2.AuthCodeOption
	token       oauth2.TokenSource
}

func (c *oAuthConfig) GetToken(ctx context.Context) (*oauth2.Token, error) {
	if c.token == nil {
		tok, err := c.conf.Exchange(ctx, "", c.flowOptions...)
		if err != nil {
			return nil, err
		}

		c.token = c.conf.TokenSource(ctx, tok)
		return tok, nil
	}

	return c.token.Token()
}

var (
	clientCredentialsParams = map[string]oAuthOption{
		OptionKeyClientId:     {true, "client_id"},
		OptionKeyClientSecret: {true, "client_secret"},
		OptionKeyTokenURI:     {true, "token_uri"},
		OptionKeyScope:        {false, "scope"},
	}
)

func newClientCredentials(options map[string]string) (*oAuthConfig, error) {

	params := map[string]string{}

	for key, param := range clientCredentialsParams {
		if value, ok := options[key]; ok {
			params[key] = value
			delete(options, key)
		} else if param.isRequired {
			return nil, fmt.Errorf("client credentials grant requires %s", key)
		}
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

	return &oAuthConfig{
		conf: conf,
		flowOptions: []oauth2.AuthCodeOption{
			oauth2.SetAuthURLParam("grant_type", "client_credentials"),
		},
	}, nil
}

var (
	tokenExchangParams = map[string]oAuthOption{
		OptionKeyToken:            {true, "subject_token"},
		OptionKeySubjectTokenType: {true, "subject_token_type"},
		OptionKeyReqTokenType:     {false, "requested_token_type"},
		OptionKeyExchangeAud:      {false, "audience"},
		OptionKeyExchangeResource: {false, "resource"},
		OptionKeyExchangeScope:    {false, "scope"},
	}
)

func newTokenExchangeFlow(options map[string]string) (*oAuthConfig, error) {

	tokenURI, ok := options[OptionKeyTokenURI]
	if !ok {
		return nil, fmt.Errorf("token exchange grant requires adbc.flight.sql.oauth.token_uri")
	}
	delete(options, OptionKeyTokenURI)

	conf := &oauth2.Config{
		Endpoint: oauth2.Endpoint{
			TokenURL: tokenURI,
		},
	}

	tokOptions := []oauth2.AuthCodeOption{
		oauth2.SetAuthURLParam("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange"),
	}

	for key, param := range tokenExchangParams {
		if value, ok := options[key]; ok {
			tokOptions = append(tokOptions, oauth2.SetAuthURLParam(param.oAuthKey, value))
			delete(options, key)
		} else if param.isRequired {
			return nil, fmt.Errorf("token exchange grant requires %s", key)
		}
	}

	// actor token and actor token type are optional
	// but if one is present, the other must be present
	if actor, ok := options[OptionKeyActorToken]; ok {
		tokOptions = append(tokOptions, oauth2.SetAuthURLParam("actor_token", actor))
		delete(options, OptionKeyActorToken)
		if actorTokenType, ok := options[OptionKeyActorTokenType]; ok {
			tokOptions = append(tokOptions, oauth2.SetAuthURLParam("actor_token_type", actorTokenType))
			delete(options, OptionKeyActorTokenType)
		} else {
			return nil, fmt.Errorf("token exchange grant requires actor_token_type")
		}
	}

	return &oAuthConfig{
		conf:        conf,
		flowOptions: tokOptions,
	}, nil
}
