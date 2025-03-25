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

	"github.com/apache/arrow-adbc/go/adbc"
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

type clientCredentials struct {
	conf  *oauth2.Config
	token oauth2.TokenSource
}

func newClientCredentials(options map[string]string) (*clientCredentials, error) {
	clientId, ok := options[OptionKeyClientId]
	if !ok {
		return nil, fmt.Errorf("client credentials grant requires client_id")
	}

	clientSecret, ok := options[OptionKeyClientSecret]
	if !ok {
		return nil, fmt.Errorf("client credentials grant requires client_secret")
	}

	tokenURI, ok := options[OptionKeyTokenURI]
	if !ok {
		return nil, fmt.Errorf("client credentials grant requires token_uri")
	}

	delete(options, OptionKeyClientId)
	delete(options, OptionKeyClientSecret)
	delete(options, OptionKeyTokenURI)
	return &clientCredentials{
		conf: &oauth2.Config{
			ClientID:     clientId,
			ClientSecret: clientSecret,
			Endpoint: oauth2.Endpoint{
				TokenURL: tokenURI,
			},
		},
	}, nil
}

func (c *clientCredentials) GetToken(ctx context.Context) (*oauth2.Token, error) {
	option := []oauth2.AuthCodeOption{oauth2.SetAuthURLParam("grant_type", "client_credentials")}
	if c.token == nil {
		tok, err := c.conf.Exchange(ctx, "", option...)
		if err != nil {
			return nil, err
		}

		c.token = c.conf.TokenSource(ctx, tok)
		return tok, nil
	}

	return c.token.Token()
}

type tokenExchange struct {
	conf                 *oauth2.Config
	tokenExchangeOptions []oauth2.AuthCodeOption
	token                oauth2.TokenSource
}

func newTokenExchangeFlow(options map[string]string) (*tokenExchange, error) {
	token, ok := options[adbc.OptionKeyToken]
	if !ok {
		return nil, fmt.Errorf("token Exchange grant requires token")
	}

	subjectTokenType, ok := options[OptionKeySubjectTokenType]
	if !ok {
		return nil, fmt.Errorf("token Exchange grant requires subject token type")
	}

	tokenURI, ok := options[OptionKeyTokenURI]
	if !ok {
		return nil, fmt.Errorf("token exchange grant requires token URI")
	}

	tokOptions := []oauth2.AuthCodeOption{
		oauth2.SetAuthURLParam("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange"),
		oauth2.SetAuthURLParam("subject_token", token),
		oauth2.SetAuthURLParam("subject_token_type", subjectTokenType),
	}

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

	if reqTokenType, ok := options[OptionKeyReqTokenType]; ok {
		tokOptions = append(tokOptions, oauth2.SetAuthURLParam("requested_token_type", reqTokenType))
		delete(options, OptionKeyReqTokenType)
	}

	if aud, ok := options[OptionKeyExchangeAud]; ok {
		tokOptions = append(tokOptions, oauth2.SetAuthURLParam("audience", aud))
		delete(options, OptionKeyExchangeAud)
	}

	if resource, ok := options[OptionKeyExchangeResource]; ok {
		tokOptions = append(tokOptions, oauth2.SetAuthURLParam("resource", resource))
		delete(options, OptionKeyExchangeResource)
	}

	if scope, ok := options[OptionKeyExchangeScope]; ok {
		tokOptions = append(tokOptions, oauth2.SetAuthURLParam("scope", scope))
		delete(options, OptionKeyExchangeScope)
	}

	delete(options, OptionKeyTokenURI)
	delete(options, OptionKeySubjectTokenType)
	delete(options, adbc.OptionKeyToken)

	return &tokenExchange{
		conf: &oauth2.Config{
			Endpoint: oauth2.Endpoint{
				TokenURL: tokenURI,
			},
		},
		tokenExchangeOptions: tokOptions,
	}, nil
}

func (f *tokenExchange) GetToken(ctx context.Context) (*oauth2.Token, error) {
	if f.token == nil {
		tok, err := f.conf.Exchange(ctx, "", f.tokenExchangeOptions...)
		if err != nil {
			return nil, err
		}

		f.token = f.conf.TokenSource(ctx, tok)
		return tok, nil
	}

	token, err := f.token.Token()
	if err != nil {
		return nil, err
	}
	return token, nil
}
