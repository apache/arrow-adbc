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

package bigquery

import (
	"context"
	"strings"
	"testing"
)

func TestCustomAccessTokenEndpointAndServerName(t *testing.T) {
	accessTokenEndpoint := "https://example.com/oauth2/token"
	accessTokenServerName := "example.com"
	conn := &connectionImpl{
		authType:              "adbc.bigquery.sql.auth_type.user_authentication",
		catalog:               "test-project-id",
		dbSchema:              "test-dataset-id",
		tableID:               "test-table-id",
		clientID:              "test-client-id",
		clientSecret:          "test-client-secret",
		refreshToken:          "test-refresh-token",
		accessTokenEndpoint:   accessTokenEndpoint,
		accessTokenServerName: accessTokenServerName,
	}

	err := conn.newClient(context.Background())
	if err != nil {
		t.Errorf("Error occurred %v", err)
	}
	if conn.accessTokenEndpoint != accessTokenEndpoint {
		t.Errorf("Expected access token endpoint to be %s, but got %s", accessTokenEndpoint, conn.accessTokenEndpoint)
	}
	if conn.accessTokenServerName != accessTokenServerName {
		t.Errorf("Expected access token server name to be %s, but got %s", accessTokenServerName, conn.accessTokenServerName)
	}
}

func TestDefaultAccessTokenEndpoint(t *testing.T) {
	conn := &connectionImpl{
		authType:     "adbc.bigquery.sql.auth_type.user_authentication",
		catalog:      "test-project-id",
		dbSchema:     "test-dataset-id",
		tableID:      "test-table-id",
		clientID:     "test-client-id",
		clientSecret: "test-client-secret",
		refreshToken: "test-refresh-token",
	}

	err := conn.newClient(context.Background())
	if err != nil {
		t.Errorf("Error occurred %v", err)
	}
	if conn.accessTokenEndpoint != DefaultAccessTokenEndpoint {
		t.Errorf("Expected access token endpoint to be %s, but got %s", DefaultAccessTokenEndpoint, conn.accessTokenEndpoint)
	}
	if conn.accessTokenServerName != DefaultAccessTokenServerName {
		t.Errorf("Expected access token server name to be %s, but got %s", DefaultAccessTokenServerName, conn.accessTokenServerName)
	}
}

func TestCustomAccessTokenEndpoint(t *testing.T) {
	accessTokenEndpoint := "https://example.com/oauth2/token"
	conn := &connectionImpl{
		authType:            "adbc.bigquery.sql.auth_type.user_authentication",
		catalog:             "test-project-id",
		dbSchema:            "test-dataset-id",
		tableID:             "test-table-id",
		clientID:            "test-client-id",
		clientSecret:        "test-client-secret",
		refreshToken:        "test-refresh-token",
		accessTokenEndpoint: accessTokenEndpoint,
	}

	err := conn.newClient(context.Background())
	if err != nil {
		t.Errorf("Error occurred %v", err)
	}
	if conn.accessTokenEndpoint != accessTokenEndpoint {
		t.Errorf("Expected access token endpoint to be %s, but got %s", accessTokenEndpoint, conn.accessTokenEndpoint)
	}
	if conn.accessTokenServerName != "" {
		t.Errorf("Expected access token server name to be blank, but got %s", conn.accessTokenServerName)
	}
}

func TestTemporaryAccessToken_Valid(t *testing.T) {
	conn := &connectionImpl{
		authType:    "adbc.bigquery.sql.auth_type.temporary_access_token",
		catalog:     "test-project-id",
		dbSchema:    "test-dataset-id",
		tableID:     "test-table-id",
		accessToken: "ya29.fake-access-token-for-test",
	}

	err := conn.newClient(context.Background())
	if err != nil {
		t.Errorf("Expected no error with valid temporary access token, got: %v", err)
	}
}

func TestTemporaryAccessToken_MissingToken(t *testing.T) {
	conn := &connectionImpl{
		authType: "adbc.bigquery.sql.auth_type.temporary_access_token",
		catalog:  "test-project-id",
		dbSchema: "test-dataset-id",
		tableID:  "test-table-id",
		// accessToken intentionally missing
	}

	err := conn.newClient(context.Background())
	if err == nil {
		t.Fatal("Expected error due to missing access token, got nil")
	}
	if !strings.Contains(err.Error(), "adbc.bigquery.sql.auth.access_token") {
		t.Errorf("Expected error message to mention missing access token, got: %v", err)
	}
}
