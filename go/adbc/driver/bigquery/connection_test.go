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
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/bigquery"
	"golang.org/x/oauth2/google/externalaccount"
)

func TestDatabaseAPIEndpointOption(t *testing.T) {
	db := &databaseImpl{}

	err := db.SetOption(OptionStringAPIEndpoint, "https://bigquery.googleapis.com/bigquery/v2/")
	if err != nil {
		t.Fatalf("SetOption returned error: %v", err)
	}

	got, err := db.GetOption(OptionStringAPIEndpoint)
	if err != nil {
		t.Fatalf("GetOption returned error: %v", err)
	}
	if got != "https://bigquery.googleapis.com/bigquery/v2/" {
		t.Fatalf("expected endpoint to round-trip, got %q", got)
	}
}

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

// --- External-account (Workload Identity Federation) -----------------------

const testWIFAudience = "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/pool/providers/prov"

func TestExternalAccountAuthTypeAccepted(t *testing.T) {
	db := &databaseImpl{}
	if err := db.SetOption(OptionStringAuthType, OptionValueAuthTypeExternalAccount); err != nil {
		t.Fatalf("external_account auth type should be accepted: %v", err)
	}
	if err := db.SetOption(OptionStringAuthExternalAccountRequestData, "grant_type=client_credentials"); err != nil {
		t.Fatalf("SetOption request_data: %v", err)
	}
	if got, _ := db.GetOption(OptionStringAuthExternalAccountRequestData); got != "grant_type=client_credentials" {
		t.Errorf("request_data should round-trip, got %q", got)
	}
}

func TestExternalAccount_AuthOptions_Valid(t *testing.T) {
	conn := &connectionImpl{
		authType:                   OptionValueAuthTypeExternalAccount,
		catalog:                    "test-project",
		dbSchema:                   "test-dataset",
		externalAccountAudience:    testWIFAudience,
		externalAccountRequestURL:  "https://idp.example.com/oauth2/v1/token",
		externalAccountRequestData: "grant_type=client_credentials&client_id=abc&client_secret=secret&scope=s",
	}
	opts, err := conn.authOptions(context.Background())
	if err != nil {
		t.Fatalf("expected no error building external-account options, got: %v", err)
	}
	if len(opts) == 0 {
		t.Fatal("expected at least one client option (the token source)")
	}
}

func TestExternalAccount_MissingFields(t *testing.T) {
	base := func() *connectionImpl {
		return &connectionImpl{
			authType:                   OptionValueAuthTypeExternalAccount,
			externalAccountAudience:    testWIFAudience,
			externalAccountRequestURL:  "https://idp/token",
			externalAccountRequestData: "grant_type=client_credentials",
		}
	}
	cases := []struct {
		name   string
		mutate func(*connectionImpl)
		want   string
	}{
		{"audience", func(c *connectionImpl) { c.externalAccountAudience = "" }, OptionStringAuthExternalAccountAudience},
		{"request_url", func(c *connectionImpl) { c.externalAccountRequestURL = "" }, OptionStringAuthExternalAccountRequestURL},
		{"request_data", func(c *connectionImpl) { c.externalAccountRequestData = "" }, OptionStringAuthExternalAccountRequestData},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			conn := base()
			tt.mutate(conn)
			_, err := conn.authOptions(context.Background())
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected error mentioning %q, got: %v", tt.want, err)
			}
		})
	}
}

func TestIdpTokenSupplier_SubjectToken(t *testing.T) {
	const reqData = "grant_type=client_credentials&scope=s"

	cases := []struct {
		name      string
		handler   func(w http.ResponseWriter, r *http.Request, attempt int32)
		wantToken string
		wantErr   string // expected error substring; empty means success
		wantCalls int32  // 0 means don't assert
	}{
		{
			name: "returns token on success",
			handler: func(w http.ResponseWriter, r *http.Request, _ int32) {
				if r.Method != http.MethodPost {
					t.Errorf("expected POST, got %s", r.Method)
				}
				if got := r.Header.Get("Content-Type"); got != "application/x-www-form-urlencoded" {
					t.Errorf("unexpected Content-Type: %s", got)
				}
				if body, _ := io.ReadAll(r.Body); string(body) != reqData {
					t.Errorf("request body was modified before sending: %q", string(body))
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"access_token":"tok-123","expires_in":3600}`))
			},
			wantToken: "tok-123",
			wantCalls: 1,
		},
		{
			name: "retries on 500 then succeeds",
			handler: func(w http.ResponseWriter, r *http.Request, attempt int32) {
				if attempt == 1 {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"access_token":"tok-after-retry"}`))
			},
			wantToken: "tok-after-retry",
			wantCalls: 2,
		},
		{
			name: "no retry on 4xx, surfaces oauth error",
			handler: func(w http.ResponseWriter, r *http.Request, _ int32) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":"invalid_client","error_description":"bad secret"}`))
			},
			wantErr:   "invalid_client",
			wantCalls: 1,
		},
		{
			name: "non-json body reports parse error",
			handler: func(w http.ResponseWriter, r *http.Request, _ int32) {
				w.Header().Set("Content-Type", "text/html")
				_, _ = w.Write([]byte("<html>proxy login page</html>"))
			},
			wantErr: "as JSON",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			var calls int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				tt.handler(w, r, atomic.AddInt32(&calls, 1))
			}))
			defer srv.Close()

			s := &idpTokenSupplier{requestURL: srv.URL, requestData: reqData}
			tok, err := s.SubjectToken(context.Background(), externalaccount.SupplierOptions{})

			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got: %v", tt.wantErr, err)
			}
			if tok != tt.wantToken {
				t.Fatalf("expected token %q, got %q", tt.wantToken, tok)
			}
			if tt.wantCalls != 0 && atomic.LoadInt32(&calls) != tt.wantCalls {
				t.Fatalf("expected %d call(s), got %d", tt.wantCalls, atomic.LoadInt32(&calls))
			}
		})
	}
}

func TestBuildField(t *testing.T) {
	tests := []struct {
		name            string
		schema          *bigquery.FieldSchema
		expectedTypeStr string
		expectError     bool
	}{
		{
			name: "ArrayOfScalar",
			schema: &bigquery.FieldSchema{
				Name:        "test_array_scalar_field",
				Type:        bigquery.IntegerFieldType,
				Repeated:    true,
				Required:    false,
				Description: "Test array field with scalar type",
				Schema:      nil,
			},
			expectedTypeStr: "list<item: int64, nullable>",
			expectError:     false,
		},
		{
			name: "ArrayOfRecordWithMultipleFields",
			schema: &bigquery.FieldSchema{
				Name:        "test_array_field",
				Type:        bigquery.RecordFieldType,
				Repeated:    true,
				Required:    false,
				Description: "Test array field with multiple nested fields",
				Schema: []*bigquery.FieldSchema{
					{
						Name:     "field1",
						Type:     bigquery.StringFieldType,
						Required: false,
					},
					{
						Name:     "field2",
						Type:     bigquery.IntegerFieldType,
						Required: false,
					},
				},
			},
			expectedTypeStr: "list<item: struct<field1: utf8, field2: int64>, nullable>",
			expectError:     false,
		},
		{
			name: "ArrayOfRecordWithSingleField",
			schema: &bigquery.FieldSchema{
				Name:        "test_single_array_field",
				Type:        bigquery.RecordFieldType,
				Repeated:    true,
				Required:    false,
				Description: "Test array field with single nested field",
				Schema: []*bigquery.FieldSchema{
					{
						Name:     "single_field",
						Type:     bigquery.StringFieldType,
						Required: false,
					},
				},
			},
			expectedTypeStr: "list<item: struct<single_field: utf8>, nullable>",
			expectError:     false,
		},
		{
			name: "NonRepeatedRecord",
			schema: &bigquery.FieldSchema{
				Name:        "test_struct_field",
				Type:        bigquery.RecordFieldType,
				Repeated:    false,
				Required:    false,
				Description: "Test struct field with multiple nested fields",
				Schema: []*bigquery.FieldSchema{
					{
						Name:     "nested_string",
						Type:     bigquery.StringFieldType,
						Required: false,
					},
					{
						Name:     "nested_int",
						Type:     bigquery.IntegerFieldType,
						Required: true,
					},
				},
			},
			expectedTypeStr: "struct<nested_string: utf8, nested_int: int64>",
			expectError:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field, err := buildField(tt.schema, 0)

			if tt.expectError {
				if err == nil {
					t.Fatalf("Expected error for test case %s, but got nil", tt.name)
				}
				return
			}

			if err != nil {
				t.Fatalf("Expected no error for test case %s, got: %v", tt.name, err)
			}

			if field.Name != tt.schema.Name {
				t.Errorf("Expected field name '%s', got '%s'", tt.schema.Name, field.Name)
			}

			typeStr := field.Type.String()
			if typeStr != tt.expectedTypeStr {
				t.Errorf("Expected field type string to be '%s', got '%s'", tt.expectedTypeStr, typeStr)
			}
		})
	}
}
