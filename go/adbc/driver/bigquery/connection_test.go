package bigquery

import (
	"context"
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
