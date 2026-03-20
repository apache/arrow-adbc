package api

import (
	"testing"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/api/types"
	"github.com/stretchr/testify/suite"
)

type ClientSuite struct {
	suite.Suite
}

func (s *ClientSuite) TestNewClient() {
	cfg := &types.AuthConfig{
		LoginURL:      "https://login.salesforce.com",
		ClientID:      "test-client-id",
		Username:      "user@example.com",
		PrivateKeyPEM: "fake-key",
		APIVersion:    "v64.0",
	}
	client, err := NewClient(cfg)
	s.Require().NoError(err)
	s.NotNil(client)
	defer client.Close()
}

func (s *ClientSuite) TestNewClient_NilConfig() {
	_, err := NewClient(nil)
	s.Error(err)
}

func (s *ClientSuite) TestNewClient_DefaultAPIVersion() {
	cfg := &types.AuthConfig{
		LoginURL:      "https://login.salesforce.com",
		ClientID:      "test-client-id",
		Username:      "user@example.com",
		PrivateKeyPEM: "fake-key",
	}
	client, err := NewClient(cfg)
	s.Require().NoError(err)
	defer client.Close()
	s.Equal("v64.0", client.config.APIVersion)
}

func (s *ClientSuite) TestNewClient_NormalizeAPIVersion() {
	cases := []struct {
		input    string
		expected string
	}{
		{"64.0", "v64.0"},
		{"v64.0", "v64.0"},
		{"64", "v64.0"},
		{"v64", "v64.0"},
		{"63.0", "v63.0"},
	}
	for _, tc := range cases {
		cfg := &types.AuthConfig{
			LoginURL:      "https://login.salesforce.com",
			ClientID:      "test-client-id",
			Username:      "user@example.com",
			PrivateKeyPEM: "fake-key",
			APIVersion:    tc.input,
		}
		client, err := NewClient(cfg)
		s.Require().NoError(err)
		s.Equal(tc.expected, client.config.APIVersion, "input %q should normalize to %q", tc.input, tc.expected)
		client.Close()
	}
}

func TestClientSuite(t *testing.T) {
	suite.Run(t, new(ClientSuite))
}
