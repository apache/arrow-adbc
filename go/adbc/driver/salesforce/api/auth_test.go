package api

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/api/types"
	"github.com/stretchr/testify/suite"
)

type AuthSuite struct {
	suite.Suite
}

func (s *AuthSuite) generateTestKey() string {
	s.T().Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	s.Require().NoError(err)
	keyBytes := x509.MarshalPKCS1PrivateKey(key)
	block := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: keyBytes}
	return string(pem.EncodeToMemory(block))
}

func (s *AuthSuite) TestAuthenticate_JWTFlow() {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.Require().Equal("/services/oauth2/token", r.URL.Path)
		s.Require().Equal("POST", r.Method)

		err := r.ParseForm()
		s.Require().NoError(err)
		s.Equal("urn:ietf:params:oauth:grant-type:jwt-bearer", r.FormValue("grant_type"))
		s.NotEmpty(r.FormValue("assertion"))

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"access_token": "test-access-token",
			"instance_url": "https://myinstance.salesforce.com",
			"token_type":   "Bearer",
		})
	}))
	defer server.Close()

	cfg := &types.AuthConfig{
		LoginURL:      server.URL,
		ClientID:      "test-client-id",
		Username:      "user@example.com",
		PrivateKeyPEM: s.generateTestKey(),
		APIVersion:    "v64.0",
	}

	client, err := NewClient(cfg)
	s.Require().NoError(err)
	defer client.Close()

	err = client.Authenticate(context.Background())
	s.Require().NoError(err)
	s.Equal("https://myinstance.salesforce.com", client.http.BaseURL())
}

func (s *AuthSuite) TestAuthenticate_InvalidKey() {
	cfg := &types.AuthConfig{
		LoginURL:      "https://login.salesforce.com",
		ClientID:      "test-client-id",
		Username:      "user@example.com",
		PrivateKeyPEM: "not-a-valid-key",
		APIVersion:    "v64.0",
	}

	client, err := NewClient(cfg)
	s.Require().NoError(err)
	defer client.Close()

	err = client.Authenticate(context.Background())
	s.Error(err)
}

func TestAuthSuite(t *testing.T) {
	suite.Run(t, new(AuthSuite))
}
