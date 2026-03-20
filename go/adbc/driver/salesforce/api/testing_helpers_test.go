package api

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/api/types"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gopkg.in/dnaeon/go-vcr.v4/pkg/cassette"
	"gopkg.in/dnaeon/go-vcr.v4/pkg/recorder"
	"resty.dev/v3"
)

// APISuite is a base test suite that uses VCR cassettes.
//
// When real credentials are available (SFDC_ env vars), it authenticates
// against live Salesforce and records HTTP interactions to cassettes.
// When credentials are absent, it replays from existing cassettes.
type APISuite struct {
	suite.Suite
	Client   *Client
	recorder *recorder.Recorder
}

// hasRealCredentials returns true if SFDC_ env vars are set for live recording.
func hasRealCredentials() bool {
	return os.Getenv("SFDC_LOGIN_URL") != "" &&
		os.Getenv("SFDC_CLIENT_ID") != "" &&
		os.Getenv("SFDC_USERNAME") != "" &&
		os.Getenv("SFDC_CLIENT_PRIVATE_KEY_PATH") != ""
}

// realAuthConfig builds an AuthConfig from SFDC_ env vars.
func realAuthConfig(t require.TestingT) *types.AuthConfig {
	keyPath := os.Getenv("SFDC_CLIENT_PRIVATE_KEY_PATH")
	if !filepath.IsAbs(keyPath) {
		keyPath = filepath.Join("..", keyPath)
	}
	keyPEM, err := os.ReadFile(keyPath)
	require.NoError(t, err)

	return &types.AuthConfig{
		LoginURL:      os.Getenv("SFDC_LOGIN_URL"),
		ClientID:      os.Getenv("SFDC_CLIENT_ID"),
		Username:      os.Getenv("SFDC_USERNAME"),
		PrivateKeyPEM: string(keyPEM),
	}
}

func (s *APISuite) SetupTest() {
	t := s.T()
	cassetteName := filepath.Join("testdata", sanitizeCassetteName(t.Name()))

	mode := recorder.ModeReplayOnly
	if hasRealCredentials() {
		mode = recorder.ModeRecordOnce // TODO: use a stdlib flag/env var to enable ModeRecordOnly for easier cassette updates
	}

	r, err := recorder.New(cassetteName,
		recorder.WithMode(mode),
		recorder.WithSkipRequestLatency(true),
		// Strip auth headers from saved cassettes
		recorder.WithHook(func(i *cassette.Interaction) error {
			delete(i.Request.Headers, "Authorization")
			return nil
		}, recorder.BeforeSaveHook),
		// Custom matcher: ignore host (cassettes record real instance URL,
		// but replay uses a fake one). Match on method + path + query only.
		recorder.WithMatcher(func(r *http.Request, i cassette.Request) bool {
			if r.Method != i.Method {
				return false
			}
			// Compare path+query, ignoring scheme+host
			requestPath := r.URL.Path
			if r.URL.RawQuery != "" {
				requestPath += "?" + r.URL.RawQuery
			}
			cassetteURL := i.URL
			// Extract path+query from the cassette URL
			if idx := strings.Index(cassetteURL, "//"); idx != -1 {
				if pathIdx := strings.Index(cassetteURL[idx+2:], "/"); pathIdx != -1 {
					cassetteURL = cassetteURL[idx+2+pathIdx:]
				}
			}
			return requestPath == cassetteURL
		}),
	)
	require.NoError(t, err)
	s.recorder = r

	withVCR := WithModifyClient(func(c *resty.Client) {
		// Use vcr transport
		c.SetTransport(r)
		// Disable gzip so cassettes store plain text (readable + replayable)
		c.SetHeader("Accept-Encoding", "identity")
	})

	if hasRealCredentials() {
		// Live mode: authenticate for real, VCR records the API calls
		cfg := realAuthConfig(t)
		client, err := NewClient(cfg, withVCR)
		require.NoError(t, err)

		err = client.Authenticate(context.Background())
		require.NoError(t, err)

		s.Client = client
	} else {
		// Replay mode: use static token and instance URL from cassettes
		client, err := NewClient(
			&types.AuthConfig{
				LoginURL:   "https://test.salesforce.com",
				ClientID:   "test-client-id",
				Username:   "test@example.com",
				APIVersion: "v64.0",
			},
			withVCR,
		)
		require.NoError(t, err)

		client.http.SetBaseURL("https://test.salesforce.com")
		client.http.SetAuthToken("test-token")

		s.Client = client
	}
}

func (s *APISuite) TearDownTest() {
	if s.recorder != nil {
		err := s.recorder.Stop()
		s.Require().NoError(err)
	}
	if s.Client != nil {
		s.Client.Close()
	}
}

// TODO: does this really need to be a separate function? We could just do this inline since I belive its only used once.
func sanitizeCassetteName(name string) string {
	return strings.ReplaceAll(name, "/", "_")
}
